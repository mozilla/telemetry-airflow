import json
import os
from collections import namedtuple

from airflow import models
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.hooks.gcp_api_base_hook import GoogleCloudBaseHook
from airflow.contrib.operators.dataproc_operator import DataprocClusterDeleteOperator, DataProcSparkOperator, DataProcPySparkOperator
from airflow.exceptions import AirflowException

# Our own dataproc operator used to install component gateway
from operators.moz_dataproc_operator import DataprocClusterCreateOperator

"""
Note: We are currently on 1.10.7 and when we upgrade, the spark operators will move.
This module is deprecated. Please use `airflow.providers.google.cloud.operators.dataproc
"""


class DataProcHelper:
    """
    This is a helper class for creating/deleting dataproc clusters.
    """

    def __init__(self,
                 cluster_name=None,
                 num_workers=2,
                 image_version='1.4',
                 zone='us-west1-a',
                 subnetwork_uri=None,
                 internal_ip_only=None,
                 idle_delete_ttl='14400',
                 auto_delete_ttl='28800',
                 master_machine_type='n1-standard-8',
                 worker_machine_type='n1-standard-4',
                 num_preemptible_workers=0,
                 service_account='dataproc-runner-prod@airflow-dataproc.iam.gserviceaccount.com',
                 init_actions_uris=None,
                 additional_metadata=None,
                 additional_properties=None,
                 optional_components=['ANACONDA'],
                 install_component_gateway=True,
                 aws_conn_id=None,
                 gcp_conn_id='google_cloud_airflow_dataproc',
                 artifact_bucket='moz-fx-data-prod-airflow-dataproc-artifacts',
                 storage_bucket='moz-fx-data-prod-dataproc-scratch',
                 master_disk_type='pd-standard',
                 master_disk_size=1024,
                 worker_disk_type='pd-standard',
                 worker_disk_size=1024,
                ):

        self.cluster_name = cluster_name
        self.num_workers = num_workers
        self.image_version = image_version
        self.zone = zone
        self.subnetwork_uri = subnetwork_uri
        self.internal_ip_only = internal_ip_only
        self.idle_delete_ttl = idle_delete_ttl
        self.auto_delete_ttl = auto_delete_ttl
        self.master_machine_type = master_machine_type
        self.worker_machine_type = worker_machine_type
        self.num_preemptible_workers = num_preemptible_workers
        self.service_account = service_account
        # The bucket with a default dataproc init script
        self.artifact_bucket = artifact_bucket
        self.storage_bucket = storage_bucket

        self.master_disk_type = master_disk_type
        self.master_disk_size = master_disk_size
        self.worker_disk_type = worker_disk_type
        self.worker_disk_size = worker_disk_size

        if init_actions_uris is None:
            self.init_actions_uris=['gs://{}/bootstrap/dataproc_init.sh'.format(self.artifact_bucket)]
        else:
            self.init_actions_uris=init_actions_uris

        if additional_metadata is None:
            self.additional_metadata = {}
        else:
            self.additional_metadata = additional_metadata

        if additional_properties is None:
            self.additional_properties = {}
        else:
            self.additional_properties = additional_properties

        self.optional_components = optional_components
        self.install_component_gateway = install_component_gateway
        self.aws_conn_id = aws_conn_id
        self.gcp_conn_id = gcp_conn_id

        self.connection = GoogleCloudBaseHook(gcp_conn_id=self.gcp_conn_id)

    def create_cluster(self):
        """
        Returns a DataprocClusterCreateOperator
        """
        properties = {}

        # Google cloud storage requires object.create permission when reading from pyspark
        properties["core:fs.gs.implicit.dir.repair.enable"] = "false"

        # Set hadoop properties to access s3 from dataproc
        if self.aws_conn_id:
            for key, value in zip(
                ("access.key", "secret.key", "session.token"),
                AwsHook(self.aws_conn_id).get_credentials(),
            ):
                if value is not None:
                    properties["core:fs.s3a." + key] = value
                    # For older spark versions we need to set the properties differently
                    if key == "access.key":
                        properties["core:fs.s3.awsAccessKeyId"] = value
                    elif key == "secret.key":
                        properties["core:fs.s3.awsSecretAccessKey"] = value

        properties.update(self.additional_properties)

        metadata = {
                'gcs-connector-version': '1.9.16',
                'bigquery-connector-version': '0.13.6'
            }
        metadata.update(self.additional_metadata)

        return DataprocClusterCreateOperator(
            task_id='create_dataproc_cluster',
            cluster_name=self.cluster_name,
            gcp_conn_id=self.gcp_conn_id,
            service_account=self.service_account,
            project_id=self.connection.project_id,
            storage_bucket=self.storage_bucket,
            num_workers=self.num_workers,
            image_version=self.image_version,
            properties=properties,
            zone=self.zone,
            subnetwork_uri=self.subnetwork_uri,
            internal_ip_only=self.internal_ip_only,
            idle_delete_ttl=self.idle_delete_ttl,
            auto_delete_ttl=self.auto_delete_ttl,
            master_machine_type=self.master_machine_type,
            worker_machine_type=self.worker_machine_type,
            num_preemptible_workers=self.num_preemptible_workers,
            optional_components = self.optional_components,
            install_component_gateway = self.install_component_gateway,
            init_actions_uris=self.init_actions_uris,
            master_disk_type=self.master_disk_type,
            master_disk_size=self.master_disk_size,
            worker_disk_type=self.worker_disk_type,
            worker_disk_size=self.worker_disk_size,
            metadata=metadata,
        )

    def delete_cluster(self):
        """
        Returns a DataprocClusterDeleteOperator
        """
        return DataprocClusterDeleteOperator(
            task_id='delete_dataproc_cluster',
            cluster_name=self.cluster_name,
            gcp_conn_id=self.gcp_conn_id,
            project_id=self.connection.project_id)
# End DataProcHelper


def moz_dataproc_pyspark_runner(parent_dag_name=None,
                                dag_name='run_pyspark_on_dataproc',
                                default_args=None,
                                cluster_name=None,
                                num_workers=2,
                                image_version='1.4',
                                zone='us-west1-a',
                                subnetwork_uri=None,
                                internal_ip_only=None,
                                idle_delete_ttl='10800',
                                auto_delete_ttl='21600',
                                master_machine_type='n1-standard-8',
                                worker_machine_type='n1-standard-4',
                                num_preemptible_workers=0,
                                service_account='dataproc-runner-prod@airflow-dataproc.iam.gserviceaccount.com',
                                init_actions_uris=None,
                                additional_metadata=None,
                                additional_properties=None,
                                optional_components=['ANACONDA'],
                                install_component_gateway=True,
                                python_driver_code=None,
                                py_args=None,
                                job_name=None,
                                aws_conn_id=None,
                                gcp_conn_id='google_cloud_airflow_dataproc',
                                artifact_bucket='moz-fx-data-prod-airflow-dataproc-artifacts',
                                storage_bucket='moz-fx-data-prod-dataproc-scratch',
                                master_disk_type='pd-standard',
                                worker_disk_type='pd-standard',
                                master_disk_size=1024,
                                worker_disk_size=1024,
                            ):

    """
    This will initially create a GCP Dataproc cluster with Anaconda/Jupyter/Component gateway.
    Then we call DataProcPySparkOperator to execute the pyspark script defined by the argument
    python_driver_code. Once that succeeds, we teardown the cluster.

    **Example**: ::

        # Unsalted cluster name so subsequent runs fail if the cluster name exists
        cluster_name = 'test-dataproc-cluster-hwoo'

        # Defined in Airflow's UI -> Admin -> Connections
        gcp_conn_id = 'google_cloud_airflow_dataproc'

        run_dataproc_pyspark = SubDagOperator(
            task_id='run_dataproc_pyspark',
            dag=dag,
            subdag = moz_dataproc_pyspark_runner(
                parent_dag_name=dag.dag_id,
                dag_name='run_dataproc_pyspark',
                job_name='Do_something_on_pyspark',
                default_args=default_args,
                cluster_name=cluster_name,
                python_driver_code='gs://some_bucket/some_py_script.py',
                py_args=["-d", "{{ ds_nodash }}"],
                gcp_conn_id=gcp_conn_id)
        )

    Airflow related args:
    ---
    :param str parent_dag_name:           Parent dag name.
    :param str dag_name:                  Dag name.
    :param dict default_args:             Dag configuration.

    Dataproc Cluster related args:
    ---
    :param str cluster_name:              The name of the dataproc cluster.
    :param int num_workers:               The number of spark workers.
    :param str image_version:             The image version of software to use for dataproc
                                          cluster.
    :param str zone:                      The zone where the dataproc cluster will be located.
    :param str subnetwork_uri:            The subnetwork uri to be used for machine communication,
                                          cannot be specified with network_uri. Only need this if
                                          setting internal_ip_only = True. (See next parameter)
    :param bool internal_ip_only:         If True, cluster nodes will only have internal IP addresses.
                                          Can only be enabled with subnetwork_uri enabled networks.
                                          We use this for NAT'd dataproc clusters whose outbound traffic
                                          needs to be whitelisted. To use a NAT'd cluster, set
                                          subnetwork_uri='default', internal_ip_only=True, and
                                          region=us-west2-a|b|c
    :param str idle_delete_ttl:           The duration in seconds to keep idle cluster alive.
    :param str auto_delete_ttl:           The duration in seconds that the cluster will live.
    :param str master_machine_type:       Compute engine machine type to use for master.
    :param str worker_machine_type:       Compute engine machine type to use for the workers.
    :param int num_preemptible_workers:   Number of preemptible worker nodes to spin up.
    :param str service_account:           The service account for spark VMs to use. For example
                                          if cross project access is needed. Note that this svc
                                          account needs the following permissions:
                                          roles/logging.logWriter and roles/storage.objectAdmin.
    :param list init_actions_uris:        List of GCS uri's containing dataproc init scripts.
    :param dict additional_metadata       Custom metadata keys and values, might be used to
                                          configure initialization actions.
    :param dict additional_properties     Custom cluster properties, can be used to configure
                                          cluster components, add Spark packages, etc.
    :param str job_name:                  Name of the spark job to run.

    :param str aws_conn_id:               Airflow connection id for S3 access (if needed).
    :param str gcp_conn_id:               The connection ID to use connecting to GCP.
    :param str artifact_bucket:           Path to resources for bootstrapping the dataproc cluster
    :param str storage_bucket:            Path to scratch bucket for intermediate cluster results
    :param list optional_components:      List of optional components to install on cluster
                                          Defaults to ['ANACONDA'] for now since JUPYTER is broken.
    :param str install_component_gateway: Enable alpha feature component gateway.
    :param master_disk_type:              Type of the boot disk for the master node
                                            (default is ``pd-standard``).
                                            Valid values: ``pd-ssd`` (Persistent Disk Solid State Drive) or
                                            ``pd-standard`` (Persistent Disk Hard Disk Drive).
    :type master_disk_type: str
    :param master_disk_size:              Disk size for the master node
    :type master_disk_size: int
    :param worker_disk_type:              Type of the boot disk for the worker node
                                            (default is ``pd-standard``).
                                            Valid values: ``pd-ssd`` (Persistent Disk Solid State Drive) or
                                            ``pd-standard`` (Persistent Disk Hard Disk Drive).
    :type worker_disk_type: str
    :param worker_disk_size:              Disk size for the worker node
    :type worker_disk_size: int

    Pyspark related args:
    ---
    :param str python_driver_code:        The Hadoop Compatible Filesystem (HCFS) URI of the main
                                          Python file to use as the driver. Must be a .py file.
    :param list py_args:                  Arguments for the pyspark job.

    """

    if cluster_name is None or python_driver_code is None:
        raise AirflowException('Please specify cluster_name and/or python_driver_code.')

    dataproc_helper = DataProcHelper(cluster_name=cluster_name,
                                     num_workers=num_workers,
                                     image_version=image_version,
                                     zone=zone,
                                     subnetwork_uri=subnetwork_uri,
                                     internal_ip_only=internal_ip_only,
                                     idle_delete_ttl=idle_delete_ttl,
                                     auto_delete_ttl=auto_delete_ttl,
                                     master_machine_type=master_machine_type,
                                     worker_machine_type=worker_machine_type,
                                     num_preemptible_workers=num_preemptible_workers,
                                     service_account=service_account,
                                     init_actions_uris=init_actions_uris,
                                     optional_components=optional_components,
                                     additional_metadata=additional_metadata,
                                     additional_properties=additional_properties,
                                     install_component_gateway=install_component_gateway,
                                     aws_conn_id=aws_conn_id,
                                     gcp_conn_id=gcp_conn_id,
                                     artifact_bucket=artifact_bucket,
                                     storage_bucket=storage_bucket,
                                     master_disk_type=master_disk_type,
                                     master_disk_size=master_disk_size,
                                     worker_disk_type=worker_disk_type,
                                     worker_disk_size=worker_disk_size,
                                     )

    _dag_name = '{}.{}'.format(parent_dag_name, dag_name)

    with models.DAG(_dag_name, default_args=default_args) as dag:
        create_dataproc_cluster = dataproc_helper.create_cluster()

        run_pyspark_on_dataproc = DataProcPySparkOperator(
            task_id='run_dataproc_pyspark',
            job_name=job_name,
            cluster_name=cluster_name,
            main=python_driver_code,
            arguments=py_args,
            gcp_conn_id=gcp_conn_id,
        )

        delete_dataproc_cluster = dataproc_helper.delete_cluster()

        create_dataproc_cluster >> run_pyspark_on_dataproc >> delete_dataproc_cluster
        return dag
# End moz_dataproc_pyspark_runner


def moz_dataproc_jar_runner(parent_dag_name=None,
                            dag_name='run_script_on_dataproc',
                            default_args=None,
                            cluster_name=None,
                            num_workers=2,
                            image_version='1.4',
                            zone='us-west1-a',
                            subnetwork_uri=None,
                            internal_ip_only=None,
                            idle_delete_ttl='14400',
                            auto_delete_ttl='28800',
                            master_machine_type='n1-standard-8',
                            worker_machine_type='n1-standard-4',
                            num_preemptible_workers=0,
                            service_account='dataproc-runner-prod@airflow-dataproc.iam.gserviceaccount.com',
                            init_actions_uris=None,
                            optional_components=['ANACONDA'],
                            install_component_gateway=True,
                            jar_urls=None,
                            main_class=None,
                            jar_args=None,
                            job_name=None,
                            aws_conn_id=None,
                            gcp_conn_id='google_cloud_airflow_dataproc',
                            master_disk_type='pd-standard',
                            worker_disk_type='pd-standard',
                            master_disk_size=1024,
                            worker_disk_size=1024,
                            ):

    """
    This will initially create a GCP Dataproc cluster with Anaconda/Jupyter/Component gateway.
    Then we call DataProcSparkOperator to execute the jar defined by the arguments
    jar_urls and main_class. Once that succeeds, we teardown the cluster.

    **Example**: ::

        # Unsalted cluster name so subsequent runs fail if the cluster name exists
        cluster_name = 'test-dataproc-cluster-hwoo'

        # Defined in Airflow's UI -> Admin -> Connections
        gcp_conn_id = 'google_cloud_airflow_dataproc'

        run_dataproc_jar = SubDagOperator(
            task_id='run_dataproc_jar',
            dag=dag,
            subdag = moz_dataproc_jar_runner(
                parent_dag_name=dag.dag_id,
                dag_name='run_dataproc_jar',
                job_name='Run_some_spark_jar_on_dataproc',
                default_args=default_args,
                cluster_name=cluster_name,
                jar_urls=['gs://some_bucket/some_jar.jar'],
                main_class='com.mozilla.path.to.ClassName',
                jar_args=["-d", "{{ ds_nodash }}"],
                gcp_conn_id=gcp_conn_id)
        )

    Airflow related args:
    ---
    See moz_dataproc_pyspark_runner

    Dataproc Cluster related args:
    ---
    See moz_dataproc_pyspark_runner

    Jar runner related args:
    ---
    :param list jar_urls:               URIs to jars provisioned in Cloud Storage (example:
                                        for UDFs and libs) and are ideal to put in default arguments.
    :param str main_class:              Name of the job class entrypoint to execute.
    :param list jar_args:               Arguments for the job.

    """

    if cluster_name is None or jar_urls is None or main_class is None:
        raise AirflowException('Please specify cluster_name, jar_urls, and/or main_class.')

    dataproc_helper = DataProcHelper(cluster_name=cluster_name,
                                     num_workers=num_workers,
                                     image_version=image_version,
                                     zone=zone,
                                     subnetwork_uri=subnetwork_uri,
                                     internal_ip_only=internal_ip_only,
                                     idle_delete_ttl=idle_delete_ttl,
                                     auto_delete_ttl=auto_delete_ttl,
                                     master_machine_type=master_machine_type,
                                     worker_machine_type=worker_machine_type,
                                     num_preemptible_workers=num_preemptible_workers,
                                     service_account=service_account,
                                     init_actions_uris=init_actions_uris,
                                     optional_components=optional_components,
                                     install_component_gateway=install_component_gateway,
                                     aws_conn_id=aws_conn_id,
                                     gcp_conn_id=gcp_conn_id,
                                     master_disk_type=master_disk_type,
                                     master_disk_size=master_disk_size,
                                     worker_disk_type=worker_disk_type,
                                     worker_disk_size=worker_disk_size,
                                     )

    _dag_name = '{}.{}'.format(parent_dag_name, dag_name)

    with models.DAG(_dag_name, default_args=default_args) as dag:
        create_dataproc_cluster = dataproc_helper.create_cluster()

        run_jar_on_dataproc = DataProcSparkOperator(
            cluster_name=cluster_name,
            task_id='run_jar_on_dataproc',
            job_name=job_name,
            dataproc_spark_jars=jar_urls,
            main_class=main_class,
            arguments=jar_args,
            gcp_conn_id=gcp_conn_id)

        delete_dataproc_cluster = dataproc_helper.delete_cluster()

        create_dataproc_cluster >> run_jar_on_dataproc >> delete_dataproc_cluster
        return dag
# End moz_dataproc_jar_runner


def _format_envvar(env=None):
    # Use a default value if an environment dictionary isn't supplied
    return ' '.join(['{}={}'.format(k, v) for k, v in (env or {}).items()])


def moz_dataproc_scriptrunner(parent_dag_name=None,
                              dag_name='run_script_on_dataproc',
                              default_args=None,
                              cluster_name=None,
                              num_workers=2,
                              image_version='1.4',
                              zone='us-west1-a',
                              subnetwork_uri=None,
                              internal_ip_only=None,
                              idle_delete_ttl='14400',
                              auto_delete_ttl='28800',
                              master_machine_type='n1-standard-8',
                              worker_machine_type='n1-standard-4',
                              num_preemptible_workers=0,
                              service_account='dataproc-runner-prod@airflow-dataproc.iam.gserviceaccount.com',
                              init_actions_uris=None,
                              optional_components=['ANACONDA'],
                              install_component_gateway=True,
                              uri=None,
                              env=None,
                              arguments=None,
                              job_name=None,
                              aws_conn_id=None,
                              gcp_conn_id='google_cloud_airflow_dataproc',
                              master_disk_type='pd-standard',
                              worker_disk_type='pd-standard',
                              master_disk_size=1024,
                              worker_disk_size=1024,
                              ):

    """
    This will initially create a GCP Dataproc cluster with Anaconda/Jupyter/Component gateway.
    Then we execute a script uri (either https or gcs) similar to how we use our custom AWS
    EmrSparkOperator. This will call DataProcSparkOperator using EMR's script-runner.jar, which
    then executes the airflow_gcp.sh entrypoint script. The entrypoint script expects another
    script uri, along with it's arguments, as parameters. Once that succeeds, we teardown the
    cluster.

    **Example**: ::

        # Unsalted cluster name so subsequent runs fail if the cluster name exists
        cluster_name = 'test-dataproc-cluster-hwoo'

        # Defined in Airflow's UI -> Admin -> Connections
        gcp_conn_id = 'google_cloud_airflow_dataproc'

        run_dataproc_script = SubDagOperator(
            task_id='run_dataproc_script',
            dag=dag,
            subdag = moz_dataproc_scriptrunner(
                parent_dag_name=dag.dag_id,
                dag_name='run_dataproc_script',
                default_args=default_args,
                cluster_name=cluster_name,
                job_name='Run_a_script_on_dataproc',
                uri='https://raw.githubusercontent.com/mozilla/telemetry-airflow/master/jobs/some_bash_or_py_script.py',
                env={"date": "{{ ds_nodash }}"},
                arguments="-d {{ ds_nodash }}",
                gcp_conn_id=gcp_conn_id)
        )

    Airflow related args:
    ---
    See moz_dataproc_pyspark_runner

    Dataproc Cluster related args:
    ---
    See moz_dataproc_pyspark_runner

    Scriptrunner specific args:
    ---
    :param str uri:                     The HTTP or GCS URI of the script to run. Can be
                                        .py, .jar, or other type of script (e.g. bash). Is ran
                                        via the airflow_gcp.sh entrypoint. Ipynb is no longer
                                        supported.
    :param dict env:                    If env is not None, it must be a mapping that defines
                                        the environment variables for the new process
                                        (templated).
    :param str arguments:               Passed to `airflow_gcp.sh`, passed as one long string
                                        of space separated args.

    """

    if job_name is None or uri is None or cluster_name is None:
        raise AirflowException('Please specify job_name, uri, and cluster_name.')

    dataproc_helper = DataProcHelper(cluster_name=cluster_name,
                                     num_workers=num_workers,
                                     image_version=image_version,
                                     zone=zone,
                                     subnetwork_uri=subnetwork_uri,
                                     internal_ip_only=internal_ip_only,
                                     idle_delete_ttl=idle_delete_ttl,
                                     auto_delete_ttl=auto_delete_ttl,
                                     master_machine_type=master_machine_type,
                                     worker_machine_type=worker_machine_type,
                                     num_preemptible_workers=num_preemptible_workers,
                                     service_account=service_account,
                                     init_actions_uris=init_actions_uris,
                                     optional_components=optional_components,
                                     install_component_gateway=install_component_gateway,
                                     aws_conn_id=aws_conn_id,
                                     gcp_conn_id=gcp_conn_id,
                                     master_disk_type=master_disk_type,
                                     master_disk_size=master_disk_size,
                                     worker_disk_type=worker_disk_type,
                                     worker_disk_size=worker_disk_size,
                                     )

    _dag_name = '{}.{}'.format(parent_dag_name, dag_name)
    environment = _format_envvar(env)

    script_bucket = 'moz-fx-data-prod-airflow-dataproc-artifacts'
    jar_url = 'gs://{}/bin/script-runner.jar'.format(script_bucket)

    args = [
        'gs://{}/bootstrap/airflow_gcp.sh'.format(script_bucket),
        '--job-name', job_name,
        '--uri', uri,
        '--environment', environment
    ]

    if arguments:
        args += ['--arguments', arguments]

    with models.DAG(_dag_name, default_args=default_args) as dag:
        create_dataproc_cluster = dataproc_helper.create_cluster()

        # Run DataprocSparkOperator with script-runner.jar pointing to airflow_gcp.sh.

        run_script_on_dataproc = DataProcSparkOperator(
            cluster_name=cluster_name,
            task_id='run_script_on_dataproc',
            job_name=job_name,
            dataproc_spark_jars=[jar_url],
            main_class='com.amazon.elasticmapreduce.scriptrunner.ScriptRunner',
            arguments=args,
            gcp_conn_id=gcp_conn_id)

        delete_dataproc_cluster = dataproc_helper.delete_cluster()

        create_dataproc_cluster >> run_script_on_dataproc >> delete_dataproc_cluster
        return dag
# End moz_dataproc_scriptrunner


def copy_artifacts_dev(dag, project_id, artifact_bucket, storage_bucket):
    """Bootstrap a dataproc job for local testing.

    This job requires setting GOOGLE_APPLICATION_CREDENTIALS before starting the
    airflow container. It will copy the contents of the local jobs and
    dataproc_boostrap folders to the artifacts bucket, and create a scratch
    storage bucket for dataproc.

    :dag DAG: The dag to register the job
    :project_id str: The project id, necessary for setting the default project
    :artifact_bucket str: The bucket for storing bootstrap artifacts
    :storage_bucket str: The scratch bucket for dataproc
    """
    return BashOperator(
        task_id="copy_to_dev_artifacts",
        bash_command="""
        gcloud auth activate-service-account --key-file ~/.credentials
        gcloud config set project ${PROJECT_ID}

        gsutil mb gs://${ARTIFACT_BUCKET}
        gsutil mb gs://${STORAGE_BUCKET}

        gsutil -m cp -r ~/dataproc_bootstrap gs://${ARTIFACT_BUCKET}
        gsutil -m cp -r ~/jobs gs://${ARTIFACT_BUCKET}

        echo "listing artifacts..."
        gsutil ls -r gs://${ARTIFACT_BUCKET}
        """,
        env={
            # https://github.com/GoogleCloudPlatform/gsutil/issues/236
            "CLOUDSDK_PYTHON": "python",
            "PROJECT_ID": project_id,
            "ARTIFACT_BUCKET": artifact_bucket,
            "STORAGE_BUCKET": storage_bucket,
        },
        dag=dag,
    )


# parameters that can be used to reconfigure a dataproc job for dev testing
DataprocParameters = namedtuple(
    "DataprocParameters",
    [
        "conn_id",
        "project_id",
        "is_dev",
        "client_email",
        "artifact_bucket",
        "storage_bucket",
        "output_bucket",
    ],
)


def get_dataproc_parameters(conn_id="google_cloud_airflow_dataproc"):
    """This function can be used to gather parameters that correspond to development
    parameters. The provided connection string should be a Google Cloud connection
    and should either be the production default ("dataproc-runner-prod"), or a
    service key associated with a sandbox account.
    """
    gcp_conn = GoogleCloudBaseHook(conn_id)
    keyfile = json.loads(gcp_conn.extras["extra__google_cloud_platform__keyfile_dict"])

    project_id = keyfile["project_id"]
    is_dev = os.environ.get("DEPLOY_ENVIRONMENT") == "dev"
    client_email = (
        keyfile["client_email"]
        if is_dev
        else "dataproc-runner-prod@airflow-dataproc.iam.gserviceaccount.com"
    )
    artifact_bucket = (
        "{}-dataproc-artifacts".format(project_id)
        if is_dev
        else "moz-fx-data-prod-airflow-dataproc-artifacts"
    )
    storage_bucket = (
        "{}-dataproc-scratch".format(project_id)
        if is_dev
        else "moz-fx-data-prod-dataproc-scratch"
    )
    output_bucket = (
        artifact_bucket if is_dev else "moz-fx-data-derived-datasets-parquet"
    )
    return DataprocParameters(
        conn_id,
        project_id,
        is_dev,
        client_email,
        artifact_bucket,
        storage_bucket,
        output_bucket
    )
