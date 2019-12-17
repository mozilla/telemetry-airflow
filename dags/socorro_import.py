from airflow import DAG
from airflow.operators.subdag_operator import SubDagOperator
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.contrib.hooks.gcp_api_base_hook import GoogleCloudBaseHook
from airflow.contrib.operators.bigquery_table_delete_operator import BigQueryTableDeleteOperator # noqa
from airflow.contrib.operators.s3_to_gcs_transfer_operator import S3ToGoogleCloudStorageTransferOperator # noqa

from datetime import datetime, timedelta

from operators.gcp_container_operator import GKEPodOperator
from utils.gcp import load_to_bigquery
from utils.status import register_status
from utils.dataproc import moz_dataproc_pyspark_runner

"""
Originally, this job read json (non-ndjson) from aws prod at:
s3://crashstats-telemetry-crashes-prod-us-west-2/v1/crash_report 
and wrote the data to parquet format in aws dev at:
s3://telemetry-parquet/socorro_crash/v2

Until we migrate socorro to gcp (https://bugzilla.mozilla.org/show_bug.cgi?id=1512641),
or at least modify it to write crash stats to gcs, this job will now copy json from s3
to gcs, use dataproc to rewrite the data to parquet in gcs, and load the parquet data
into bigquery.

The following WTMO connections are needed in order for this job to run:
conn - google_cloud_airflow_dataproc
conn - google_cloud_derived_datasets
conn - aws_socorro_readonly_s3
"""

default_args = {
    "owner": "amiyaguchi@mozilla.com",
    "depends_on_past": False,
    "start_date": datetime(2019, 9, 10),
    "email": ["telemetry-alerts@mozilla.com", "amiyaguchi@mozilla.com"],
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
    "retry_delay": timedelta(minutes=30),
}

dag = DAG("socorro_import", default_args=default_args, schedule_interval="@daily")

# Unsalted cluster name so subsequent runs fail if the cluster name exists
cluster_name = 'socorro-import-dataproc-cluster'

# Defined in Airflow's UI -> Admin -> Connections
gcp_conn_id = 'google_cloud_airflow_dataproc'
connection = GoogleCloudBaseHook(gcp_conn_id=gcp_conn_id)

# Required to copy socorro json data from aws prod s3 to gcs
read_aws_conn_id='aws_socorro_readonly_s3'

# We use an application-specific gcs bucket since the copy operator can't set the destination
# bucket prefix, and unfortunately socorro data in s3 has prefix version/dataset instead 
# of having the dataset name come first
gcs_data_bucket = 'moz-fx-data-prod-socorro-data'

dataset = 'socorro_crash'
dataset_version='v2'
date_submission_col='crash_date'

objects_prefix='{}/{}/{}={}'.format(dataset,
                                    dataset_version,
                                    date_submission_col,
                                    "{{ ds_nodash }}")

# copy json crashstats from s3 to gcs
s3_to_gcs = S3ToGoogleCloudStorageTransferOperator(
    task_id='s3_to_gcs',
    s3_bucket='crashstats-telemetry-crashes-prod-us-west-2',
    gcs_bucket=gcs_data_bucket,
    description='socorro crash report copy from s3 to gcs',
    aws_conn_id=read_aws_conn_id,
    gcp_conn_id=gcp_conn_id,
    project_id=connection.project_id,
    object_conditions={
        'includePrefixes': 'v1/crash_report/{{ ds_nodash }}'
    },
    transfer_options={
        'deleteObjectsUniqueInSink': True
    },
    dag=dag,
)

# Spark job reads gcs json and writes gcs parquet
crash_report_parquet = SubDagOperator(
    task_id="crash_report_parquet",
    dag=dag,
    subdag = moz_dataproc_pyspark_runner(
        parent_dag_name=dag.dag_id,
        dag_name='crash_report_parquet',
        default_args=default_args,
        cluster_name=cluster_name,
        job_name="Socorro_Crash_Reports_to_Parquet",
        python_driver_code="gs://moz-fx-data-prod-airflow-dataproc-artifacts/jobs/socorro_import_crash_data.py",
        py_args=[
            "--date", "{{ ds_nodash }}",
            "--source-gcs-path", "gs://{}/v1/crash_report".format(gcs_data_bucket),
            "--dest-gcs-path", "gs://{}/{}".format(gcs_data_bucket, dataset)
        ],
        idle_delete_ttl='14400',
        num_workers=8,
        worker_machine_type='n1-standard-8',
        aws_conn_id=read_aws_conn_id,
        gcp_conn_id=gcp_conn_id)
)

bq_gcp_conn_id= 'google_cloud_derived_datasets'
bq_connection = GoogleCloudBaseHook(gcp_conn_id=bq_gcp_conn_id)
gke_location="us-central1-a"
gke_cluster_name="bq-load-gke-1"
dest_s3_key="s3://telemetry-parquet"

# Not using load_to_bigquery since our source data is on GCS.
# We do use the parquet2bigquery container to load gcs parquet into bq though.
bq_dataset='telemetry_derived'
bq_table_name = '{}_{}'.format(dataset, dataset_version)

docker_image='docker.io/mozilla/parquet2bigquery:20190722' # noqa

gke_args = [
    '--dataset', bq_dataset,
    '--concurrency', '10',
    '--bucket', gcs_data_bucket,
    '--no-resume',
    '--prefix', objects_prefix,
    '--cluster-by', 'crash_date'
]

# We remove the current date partition for idempotency.
remove_bq_table_partition = BigQueryTableDeleteOperator(
    task_id='remove_bq_table_partition',
    bigquery_conn_id=bq_gcp_conn_id,
    deletion_dataset_table='{}.{}${{{{ds_nodash}}}}'.format(bq_dataset, bq_table_name), # noqa
    ignore_if_missing=True,
    dag=dag
)

bq_load = GKEPodOperator(
    task_id='bigquery_load',
    gcp_conn_id=bq_gcp_conn_id,
    project_id=bq_connection.project_id,
    location=gke_location,
    cluster_name=gke_cluster_name,
    name='load-socorro-crash-parquet-to-bq',
    namespace='default',
    image=docker_image,
    arguments=gke_args,
    dag=dag,
)

register_status(
    bq_load,
    "Socorro Crash Reports Parquet",
    "Convert processed crash reports into parquet for analysis",
)

s3_to_gcs >> crash_report_parquet
crash_report_parquet >> remove_bq_table_partition >> bq_load
