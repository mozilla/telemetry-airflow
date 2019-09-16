from airflow import DAG
from datetime import datetime, timedelta

from operators.emr_spark_operator import EMRSparkOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.contrib.hooks.gcp_api_base_hook import GoogleCloudBaseHook

from utils.mozetl import mozetl_envvar
from utils.dataproc import moz_dataproc_pyspark_runner

# Unsalted cluster name so subsequent runs fail if the cluster name exists
cluster_name = "taarlite-dataproc-cluster"

# We use an application-specific gcs bucket since the copy operator can't set the destination
# bucket prefix, and unfortunately socorro data in s3 has prefix version/dataset instead
# of having the dataset name come first
gcs_data_bucket = "moz-fx-data-prod-taarlite-data"

dataset = "taarlite_guidranking"

# Defined in Airflow's UI -> Admin -> Connections
gcp_conn_id = "google_cloud_airflow_dataproc"
connection = GoogleCloudBaseHook(gcp_conn_id=gcp_conn_id)

# Required to copy socorro json data from aws prod s3 to gcs
read_aws_conn_id = "aws_taarlite_readonly_s3"

# Required to write parquet data back to s3://telemetry-parquet
write_aws_conn_id = "aws_dev_taarlite_telemetry_parquet_s3"
aws_access_key, aws_secret_key, session = AwsHook(write_aws_conn_id).get_credentials()


default_args = {
    "owner": "amiyaguchi@mozilla.com",
    "depends_on_past": False,
    "start_date": datetime(2018, 11, 26),
    "email": ["telemetry-alerts@mozilla.com", "amiyaguchi@mozilla.com"],
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 3,
    "retry_delay": timedelta(minutes=30),
}

dag = DAG("taar_amodump", default_args=default_args, schedule_interval="@daily")

amodump = EMRSparkOperator(
    task_id="taar_amodump",
    job_name="Dump AMO JSON blobs with oldest creation date",
    execution_timeout=timedelta(hours=1),
    instance_count=1,
    owner="vng@mozilla.com",
    email=["mlopatka@mozilla.com", "vng@mozilla.com"],
    env=mozetl_envvar(
        "taar_amodump",
        {"date": "{{ ds_nodash }}"},
        {"MOZETL_SUBMISSION_METHOD": "python"},
    ),
    uri="https://raw.githubusercontent.com/mozilla/python_mozetl/master/bin/mozetl-submit.sh",
    output_visibility="private",
    dag=dag,
)

amowhitelist = EMRSparkOperator(
    task_id="taar_amowhitelist",
    job_name="Generate an algorithmically defined set of whitelisted addons for TAAR",
    execution_timeout=timedelta(hours=1),
    instance_count=1,
    owner="vng@mozilla.com",
    email=["mlopatka@mozilla.com", "vng@mozilla.com"],
    env=mozetl_envvar("taar_amowhitelist", {}, {"MOZETL_SUBMISSION_METHOD": "spark"}),
    uri="https://raw.githubusercontent.com/mozilla/python_mozetl/master/bin/mozetl-submit.sh",
    output_visibility="private",
    dag=dag,
)

editorial_whitelist = EMRSparkOperator(
    task_id="taar_update_whitelist",
    job_name="Generate a JSON blob from editorial reviewed addons for TAAR",
    execution_timeout=timedelta(hours=1),
    instance_count=1,
    owner="vng@mozilla.com",
    email=["mlopatka@mozilla.com", "vng@mozilla.com"],
    env=mozetl_envvar(
        "taar_update_whitelist",
        {"date": "{{ ds_nodash }}"},
        {"MOZETL_SUBMISSION_METHOD": "spark"},
    ),
    uri="https://raw.githubusercontent.com/mozilla/python_mozetl/master/bin/mozetl-submit.sh",
    output_visibility="private",
    dag=dag,
)


# Spark job reads gcs json and writes gcs parquet
taar_lite = SubDagOperator(
    task_id="taar_lite",
    dag=dag,
    subdag=moz_dataproc_pyspark_runner(
        parent_dag_name=dag.dag_id,
        default_args=default_args,
        cluster_name=cluster_name,
        job_name="Generate GUID coinstallation JSON for TAAR",
        # TODO - CI/CD for gcs and repo sync for artifact bucket jobs and gcp bootstrap folders
        # TODO: the currentcode lives at - need to get it into GCS
        # https://github.com/mozilla/python_mozetl/blob/master/mozetl/taar/taar_lite_guidguid.py
        python_driver_code="gs://moz-fx-data-prod-airflow-dataproc-artifacts/jobs/socorro_import_crash_data.py",
        py_args=["--date", "{{ ds_nodash }}"],
        idle_delete_ttl="14400",
        num_workers=5,
        worker_machine_type="n1-standard-8",
        aws_conn_id=read_aws_conn_id,
        gcp_conn_id=gcp_conn_id,
    ),
)

# Set a dependency on amodump from amowhitelist
amowhitelist.set_upstream(amodump)

# Set a dependency on amodump for the editorial reviewed whitelist of
# addons
editorial_whitelist.set_upstream(amodump)

# Set a dependency on amowhitelist from taar_lite
taar_lite.set_upstream(amowhitelist)
