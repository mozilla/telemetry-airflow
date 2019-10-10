from datetime import datetime, timedelta

from airflow import DAG
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.contrib.hooks.gcp_api_base_hook import GoogleCloudBaseHook
from airflow.operators.subdag_operator import SubDagOperator

from operators.gcp_container_operator import GKEPodOperator  # noqa
from utils.dataproc import moz_dataproc_pyspark_runner

gke_cluster_name = "bq-load-gke-1"

taarlite_cluster_name = "dataproc-taarlite-guidguid"

# Defined in Airflow's UI -> Admin -> Connections
gcp_conn_id = "google_cloud_derived_datasets"
connection = GoogleCloudBaseHook(gcp_conn_id=gcp_conn_id)

# Dataproc connection to GCP
gcpdataproc_conn_id = "google_cloud_airflow_dataproc"


aws_conn_id = "airflow_taar_rw_s3"
aws_access_key, aws_secret_key, session = AwsHook(aws_conn_id).get_credentials()

default_args = {
    "owner": "vng@mozilla.com",
    "depends_on_past": False,
    "start_date": datetime(2019, 10, 7),
    "email": ["telemetry-alerts@mozilla.com", "amiyaguchi@mozilla.com"],
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 3,
    "retry_delay": timedelta(minutes=30),
}

dag = DAG("taar_amodump", default_args=default_args, schedule_interval="@daily")

amodump = GKEPodOperator(
    task_id="taar_amodump",
    gcp_conn_id=gcp_conn_id,
    project_id=connection.project_id,
    location="us-central1-a",
    cluster_name=gke_cluster_name,
    name="taar-amodump",
    namespace="default",
    # This uses a circleci built docker image from github.com/mozilla/taar_gcp_etl
    image="gcr.io/moz-fx-data-airflow-prod-88e0/taar_gcp_etl:0.1",
    owner="vng@mozilla.com",
    email=["mlopatka@mozilla.com", "vng@mozilla.com", "hwoo@mozilla.com"],
    arguments=["-m", "taar_etl.taar_amodump", "--date", "{{ ds_nodash }}"],
    env_vars={
        "AWS_ACCESS_KEY_ID": aws_access_key,
        "AWS_SECRET_ACCESS_KEY": aws_secret_key,
    },
    dag=dag,
)

amowhitelist = GKEPodOperator(
    task_id="taar_amowhitelist",
    gcp_conn_id=gcp_conn_id,
    project_id=connection.project_id,
    location="us-central1-a",
    cluster_name=gke_cluster_name,
    name="taar-amowhitelist",
    namespace="default",
    # This uses a circleci built docker image from github.com/mozilla/taar_gcp_etl
    image="gcr.io/moz-fx-data-airflow-prod-88e0/taar_gcp_etl:0.1",
    owner="vng@mozilla.com",
    email=["mlopatka@mozilla.com", "vng@mozilla.com", "hwoo@mozilla.com"],
    # We are extracting addons from the AMO server's APIs which don't
    # support date based queries, so no date parameter is required
    # here.
    arguments=["-m", "taar_etl.taar_amowhitelist"],
    env_vars={
        "AWS_ACCESS_KEY_ID": aws_access_key,
        "AWS_SECRET_ACCESS_KEY": aws_secret_key,
    },
    dag=dag,
)

editorial_whitelist = GKEPodOperator(
    task_id="taar_update_whitelist",
    gcp_conn_id=gcp_conn_id,
    project_id=connection.project_id,
    location="us-central1-a",
    cluster_name=gke_cluster_name,
    name="taar-update-whitelist",
    namespace="default",
    # This uses a circleci built docker image from github.com/mozilla/taar_gcp_etl
    image="gcr.io/moz-fx-data-airflow-prod-88e0/taar_gcp_etl:0.1",
    owner="vng@mozilla.com",
    email=["mlopatka@mozilla.com", "vng@mozilla.com", "hwoo@mozilla.com"],
    arguments=["-m", "taar_etl.taar_update_whitelist", "--date", "{{ ds_nodash }}"],
    env_vars={
        "AWS_ACCESS_KEY_ID": aws_access_key,
        "AWS_SECRET_ACCESS_KEY": aws_secret_key,
    },
    dag=dag,
)


taar_lite = SubDagOperator(
    task_id="taar_lite",
    subdag=moz_dataproc_pyspark_runner(
        parent_dag_name="taar_amodump",
        dag_name="taar_lite",
        default_args=default_args,
        cluster_name=taarlite_cluster_name,
        job_name="TAAR_Lite_GUID_GUID",
        python_driver_code="gs://moz-fx-data-prod-airflow-dataproc-artifacts/jobs/taar_lite_guidguid.py",
        # python_driver_code="gs://temp-hwoo-removemelater/taar_lite_guidguid.py",
        num_workers=8,
        py_args=[
            "--date",
            "{{ ds_nodash }}",
            "--aws_access_key_id",
            aws_access_key,
            "--aws_secret_access_key",
            aws_secret_key,
        ],
        aws_conn_id=aws_conn_id,
        gcp_conn_id=gcpdataproc_conn_id,
    ),
    dag=dag,
)
# Set a dependency on amodump from amowhitelist
amowhitelist.set_upstream(amodump)

# Set a dependency on amodump for the editorial reviewed whitelist of
# addons
editorial_whitelist.set_upstream(amodump)

# Set a dependency on amowhitelist from taar_lite
taar_lite.set_upstream(amowhitelist)
