from airflow import DAG
from datetime import datetime, timedelta

from airflow.contrib.hooks.gcp_api_base_hook import GoogleCloudBaseHook
from operators.gcp_container_operator import GKEPodOperator

# Unsalted cluster name so subsequent runs fail if the cluster name exists
cluster_name = "taarlite-dataproc-cluster"

# We use an application-specific gcs bucket since the copy operator can't set the destination
# bucket prefix, and unfortunately socorro data in s3 has prefix version/dataset instead
# of having the dataset name come first
gcs_data_bucket = "moz-fx-data-prod-taarlite-data"

dataset = "taarlite_guidranking"

# Defined in Airflow's UI -> Admin -> Connections
gcp_conn_id = "google_cloud_derived_datasets_2"
connection = GoogleCloudBaseHook(gcp_conn_id=gcp_conn_id)

default_args = {
    "owner": "amiyaguchi@mozilla.com",
    "depends_on_past": False,
    "start_date": datetime(2019, 9, 20),
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
    cluster_name="bq-load-gke-1",
    name="taar-amodump",
    namespace="default",
    image="gcr.io/moz-fx-data-airflow-prod-88e0/taar_gcp_etl:latest",
    owner="vng@mozilla.com",
    email=["mlopatka@mozilla.com", "vng@mozilla.com", "hwoo@mozilla.com"],
    arguments=["taar_etl/taar_amodump.py", "--date", "{{ ds_nodash }}"],
    dag=dag,
)
