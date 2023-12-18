"""
Powers https://telemetry.mozilla.org/update-orphaning/.

See [jobs/update_orphaning_dashboard_etl.py](https://github.com/mozilla/telemetry-airflow/blob/main/jobs/update_orphaning_dashboard_etl.py).
"""

from datetime import datetime, timedelta

from airflow import DAG

from utils.constants import DS_WEEKLY
from utils.dataproc import moz_dataproc_pyspark_runner
from utils.tags import Tag

"""

The following WTMO connections are needed in order for this job to run:
conn - google_cloud_airflow_dataproc
conn - aws_dev_telemetry_public_analysis_2_rw
"""

default_args = {
    "owner": "akomar@mozilla.com",
    "depends_on_past": False,
    "start_date": datetime(2019, 10, 12),
    "email": [
        "telemetry-alerts@mozilla.com",
        "ahabibi@mozilla.com",
        "rsteuber@mozilla.com",
        "akomar@mozilla.com",
    ],
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
}

tags = [Tag.ImpactTier.tier_3]

# run every Monday to maintain compatibility with legacy ATMO schedule
dag = DAG(
    "update_orphaning_dashboard_etl",
    default_args=default_args,
    schedule_interval="0 2 * * MON",
    doc_md=__doc__,
    tags=tags,
)

# Unsalted cluster name so subsequent runs fail if the cluster name exists
cluster_name = "app-update-out-of-date-dataproc-cluster"

# Defined in Airflow's UI -> Admin -> Connections
gcp_conn_id = "google_cloud_airflow_dataproc"

moz_dataproc_pyspark_runner(
    task_group_name="update_orphaning_dashboard_etl",
    dag=dag,
    default_args=default_args,
    cluster_name=cluster_name,
    job_name="update_orphaning_dashboard_etl",
    python_driver_code="gs://moz-fx-data-prod-airflow-dataproc-artifacts/jobs/update_orphaning_dashboard_etl.py",
    init_actions_uris=["gs://dataproc-initialization-actions/python/pip-install.sh"],
    additional_metadata={
        "PIP_PACKAGES": "google-cloud-bigquery==1.20.0 google-cloud-storage==1.19.1 boto3==1.9.253"
    },
    additional_properties={
        "spark:spark.jars.packages": "org.apache.spark:spark-avro_2.11:2.4.3"
    },
    py_args=[
        "--run-date",
        DS_WEEKLY,
        "--gcs-bucket",
        "mozdata-analysis",
        "--gcs-prefix",
        "update-orphaning-airflow",
        "--gcs-output-bucket",
        "moz-fx-data-static-websit-8565-analysis-output",
        "--gcs-output-path",
        "app-update/data/out-of-date/",
    ],
    idle_delete_ttl=14400,
    num_workers=20,
    worker_machine_type="n1-standard-8",
    gcp_conn_id=gcp_conn_id,
)
