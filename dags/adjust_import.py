from datetime import datetime, timedelta

from airflow import DAG

from utils.dataproc import (
    copy_artifacts_dev,
    get_dataproc_parameters,
    moz_dataproc_pyspark_runner,
)
from utils.tags import Tag

default_args = {
    "owner": "frank@mozilla.com",
    "depends_on_past": True,
    "start_date": datetime(2020, 3, 15),
    "email": [
        "telemetry-alerts@mozilla.com",
        "frank@mozilla.com",
    ],
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 3,
    "retry_delay": timedelta(minutes=30),
}

tags = [Tag.ImpactTier.tier_1]

dag = DAG(
    "adjust_import",
    default_args=default_args,
    schedule_interval="@daily",
    tags=tags,
)

params = get_dataproc_parameters("google_cloud_airflow_dataproc")

task_group_args = default_args.copy()
task_group_args["retries"] = 0

task_id = "fenix_beta_adjust_import"
project = params.project_id if params.is_dev else "moz-fx-data-shared-prod"
fenix_beta_adjust_import = moz_dataproc_pyspark_runner(
    task_group_name=task_id,
    dag=dag,
    job_name="firefox-android-beta-adjust-import",
    cluster_name="firefox-android-beta-adjust-import-{{ ds_nodash }}",
    idle_delete_ttl="600",
    num_workers=40,
    worker_machine_type="n1-standard-8",
    init_actions_uris=["gs://dataproc-initialization-actions/python/pip-install.sh"],
    additional_properties={
        "spark:spark.jars": "gs://spark-lib/bigquery/spark-bigquery-latest.jar"
    },
    additional_metadata={"PIP_PACKAGES": "click==7.1.2"},
    python_driver_code=f"gs://{params.artifact_bucket}/jobs/adjust_import.py",
    py_args=[
        "--pbkdf2",
        "--salt",
        "org.mozilla.fenix-salt",
        "--project",
        project,
        "--input_table",
        "tmp.adjust_firefox_preview",
        "--output_table",
        "firefox_android_beta_external.adjust_install_time_v1",
        "--bucket",
        params.storage_bucket,
    ],
    gcp_conn_id=params.conn_id,
    service_account=params.client_email,
    artifact_bucket=params.artifact_bucket,
    storage_bucket=params.storage_bucket,
    default_args=task_group_args,
)

task_id = "fennec_adjust_import"
project = params.project_id if params.is_dev else "moz-fx-data-shared-prod"
fennec_adjust_import = moz_dataproc_pyspark_runner(
    task_group_name=task_id,
    dag=dag,
    job_name="fennec-adjust-import",
    cluster_name="fennec-adjust-import-{{ ds_nodash }}",
    idle_delete_ttl="600",
    num_workers=40,
    worker_machine_type="n1-standard-8",
    init_actions_uris=["gs://dataproc-initialization-actions/python/pip-install.sh"],
    additional_properties={
        "spark:spark.jars": "gs://spark-lib/bigquery/spark-bigquery-latest.jar"
    },
    additional_metadata={"PIP_PACKAGES": "click==7.1.2 bcrypt==3.1.7"},
    python_driver_code=f"gs://{params.artifact_bucket}/jobs/adjust_import.py",
    py_args=[
        "--bcrypt",
        "--salt",
        "$2a$10$ZfglUfcbmTyaBbAQ7SL9OO",
        "--project",
        project,
        "--input_table",
        "tmp.adjust_fennec_release",
        "--output_table",
        "firefox_android_release_external.adjust_install_time_v1",
        "--bucket",
        params.storage_bucket,
    ],
    gcp_conn_id=params.conn_id,
    service_account=params.client_email,
    artifact_bucket=params.artifact_bucket,
    storage_bucket=params.storage_bucket,
    default_args=task_group_args,
)

if params.is_dev:
    copy_to_dev = copy_artifacts_dev(
        dag, params.project_id, params.artifact_bucket, params.storage_bucket
    )
    copy_to_dev >> fenix_beta_adjust_import
    copy_to_dev >> fennec_adjust_import
