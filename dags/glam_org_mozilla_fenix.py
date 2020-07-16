from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.sensors import ExternalTaskSensor
from glam_subdags.generate_query import generate_and_run_glean_query
from utils.gcp import gke_command

default_args = {
    "owner": "amiyaguchi@mozilla.com",
    "depends_on_past": False,
    "start_date": datetime(2020, 2, 19),
    "email": ["telemetry-alerts@mozilla.com", "amiyaguchi@mozilla.com", "bewu@mozilla.com"],
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 1,
    "retry_delay": timedelta(minutes=30),
}

dag = DAG(
    "glam_org_mozilla_fenix", default_args=default_args, schedule_interval="0 2 * * *"
)

wait_for_copy_deduplicate = ExternalTaskSensor(
    task_id="wait_for_copy_deduplicate",
    external_dag_id="copy_deduplicate",
    external_task_id="copy_deduplicate_all",
    execution_delta=timedelta(hours=1),
    check_existence=True,
    dag=dag,
)

org_mozilla_fenix = generate_and_run_glean_query(
    task_id="org_mozilla_fenix",
    product="org_mozilla_fenix",
    destination_project_id="glam-fenix-dev",
    dag=dag,
)

org_mozilla_firefox = generate_and_run_glean_query(
    task_id="org_mozilla_firefox",
    product="org_mozilla_firefox",
    destination_project_id="glam-fenix-dev",
    dag=dag,
)

org_mozilla_firefox_beta = generate_and_run_glean_query(
    task_id="org_mozilla_firefox_beta",
    product="org_mozilla_firefox_beta",
    destination_project_id="glam-fenix-dev",
    dag=dag,
)

org_mozilla_fennec_aurora = generate_and_run_glean_query(
    task_id="org_mozilla_fennec_aurora",
    product="org_mozilla_fennec_aurora",
    destination_project_id="glam-fenix-dev",
    dag=dag,
)

export_org_mozilla_fenix = gke_command(
    task_id="export_org_mozilla_fenix",
    cmds=["bash"],
    env_vars={"DATASET": "glam_etl"},
    command=["script/glam/export_csv"],
    docker_image="mozilla/bigquery-etl:latest",
    gcp_conn_id="google_cloud_derived_datasets",
    dag=dag,
)

wait_for_copy_deduplicate >> org_mozilla_fenix >> export_org_mozilla_fenix
