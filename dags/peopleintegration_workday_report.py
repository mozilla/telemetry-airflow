# Adapted from https://github.com/mozilla-services/cloudops-infra/blob/a640bb473b94885689cb4857700ef50b1921c07f/projects/data-composer/tf/modules/dags/dags/peopleintegration-workday-report.py
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.cncf.kubernetes.secret import Secret
from airflow.providers.google.cloud.operators import bigquery
from airflow.providers.google.cloud.transfers import gcs_to_sftp

from operators.gcp_container_operator import GKEPodOperator
from utils.tags import Tag

BUCKET = "moz-fx-data-bq-people-workday-tripactions-export"
# This image was manually copied from moz-fx-data-composer-prod to moz-fx-data-artifacts-prod by mducharme
IMAGE = "us-docker.pkg.dev/moz-fx-data-artifacts-prod/legacy-images/integrations_workday_to_tripactions:latest"
PROJECT = "moz-fx-data-bq-people"

WORKDAY_USERNAME = Secret(
    deploy_type="env",
    deploy_target="WORKDAY_USERNAME",
    secret="airflow-gke-secrets",
    key="EVERFI_INTEG_WORKDAY_USERNAME",
)
WORKDAY_PASSWORD = Secret(
    deploy_type="env",
    deploy_target="WORKDAY_USERNAME",
    secret="airflow-gke-secrets",
    key="EVERFI_INTEG_WORKDAY_PASSWORD",
)
WORKDAY_QUERY_URI = Secret(
    deploy_type="env",
    deploy_target="WORKDAY_QUERY_URI",
    secret="airflow-gke-secrets",
    key="WORKDAY_QUERY_URI",
)
WORKDAY_BASE_URL = "https://services1.myworkday.com"


default_args = {
    "retries": 3,
    "retry_delay": timedelta(seconds=10),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=10),
    "start_date": datetime(2022, 5, 3),
    "email": ["telemetry-alerts@mozilla.com"],
}
tags = [Tag.ImpactTier.tier_3]

dag = DAG(
    dag_id="peopleintegration-workday-get-report",
    schedule_interval="50 13 * * *",
    max_active_runs=1,
    catchup=False,
    default_args=default_args,
    tags=tags,
)

get_report = GKEPodOperator(
    task_id="peopleintegration-workday-to-bigquery",
    name="peopleintegration-workday-to-bigquery",
    image=IMAGE,
    image_pull_policy="Always",
    env_vars={
        "WORKDAY_BASE_URL": WORKDAY_BASE_URL,
        "BIGQUERY_DATA_PROJECT": PROJECT,
    },
    secrets=[WORKDAY_USERNAME, WORKDAY_PASSWORD, WORKDAY_QUERY_URI],
    cmds=["python", "/workspace/workday_download_report.py"],
    dag=dag,
)

RUN_DATE = '{{ macros.ds_format(ds, "%Y-%m-%d", "%m%d%Y") }}'
TABLE_ID = f"{ PROJECT }.composer_workday.worker"
GCS_OBJECT = f"workday-masterdata/tripactions/mozilla_{ RUN_DATE }*.csv"

WORKER_TO_TA_QUERY = f"""EXPORT DATA OPTIONS(
uri='gs://{ BUCKET }/{ GCS_OBJECT }',
format='CSV',
overwrite=true,
header=true,
field_delimiter=','
) AS SELECT
    email as email,
    first_name as firstName,
    last_name as lastName,
    currently_active as enabled,
    moco_or_mofo as policyLevel,
    employee_id as employeeId,
    position_worker_type as title,
    cost_center_hierarchy as department,
    cost_center as costCenter,
    location_country as region,
    company as subsidiary,
    manager_email as managerEmail,
    moco_or_mofo as companyOffice,
FROM { TABLE_ID }
WHERE
    currently_active=TRUE
"""

upload_to_bucket = bigquery.BigQueryInsertJobOperator(
    task_id="upload-to-bucket",
    configuration={
        "query": {
            "query": WORKER_TO_TA_QUERY,
            "useLegacySql": False,
        }
    },
    dag=dag,
)

transfer_to_tripactions = gcs_to_sftp.GCSToSFTPOperator(
    task_id="workday-to-tripaction",
    sftp_conn_id="tripactions_sftp",
    source_bucket=BUCKET,
    source_object=GCS_OBJECT,
    destination_path="/data/prod",
    keep_directory_structure=False,
    dag=dag,
)

get_report >> upload_to_bucket >> transfer_to_tripactions
