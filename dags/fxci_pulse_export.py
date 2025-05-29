"""
Exports Firefox-CI task and run data from Taskcluster to BigQuery.

This connects to and drains three separate Taskcluster pulse queues, and
exports each message into BigQuery.

The container is defined in [fxci-etl](https://github.com/mozilla-releng/fxci-etl).
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.cncf.kubernetes.secret import Secret

from operators.gcp_container_operator import GKEPodOperator
from utils.tags import Tag

default_args = {
    "owner": "ahalberstadt@mozilla.com",
    "depends_on_past": False,
    "start_date": datetime(2024, 7, 8),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=30),
}

tags = [Tag.ImpactTier.tier_3]

env_vars = {
    "FXCI_ETL_BIGQUERY_PROJECT": "moz-fx-data-shared-prod",
    "FXCI_ETL_BIGQUERY_DATASET": "fxci_derived",
    "FXCI_ETL_STORAGE_PROJECT": "moz-fx-dev-releng",
    "FXCI_ETL_STORAGE_BUCKET": "fxci-etl",
    "FXCI_ETL_PULSE_USER": "fxci-etl",
}

secrets = [
    Secret(
        deploy_type="env",
        deploy_target="FXCI_ETL_STORAGE_CREDENTIALS",
        secret="airflow-gke-secrets",
        key="fxci_etl_secret__gcp-credentials",
    ),
    Secret(
        deploy_type="env",
        deploy_target="FXCI_ETL_PULSE_PASSWORD",
        secret="airflow-gke-secrets",
        key="fxci_etl_secret__pulse-password",
    ),
]

with DAG(
    "fxci_pulse_export",
    default_args=default_args,
    doc_md=__doc__,
    schedule_interval="30 */4 * * *",
    tags=tags,
) as dag:
    fxci_pulse_export = GKEPodOperator(
        task_id="fxci_pulse_export",
        arguments=[
            "fxci-etl",
            "pulse",
            "drain",
            "-vv",
        ],
        env_vars=env_vars,
        secrets=secrets,
        image="gcr.io/moz-fx-data-airflow-prod-88e0/fxci-taskcluster-export_docker_etl:latest",
        gcp_conn_id="google_cloud_airflow_gke",
        dag=dag,
        email=[
            "ahalberstadt@mozilla.com",
        ],
    )
