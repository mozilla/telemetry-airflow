"""
Exports Firefox-CI worker data from the Google Cloud Monitoring to BigQuery.

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
        deploy_target="FXCI_ETL_MONITORING_CREDENTIALS",
        secret="airflow-gke-secrets",
        key="fxci_etl_secret__gcp-credentials",
    ),
]

with DAG(
    "fxci_metric_export",
    default_args=default_args,
    doc_md=__doc__,
    schedule_interval="30 0 * * *",
    tags=tags,
) as dag:
    fxci_metric_export = GKEPodOperator(
        task_id="fxci_metric_export",
        arguments=[
            "fxci-etl",
            "metric",
            "export",
            "-vv",
            "--date={{ ds }}",
        ],
        env_vars=env_vars,
        secrets=secrets,
        image="us-docker.pkg.dev/moz-fx-data-artifacts-prod/docker-etl/fxci-taskcluster-export:latest",
        gcp_conn_id="google_cloud_airflow_gke",
        dag=dag,
        email=[
            "ahalberstadt@mozilla.com",
        ],
    )
