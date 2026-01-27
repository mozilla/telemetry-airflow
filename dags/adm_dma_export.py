import datetime

from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.providers.cncf.kubernetes.secret import Secret
from airflow.sensors.external_task import ExternalTaskSensor

from operators.gcp_container_operator import GKEPodOperator
from utils.constants import ALLOWED_STATES, FAILED_STATES
from utils.tags import Tag

DOCS = """\
Daily data exports of contextual services data aggregates to adMarketplace including DMA (Designated Market Area).
This is a complementary approach to the near real-time sharing that is implemented
in gcp-ingestion.

Relies on the [`bq2stfp` container defined in `docker-etl`](https://github.com/mozilla/docker-etl/tree/main/jobs/bq2sftp)
and credentials stored in the `adm_sftp` connection.
"""

default_args = {
    "owner": "llisi@mozilla.com",
    "start_date": datetime.datetime(2025, 6, 23),
    "email": [
        "telemetry-alerts@mozilla.com",
        "llisi@mozilla.com",
    ],
    "email_on_failure": True,
    "email_on_retry": True,
    "depends_on_past": False,
    # If a task fails, retry it once after waiting at least 5 minutes
    "retries": 1,
    "retry_delay": datetime.timedelta(minutes=5),
}

dag_name = "adm_dma_export"
tags = [Tag.ImpactTier.tier_3]

adm_sftp_secret = Secret(
    deploy_type="env",
    deploy_target="SFTP_PASSWORD",
    secret="airflow-gke-secrets",
    key="adm_export_secret__sftp_password",
)

with DAG(
    dag_name,
    schedule_interval="0 5 * * *",
    doc_md=DOCS,
    default_args=default_args,
    tags=tags,
) as dag:
    conn = BaseHook.get_connection("adm_sftp")

    adm_daily_dma_aggregates_to_sftp = GKEPodOperator(
        task_id="adm_daily_dma_aggregates_to_sftp",
        name="adm_daily_dma_aggregates_to_sftp",
        # See https://github.com/mozilla/docker-etl/pull/28
        image="us-docker.pkg.dev/moz-fx-data-artifacts-prod/docker-etl/bq2sftp:latest",
        project_id="moz-fx-data-airflow-gke-prod",
        gcp_conn_id="google_cloud_airflow_gke",
        cluster_name="workloads-prod-v1",
        location="us-west1",
        env_vars={
            "SFTP_USERNAME": conn.login,
            "SFTP_HOST": conn.host,
            "SFTP_PORT": str(conn.port),
            "KNOWN_HOSTS": conn.extra_dejson["known_hosts"],
            "SRC_TABLE": "moz-fx-data-shared-prod.search_terms_derived.adm_daily_dma_aggregates_v1",
            # The run for submission_date=2022-03-04 will be named:
            # Aggregated-DMA-Query-Data-03042022.csv.gz
            "DST_PATH": 'files/Aggregated-DMA-Query-Data-{{ macros.ds_format(macros.ds_add(ds, -1), "%Y-%m-%d", "%m%d%Y") }}.csv.gz',
            # Subtract 1 day from execution date to match the upstream date_partition_offset: -1
            "SUBMISSION_DATE": '{{ macros.ds_add(ds, -1) }}',
        },
        secrets=[adm_sftp_secret],
        email=[
            "llisi@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
    )

    wait_for_clients_daily_export = ExternalTaskSensor(
        task_id="wait_for_adm_daily_dma_aggregates",
        external_dag_id="bqetl_search_terms_daily",
        external_task_id="search_terms_derived__adm_daily_dma_aggregates__v1",
        execution_delta=datetime.timedelta(hours=2),
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
        email_on_retry=False,
    )

    wait_for_clients_daily_export >> adm_daily_dma_aggregates_to_sftp
