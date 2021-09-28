import datetime

from airflow import models
from airflow.hooks.base_hook import BaseHook

from operators.gcp_container_operator import GKEPodOperator
from operators.task_sensor import ExternalTaskCompletedSensor


DOCS = """\
Weekly data exports of contextual services data aggregates to adMarketplace.
This is a complementary approach to the near real-time sharing that is implemented
in gcp-ingestion.

For more context, see https://bugzilla.mozilla.org/show_bug.cgi?id=1729524
"""

default_args = {
    "owner": "jklukas@mozilla.com",
    "start_date": datetime.datetime(2019, 7, 25),
    "email": ["telemetry-alerts@mozilla.com", "jklukas@mozilla.com"],
    "email_on_failure": True,
    "email_on_retry": True,
    "depends_on_past": False,
    # If a task fails, retry it once after waiting at least 5 minutes
    "retries": 1,
    "retry_delay": datetime.timedelta(minutes=5),
}

dag_name = "adm_export"

with models.DAG(
    dag_name, schedule_interval="0 5 * * *", doc_md=DOCS, default_args=default_args
) as dag:

    conn = BaseHook.get_connection('adm_sftp')

    adm_weekly_aggregates_to_sftp = GKEPodOperator(
        task_id="adm_weekly_aggregates_to_sftp",
        name="adm_weekly_aggregates_to_sftp",
        # See https://github.com/mozilla/docker-etl/pull/28
        image="gcr.io/moz-fx-data-airflow-prod-88e0/bq2sftp_docker_etl:latest",
        project_id='moz-fx-data-shared-prod',
        gcp_conn_id="google_cloud_airflow_gke",
        cluster_name="workloads-prod-v1",
        location="us-west1",
        env_vars=dict(
            SFTP_USERNAME=conn.login,
            SFTP_PASSWORD=conn.password,
            SFTP_HOST=conn.host,
            SFTP_PORT=str(conn.port),
            KNOWN_HOSTS=conn.extra_dejson["known_hosts"],
            SRC_TABLE="moz-fx-data-shared-prod.search_terms_derived.aggregated_search_terms_v1",
            DST_PATH="files/firefox-suggest-queries-{{ ds_nodash }}.csv.gz",
            SUBMISSION_DATE="{{ ds }}",
        ),
        email=[
            "jklukas@mozilla.com",
        ],
    )

    wait_for_clients_daily_export = ExternalTaskCompletedSensor(
        task_id="wait_for_adm_weekly_aggregates",
        external_dag_id="bqetl_search_terms_daily",
        external_task_id="search_terms_derived__adm_weekly_aggregates__v1",
        execution_delta=datetime.timedelta(hours=2),
        mode="reschedule",
        pool="DATA_ENG_EXTERNALTASKSENSOR",
        email_on_retry=False,
    )

    wait_for_clients_daily_export >> adm_weekly_aggregates_to_sftp
