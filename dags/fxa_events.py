import datetime

from airflow import models
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from utils.gcp import bigquery_etl_query

default_args = {
    'owner': 'jklukas@mozilla.com',
    'start_date': datetime.datetime(2019, 3, 1),
    'email': ['telemetry-alerts@mozilla.com', 'jklukas@mozilla.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'depends_on_past': True,
    # If a task fails, retry it once after waiting at least 5 minutes
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=10),
}

dag_name = 'fxa_events'

with models.DAG(
        dag_name,
        # Continue to run DAG once per day
        schedule_interval='0 10 * * *',
        default_args=default_args) as dag:

    fxa_auth_events = bigquery_etl_query(
        task_id='fxa_auth_events',
        destination_table='fxa_auth_events_v1',
        arguments=('--schema_update_option=ALLOW_FIELD_ADDITION',),
    )

    fxa_auth_bounce_events = bigquery_etl_query(
        task_id='fxa_auth_bounce_events',
        destination_table='fxa_auth_bounce_events_v1',
        arguments=('--schema_update_option=ALLOW_FIELD_ADDITION',),
    )

    fxa_content_events = bigquery_etl_query(
        task_id='fxa_content_events',
        destination_table='fxa_content_events_v1',
        arguments=('--schema_update_option=ALLOW_FIELD_ADDITION',),
    )

    fxa_users_daily = BigQueryOperator(
        task_id='fxa_users_daily',
        bql='sql/fxa_users_daily_v1.sql',
        destination_dataset_table='telemetry.fxa_users_daily_v1${{ds_nodash}}',
        write_disposition='WRITE_TRUNCATE',
        use_legacy_sql=False,
        bigquery_conn_id="google_cloud_derived_datasets",
    )

    fxa_users_daily << fxa_auth_events
    fxa_users_daily << fxa_auth_bounce_events
    fxa_users_daily << fxa_content_events

    fxa_users_last_seen = BigQueryOperator(
        task_id='fxa_users_last_seen',
        bql='sql/fxa_users_last_seen_v1.sql',
        destination_dataset_table='telemetry.fxa_users_last_seen_v1${{ds_nodash}}', ## noqa
        depends_on_past=True,
        start_date=datetime.datetime(2019, 4, 23),
        write_disposition='WRITE_TRUNCATE',
        use_legacy_sql=False,
        bigquery_conn_id="google_cloud_derived_datasets",
    )

    fxa_users_daily >> fxa_users_last_seen

    firefox_accounts_exact_mau28_raw = BigQueryOperator(
        task_id='firefox_accounts_exact_mau28_raw',
        bql='sql/firefox_accounts_exact_mau28_raw_v1.sql',
        destination_dataset_table='telemetry.firefox_accounts_exact_mau28_raw_v1${{ds_nodash}}', ## noqa
        write_disposition='WRITE_TRUNCATE',
        use_legacy_sql=False,
        bigquery_conn_id="google_cloud_derived_datasets",
    )

    fxa_users_last_seen >> firefox_accounts_exact_mau28_raw
