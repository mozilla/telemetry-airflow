from airflow import DAG
from airflow.operators.sensors import ExternalTaskSensor
from datetime import datetime, timedelta
from airflow.operators.subdag_operator import SubDagOperator
from utils.gcp import bigquery_etl_query, load_to_bigquery

default_args = {
    'owner': 'jthomas@mozilla.com',
    'depends_on_past': False,
    'start_date': datetime(2019, 4, 23),
    'email': ['telemetry-alerts@mozilla.com', 'jthomas@mozilla.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=30),
}


dag_name = 'direct2parquet_bigquery_load'

with DAG(
        dag_name,
        schedule_interval="0 2 * * *",
        default_args=default_args) as dag:

    datasets = {
        'telemetry-core-parquet': {
            'dataset_version': 'v3',
            'cluster_by': ['app_name', 'os'],
            'rename': {'submission_date_s3': 'submission_date'},
            },
        'telemetry-anonymous-parquet': {
            'dataset_version': 'v1',
            },
        'telemetry-shield-study-parquet': {
            'dataset_version': 'v1',
            'date_submission_col': 'submission',
            },
        'telemetry-new-profile-parquet': {
            'dataset_version': 'v2',
            'date_submission_col': 'submission',
            },
        'telemetry-mobile-event-parquet': {
            'dataset_version': 'v2',
            'cluster_by': ['app_name', 'os'],
            'rename': {'submission_date_s3': 'submission_date'},
            },
        'telemetry-heartbeat-parquet': {
            'dataset_version': 'v1',
            },
        'telemetry-focus-event-parquet': {
            'dataset_version': 'v1',
            'cluster_by': ['channel'],
            'rename': {'submission_date_s3': 'submission_date'},
            },
        'eng-workflow-hgpush-parquet': {
            'dataset_version': 'v1',
            },
        'eng-workflow-build-parquet': {
            'dataset_version': 'v1',
            },
        'firefox-installer-install-parquet': {
            'dataset_version': 'v1',
            'bigquery_dataset': 'telemetry',
            },
    }

    tasks = {}

    for dataset, values in datasets.iteritems():

        task_name = dataset.replace('-', '_') + '_bigquery_load'

        kwargs = {
            'parent_dag_name': dag.dag_id,
            'dag_name': task_name,
            'default_args': default_args,
            'dataset_s3_bucket': 'net-mozaws-prod-us-west-2-pipeline-data',
            'aws_conn_id': 'aws_prod_iam_s3',
            'dataset': dataset,
            'gke_cluster_name': 'bq-load-gke-1',
            'bigquery_dataset': 'telemetry_derived',
        }

        kwargs.update(values)

        tasks[task_name] = SubDagOperator(
                            subdag=load_to_bigquery(**kwargs),
                            task_id=task_name)


    # Daily and last seen views on top of core pings.

    core_clients_daily = bigquery_etl_query(
        task_id='core_clients_daily',
        destination_table='core_clients_daily_v1',
        dataset_id='telemetry',
    )

    tasks['telemetry_core_parquet_bigquery_load'] >> core_clients_daily

    core_clients_last_seen = bigquery_etl_query(
        task_id='core_clients_last_seen',
        destination_table='core_clients_last_seen_raw_v1',
        dataset_id='telemetry',
        depends_on_past=True,
    )

    core_clients_daily >> core_clients_last_seen


    # Daily and last seen views on top of glean pings.

    wait_for_copy_deduplicate = ExternalTaskSensor(
        task_id="wait_for_copy_deduplicate",
        external_dag_id="copy_deduplicate",
        external_task_id="copy_deduplicate_all",
        execution_delta=timedelta(hours=1),
        dag=dag,
    )

    fenix_clients_daily = bigquery_etl_query(
        task_id='fenix_clients_daily',
        destination_table='moz-fx-data-shared-prod:org_mozilla_fenix_derived.clients_daily_v1',
        sql_file_path='sql/org_mozilla_fenix_derived/clients_daily_v1/query.sql',
        dataset_id='org_mozilla_fenix_derived',
        start_date=datetime(2019, 9, 5),
    )

    fenix_clients_daily << wait_for_copy_deduplicate

    fenix_clients_last_seen = bigquery_etl_query(
        task_id='fenix_clients_last_seen',
        destination_table='moz-fx-data-shared-prod:org_mozilla_fenix_derived.clients_last_seen_v1',
        sql_file_path='sql/org_mozilla_fenix_derived/clients_last_seen_v1/query.sql',
        dataset_id='org_mozilla_fenix_derived',
        start_date=datetime(2019, 9, 5),
        depends_on_past=True,
    )

    fenix_clients_daily >> fenix_clients_last_seen


    # Aggregated nondesktop tables.

    firefox_nondesktop_exact_mau28_raw = bigquery_etl_query(
        task_id='firefox_nondesktop_exact_mau28_raw',
        destination_table='firefox_nondesktop_exact_mau28_raw_v1',
        dataset_id='telemetry',
    )

    core_clients_last_seen >> firefox_nondesktop_exact_mau28_raw
    fenix_clients_last_seen >> firefox_nondesktop_exact_mau28_raw

    smoot_usage_nondesktop_v2 = bigquery_etl_query(
        task_id='smoot_usage_nondesktop_v2',
        destination_table='moz-fx-data-shared-prod:telemetry_derived.smoot_usage_nondesktop_v2',
        sql_file_path='sql/telemetry_derived/smoot_usage_nondesktop_v2/query.sql',
        dataset_id='telemetry_derived',
    )

    core_clients_last_seen >> smoot_usage_nondesktop_v2
    fenix_clients_last_seen >> smoot_usage_nondesktop_v2
