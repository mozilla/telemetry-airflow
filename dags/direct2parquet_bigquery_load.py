from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.subdag_operator import SubDagOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from utils.gcp import load_to_bigquery

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
            'dataset_version': 'v3'
            },
        'telemetry-anonymous-parquet': {
            'dataset_version': 'v1'
            },
        'telemetry-shield-study-parquet': {
            'dataset_version': 'v1',
            'date_submission_col': 'submission',
            },
        'telemetry-new-profile-parquet': {
            'dataset_version': 'v2',
            'date_submission_col': 'submission'
            },
        'telemetry-mobile-event-parquet': {
            'dataset_version': 'v2',
            },
        'telemetry-heartbeat-parquet': {
            'dataset_version': 'v1',
            },
        'telemetry-focus-event-parquet': {
            'dataset_version': 'v1',
            },
        'eng-workflow-hgpush-parquet': {
            'dataset_version': 'v1',
            },
        'eng-workflow-build-parquet': {
            'dataset_version': 'v1',
            },
        'firefox-installer-install-parquet': {
            'dataset_version': 'v1',
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
            'dataset_version': values['dataset_version'],
            'gke_cluster_name': 'bq-load-gke-1',
        }

        try:
            kwargs['date_submission_col'] = values['date_submission_col']
        except KeyError:
            pass

        tasks[task_name] = SubDagOperator(
                            subdag=load_to_bigquery(**kwargs),
                            task_id=task_name)

    core_clients_daily = BigQueryOperator(
        task_id='core_clients_daily',
        bql='sql/core_clients_daily_v1.sql',
        destination_dataset_table='telemetry.core_clients_daily_v1${{ds_nodash}}', # noqa
        write_disposition='WRITE_TRUNCATE',
        use_legacy_sql=False,
    )

    tasks['telemetry_core_parquet_bigquery_load'] >> core_clients_daily

    core_clients_last_seen = BigQueryOperator(
        task_id='core_clients_last_seen',
        bql='sql/core_clients_last_seen_v1.sql',
        destination_dataset_table='telemetry.core_clients_last_seen_v1${{ds_nodash}}', # noqa
        write_disposition='WRITE_TRUNCATE',
        use_legacy_sql=False,
        depends_on_past=True,
    )

    core_clients_daily >> core_clients_last_seen

    firefox_nondesktop_exact_mau28_raw = BigQueryOperator(
        task_id='firefox_nondesktop_exact_mau28_raw',
        bql='sql/firefox_nondesktop_exact_mau28_raw_v1.sql',
        destination_dataset_table='telemetry.firefox_nondesktop_exact_mau28_raw_v1${{ds_nodash}}', # noqa
        write_disposition='WRITE_TRUNCATE',
        use_legacy_sql=False,
    )

    core_clients_last_seen >> firefox_nondesktop_exact_mau28_raw
