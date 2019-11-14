from airflow import DAG
from datetime import datetime, timedelta
from utils.gcp import bigquery_etl_query
from airflow.operators.sensors import ExternalTaskSensor


dataset_id = "telemetry_derived"
default_args = {
    'owner': 'msamuel@mozilla.com',
    'depends_on_past': False,
    'start_date': datetime(2019, 10, 22),
    'email': ['telemetry-alerts@mozilla.com', 'msamuel@mozilla.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=30),
}

dag = DAG('glam', default_args=default_args, schedule_interval='@daily')

# Make sure all the data for the given day has arrived before running.
wait_for_main_ping = ExternalTaskSensor(
    task_id="wait_for_main_ping",
    project_id="moz-fx-data-shared-prod",
    external_dag_id="main_summary",
    external_task_id="copy_deduplicate_main_ping",
    execution_delta=timedelta(hours=-1),
    check_existence=True,
    dag=dag,
)

# This task runs first and replaces the relevant partition, followed
# by the next two tasks that append to the same partition of the same table.
clients_daily_scalar_aggregates = bigquery_etl_query(
    task_id="clients_daily_scalar_aggregates",
    destination_table="clients_daily_scalar_aggregates_v1",
    dataset_id=dataset_id,
    project_id="moz-fx-data-shared-prod",
    owner="msamuel@mozilla.com",
    email=["telemetry-alerts@mozilla.com", "msamuel@mozilla.com"],
    dag=dag)

sql_file_path = "sql/{}/{}/query.sql".format(dataset_id, "clients_daily_keyed_scalar_aggregates_v1")
clients_daily_keyed_scalar_aggregates = bigquery_etl_query(
    task_id="clients_daily_keyed_scalar_aggregates",
    destination_table="clients_daily_scalar_aggregates_v1",
    sql_file_path=sql_file_path,
    dataset_id=dataset_id,
    project_id="moz-fx-data-shared-prod",
    owner="msamuel@mozilla.com",
    email=["telemetry-alerts@mozilla.com", "msamuel@mozilla.com"],
    arguments=('--append_table', '--noreplace',),
    dag=dag)

sql_file_path = "sql/{}/{}/query.sql".format(dataset_id, "clients_daily_keyed_boolean_aggregates_v1")
clients_daily_keyed_boolean_aggregates = bigquery_etl_query(
    task_id="clients_daily_keyed_boolean_aggregates",
    destination_table="clients_daily_scalar_aggregates_v1",
    sql_file_path=sql_file_path,
    dataset_id=dataset_id,
    project_id="moz-fx-data-shared-prod",
    owner="msamuel@mozilla.com",
    email=["telemetry-alerts@mozilla.com", "msamuel@mozilla.com"],
    arguments=('--append_table','--noreplace',),
    dag=dag)

clients_scalar_aggregates = bigquery_etl_query(
    task_id="clients_scalar_aggregates",
    destination_table="clients_scalar_aggregates_v1",
    dataset_id=dataset_id,
    project_id="moz-fx-data-shared-prod",
    owner="msamuel@mozilla.com",
    email=["telemetry-alerts@mozilla.com", "msamuel@mozilla.com"],
    date_partition_parameter=None,
    arguments=('--replace',),
    dag=dag)

# This task runs first and replaces the relevant partition, followed
# by the next task below that appends to the same partition of the same table.
clients_daily_histogram_aggregates = bigquery_etl_query(
    task_id="clients_daily_histogram_aggregates",
    destination_table="clients_daily_histogram_aggregates_v1",
    dataset_id=dataset_id,
    project_id="moz-fx-data-shared-prod",
    owner="msamuel@mozilla.com",
    email=["telemetry-alerts@mozilla.com", "msamuel@mozilla.com"],
    dag=dag)

sql_file_path = "sql/{}/{}/query.sql".format(dataset_id, "clients_daily_keyed_histogram_aggregates_v1")
clients_daily_keyed_histogram_aggregates = bigquery_etl_query(
    task_id="clients_daily_keyed_histogram_aggregates",
    destination_table="clients_daily_histogram_aggregates_v1",
    sql_file_path=sql_file_path,
    dataset_id=dataset_id,
    project_id="moz-fx-data-shared-prod",
    owner="msamuel@mozilla.com",
    email=["telemetry-alerts@mozilla.com", "msamuel@mozilla.com"],
    arguments=('--append_table','--noreplace',),
    dag=dag)

clients_histogram_aggregates = bigquery_etl_query(
    task_id="clients_histogram_aggregates",
    destination_table="clients_histogram_aggregates_v1",
    dataset_id=dataset_id,
    project_id="moz-fx-data-shared-prod",
    owner="msamuel@mozilla.com",
    email=["telemetry-alerts@mozilla.com", "msamuel@mozilla.com"],
    date_partition_parameter=None,
    arguments=('--replace',),
    dag=dag)

sql_file_path = "sql/{}/{}/query.sql".format(dataset_id, "clients_scalar_probe_counts_v1")
client_scalar_probe_counts = bigquery_etl_query(
    task_id="client_scalar_probe_counts",
    destination_table="client_probe_counts_v1",
    sql_file_path=sql_file_path,
    dataset_id=dataset_id,
    project_id="moz-fx-data-shared-prod",
    owner="msamuel@mozilla.com",
    email=["telemetry-alerts@mozilla.com", "msamuel@mozilla.com"],
    date_partition_parameter=None,
    dag=dag)

sql_file_path = "sql/{}/{}/query.sql".format(dataset_id, "clients_histogram_probe_counts_v1")
client_histogram_probe_counts = bigquery_etl_query(
    task_id="client_histogram_probe_counts",
    destination_table="client_probe_counts_v1",
    sql_file_path=sql_file_path,
    dataset_id=dataset_id,
    project_id="moz-fx-data-shared-prod",
    owner="msamuel@mozilla.com",
    email=["telemetry-alerts@mozilla.com", "msamuel@mozilla.com"],
    date_partition_parameter=None,
    arguments=('--append_table','--noreplace',),
    dag=dag)


wait_for_main_ping >> clients_daily_scalar_aggregates
clients_daily_scalar_aggregates >> clients_daily_keyed_scalar_aggregates
clients_daily_scalar_aggregates >> clients_daily_keyed_boolean_aggregates
clients_daily_keyed_boolean_aggregates >> clients_scalar_aggregates
clients_daily_keyed_scalar_aggregates >> clients_scalar_aggregates

wait_for_main_ping >> clients_daily_histogram_aggregates
clients_daily_histogram_aggregates >> clients_daily_keyed_histogram_aggregates
clients_daily_keyed_histogram_aggregates >> clients_histogram_aggregates

clients_scalar_aggregates >> client_scalar_probe_counts
client_scalar_probe_counts >> client_histogram_probe_counts
clients_histogram_aggregates >> client_histogram_probe_counts
