from datetime import datetime, timedelta

from airflow import DAG
from airflow.contrib.hooks.gcp_api_base_hook import GoogleCloudBaseHook
from airflow.contrib.operators.bigquery_to_gcs import BigQueryToCloudStorageOperator
from airflow.operators.sensors import ExternalTaskSensor

from airflow.contrib.operators.gcs_delete_operator import GoogleCloudStorageDeleteOperator
from utils.gcp import bigquery_etl_query


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
glam_bucket = "gs://glam-dev-bespoke-nonprod-dataops-mozgcp-net"

dag = DAG('glam', default_args=default_args, schedule_interval='@daily')

gcp_conn = GoogleCloudBaseHook("google_cloud_airflow_dataproc")

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

latest_versions = bigquery_etl_query(
    task_id="latest_versions",
    destination_table="latest_versions",
    dataset_id=dataset_id,
    project_id="moz-fx-data-shared-prod",
    owner="msamuel@mozilla.com",
    date_partition_parameter=None,
    arguments=('--replace',),
    email=["telemetry-alerts@mozilla.com", "msamuel@mozilla.com"],
    dag=dag)

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
    depends_on_past=True,
    date_partition_parameter=None,
    parameters=("submission_date:DATE:{{ds}}",),
    arguments=('--replace',),
    dag=dag)

scalar_percentiles = bigquery_etl_query(
    task_id="scalar_percentiles",
    destination_table="scalar_percentiles_v1",
    dataset_id=dataset_id,
    project_id="moz-fx-data-shared-prod",
    owner="msamuel@mozilla.com",
    email=["telemetry-alerts@mozilla.com", "msamuel@mozilla.com"],
    date_partition_parameter=None,
    arguments=('--replace',),
    dag=dag)

clients_scalar_bucket_counts = bigquery_etl_query(
    task_id="clients_scalar_bucket_counts",
    destination_table="clients_scalar_bucket_counts_v1",
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

clients_histogram_aggregates_new = bigquery_etl_query(
    task_id="clients_histogram_aggregates_new",
    destination_table="clients_histogram_aggregates_new_v1",
    dataset_id=dataset_id,
    project_id="moz-fx-data-shared-prod",
    owner="msamuel@mozilla.com",
    email=["telemetry-alerts@mozilla.com", "msamuel@mozilla.com"],
    date_partition_parameter=None,
    parameters=("submission_date:DATE:{{ds}}",),
    arguments=('--replace',),
    dag=dag)

clients_histogram_aggregates_old = bigquery_etl_query(
    task_id="clients_histogram_aggregates_old",
    destination_table="clients_histogram_aggregates_old_v1",
    dataset_id=dataset_id,
    project_id="moz-fx-data-shared-prod",
    owner="msamuel@mozilla.com",
    email=["telemetry-alerts@mozilla.com", "msamuel@mozilla.com"],
    depends_on_past=True,
    date_partition_parameter=None,
    arguments=('--replace',),
    dag=dag)

clients_histogram_aggregates_merged_0 = bigquery_etl_query(
    task_id="clients_histogram_aggregates_merged_0",
    destination_table="clients_histogram_aggregates_merged_v1",
    dataset_id=dataset_id,
    project_id="moz-fx-data-shared-prod",
    owner="msamuel@mozilla.com",
    email=["telemetry-alerts@mozilla.com", "msamuel@mozilla.com"],
    date_partition_parameter=None,
    parameters=("min_sample_id:INT64:0", "max_sample_id:INT64:24",),
    arguments=('--replace',),
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

NUM_PARTITIONS = 4
NUM_SAMPLE_IDS = 100
PARTITION_SIZE = NUM_SAMPLE_IDS / NUM_PARTITIONS
for partition in range(1, NUM_PARTITIONS):
    min_param = partition * PARTITION_SIZE
    max_param = min_param + PARTITION_SIZE - 1
    clients_histogram_aggregates_merged = bigquery_etl_query(
        task_id="clients_histogram_aggregates_merged_{}".format(partition),
        destination_table="clients_histogram_aggregates_merged_v1",
        dataset_id=dataset_id,
        project_id="moz-fx-data-shared-prod",
        owner="msamuel@mozilla.com",
        email=["telemetry-alerts@mozilla.com", "msamuel@mozilla.com"],
        date_partition_parameter=None,
        parameters=(
            "min_sample_id:INT64:{}".format(min_param),
            "max_sample_id:INT64:{}".format(max_param),
        ),
        arguments=('--append_table','--noreplace',),
        dag=dag)
    clients_histogram_aggregates_merged_0 >> clients_histogram_aggregates_merged
    clients_histogram_aggregates_merged >> clients_histogram_aggregates

clients_histogram_bucket_counts = bigquery_etl_query(
    task_id="clients_histogram_bucket_counts",
    destination_table="clients_histogram_bucket_counts_v1",
    dataset_id=dataset_id,
    project_id="moz-fx-data-shared-prod",
    owner="msamuel@mozilla.com",
    email=["telemetry-alerts@mozilla.com", "msamuel@mozilla.com"],
    date_partition_parameter=None,
    arguments=('--replace',),
    dag=dag)

histogram_percentiles = bigquery_etl_query(
    task_id="histogram_percentiles",
    destination_table="histogram_percentiles_v1",
    dataset_id=dataset_id,
    project_id="moz-fx-data-shared-prod",
    owner="msamuel@mozilla.com",
    email=["telemetry-alerts@mozilla.com", "msamuel@mozilla.com"],
    date_partition_parameter=None,
    arguments=('--replace',),
    dag=dag)

glam_user_counts = bigquery_etl_query(
    task_id="glam_user_counts",
    destination_table="glam_user_counts_v1",
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

glam_client_probe_counts_extract = bigquery_etl_query(
    task_id="glam_client_probe_counts_extract",
    destination_table="glam_client_probe_counts_extract_v1",
    dataset_id=dataset_id,
    project_id="moz-fx-data-shared-prod",
    owner="robhudson@mozilla.com",
    email=["telemetry-alerts@mozilla.com", "msamuel@mozilla.com", "robhudson@mozilla.com"],
    date_partition_parameter=None,
    arguments=('--replace',),
    dag=dag)

glam_gcs_delete_old_extracts = GoogleCloudStorageDeleteOperator(
    task_id="glam_gcs_delete_old_extracts",
    bucket_name=glam_bucket,
    prefix="extract-",
    google_cloud_storage_conn_id=gcp_conn.gcp_conn_id,
    dag=dag)

gcs_destination = "{}/extract-*.csv".format(glam_bucket)
glam_extract_to_csv = BigQueryToCloudStorageOperator(
    task_id="glam_extract_to_csv",
    source_project_dataset_table="glam_client_probe_counts_extract_v1",
    destination_cloud_storage_uris=gcs_destination,
    export_format="CSV",
    print_header=False,
    dag=dag)

wait_for_main_ping >> latest_versions

latest_versions >> clients_daily_scalar_aggregates
clients_daily_scalar_aggregates >> clients_daily_keyed_scalar_aggregates
clients_daily_scalar_aggregates >> clients_daily_keyed_boolean_aggregates
clients_daily_keyed_boolean_aggregates >> clients_scalar_aggregates
clients_daily_keyed_scalar_aggregates >> clients_scalar_aggregates
clients_scalar_aggregates >> clients_scalar_bucket_counts
clients_scalar_aggregates >> scalar_percentiles

latest_versions >> clients_daily_histogram_aggregates
clients_daily_histogram_aggregates >> clients_daily_keyed_histogram_aggregates
clients_daily_keyed_histogram_aggregates >> clients_histogram_aggregates_new
clients_daily_keyed_histogram_aggregates >> clients_histogram_aggregates_old

clients_histogram_aggregates_old >> clients_histogram_aggregates_merged_0
clients_histogram_aggregates_new >> clients_histogram_aggregates_merged_0

clients_scalar_bucket_counts >> client_scalar_probe_counts
client_scalar_probe_counts >> client_histogram_probe_counts
clients_histogram_aggregates >> clients_histogram_bucket_counts
clients_histogram_bucket_counts >> client_histogram_probe_counts
client_histogram_probe_counts >> histogram_percentiles

clients_scalar_aggregates >> glam_user_counts
clients_histogram_aggregates >> glam_user_counts

glam_user_counts >> glam_client_probe_counts_extract
histogram_percentiles >> glam_client_probe_counts_extract
glam_client_probe_counts_extract >> glam_gcs_delete_old_extracts
glam_gcs_delete_old_extracts >> glam_extract_to_csv
