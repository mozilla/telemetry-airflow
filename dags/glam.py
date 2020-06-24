from datetime import datetime, timedelta

from airflow import DAG
from airflow.contrib.hooks.gcp_api_base_hook import GoogleCloudBaseHook
from airflow.executors import get_default_executor
from airflow.operators.sensors import ExternalTaskSensor
from airflow.operators.subdag_operator import SubDagOperator

from glam_subdags.extract import extracts_subdag, extract_user_counts
from glam_subdags.histograms import histogram_aggregates_subdag
from glam_subdags.general import repeated_subdag
from utils.gcp import bigquery_etl_query, gke_command


project_id = "moz-fx-data-shared-prod"
dataset_id = "telemetry_derived"
default_args = {
    "owner": "msamuel@mozilla.com",
    "depends_on_past": False,
    "start_date": datetime(2019, 10, 22),
    "email": [
        "telemetry-alerts@mozilla.com",
        "msamuel@mozilla.com",
        "robhudson@mozilla.com",
    ],
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 1,
    "retry_delay": timedelta(minutes=30),
}

GLAM_DAG = "glam"
GLAM_CLIENTS_HISTOGRAM_AGGREGATES_SUBDAG = "clients_histogram_aggregates"
PERCENT_RELEASE_WINDOWS_SAMPLING = "10"

dag = DAG(GLAM_DAG, default_args=default_args, schedule_interval="@daily")

gcp_conn = GoogleCloudBaseHook("google_cloud_airflow_dataproc")

# Make sure all the data for the given day has arrived before running.
wait_for_main_ping = ExternalTaskSensor(
    task_id="wait_for_main_ping",
    project_id=project_id,
    external_dag_id="copy_deduplicate",
    external_task_id="copy_deduplicate_main_ping",
    execution_delta=timedelta(hours=-1),
    check_existence=True,
    dag=dag,
)

latest_versions = bigquery_etl_query(
    task_id="latest_versions",
    destination_table="latest_versions",
    dataset_id=dataset_id,
    project_id=project_id,
    owner="msamuel@mozilla.com",
    date_partition_parameter=None,
    arguments=("--replace",),
    email=[
        "telemetry-alerts@mozilla.com",
        "msamuel@mozilla.com",
        "robhudson@mozilla.com",
    ],
    dag=dag,
)

# This task runs first and replaces the relevant partition, followed
# by the next two tasks that append to the same partition of the same table.
clients_daily_scalar_aggregates = gke_command(
    task_id="clients_daily_scalar_aggregates",
    owner="msamuel@mozilla.com",
    email=[
        "telemetry-alerts@mozilla.com",
        "msamuel@mozilla.com",
        "robhudson@mozilla.com",
        "bewu@mozilla.com",
    ],
    cmds=["bash"],
    env_vars={
        "PROJECT": project_id,
        "PROD_DATASET": dataset_id,
        "DATASET": dataset_id,
        "SUBMISSION_DATE": "{{ ds }}",
        "RUN_QUERY": "t",
    },
    command=[
        "script/glam/generate_and_run_desktop_sql",
        "scalar",
        PERCENT_RELEASE_WINDOWS_SAMPLING,
    ],
    docker_image="mozilla/bigquery-etl:latest",
    gcp_conn_id="google_cloud_derived_datasets",
    dag=dag,
)

clients_daily_keyed_scalar_aggregates = gke_command(
    task_id="clients_daily_keyed_scalar_aggregates",
    owner="msamuel@mozilla.com",
    email=[
        "telemetry-alerts@mozilla.com",
        "msamuel@mozilla.com",
        "robhudson@mozilla.com",
        "bewu@mozilla.com",
    ],
    cmds=["bash"],
    env_vars={
        "PROJECT": project_id,
        "PROD_DATASET": dataset_id,
        "DATASET": dataset_id,
        "SUBMISSION_DATE": "{{ ds }}",
        "RUN_QUERY": "t",
        "APPEND": "t",
    },
    command=[
        "script/glam/generate_and_run_desktop_sql",
        "keyed_scalar",
        PERCENT_RELEASE_WINDOWS_SAMPLING,
    ],
    docker_image="mozilla/bigquery-etl:latest",
    gcp_conn_id="google_cloud_derived_datasets",
    dag=dag,
)

clients_daily_keyed_boolean_aggregates = gke_command(
    task_id="clients_daily_keyed_boolean_aggregates",
    owner="msamuel@mozilla.com",
    email=[
        "telemetry-alerts@mozilla.com",
        "msamuel@mozilla.com",
        "robhudson@mozilla.com",
        "bewu@mozilla.com",
    ],
    cmds=["bash"],
    env_vars={
        "PROJECT": project_id,
        "PROD_DATASET": dataset_id,
        "DATASET": dataset_id,
        "SUBMISSION_DATE": "{{ ds }}",
        "RUN_QUERY": "t",
        "APPEND": "t",
    },
    command=[
        "script/glam/generate_and_run_desktop_sql",
        "keyed_boolean",
        PERCENT_RELEASE_WINDOWS_SAMPLING,
    ],
    docker_image="mozilla/bigquery-etl:latest",
    gcp_conn_id="google_cloud_derived_datasets",
    dag=dag,
)

clients_scalar_aggregates = bigquery_etl_query(
    task_id="clients_scalar_aggregates",
    destination_table="clients_scalar_aggregates_v1",
    dataset_id=dataset_id,
    project_id=project_id,
    owner="msamuel@mozilla.com",
    email=[
        "telemetry-alerts@mozilla.com",
        "msamuel@mozilla.com",
        "robhudson@mozilla.com",
    ],
    depends_on_past=True,
    date_partition_parameter=None,
    parameters=("submission_date:DATE:{{ds}}",),
    arguments=("--replace",),
    dag=dag,
)

scalar_percentiles = bigquery_etl_query(
    task_id="scalar_percentiles",
    destination_table="scalar_percentiles_v1",
    dataset_id=dataset_id,
    project_id=project_id,
    owner="msamuel@mozilla.com",
    email=[
        "telemetry-alerts@mozilla.com",
        "msamuel@mozilla.com",
        "robhudson@mozilla.com",
    ],
    date_partition_parameter=None,
    arguments=("--replace",),
    dag=dag,
)

# This task runs first and replaces the relevant partition, followed
# by the next task below that appends to the same partition of the same table.
clients_daily_histogram_aggregates = bigquery_etl_query(
    task_id="clients_daily_histogram_aggregates",
    destination_table="clients_daily_histogram_aggregates_v1",
    dataset_id=dataset_id,
    project_id=project_id,
    owner="msamuel@mozilla.com",
    email=[
        "telemetry-alerts@mozilla.com",
        "msamuel@mozilla.com",
        "robhudson@mozilla.com",
        "bewu@mozilla.com",
    ],
    arguments=("--replace",),
    parameters=(
        "sample_size:INT64:{}".format(PERCENT_RELEASE_WINDOWS_SAMPLING),
    ),
    dag=dag,
)

clients_daily_keyed_histogram_aggregates = gke_command(
    task_id="clients_daily_keyed_histogram_aggregates",
    owner="msamuel@mozilla.com",
    email=[
        "telemetry-alerts@mozilla.com",
        "msamuel@mozilla.com",
        "robhudson@mozilla.com",
        "bewu@mozilla.com",
    ],
    cmds=["bash"],
    env_vars={
        "PROJECT": project_id,
        "PROD_DATASET": dataset_id,
        "DATASET": dataset_id,
        "SUBMISSION_DATE": "{{ ds }}",
        "RUN_QUERY": "t",
        "APPEND": "t",
    },
    command=[
        "script/glam/generate_and_run_desktop_sql",
        "keyed_histogram",
        PERCENT_RELEASE_WINDOWS_SAMPLING,
    ],
    docker_image="mozilla/bigquery-etl:latest",
    gcp_conn_id="google_cloud_derived_datasets",
    dag=dag,
    get_logs=False,
)

clients_histogram_aggregates = SubDagOperator(
    subdag=histogram_aggregates_subdag(
        GLAM_DAG,
        GLAM_CLIENTS_HISTOGRAM_AGGREGATES_SUBDAG,
        default_args,
        dag.schedule_interval,
        dataset_id,
    ),
    task_id=GLAM_CLIENTS_HISTOGRAM_AGGREGATES_SUBDAG,
    executor=get_default_executor(),
    dag=dag,
)

histogram_percentiles = bigquery_etl_query(
    task_id="histogram_percentiles",
    destination_table="histogram_percentiles_v1",
    dataset_id=dataset_id,
    project_id=project_id,
    owner="msamuel@mozilla.com",
    email=[
        "telemetry-alerts@mozilla.com",
        "msamuel@mozilla.com",
        "robhudson@mozilla.com",
    ],
    date_partition_parameter=None,
    arguments=("--replace",),
    dag=dag,
)

glam_user_counts = bigquery_etl_query(
    task_id="glam_user_counts",
    destination_table="glam_user_counts_v1",
    dataset_id=dataset_id,
    project_id=project_id,
    owner="msamuel@mozilla.com",
    email=[
        "telemetry-alerts@mozilla.com",
        "msamuel@mozilla.com",
        "robhudson@mozilla.com",
    ],
    date_partition_parameter=None,
    parameters=("submission_date:DATE:{{ds}}",),
    arguments=("--replace",),
    dag=dag,
)

client_scalar_probe_counts = bigquery_etl_query(
    task_id="client_scalar_probe_counts",
    destination_table="clients_scalar_probe_counts_v1",
    dataset_id=dataset_id,
    project_id=project_id,
    owner="msamuel@mozilla.com",
    email=[
        "telemetry-alerts@mozilla.com",
        "msamuel@mozilla.com",
        "robhudson@mozilla.com",
    ],
    date_partition_parameter=None,
    arguments=("--replace",),
    dag=dag,
)

# SubdagOperator uses a SequentialExecutor by default
# so its tasks will run sequentially.
clients_histogram_bucket_counts = SubDagOperator(
    subdag=repeated_subdag(
        GLAM_DAG,
        "clients_histogram_bucket_counts",
        default_args,
        dag.schedule_interval,
        dataset_id,
        ("submission_date:DATE:{{ds}}",),
        10,
        None,
    ),
    task_id="clients_histogram_bucket_counts",
    dag=dag,
)

clients_histogram_probe_counts = bigquery_etl_query(
    task_id="clients_histogram_probe_counts",
    destination_table="clients_histogram_probe_counts_v1",
    dataset_id=dataset_id,
    project_id=project_id,
    owner="msamuel@mozilla.com",
    email=[
        "telemetry-alerts@mozilla.com",
        "msamuel@mozilla.com",
        "robhudson@mozilla.com",
    ],
    date_partition_parameter=None,
    arguments=("--replace",),
    dag=dag,
)

extract_counts = SubDagOperator(
    subdag=extract_user_counts(
        GLAM_DAG,
        "extract_user_counts",
        default_args,
        dag.schedule_interval,
        dataset_id
    ),
    task_id="extract_user_counts",
    executor=get_default_executor(),
    dag=dag
)


extracts_per_channel = SubDagOperator(
    subdag=extracts_subdag(
        GLAM_DAG,
        "extracts",
        default_args,
        dag.schedule_interval,
        dataset_id
    ),
    task_id="extracts",
    executor=get_default_executor(),
    dag=dag,
)


wait_for_main_ping >> latest_versions

latest_versions >> clients_daily_scalar_aggregates
clients_daily_scalar_aggregates >> clients_daily_keyed_scalar_aggregates
clients_daily_scalar_aggregates >> clients_daily_keyed_boolean_aggregates
clients_daily_keyed_boolean_aggregates >> clients_scalar_aggregates
clients_daily_keyed_scalar_aggregates >> clients_scalar_aggregates
clients_scalar_aggregates >> scalar_percentiles
clients_scalar_aggregates >> client_scalar_probe_counts

latest_versions >> clients_daily_histogram_aggregates
clients_daily_histogram_aggregates >> clients_daily_keyed_histogram_aggregates
clients_daily_keyed_histogram_aggregates >> clients_histogram_aggregates

clients_histogram_aggregates >> clients_histogram_bucket_counts
clients_histogram_aggregates >> glam_user_counts

clients_histogram_bucket_counts >> clients_histogram_probe_counts
clients_histogram_probe_counts >> histogram_percentiles

clients_scalar_aggregates >> glam_user_counts

glam_user_counts >> extract_counts

extract_counts >> extracts_per_channel
client_scalar_probe_counts >> extracts_per_channel
scalar_percentiles >> extracts_per_channel
histogram_percentiles >> extracts_per_channel
