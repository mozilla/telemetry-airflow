import datetime

from airflow import models
from airflow.operators.sensors import ExternalTaskSensor
from airflow.operators.subdag_operator import SubDagOperator
from utils.gcp import (bigquery_etl_copy_deduplicate,
                       bigquery_etl_query,
                       gke_command,
                       bigquery_xcom_query)

from airflow.contrib.hooks.gcp_api_base_hook import GoogleCloudBaseHook
from operators.gcp_container_operator import GKEPodOperator

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

dag_name = "copy_deduplicate"

with models.DAG(
        dag_name,
        schedule_interval="0 1 * * *",
        default_args=default_args) as dag:

    # This single task is responsible for sequentially running copy queries
    # over all the tables in _live datasets into _stable datasets except those
    # that are specifically used in another DAG.
    resources = {'request_memory':'10240Mi', 'request_cpu': None,
                'limit_memory':'20480Mi', 'limit_cpu': None, 'limit_gpu': None}

    copy_deduplicate_all = bigquery_etl_copy_deduplicate(
        task_id="copy_deduplicate_all",
        target_project_id="moz-fx-data-shared-prod",
        billing_projects=("moz-fx-data-shared-prod",),
        priority_weight=100,
        # Any table listed here under except_tables _must_ have a corresponding
        # copy_deduplicate job in another DAG.
        except_tables=["telemetry_live.main_v4"],
        node_selectors={"nodepool" : "highmem"},
        resources=resources)

    copy_deduplicate_main_ping = bigquery_etl_copy_deduplicate(
        task_id="copy_deduplicate_main_ping",
        target_project_id="moz-fx-data-shared-prod",
        billing_projects=("moz-fx-data-shared-prod",),
        only_tables=["telemetry_live.main_v4"],
        parallelism=24,
        slices=150,
        owner="jklukas@mozilla.com",
        email=["telemetry-alerts@mozilla.com", "relud@mozilla.com", "jklukas@mozilla.com"],
        priority_weight=100,
        dag=dag)


    # Events.

    event_events = bigquery_etl_query(
        task_id="event_events",
        project_id="moz-fx-data-shared-prod",
        destination_table="event_events_v1",
        dataset_id="telemetry_derived",
        priority_weight=90,
        owner="ssuh@mozilla.com",
        email=["telemetry-alerts@mozilla.com", "ssuh@mozilla.com"],
        arguments=('--schema_update_option=ALLOW_FIELD_ADDITION',),
    )

    copy_deduplicate_all >> event_events

    bq_main_events = bigquery_etl_query(
        task_id="bq_main_events",
        project_id="moz-fx-data-shared-prod",
        destination_table="main_events_v1",
        dataset_id="telemetry_derived",
        priority_weight=90,
        owner="ssuh@mozilla.com",
        email=["telemetry-alerts@mozilla.com", "ssuh@mozilla.com"],
        dag=dag,
        arguments=('--schema_update_option=ALLOW_FIELD_ADDITION',),
    )

    copy_deduplicate_main_ping >> bq_main_events

    # Daily and last seen views on top of every Glean application.

    gcp_conn_id = "google_cloud_derived_datasets"
    baseline_etl_kwargs = dict(
        gcp_conn_id=gcp_conn_id,
        project_id=GoogleCloudBaseHook(gcp_conn_id=gcp_conn_id).project_id,
        location="us-central1-a",
        cluster_name="bq-load-gke-1",
        namespace="default",
        image="mozilla/bigquery-etl:latest",
    )
    baseline_args = [
        "--project-id=moz-fx-data-shared-prod",
        "--date={{ ds }}",
        "--only=*_stable.baseline_v1"
    ]
    baseline_clients_daily = GKEPodOperator(
        task_id='baseline_clients_daily',
        name='baseline-clients-daily',
        arguments=["script/run_glean_baseline_clients_daily"] + baseline_args,
        **baseline_etl_kwargs
    )
    baseline_clients_last_seen = GKEPodOperator(
        task_id='baseline_clients_last_seen',
        name='baseline-clients-last-seen',
        arguments=["script/run_glean_baseline_clients_last_seen"] + baseline_args,
        depends_on_past=True,
        **baseline_etl_kwargs
    )

    (copy_deduplicate_all >>
     baseline_clients_daily >>
     baseline_clients_last_seen)
