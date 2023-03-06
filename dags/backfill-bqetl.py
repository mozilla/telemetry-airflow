from __future__ import annotations

import datetime

from airflow.decorators import dag
from airflow.models.param import Param
from airflow.operators.python import PythonOperator

from dags.operators.gcp_container_operator import GKEPodOperator
from dags.utils.backfill import BigQueryETLBackfillParams
from dags.utils.tags import Tag


def param_validation(params: dict) -> bool:
    backfill_params = BigQueryETLBackfillParams(**params)
    backfill_params.validate_date_range()
    return True


def generate_container_args(params: dict) -> list[str]:
    backfill_params = BigQueryETLBackfillParams(**params)
    return backfill_params.generate_backfill_command()


doc_md = """
# Backfill Bqetl Query

### Some tips/notes:

* Recommend to dryrun first
* Date formats are e.g. 2022-03-01
"""


@dag(
    dag_id="backfill_bqetl",
    schedule_interval=None,
    doc_md=doc_md,
    catchup=False,
    start_date=datetime.datetime(2022, 12, 1),
    dagrun_timeout=datetime.timedelta(days=1),
    tags=[Tag.ImpactTier.tier_3],
    render_template_as_native_obj=True,
    params={
        "dataset_id": Param(None, type=["string", "null"]),
        "destination_table": Param(None, type=["string", "null"]),
        "dry_run": Param(True, type="boolean"),
        "end_date": Param(
            datetime.date.today().isoformat(), type="string", format="date-time"
        ),
        "excludes": Param(
            None,
            type=["string", "null"],
            description="Comma-separated list of dates to exclude from backfill. Date "
            "format: yyyy-mm-dd",
        ),
        "no_partition": Param(False, type="boolean"),
        "parallelism": Param(10, type="number"),
        "project_id": Param(None, type=["string", "null"]),
        "start_date": Param(
            (datetime.date.today() - datetime.timedelta(days=1)).isoformat(),
            type="string",
            format="date-time",
        ),
    },
)
def backfill_bqetl_dag():
    param_validation_task = PythonOperator(
        task_id="param_validation",
        python_callable=param_validation,
        op_kwargs={"params": "{{ dag_run.conf }}"},
    )

    generate_backfill_command_task = PythonOperator(
        task_id="generate_backfill_command",
        python_callable=generate_container_args,
        op_kwargs={"params": "{{ dag_run.conf }}"},
    )

    bigquery_etl_backfill_task = GKEPodOperator(
        task_id="bigquery_etl_backfill",
        name="bigquery-etl-backfill",
        gcp_conn_id="google_cloud_gke_sandbox",
        project_id="moz-fx-data-gke-sandbox",
        location="us-west1",
        cluster_name="mducharme-gke-sandbox",
        arguments="{{ ti.xcom_pull(task_ids='generate_backfill_command_task') }}",
        is_delete_operator_pod=False,
        image="gcr.io/moz-fx-data-airflow-prod-88e0/bigquery-etl:latest",
    )

    (
        param_validation_task
        >> generate_backfill_command_task
        >> bigquery_etl_backfill_task
    )


dag = backfill_bqetl_dag()
