import ast
import dataclasses
import datetime

from typing import Optional, List
from airflow.decorators import dag
from airflow.models.param import Param
from airflow.operators.python import PythonOperator
from utils.tags import Tag
from utils.gcp import bigquery_etl_backfill


@dataclasses.dataclass
class BackfillParams:
    destination_table: str
    dataset_id: str
    start_date: str
    end_date: str
    project_id: str
    excludes: List[str]
    dry_run: bool
    parallelism: int
    no_partition: bool

    def validate_date_range(self) -> None:
        start_date = datetime.datetime.fromisoformat(self.start_date)
        end_date = datetime.datetime.fromisoformat(self.end_date)
        if start_date > end_date:
            raise ValueError(f"`start_date`={self.start_date} is greater than `end_date`={self.end_date}")

def param_validation(params: dict) -> bool:
    backfill_params = BackfillParams(**params)
    backfill_params.validate_date_range()
    return True

doc_md = """
# Backfill Bqetl Query

### Some tips/notes:

* Recommend to dryrun first
* Date formats are e.g. 2022-03-01

Shamelessly stolen from backfill.py
"""


@dag(
    dag_id='backfill_bqetl',
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
        "end_date": Param(datetime.date.today().isoformat(),
                          type="string",
                          format="date-time"),
        "excludes": Param(None, type=["string", "null"], description="Comma-separated list of dates to exclude from backfill."),
        "no_partition": Param(False, type="boolean"),
        "parallelism": Param(10, type="number"),
        "project_id": Param(None, type=["string", "null"]),
        "start_date": Param((datetime.date.today() - datetime.timedelta(days=1)).isoformat(),
                             type="string",
                             format="date-time"),
    }
)
def backfill_bqetl_dag():

    param_validation_task = PythonOperator(
        task_id="param_validation",
        python_callable=param_validation,
        op_kwargs={"params": "{{ params }}"},
    )

    backfill_task = bigquery_etl_backfill(
        destination_table = "{{ params.destination_table }}",
        dataset_id = "{{ params.dataset_id }}",
        start_date = "{{ params.start_date }}",
        end_date = "{{ params.end_date }}",
        project_id = "{{ params.project_id }}",
        excludes = "{{ params.excludes }}",
        dry_run = "{{ params.dry_run }}",
        parallelism = "{{ params.parallelism }}",
        no_partition = "{{ params.no_partition }}",
        gcp_conn_id="google_cloud_gke_sandbox",
        gke_project_id="moz-fx-data-gke-sandbox",
        gke_location="us-west1",
        gke_cluster_name="fbertsch-gke-sandbox",
    )

    param_validation_task >> backfill_task


dag = backfill_bqetl_dag()
