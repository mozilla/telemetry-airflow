import ast
import dataclasses
import datetime

from typing import Optional, List
from airflow.decorators import dag
from airflow.operators.python import PythonOperator
from utils.tags import Tag


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


def __parse_string_params(string_params: str) -> Optional[BackfillParams]:
    """
    dag_run.conf is string representation of a Python dictionary in Airflow 2.1
    ast.literal_eval() is used to convert from string to dictionary as a workaround
    this workaround will no longer be required in Airflow >=2.2, see link below for future implementation
    https://airflow.apache.org/docs/apache-airflow/stable/concepts/params.html
    """
    params_parsed = ast.literal_eval(string_params)
    return BackfillParams(**params_parsed)


def param_validation(params: str) -> bool:
    backfill_params = __parse_string_params(params)
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
    params={
        "dataset_id": None,
        "destination_table": None,
        "dry_run": True,
        "end_date": datetime.date.today().isoformat(),
        "excludes": [],
        "no_partition": False,
        "parallelism": 10,
        "project_id": None,
        "start_date": (datetime.date.today() - datetime.timedelta(days=10)).isoformat(),
    }
)
def backfill_bqetl_dag():
    param_validation_task = PythonOperator(
        task_id = "param_validation",
        python_callable = param_validation,
        op_kwargs = {"params": "{{ dag_run.conf }}"},
    )

    params = __parse_string_params("{{ dag_run.conf }}")

    backfill_task = bigquery_etl_backfill(
        destination_table = params.destination_table,
        dataset_id = params.dataset_id,
        start_date = params.start_date,
        end_date = params.end_date,
        project_id = params.project_id,
        excludes = params.excludes,
        dry_run = params.dry_run,
        parallelism = params.parallelism,
        no_partition = params.no_partition,
    )

    param_validation_task >> backfill_task


dag = backfill_bqetl_dag()
