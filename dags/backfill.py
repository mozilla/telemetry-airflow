import datetime
from enum import Enum

from airflow.decorators import dag
from airflow.models import DagModel
from airflow.models.param import Param
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule

from utils.backfill import BackfillParams
from utils.tags import Tag


class TaskId(Enum):
    dry_run = "dry_run"
    real_deal = "real_deal"
    clear_tasks = "clear_tasks"
    do_not_clear_tasks = "do_not_clear_tasks"


def dry_run_branch_callable(params: dict) -> str:
    backfill_params = BackfillParams(**params)
    return TaskId.dry_run.value if backfill_params.dry_run else TaskId.real_deal.value


def clear_branch_callable(params: dict) -> str:
    backfill_params = BackfillParams(**params)
    return TaskId.clear_tasks.value if backfill_params.clear else TaskId.do_not_clear_tasks.value


def param_validation(params: dict) -> bool:
    backfill_params = BackfillParams(**params)
    backfill_params.validate_date_range()
    validate_dag_exists(dag_name=backfill_params.dag_name)
    backfill_params.validate_regex_pattern()
    return True


def validate_dag_exists(dag_name: str) -> None:
    dag_instance = DagModel.get_dagmodel(dag_name)
    if dag_instance is None:
        raise ValueError(f"`dag_name`={dag_name} does not exist")


def generate_bash_command(params: dict) -> str:
    backfill_params = BackfillParams(**params)
    return " ".join(backfill_params.generate_backfill_command())


doc_md = """
# Backfill DAG

#### Use with caution 

#### Some tips/notes:

* Always use dry run first. Especially when using task regex
* Date formats are 2020-03-01 or 2020-03-01T00:00:00
* Dry run for clearing tasks will show you the list of tasks that will be cleared
* Dry run for backfilling will not show the list, but is useful in testing for input errors

"""


@dag(
    dag_id='backfill',
    schedule_interval=None,
    doc_md=doc_md,
    catchup=False,
    start_date=datetime.datetime(2022, 11, 1),
    dagrun_timeout=datetime.timedelta(days=1),
    tags=[Tag.ImpactTier.tier_3],
    render_template_as_native_obj=True,
    params={"dag_name": Param("dag_name", type="string"),
            "start_date": Param((datetime.date.today() - datetime.timedelta(days=10)).isoformat(),
                                type="string",
                                format="date-time"),
            "end_date": Param(datetime.date.today().isoformat(),
                              type="string",
                              format="date-time"),
            "clear": Param(False, type="boolean"),
            "dry_run": Param(True, type="boolean"),
            "task_regex": Param(None, type=["string", "null"]),
            }
)
def backfill_dag():
    param_validation_task = PythonOperator(
        task_id="param_validation",
        python_callable=param_validation,
        op_kwargs={"params": "{{ dag_run.conf }}"},
    )

    dry_run_branch_task = BranchPythonOperator(
        task_id="dry_run_parameter",
        python_callable=dry_run_branch_callable,
        op_kwargs={"params": "{{ dag_run.conf }}"},
        trigger_rule=TriggerRule.ONE_SUCCESS,
    )

    dry_run_task = EmptyOperator(task_id=TaskId.dry_run.value)
    real_deal_task = EmptyOperator(task_id=TaskId.real_deal.value)

    clear_branch_task = BranchPythonOperator(
        task_id="clear_parameter",
        python_callable=clear_branch_callable,
        op_kwargs={"params": "{{ dag_run.conf }}"},
        trigger_rule=TriggerRule.ONE_SUCCESS,
    )

    clear_tasks_task = EmptyOperator(task_id=TaskId.clear_tasks.value)
    do_not_clear_tasks_task = EmptyOperator(task_id=TaskId.do_not_clear_tasks.value)

    generate_backfill_command_task = PythonOperator(
        task_id="generate_backfill_command",
        python_callable=generate_bash_command,
        op_kwargs={"params": "{{ dag_run.conf }}"},
        trigger_rule=TriggerRule.ONE_SUCCESS,
    )

    backfill_task = BashOperator(
        task_id='execute_backfill',
        bash_command="{{ ti.xcom_pull(task_ids='generate_backfill_command') }}",
    )

    param_validation_task >> dry_run_branch_task >> [dry_run_task, real_deal_task] >> clear_branch_task >> [
        clear_tasks_task, do_not_clear_tasks_task] >> generate_backfill_command_task >> backfill_task


dag = backfill_dag()
