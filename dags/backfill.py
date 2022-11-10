import ast
import datetime
from typing import Optional

from airflow.decorators import dag
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.models import DagModel


from utils.backfill import BackfillParams


def __parse_string_params(string_params: str) -> Optional[BackfillParams]:
    """
    dag_run.conf is string representation of a Python dictionary in Airflow 2.1
    ast.literal_eval() is used to convert from string to dictionary as a workaround
    this workaround will no longer be required in Airflow >=2.2, see link below for future implementation
    https://airflow.apache.org/docs/apache-airflow/stable/concepts/params.html
    """
    params_parsed = ast.literal_eval(string_params)
    return BackfillParams(**params_parsed)


def dry_run_branch_callable(params: str) -> str:
    backfill_params = __parse_string_params(params)
    return "dry_run" if backfill_params.dry_run else "real_deal"


def clear_branch_callable(params: str) -> str:
    backfill_params = __parse_string_params(params)
    return "clear_tasks" if backfill_params.clear else "do_not_clear_tasks"


def param_validation(params: str) -> bool:
    backfill_params = __parse_string_params(params)
    backfill_params.validate_date_range()
    validate_dag_exists(dag_name=backfill_params.dag_name)
    backfill_params.validate_regex_pattern()
    return True


def validate_dag_exists(dag_name: str) -> None:
    dag_instance = DagModel.get_dagmodel(dag_name)
    if dag_instance is None:
        raise ValueError(f"`dag_name`={dag_name} does not exist")


def generate_bash_command(params: str) -> str:
    backfill_params = __parse_string_params(params)
    return " ".join(backfill_params.generate_backfill_command())


doc_md = """
# Backfill (Alpha)

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
    params={"dag_name": "dag_name",
            "start_date": (datetime.date.today() - datetime.timedelta(days=10)).isoformat(),
            "end_date": datetime.date.today().isoformat(),
            "clear": False,
            "dry_run": True,
            "task_regex": None,
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

    dry_run_task = DummyOperator(task_id="dry_run")
    real_deal_task = DummyOperator(task_id="real_deal")

    clear_branch_task = BranchPythonOperator(
        task_id="clear_parameter",
        python_callable=clear_branch_callable,
        op_kwargs={"params": "{{ dag_run.conf }}"},
        trigger_rule=TriggerRule.ONE_SUCCESS,
    )

    clear_tasks_task = DummyOperator(task_id="clear_tasks")
    do_not_clear_tasks_task = DummyOperator(task_id="do_not_clear_tasks")

    generate_backfill_command_task = PythonOperator(
        task_id="generate_backfill_command",
        python_callable=generate_bash_command,
        op_kwargs={"params": "{{ dag_run.conf }}"},
        trigger_rule=TriggerRule.ONE_SUCCESS,
    )

    backfill_task = BashOperator(
        task_id='execute_backfill',
        # bash_command="echo 1",
        bash_command="{{ ti.xcom_pull(task_ids='generate_backfill_command') }}",
    )

    param_validation_task >> dry_run_branch_task >> [dry_run_task, real_deal_task] >> clear_branch_task >> [
        clear_tasks_task, do_not_clear_tasks_task] >> generate_backfill_command_task >> backfill_task


dag = backfill_dag()
