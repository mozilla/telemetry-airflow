# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import sys
from datetime import datetime, timedelta

import pytest

from airflow import configuration
from airflow.exceptions import AirflowException
from airflow.models import DAG, DagRun
from airflow.models import TaskInstance as TI
from airflow.operators.python_operator import PythonOperator
from airflow.settings import Session
from airflow.utils.state import State

# fix relative path errors -- ordering is important in this section
import plugins.statuspage.operator

sys.modules["airflow.operators.dataset_status"] = plugins.statuspage.operator
from dags.utils.status import register_status


DEFAULT_DATE = datetime(2016, 1, 1)
END_DATE = datetime(2016, 1, 2)
INTERVAL = timedelta(hours=12)


def clear_session():
    """Manage airflow database state for tests"""
    session = Session()
    session.query(DagRun).delete()
    session.query(TI).delete()
    session.commit()
    session.close()


@pytest.fixture
def dag(mocker):
    clear_session()
    configuration.load_test_config()
    dag = DAG(
        "test_dag",
        default_args=dict(owner="airflow", start_date=DEFAULT_DATE),
        schedule_interval=INTERVAL,
    )
    yield dag
    dag.clear()
    clear_session()


@pytest.fixture
def mock_hook(mocker):
    return mocker.patch("plugins.statuspage.operator.DatasetStatusHook")


def test_subdag_structure(dag, mock_hook):
    operator = PythonOperator(task_id="test", python_callable=lambda: True, dag=dag)
    _ = register_status(operator, "test", "test description")
    assert {ti.task_id for ti in dag.topological_sort()} == {
        "test",
        "test_success",
        "test_failure",
    }


def test_execute_success(dag, mock_hook):
    testing_component_id = 42

    mock_conn = mock_hook.return_value.get_conn()
    mock_conn.get_or_create.return_value = testing_component_id

    success_op = PythonOperator(task_id="test", python_callable=lambda: True, dag=dag)
    _ = register_status(success_op, "Test Success", "Testing operational status")

    # run each of the operators in order
    for operator in dag.topological_sort():
        operator.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

    # get the dag state
    dagrun = dag.create_dagrun(
        run_id="manual__",
        start_date=datetime.utcnow(),
        execution_date=DEFAULT_DATE,
        state=State.RUNNING,
    )

    expected = {
        "test": State.SUCCESS,
        "test_success": State.SUCCESS,
        "test_failure": State.NONE,
    }

    assert all([expected[ti.task_id] == ti.state for ti in dagrun.get_task_instances()])
    mock_conn.update.assert_called_once_with(testing_component_id, "operational")


def test_execute_failure(dag, mock_hook):
    testing_component_id = 42

    mock_conn = mock_hook.return_value.get_conn()
    mock_conn.get_or_create.return_value = testing_component_id

    def exception():
        raise Exception()

    failure_op = PythonOperator(task_id="test", python_callable=exception, dag=dag)
    _ = register_status(failure_op, "Test Failure", "Testing partial_outage status")

    # run each of the operators in order
    for operator in dag.topological_sort():
        try:
            operator.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)
        except Exception:
            continue

    # get the dag state
    dagrun = dag.create_dagrun(
        run_id="manual__",
        start_date=datetime.utcnow(),
        execution_date=DEFAULT_DATE,
        state=State.RUNNING,
    )

    expected = {
        "test": State.FAILED,
        "test_success": State.NONE,
        "test_failure": State.SUCCESS,
    }

    assert all([expected[ti.task_id] == ti.state for ti in dagrun.get_task_instances()])
    mock_conn.update.assert_called_once_with(testing_component_id, "partial_outage")


def test_no_execution_on_retry(dag, mock_hook):
    operator = PythonOperator(task_id="test", python_callable=lambda: True, dag=dag)
    _ = register_status(operator, "test", "test no trigger on retry")

    # get the dag state
    dagrun = dag.create_dagrun(
        run_id="manual__",
        start_date=DEFAULT_DATE,
        execution_date=DEFAULT_DATE,
        state=State.RUNNING,
    )

    dagrun.get_task_instance("test").set_state(State.UP_FOR_RETRY, Session())

    # run the test operator dependencies
    for dep in operator.get_direct_relatives():
        dep.run(start_date=DEFAULT_DATE, end_date=END_DATE)

    expected = {
        "test": State.UP_FOR_RETRY,
        "test_success": State.NONE,
        "test_failure": State.NONE,
    }

    assert all([expected[ti.task_id] == ti.state for ti in dagrun.get_task_instances()])
    assert not mock_hook.return_value.get_conn().update.called
