from typing import List

import pytest

from dags.utils.backfill import BackfillParams


@pytest.fixture(scope="function")
def base_params() -> dict:
    return {
        'clear': False,
        'dry_run': True,
        'dag_name': 'dag_name',
        'end_date': '2022-11-10',
        'start_date': '2022-10-31',
        'task_regex': None,
    }


@pytest.fixture(scope="function")
def base_backfill_params(base_params: dict) -> BackfillParams:
    return BackfillParams(**base_params)


def test_date_validation(base_backfill_params) -> None:
    # valid date range
    base_backfill_params.validate_date_range()

    # invalid date range
    base_backfill_params.start_date, base_backfill_params.end_date = base_backfill_params.end_date, base_backfill_params.start_date
    with pytest.raises(ValueError):
        base_backfill_params.validate_date_range()


def test_validate_regex_pattern(base_backfill_params) -> None:
    # task_regex is None
    base_backfill_params.validate_regex_pattern()

    # valid regex pattern
    base_backfill_params.task_regex = "/ab+c/"
    base_backfill_params.validate_regex_pattern()

    # invalid regex pattern
    base_backfill_params.task_regex = "[.*"
    with pytest.raises(ValueError):
        base_backfill_params.validate_regex_pattern()


def test_generate_backfill_command(base_backfill_params) -> None:
    """Assert backfill commands are equivalent between the backfill plugin and backfill DAG

    Expected results were generated from the plugin implementation

    """
    test_start_date = "2022-01-01"
    test_end_date = "2022-01-10"

    test_params: List[BackfillParams] = [
        BackfillParams(clear=True, dry_run=True, task_regex=None, dag_name="test_value", start_date=test_start_date,
                       end_date=test_end_date),
        BackfillParams(clear=False, dry_run=True, task_regex=None, dag_name="test_value", start_date=test_start_date,
                       end_date=test_end_date),
        BackfillParams(clear=True, dry_run=False, task_regex=None, dag_name="test_value", start_date=test_start_date,
                       end_date=test_end_date),
        BackfillParams(clear=False, dry_run=False, task_regex=None, dag_name="test_value", start_date=test_start_date,
                       end_date=test_end_date),
        BackfillParams(clear=False, dry_run=False, task_regex="/ab+c/", dag_name="test_value",
                       start_date=test_start_date,
                       end_date=test_end_date),
    ]

    expected_results = [
        [
            'timeout', '60', 'airflow', 'tasks', 'clear', '-s', '2022-01-01', '-e', '2022-01-10', 'test_value'],
        [
            'airflow', 'dags', 'backfill', '--donot-pickle', '--dry-run', '-s', '2022-01-01', '-e', '2022-01-10',
            'test_value'],
        [
            'airflow', 'tasks', 'clear', '-y', '-s', '2022-01-01', '-e', '2022-01-10', 'test_value'],
        [
            'airflow', 'dags', 'backfill', '--donot-pickle', '-s', '2022-01-01', '-e', '2022-01-10', 'test_value'],
        [
            'airflow', 'dags', 'backfill', '--donot-pickle', '-t', '/ab+c/', '-s', '2022-01-01', '-e', '2022-01-10',
            'test_value']
    ]

    for params, result in zip(test_params, expected_results):
        backfill_command = params.generate_backfill_command()
        assert backfill_command == result
