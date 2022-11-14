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

    This test can be removed once we chose between the backfill plugin and the backfill DAG

    """
    # append plugins directory to system path
    # we should fix inconsistent path between deployment and test
    from tests.conftest import PROJECT_DIR
    import sys
    sys.path.append(str(PROJECT_DIR / "plugins"))

    from plugins.backfill.main import generate_backfill_command

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

    for params in test_params:
        plugin_command = generate_backfill_command(clear=str(params.clear).lower(),
                                                   dry_run=str(params.dry_run).lower(),
                                                   use_task_regex=str(params.task_regex is not None).lower(),
                                                   task_regex=params.task_regex,
                                                   dag_name=params.dag_name,
                                                   start_date=params.start_date,
                                                   end_date=params.end_date)
        backfill_command = params.generate_backfill_command()
        assert plugin_command == backfill_command
