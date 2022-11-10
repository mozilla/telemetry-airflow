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
    # TBD validate command outputs
    assert True
