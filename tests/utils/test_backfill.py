import pytest

from dags.utils.backfill import AirflowBackfillParams, BigQueryETLBackfillParams


@pytest.fixture()
def base_params() -> dict:
    return {
        "clear": False,
        "dry_run": True,
        "dag_name": "dag_name",
        "end_date": "2022-11-10",
        "start_date": "2022-10-31",
        "task_regex": None,
    }


@pytest.fixture()
def base_bqetl_params() -> dict:
    return {
        "dataset_id": "google_ads_derived",
        "destination_table": "campaign_conversions_by_date_v1",
        "dry_run": False,
        "end_date": "2023-02-28",
        "excludes": None,
        "no_partition": False,
        "parallelism": 1,
        "project_id": "moz-fx-data-shared-prod",
        "start_date": "2023-02-27",
    }


@pytest.fixture()
def base_backfill_params(base_params: dict) -> AirflowBackfillParams:
    return AirflowBackfillParams(**base_params)


def test_date_validation(base_backfill_params) -> None:
    # valid date range
    base_backfill_params.validate_date_range()

    # invalid date range
    base_backfill_params.start_date, base_backfill_params.end_date = (
        base_backfill_params.end_date,
        base_backfill_params.start_date,
    )
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
    """
    Assert backfill commands are equivalent between the backfill plugin and backfill DAG.

    Expected results were generated from the plugin implementation

    """
    test_start_date = "2022-01-01"
    test_end_date = "2022-01-10"

    test_params: list[AirflowBackfillParams] = [
        AirflowBackfillParams(
            clear=True,
            dry_run=True,
            task_regex=None,
            dag_name="test_value",
            start_date=test_start_date,
            end_date=test_end_date,
        ),
        AirflowBackfillParams(
            clear=False,
            dry_run=True,
            task_regex=None,
            dag_name="test_value",
            start_date=test_start_date,
            end_date=test_end_date,
        ),
        AirflowBackfillParams(
            clear=True,
            dry_run=False,
            task_regex=None,
            dag_name="test_value",
            start_date=test_start_date,
            end_date=test_end_date,
        ),
        AirflowBackfillParams(
            clear=False,
            dry_run=False,
            task_regex=None,
            dag_name="test_value",
            start_date=test_start_date,
            end_date=test_end_date,
        ),
        AirflowBackfillParams(
            clear=False,
            dry_run=False,
            task_regex="/ab+c/",
            dag_name="test_value",
            start_date=test_start_date,
            end_date=test_end_date,
        ),
    ]

    expected_results = [
        [
            "timeout",
            "60",
            "airflow",
            "tasks",
            "clear",
            "-s",
            "2022-01-01",
            "-e",
            "2022-01-10",
            "test_value",
        ],
        [
            "airflow",
            "dags",
            "backfill",
            "--donot-pickle",
            "--dry-run",
            "-s",
            "2022-01-01",
            "-e",
            "2022-01-10",
            "test_value",
        ],
        [
            "airflow",
            "tasks",
            "clear",
            "-y",
            "-s",
            "2022-01-01",
            "-e",
            "2022-01-10",
            "test_value",
        ],
        [
            "airflow",
            "dags",
            "backfill",
            "--donot-pickle",
            "-s",
            "2022-01-01",
            "-e",
            "2022-01-10",
            "test_value",
        ],
        [
            "airflow",
            "dags",
            "backfill",
            "--donot-pickle",
            "-t",
            "/ab+c/",
            "-s",
            "2022-01-01",
            "-e",
            "2022-01-10",
            "test_value",
        ],
    ]

    for params, result in zip(test_params, expected_results):
        backfill_command = params.generate_backfill_command()
        assert backfill_command == result


def test_generate_bqetl_backfill_command(base_bqetl_params: dict) -> None:
    """Assert Big Query ETL backfill commands are generated as expected."""
    test_params: list[BigQueryETLBackfillParams] = [
        BigQueryETLBackfillParams(**base_bqetl_params),
        BigQueryETLBackfillParams(
            start_date="2023-02-22",
            destination_table="campaign_conversions_by_date_v1",
            dataset_id="google_ads_derived",
            project_id="moz-fx-data-shared-prod",
            dry_run=True,
            no_partition=True,
            end_date="2023-02-28",
            excludes=["2023-02-23", "2023-02-24", "2023-02-26"],
            parallelism=4,
        ),
    ]

    expected_results = [
        [
            "script/bqetl",
            "query",
            "backfill",
            "google_ads_derived" ".campaign_conversions_by_date_v1",
            "--start-date=2023-02-27",
            "--end-date=2023-02-28",
            "--parallelism=1",
        ],
        [
            "script/bqetl",
            "query",
            "backfill",
            "google_ads_derived.campaign_conversions_by_date_v1",
            "--start-date=2023-02-22",
            "--end-date=2023-02-28",
            "--exclude=2023-02-23,2023-02-24,2023-02-26",
            "--dry-run",
            "--parallelism=4",
            "--no-partition",
        ],
    ]

    for params, result in zip(test_params, expected_results):
        backfill_command = params.generate_backfill_command()
        assert backfill_command == result
