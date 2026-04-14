from airflow.models import DagBag
from airflow.operators.empty import EmptyOperator

from utils.tags import Tag


def test_dag_validity(get_dag_bag):
    """
    Test all DAGs can be parsed.

    This test should be equivalent to the integration test using airflow CLI.
    At the moment, there is a discrepancy between this unit test and the integration
    test. Once equivalent, this unit test should replace to the integration test.

    """
    dagbag = get_dag_bag

    data = []
    for filename, errors in dagbag.import_errors.items():
        data.append({"filepath": filename, "error": errors})
    if data:
        print(data)
        raise AssertionError


def test_dag_tags(get_dag_bag):
    """Check tags in all DAGs are valid."""

    valid_tags = {
        "impact/tier_1",
        "impact/tier_2",
        "impact/tier_3",
        "repo/bigquery-etl",
        "repo/telemetry-airflow",
        "repo/private-bigquery-etl",
        "triage/confidential",
        "triage/no_triage",
        "triage/record_only",
    }
    dagbag = get_dag_bag

    for dag_name, dag in dagbag.dags.items():
        for tag in dag.tags:
            assert tag in valid_tags, f"DAG: {dag_name}: Invalid tag `{tag}`"


def test_dag_tags_required(get_dag_bag):
    """Check at least one tag per DAG is of the required type."""

    required_tag_type = "impact"
    dagbag = get_dag_bag

    for dag_name, dag in dagbag.dags.items():
        # don't check tags on subdags
        if dag.is_subdag:
            continue

        assert [
            tag for tag in dag.tags if required_tag_type in tag
        ], f"DAG: {dag_name}: Missing required tag type `{required_tag_type}`"


def test_telemetry_alerts_email(get_dag_bag: DagBag):
    """Check that telemetry-alerts@mozilla.com is being emailed unless the DAG has a `no_triage` tag."""
    telemetry_alerts_email = "telemetry-alerts@mozilla.com"

    for dag_name, dag in get_dag_bag.dags.items():
        if Tag.Triage.no_triage in dag.tags:
            continue

        default_email = dag.default_args.get("email", [])
        if default_email:
            assert telemetry_alerts_email in default_email, (
                f'DAG {dag_name}: either {telemetry_alerts_email} should be included in `default_args["email"]`,'
                f' or a "{Tag.Triage.no_triage}" tag should be added to the DAG.'
            )

        for task in dag.tasks:
            if isinstance(task, EmptyOperator):
                continue

            if task.email:
                assert telemetry_alerts_email in task.email, (
                    f"DAG {dag_name} task {task.task_id}: either {telemetry_alerts_email} should be included in `email`,"
                    f' or a "{Tag.Triage.no_triage}" tag should be added to the DAG.'
                )
            else:
                assert default_email != [], (
                    f'DAG {dag_name} task {task.task_id}: either `email` or `default_args["email"]` should be specified and include {telemetry_alerts_email},'
                    f' or a "{Tag.Triage.no_triage}" tag should be added to the DAG.'
                )
