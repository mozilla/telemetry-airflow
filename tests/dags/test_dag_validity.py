from airflow.models import DagBag
from airflow.operators.empty import EmptyOperator
from airflow.sensors.external_task import ExternalTaskMarker, ExternalTaskSensor

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
        "impact/tier_0",
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


def test_external_task_sensors_have_markers(get_dag_bag: DagBag):
    """
    Check that every ExternalTaskSensor has a matching ExternalTaskMarker in the upstream DAG.

    The marker must point at the sensor task and be downstream of the task(s) the sensor
    waits on, so that clearing those tasks also clears the dependent sensor.

    Sensors waiting on their own DAG (e.g. the previous day's run) are skipped, as are
    sensors waiting on DAGs not defined in this repo (e.g. bigquery-etl generated DAGs),
    since markers for those have to be added in the repo that generates the upstream DAG.
    """
    dags = get_dag_bag.dags
    errors = []

    for dag_id, dag in dags.items():
        for sensor in dag.tasks:
            if not isinstance(sensor, ExternalTaskSensor):
                continue

            upstream_dag = dags.get(sensor.external_dag_id)
            if sensor.external_dag_id == dag_id or upstream_dag is None:
                continue

            if sensor.external_task_group_id:
                task_group = upstream_dag.task_group_dict.get(
                    sensor.external_task_group_id
                )
                sensed_task_ids = (
                    {task.task_id for task in task_group.get_leaves()}
                    if task_group
                    else set()
                )
            else:
                sensed_task_ids = set(sensor.external_task_ids or [])

            markers = [
                task
                for task in upstream_dag.tasks
                if isinstance(task, ExternalTaskMarker)
                and task.external_dag_id == dag_id
                and task.external_task_id == sensor.task_id
            ]
            if not markers:
                errors.append(
                    f"DAG {dag_id} task {sensor.task_id}: no ExternalTaskMarker in DAG"
                    f" {upstream_dag.dag_id} with external_dag_id={dag_id!r} and"
                    f" external_task_id={sensor.task_id!r}."
                )
                continue

            if not any(
                sensed_task_ids <= marker.get_flat_relative_ids(upstream=True)
                for marker in markers
            ):
                errors.append(
                    f"DAG {dag_id} task {sensor.task_id}: ExternalTaskMarker"
                    f" {[marker.task_id for marker in markers]} in DAG {upstream_dag.dag_id}"
                    f" should be downstream of {sorted(sensed_task_ids)}."
                )

    assert not errors, "\n".join(errors)


def test_external_task_markers_have_sensors(get_dag_bag: DagBag):
    """
    Check that every ExternalTaskMarker targeting a DAG in this repo points at an ExternalTaskSensor.

    Catches markers left behind after a sensor is renamed or removed, which would otherwise
    silently stop clearing the downstream DAG.
    """
    dags = get_dag_bag.dags
    errors = []

    for dag_id, dag in dags.items():
        for marker in dag.tasks:
            if not isinstance(marker, ExternalTaskMarker):
                continue

            downstream_dag = dags.get(marker.external_dag_id)
            if downstream_dag is None:
                continue

            sensor = downstream_dag.task_dict.get(marker.external_task_id)
            if not isinstance(sensor, ExternalTaskSensor):
                errors.append(
                    f"DAG {dag_id} task {marker.task_id}: external_task_id"
                    f" {marker.external_task_id!r} is not an ExternalTaskSensor in DAG"
                    f" {marker.external_dag_id}."
                )
            elif sensor.external_dag_id != dag_id:
                errors.append(
                    f"DAG {dag_id} task {marker.task_id}: sensor"
                    f" {marker.external_dag_id}.{marker.external_task_id} waits on DAG"
                    f" {sensor.external_dag_id}, not {dag_id}."
                )

    assert not errors, "\n".join(errors)
