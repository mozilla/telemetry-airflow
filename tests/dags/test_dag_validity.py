def test_dag_validity(get_dag_bag):
    """Test all DAGs can be parsed.

    This test should be equivalent to the integration test using airflow CLI.
    At the moment, there is a discrepancy between this unit test and the integration
    test. Once equivalent, this unit test should replace to the integration test.

    """
    dagbag = get_dag_bag

    data = []
    for filename, errors in dagbag.import_errors.items():
        # TODO investigate why this is the only `conn_id` causing an error
        # TODO glam.py is full of subdag abstractions, investigate why this causes
        # database error
        if ("The conn_id `google_cloud_airflow_dataproc` isn't defined" in errors) or \
                ("sqlite3.OperationalError: no such table: slot_pool" in errors):
            continue
        data.append({"filepath": filename, "error": errors})
    if data:
        print(data)
        raise AssertionError


def test_dag_tags(get_dag_bag):
    """Check tags in all DAGs are valid."""

    valid_tags = {
        "impact/tier_1", "impact/tier_2", "impact/tier_3", "repo/bigquery-etl",
        "repo/telemetry-airflow", "repo/private-bigquery-etl"}
    dagbag = get_dag_bag

    for dag_name, dag in dagbag.dags.items():
        for tag in dag.tags:
            assert tag in valid_tags, f"DAG: {dag_name}: Invalid tag `{tag}`"


def test_dag_tags_required(get_dag_bag):
    """Check at least one tag per DAG is of the required type"""

    required_tag_type = "impact"
    dagbag = get_dag_bag

    for dag_name, dag in dagbag.dags.items():
        # don't check tags on subdags
        if dag.is_subdag:
            continue

        assert [tag for tag in dag.tags if
                required_tag_type in tag], f"DAG: {dag_name}: Missing required tag " \
                                           f"type `{required_tag_type}`"
