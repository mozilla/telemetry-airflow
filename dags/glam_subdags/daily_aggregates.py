from airflow import DAG

from utils.gcp import gke_command, bigquery_xcom_query

dags = {}


def generate_tasks_for_clients_daily(
    metric_type, agg_type, dataset_id, dag, arguments=()
):
    gen_query_task_id = "clients_daily_{}_aggregates_generate_query".format(agg_type)

    # setting xcom_push to True outputs this query to an xcom
    generate_query = gke_command(
        task_id=gen_query_task_id,
        command=[
            "python",
            "sql/telemetry_derived/clients_daily_{}_aggregates_v1.sql.py".format(
                metric_type
            ),
            "--agg-type",
            agg_type,
            "--json-output",
            "--wait-seconds",
            "15",
        ],
        docker_image="mozilla/bigquery-etl:latest",
        xcom_push=True,
        owner="msamuel@mozilla.com",
        email=[
            "telemetry-alerts@mozilla.com",
            "msamuel@mozilla.com",
            "robhudson@mozilla.com",
        ],
        dag=dags[dag],
    )

    run_query = bigquery_xcom_query(
        task_id="clients_daily_{}_aggregates_run_query".format(agg_type),
        destination_table="clients_daily_{}_aggregates_v1".format(metric_type),
        dataset_id=dataset_id,
        xcom_task_id=gen_query_task_id,
        owner="msamuel@mozilla.com",
        email=[
            "telemetry-alerts@mozilla.com",
            "msamuel@mozilla.com",
            "robhudson@mozilla.com",
        ],
        arguments=arguments,
        dag=dags[dag],
    )
    generate_query >> run_query
    return run_query


def daily_scalar_aggregates_subdag(
    parent_dag_name, child_dag_name, default_args, schedule_interval, dataset_id
):
    GLAM_DAILY_SCALAR_AGGREGATES_SUBDAG = "%s.%s" % (parent_dag_name, child_dag_name)
    dags[GLAM_DAILY_SCALAR_AGGREGATES_SUBDAG] = DAG(
        GLAM_DAILY_SCALAR_AGGREGATES_SUBDAG,
        default_args=default_args,
        schedule_interval=schedule_interval,
    )

    # This task runs first and replaces the relevant partition, followed
    # by the next two tasks that append to the same partition of the same table.
    clients_daily_scalar_aggregates = generate_tasks_for_clients_daily(
        "scalar", "scalars", dataset_id, GLAM_DAILY_SCALAR_AGGREGATES_SUBDAG,
    )
    clients_daily_keyed_scalar_aggregates = generate_tasks_for_clients_daily(
        "scalar",
        "keyed_scalars",
        dataset_id,
        GLAM_DAILY_SCALAR_AGGREGATES_SUBDAG,
        ("--append_table", "--noreplace",),
    )
    clients_daily_keyed_boolean_aggregates = generate_tasks_for_clients_daily(
        "scalar",
        "keyed_booleans",
        dataset_id,
        GLAM_DAILY_SCALAR_AGGREGATES_SUBDAG,
        ("--append_table", "--noreplace",),
    )

    clients_daily_scalar_aggregates >> clients_daily_keyed_scalar_aggregates
    clients_daily_scalar_aggregates >> clients_daily_keyed_boolean_aggregates

    return dags[GLAM_DAILY_SCALAR_AGGREGATES_SUBDAG]


def daily_histogram_aggregates_subdag(
    parent_dag_name, child_dag_name, default_args, schedule_interval, dataset_id
):
    GLAM_DAILY_HISTOGRAM_AGGREGATES_SUBDAG = "%s.%s" % (parent_dag_name, child_dag_name)
    dags[GLAM_DAILY_HISTOGRAM_AGGREGATES_SUBDAG] = DAG(
        GLAM_DAILY_HISTOGRAM_AGGREGATES_SUBDAG,
        default_args=default_args,
        schedule_interval=schedule_interval,
    )

    # This task runs first and replaces the relevant partition, followed
    # by the next task below that appends to the same partition of the same table.
    clients_daily_histogram_aggregates = generate_tasks_for_clients_daily(
        "histogram", "histograms", dataset_id, GLAM_DAILY_HISTOGRAM_AGGREGATES_SUBDAG
    )
    clients_daily_keyed_histogram_aggregates = generate_tasks_for_clients_daily(
        "histogram",
        "keyed_histograms",
        dataset_id,
        GLAM_DAILY_HISTOGRAM_AGGREGATES_SUBDAG,
        ("--append_table", "--noreplace",),
    )

    clients_daily_histogram_aggregates >> clients_daily_keyed_histogram_aggregates

    return dags[GLAM_DAILY_HISTOGRAM_AGGREGATES_SUBDAG]
