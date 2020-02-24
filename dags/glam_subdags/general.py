from airflow.models import DAG

from utils.gcp import bigquery_etl_query


def repeated_subdag(
    parent_dag_name, child_dag_name, default_args, schedule_interval, dataset_id, sql_file_path=None
):
    dag = DAG(
        "%s.%s" % (parent_dag_name, child_dag_name),
        default_args=default_args,
        schedule_interval=schedule_interval,
    )

    NUM_PARTITIONS = 4
    NUM_SAMPLE_IDS = 100
    PARTITION_SIZE = NUM_SAMPLE_IDS / NUM_PARTITIONS
    task_0 = bigquery_etl_query(
        task_id="{dag_name}_0".format(dag_name=child_dag_name),
        destination_table="{dag_name}_v1".format(dag_name=child_dag_name),
        sql_file_path=sql_file_path,
        dataset_id=dataset_id,
        project_id="moz-fx-data-shared-prod",
        owner="msamuel@mozilla.com",
        email=["telemetry-alerts@mozilla.com", "msamuel@mozilla.com"],
        depends_on_past=True,
        date_partition_parameter=None,
        parameters=(
            "min_sample_id:INT64:0",
            "max_sample_id:INT64:{}".format(PARTITION_SIZE - 1),
        ),
        arguments=("--replace",),
        dag=dag,
    )

    for partition in range(1, NUM_PARTITIONS):
        min_param = partition * PARTITION_SIZE
        max_param = min_param + PARTITION_SIZE - 1

        task = bigquery_etl_query(
            task_id="{}_{}".format(child_dag_name, partition),
            destination_table="{dag_name}_v1".format(dag_name=child_dag_name),
            sql_file_path=sql_file_path,
            dataset_id=dataset_id,
            project_id="moz-fx-data-shared-prod",
            owner="msamuel@mozilla.com",
            email=["telemetry-alerts@mozilla.com", "msamuel@mozilla.com"],
            depends_on_past=True,
            date_partition_parameter=None,
            parameters=(
                "min_sample_id:INT64:{}".format(min_param),
                "max_sample_id:INT64:{}".format(max_param),
            ),
            arguments=("--append_table", "--noreplace",),
            dag=dag,
        )
        task_0 >> task

    return dag
