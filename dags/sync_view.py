from airflow import DAG
from airflow.operators.subdag_operator import SubDagOperator
from datetime import datetime, timedelta
from operators.emr_spark_operator import EMRSparkOperator
from utils.mozetl import mozetl_envvar
from utils.gcp import load_to_bigquery
from utils.tbv import tbv_envvar


default_args = {
    'owner': 'markh@mozilla.com',
    'depends_on_past': False,
    'start_date': datetime(2018, 11, 26),
    'email': ['telemetry-alerts@mozilla.com', 'markh@mozilla.com', 'tchiovoloni@mozilla.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=30),
}

dag = DAG('sync_view', default_args=default_args, schedule_interval='@daily')

sync_view = EMRSparkOperator(
    task_id="sync_view",
    job_name="Sync Pings View",
    execution_timeout=timedelta(hours=10),
    instance_count=5,
    env = tbv_envvar("com.mozilla.telemetry.views.SyncView", {
        "from": "{{ ds_nodash }}",
        "to": "{{ ds_nodash }}",
        "bucket": "{{ task.__class__.private_output_bucket }}"}),
    uri="https://raw.githubusercontent.com/mozilla/telemetry-airflow/master/jobs/telemetry_batch_view.py",
    dag=dag)

sync_view_bigquery_load = SubDagOperator(
    subdag=load_to_bigquery(
        parent_dag_name=dag.dag_id,
        dag_name="sync_view_bigquery_load",
        default_args=default_args,
        dataset_s3_bucket="telemetry-parquet",
        aws_conn_id="aws_dev_iam_s3",
        dataset="sync_summary",
        dataset_version="v2",
        gke_cluster_name="bq-load-gke-1",
        bigquery_dataset="telemetry_raw",
        ),
    task_id="sync_view_bigquery_load",
    dag=dag)

sync_events_view = EMRSparkOperator(
    task_id="sync_events_view",
    job_name="Sync Events View",
    execution_timeout=timedelta(hours=10),
    instance_count=1,
    email=['ssuh@mozilla.com'],
    env = tbv_envvar("com.mozilla.telemetry.views.SyncEventView", {
        "from": "{{ ds_nodash }}",
        "to": "{{ ds_nodash }}",
        "bucket": "{{ task.__class__.private_output_bucket }}"}),
    uri="https://raw.githubusercontent.com/mozilla/telemetry-airflow/master/jobs/telemetry_batch_view.py",
    dag=dag)

sync_events_view_bigquery_load = SubDagOperator(
    subdag=load_to_bigquery(
        parent_dag_name=dag.dag_id,
        dag_name="sync_events_view_bigquery_load",
        default_args=default_args,
        dataset_s3_bucket="telemetry-parquet",
        aws_conn_id="aws_dev_iam_s3",
        dataset="sync_events",
        dataset_version="v1",
        gke_cluster_name="bq-load-gke-1",
        bigquery_dataset="telemetry_raw",
        ),
    task_id="sync_events_view_bigquery_load",
    dag=dag)

sync_flat_view = EMRSparkOperator(
    task_id="sync_flat_view",
    job_name="Flattened Sync Pings View",
    execution_timeout=timedelta(hours=10),
    instance_count=5,
    env = tbv_envvar("com.mozilla.telemetry.views.SyncFlatView", {
        "from": "{{ ds_nodash }}",
        "to": "{{ ds_nodash }}",
        "bucket": "{{ task.__class__.private_output_bucket }}"}),
    uri="https://raw.githubusercontent.com/mozilla/telemetry-airflow/master/jobs/telemetry_batch_view.py",
    dag=dag)

sync_flat_view_bigquery_load = SubDagOperator(
    subdag=load_to_bigquery(
        parent_dag_name=dag.dag_id,
        dag_name="sync_flat_view_bigquery_load",
        default_args=default_args,
        dataset_s3_bucket="telemetry-parquet",
        aws_conn_id="aws_dev_iam_s3",
        dataset="sync_flat_summary",
        dataset_version="v1",
        gke_cluster_name="bq-load-gke-1",
        bigquery_dataset="telemetry_raw",
        ),
    task_id="sync_flat_view_bigquery_load",
    dag=dag)

sync_bookmark_validation = EMRSparkOperator(
    task_id="sync_bookmark_validation",
    job_name="Sync Bookmark Validation",
    execution_timeout=timedelta(hours=2),
    instance_count=1,
    email=["telemetry-alerts@mozilla.com", "amiyaguchi@mozilla.com"],
    env=mozetl_envvar("sync_bookmark_validation", {
        "start_date": "{{ ds_nodash }}",
        "bucket": "{{ task.__class__.private_output_bucket }}",
    }),
    uri="https://raw.githubusercontent.com/mozilla/python_mozetl/master/bin/mozetl-submit.sh",
    dag=dag)


sync_bookmark_validation.set_upstream(sync_view)

sync_view_bigquery_load.set_upstream(sync_view)

sync_events_view_bigquery_load.set_upstream(sync_events_view)

sync_flat_view_bigquery_load.set_upstream(sync_flat_view)
