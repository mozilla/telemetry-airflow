from airflow import DAG
from datetime import datetime, timedelta
from operators.emr_spark_operator import EMRSparkOperator
from airflow.operators.subdag_operator import SubDagOperator
from utils.gcp import load_to_bigquery
from utils.tbv import tbv_envvar

default_args = {
    'owner': 'amiyaguchi@mozilla.com',
    'depends_on_past': False,
    'start_date': datetime(2018, 11, 20),
    'email': ['telemetry-alerts@mozilla.com', 'amiyaguchi@mozilla.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=30),
}

# Make sure all the data for the given day has arrived before running.
# Running at 1am should suffice.
dag = DAG('first_shutdown_summary', default_args=default_args, schedule_interval='0 1 * * *')

first_shutdown_summary = EMRSparkOperator(
    task_id="first_shutdown_summary",
    job_name="First Shutdown Summary View",
    execution_timeout=timedelta(hours=1),
    instance_count=1,
    env=tbv_envvar("com.mozilla.telemetry.views.MainSummaryView", {
        "from": "{{ ds_nodash }}",
        "to": "{{ ds_nodash }}",
        "bucket": "{{ task.__class__.private_output_bucket }}",
        "doc-type": "first_shutdown",
        "read-mode": "aligned",
        "input-partition-multiplier": "4"
    }),
    uri="https://raw.githubusercontent.com/mozilla/telemetry-airflow/master/jobs/telemetry_batch_view.py",
    dag=dag)

first_shutdown_summary_bigquery_load = SubDagOperator(
    subdag=load_to_bigquery(
        parent_dag_name=dag.dag_id,
        dag_name="first_shutdown_summary_bigquery_load",
        default_args=default_args,
        dataset_s3_bucket="telemetry-parquet",
        aws_conn_id="aws_dev_iam_s3",
        dataset="first_shutdown_summary",
        dataset_version="v4",
        gke_cluster_name="bq-load-gke-1",
        ),
    task_id="first_shutdown_summary_bigquery_load",
    dag=dag)

first_shutdown_summary >> first_shutdown_summary_bigquery_load
