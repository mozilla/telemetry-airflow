from airflow import DAG
from airflow.operators.moz_databricks import MozDatabricksSubmitRunOperator
from airflow.operators.subdag_operator import SubDagOperator
from datetime import datetime, timedelta
from utils.gcp import load_to_bigquery
from utils.tbv import tbv_envvar

default_args = {
    'owner': 'wlachance@mozilla.com',
    'depends_on_past': False,
    'start_date': datetime(2018, 11, 20),
    'email': ['telemetry-alerts@mozilla.com', 'wlachance@mozilla.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 3,
    'retry_delay': timedelta(minutes=30),
}

dag = DAG('crash_summary', default_args=default_args, schedule_interval='@daily')

# we deliberately do not autoscale this job, as it seems that the bottleneck is not
# the CPU
crash_summary_view = MozDatabricksSubmitRunOperator(
    task_id="crash_summary_view",
    job_name="Crash Summary View",
    dev_instance_count=1,
    instance_count=1,
    instance_type="c4.4xlarge",
    ebs_volume_count=1,
    ebs_volume_size=250,
    execution_timeout=timedelta(hours=4),
    env = tbv_envvar("com.mozilla.telemetry.views.CrashSummaryView", {
        "from": "{{ ds_nodash }}",
        "to": "{{ ds_nodash }}",
        "outputBucket": "{{ task.__class__.private_output_bucket }}"}),
    uri="https://raw.githubusercontent.com/mozilla/telemetry-airflow/master/jobs/telemetry_batch_view.py",
    dag=dag)

crash_summary_view_bigquery_load = SubDagOperator(
    subdag=load_to_bigquery(
        parent_dag_name=dag.dag_id,
        dag_name="crash_summary_view_bigquery_load",
        default_args=default_args,
        dataset_s3_bucket="telemetry-parquet",
        ds_type="ds",
        aws_conn_id="aws_dev_iam_s3",
        dataset="crash_summary",
        dataset_version="v2",
        date_submission_col="submission_date",
        gke_cluster_name="bq-load-gke-1",
        bigquery_dataset="telemetry_raw",
        ),
    task_id="crash_summary_view_bigquery_load",
    dag=dag)

crash_summary_view >> crash_summary_view_bigquery_load
