from airflow import DAG
from airflow.operators.subdag_operator import SubDagOperator
from datetime import datetime, timedelta
from operators.emr_spark_operator import EMRSparkOperator
from utils.gcp import load_to_bigquery
from utils.status import register_status

default_args = {
    "owner": "amiyaguchi@mozilla.com",
    "depends_on_past": False,
    "start_date": datetime(2018, 11, 26),
    "email": ["telemetry-alerts@mozilla.com", "amiyaguchi@mozilla.com"],
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
    "retry_delay": timedelta(minutes=30),
}

dag = DAG("socorro_import", default_args=default_args, schedule_interval="@daily")

# input: crashstats-telemetry-crashes-prod-us-west-2/v1/crash_report
# output: telemetry-parquet/socorro_crash/v2
crash_report_parquet = EMRSparkOperator(
    task_id="crash_report_parquet",
    job_name="Socorro Crash Reports Parquet",
    execution_timeout=timedelta(hours=4),
    instance_count=10,
    env={"date": "{{ ds_nodash }}"},
    uri="https://raw.githubusercontent.com/mozilla-services/data-pipeline/master/reports/socorro_import/ImportCrashData.ipynb",
    output_visibility="public",
    dag=dag,
)

register_status(
    crash_report_parquet,
    crash_report_parquet.job_name,
    "Convert processed crash reports into parquet for analysis",
)

crash_report_parquet_bigquery_load = SubDagOperator(
    subdag=load_to_bigquery(
        parent_dag_name=dag.dag_id,
        dag_name="crash_report_parquet_bigquery_load",
        default_args=default_args,
        dataset_s3_bucket="telemetry-parquet",
        aws_conn_id="aws_dev_iam_s3",
        dataset="socorro_crash",
        dataset_version="v2",
        date_submission_col="crash_date",
        gke_cluster_name="bq-load-gke-1",
        bigquery_dataset="telemetry_derived",
        ),
    task_id="crash_report_parquet_bigquery_load",
    dag=dag)

crash_report_parquet >> crash_report_parquet_bigquery_load
