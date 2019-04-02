from airflow import DAG
from datetime import datetime, timedelta
from .operators.emr_spark_operator import EMRSparkOperator
from .utils.status import register_status

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
