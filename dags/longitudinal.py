from airflow import DAG
from airflow.operators.moz_databricks import MozDatabricksSubmitRunOperator
from datetime import datetime, timedelta
from operators.emr_spark_operator import EMRSparkOperator
from utils.constants import DS_WEEKLY
from utils.mozetl import mozetl_envvar
from utils.tbv import tbv_envvar
from utils.status import register_status

default_args = {
    'owner': 'frank@mozilla.com',
    'depends_on_past': False,
    'start_date': datetime(2018, 12, 16),
    'email': ['telemetry-alerts@mozilla.com', 'frank@mozilla.com', 'rharter@mozilla.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=30),
}

dag = DAG('longitudinal', default_args=default_args, schedule_interval='@weekly')

longitudinal = MozDatabricksSubmitRunOperator(
    task_id="longitudinal",
    job_name="Longitudinal View",
    execution_timeout=timedelta(hours=12),
    instance_count=16,
    instance_type="i3.8xlarge",
    env=tbv_envvar(
        "com.mozilla.telemetry.views.LongitudinalView",
        {
            "bucket": "{{ task.__class__.private_output_bucket }}",
            "to": DS_WEEKLY
        },
        metastore_location="s3://telemetry-parquet/longitudinal"),
    uri="https://raw.githubusercontent.com/mozilla/telemetry-airflow/master/jobs/telemetry_batch_view.py",
    dag=dag)

register_status(longitudinal, "Longitudinal", "A 6-month longitudinal view of client history.")


game_hw_survey = EMRSparkOperator(
    task_id="game_hw_survey",
    job_name="Firefox Hardware Report",
    execution_timeout=timedelta(hours=5),
    instance_count=15,
    owner="frank@mozilla.com",
    depends_on_past=True,
    email=["telemetry-alerts@mozilla.com", "frank@mozilla.com",
           "firefox-hardware-report-feedback@mozilla.com"],
    env={"date": "{{ ds_nodash }}", "bucket": "{{ task.__class__.public_output_bucket }}"},
    uri="https://raw.githubusercontent.com/mozilla/telemetry-airflow/master/jobs/hardware_report.sh",
    output_visibility="public",
    dag=dag)


game_hw_survey.set_upstream(longitudinal)
