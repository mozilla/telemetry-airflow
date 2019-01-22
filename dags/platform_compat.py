from airflow import DAG
from datetime import datetime, timedelta
from operators.emr_spark_operator import EMRSparkOperator
from airflow.operators.moz_databricks import MozDatabricksSubmitRunOperator
from airflow.operators.s3fs_check_success import S3FSCheckSuccessSensor
from utils.tbv import tbv_envvar


default_args = {
    "owner": "amiyaguchi@mozilla.com",
    "depends_on_past": False,
    "start_date": datetime.now(),
    "email": ["telemetry-alerts@mozilla.com", "amiyaguchi@mozilla.com"],
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 0,
    "retry_delay": timedelta(minutes=30),
}

# Externally triggered
dag = DAG("platform_compat", default_args=default_args, schedule_interval=None)

cases = [
    ("Databricks, Spark 2.3, Runtime 4.3", "main_summary_db_2_3", "4.3.x-scala2.11"),
    ("Databricks, Spark 2.4, Runtime 5.1", "main_summary_db_2_4", "5.1.x-scala2.11"),
    ("EMR, Spark 2.3, Runtime 4.3", "main_summary_emr_2_3", "emr-5.13.0"),
]


def ds_nodash_offset(offset):
    return datetime.strftime(datetime.now() - timedelta(offset), "%Y%m%d")


for offset, case in enumerate(cases):
    name, task, label = case
    submission_date = ds_nodash_offset(offset + 1)
    CompatETLOperator = (
        EMRSparkOperator if label.startswith("emr") else MozDatabricksSubmitRunOperator
    )

    CompatETLOperator(
        task_id=task,
        job_name=name,
        release_label=label,
        env=tbv_envvar(
            "com.mozilla.telemetry.views.MainSummaryView",
            options={
                "from": submission_date,
                "to": submission_date,
                "bucket": "telemetry-test-bucket",
                "read-mode": "aligned",
                "input-partition-multiplier": "5",
                "channel": "nightly",
            },
        ),
        execution_timeout=timedelta(hours=1),
        instance_count=3,
        instance_type="c4.4xlarge",
        uri="https://raw.githubusercontent.com/mozilla/telemetry-airflow/master/jobs/telemetry_batch_view.py",
        dag=dag,
    ) >> S3FSCheckSuccessSensor(
        task_id="check_{}".format(task),
        bucket="telemetry-test-bucket",
        prefix="main_summary/v4/submission_date_s3={}".format(submission_date),
        num_partitions=100,
        poke_interval=60,
        timeout=300,
        dag=dag,
    )
