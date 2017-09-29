from airflow import DAG
from datetime import datetime, timedelta
from operators.emr_spark_operator import EMRSparkOperator
from utils.mozetl import mozetl_envvar

default_args = {
    'owner': 'amiyaguchi@mozilla.com',
    'depends_on_past': False,
    'start_date': datetime(2016, 12, 23),
    'email': ['telemetry-alerts@mozilla.com', 'amiyaguchi@mozilla.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=30),
}

dag = DAG('churn', default_args=default_args, schedule_interval='0 0 * * 3')

churn = EMRSparkOperator(
    task_id="churn",
    job_name="Generate weekly desktop retention dataset",
    execution_timeout=timedelta(hours=4),
    instance_count=5,
    env=mozetl_envvar("churn", {
        "start_date": "{{ ds_nodash }}",
        "bucket": "{{ task.__class__.private_output_bucket }}"
    }, {
        "MOZETL_GIT_BRANCH": "churn-v2"
    }),
    uri="https://raw.githubusercontent.com/mozilla/python_mozetl/master/bin/mozetl-submit.sh",
    output_visibility="public",
    dag=dag)

churn_to_csv = EMRSparkOperator(
    task_id="churn_to_csv",
    job_name="Generate Churn CSV files",
    execution_timeout=timedelta(hours=4),
    instance_count=1,
    env={"date": "{{ ds_nodash }}"},
    uri="https://raw.githubusercontent.com/mozilla/mozilla-reports/master/etl/churn_to_csv.kp/orig_src/churn_to_csv.ipynb",
    dag=dag)

churn_to_csv.set_upstream(churn)
