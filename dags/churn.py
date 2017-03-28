from airflow import DAG
from datetime import datetime, timedelta
from operators.emr_spark_operator import EMRSparkOperator
from utils.constants import DS_WEEKLY

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

t0 = EMRSparkOperator(task_id="churn",
                      job_name="Generate weekly desktop retention dataset",
                      execution_timeout=timedelta(hours=4),
                      instance_count=5,
                      env={"date": "{{ ds_nodash }}"},
                      uri="https://raw.githubusercontent.com/mozilla/mozilla-reports/master/etl/churn.kp/orig_src/Churn.ipynb",
                      output_visibility="public",
                      dag=dag)

t1 = EMRSparkOperator(task_id="churn_to_csv",
                      job_name="Generate Churn CSV files",
                      execution_timeout=timedelta(hours=4),
                      instance_count=1,
                      env={"date": "{{ ds_nodash }}"},
                      uri="https://raw.githubusercontent.com/mozilla/mozilla-reports/master/etl/churn_to_csv.kp/orig_src/churn_to_csv.ipynb",
                      dag=dag)

t1.set_upstream(t0)
