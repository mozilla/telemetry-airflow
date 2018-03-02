from airflow import DAG
from datetime import datetime, timedelta
from operators.emr_spark_operator import EMRSparkOperator
from utils.constants import DS_WEEKLY
from utils.tbv import tbv_envvar

default_args = {
    'owner': 'ssuh@mozilla.com',
    'depends_on_past': False,
    'start_date': datetime(2017, 3, 1),
    'email': ['telemetry-alerts@mozilla.com', 'ssuh@mozilla.com', 'frank@mozilla.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=15)
}

dag = DAG('main_summary_backfill',
          default_args=default_args,
          schedule_interval='@weekly',
          catchup=True,
          max_active_runs=10)

t0 = EMRSparkOperator(task_id="main_summary_backfill",
                      job_name="Main Summary Backfill",
                      execution_timeout=timedelta(hours=48),
                      instance_count=20,
                      env=tbv_envvar("com.mozilla.telemetry.views.MainSummaryView", {
                            "from": "{{ ds_nodash }}",
                            "to": "{{ macros.ds_format(macros.ds_add(ds, 6), '%Y-%m-%d', '%Y%m%d') }}",
                            "bucket": "telemetry-backfill"
                          }, {
                              "GIT_BRANCH": "backfill"
                          }),
                      uri="https://raw.githubusercontent.com/mozilla/telemetry-airflow/master/jobs/telemetry_batch_view.py",
                      dag=dag)
