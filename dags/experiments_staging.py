from airflow import DAG
from datetime import datetime, timedelta
from operators.emr_spark_operator import EMRSparkOperator


default_args = {
    'owner': 'ssuh@mozilla.com',
    'depends_on_past': False,
    'start_date': datetime(2017, 8, 1),
    'email': [
        'telemetry-alerts@mozilla.com',
        'ssuh@mozilla.com',
        'chudson@mozilla.com'
    ],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=30),
}

dag = DAG(
    'experiments_staging',
    default_args=default_args,
    schedule_interval=None
)


t1 = EMRSparkOperator(
    task_id='main_summary_experiments_staging',
    job_name='Experiments Main Summary View (staging)',
    execution_timeout=timedelta(hours=10),
    instance_count=10,
    owner='ssuh@mozilla.com',
    email=[
        'telemetry-alerts@mozilla.com',
        'frank@mozilla.com',
        'ssuh@mozilla.com'
    ],
    env={
        'date': '{{ ds_nodash }}',
        'branch': 'experiments-staging',
        'inbucket': '{{ task.__class__.private_output_bucket }}',
        'bucket': '{{ task.__class__.staging_private_output_bucket }}'
    },
    uri=('https://raw.githubusercontent.com/mozilla/telemetry-airflow'
         '/master/jobs/experiment_main_summary_view.sh'),
    dag=dag
)

t2 = EMRSparkOperator(
    task_id='experiments_aggregates_staging',
    job_name='Experiments Aggregates View (staging)',
    execution_timeout=timedelta(hours=10),
    instance_count=10,
    owner='ssuh@mozilla.com',
    email=[
        'telemetry-alerts@mozilla.com',
        'frank@mozilla.com',
        'ssuh@mozilla.com'
    ],
    env={
        'date': '{{ ds_nodash }}',
        'branch': 'experiments-staging',
        'bucket': '{{ task.__class__.staging_private_output_bucket }}'
    },
    uri=('https://raw.githubusercontent.com/mozilla/telemetry-airflow'
         '/master/jobs/experiment_aggregates_view.sh'),
    dag=dag
)

t3 = EMRSparkOperator(
    task_id='experiments_aggregates_import_staging',
    job_name='Experiments Aggregates Import (staging)',
    execution_timeout=timedelta(hours=10),
    instance_count=1,
    owner='chudson@mozilla.com',
    email=[
        'telemetry-alerts@mozilla.com',
        'chudson@mozilla.com'
    ],
    env={
        'date': '{{ ds_nodash }}',
        'db': 'experiments-viewer-staging-db',
        'bucket': '{{ task.__class__.staging_private_output_bucket }}'
    },
    uri=('https://raw.githubusercontent.com/mozilla/experiments-viewer'
         '/import-staging/notebooks/import.py'),
    dag=dag
)

t2.set_upstream(t1)
t3.set_upstream(t2)
