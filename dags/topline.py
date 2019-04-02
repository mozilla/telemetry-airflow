from airflow import DAG
from datetime import timedelta, datetime
from .operators.emr_spark_operator import EMRSparkOperator

default_args_weekly = {
    'owner': 'amiyaguchi@mozilla.com',
    'depends_on_past': False,
    'start_date': datetime(2018, 12, 9),
    'email': ['telemetry-alerts@mozilla.com', 'amiyaguchi@mozilla.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=30),
}

default_args_monthly = {
    'owner': 'amiyaguchi@mozilla.com',
    'depends_on_past': False,
    'start_date': datetime(2018, 12, 1),
    'email': ['telemetry-alerts@mozilla.com', 'amiyaguchi@mozilla.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=30),
}


def topline_dag(dag, mode, instance_count):
    topline_summary = EMRSparkOperator(
        task_id="topline_summary",
        job_name="Topline Summary View",
        execution_timeout=timedelta(hours=8),
        instance_count=instance_count,
        env={
            "date": "{{ ds_nodash }}",
            "bucket": "{{ task.__class__.private_output_bucket }}",
            "mode": mode
        },
        uri="https://raw.githubusercontent.com/mozilla/telemetry-airflow/master/jobs/topline_summary_view.sh",
        dag=dag)

    topline_dashboard = EMRSparkOperator(
        task_id="topline_dashboard",
        job_name="Topline Dashboard",
        execution_timeout=timedelta(hours=2),
        instance_count=1,
        env={"mode": mode},
        uri="https://raw.githubusercontent.com/mozilla/telemetry-airflow/master/jobs/topline_dashboard.sh",
        dag=dag)

    topline_dashboard.set_upstream(topline_summary)


dag_weekly = DAG('topline_weekly',
                 default_args=default_args_weekly,
                 schedule_interval='@weekly')
dag_monthly = DAG('topline_monthly',
                  default_args=default_args_monthly,
                  schedule_interval='@monthly')

topline_dag(dag_weekly, "weekly", instance_count=5)
topline_dag(dag_monthly, "monthly", instance_count=20)
