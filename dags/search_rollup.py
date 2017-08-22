from datetime import timedelta, datetime

from airflow import DAG
from airflow.operators import ExternalTaskSensor

from operators.emr_spark_operator import EMRSparkOperator
from utils.mozetl import mozetl_envvar


default_args = {
    'owner': 'amiyaguchi@mozilla.com',
    'depends_on_past': False,
    'start_date': datetime(2017, 8, 20),
    'email': [
        'telemetry-alerts@mozilla.com',
        'amiyaguchi@mozilla.com',
        'harterrt@mozilla.com',
        'spenrose@mozilla.com'
    ],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=30),
}


dag_daily = DAG('search_rollup_daily',
                 default_args=default_args,
                 schedule_interval='@daily')


dag_monthly = DAG('search_rollup_monthly',
                  default_args=default_args,
                  schedule_interval='@monthly')


def search_rollup_dag(dag, mode, instance_count):
    """Create a sensor for main_summary and attach the search rollup."""

    main_summary_sensor = ExternalTaskSensor(
        task_id="main_summary_sensor",
        external_dag_id="main_summary",
        external_task_id="main_summary",
        dag=dag
    )

    search_rollup = EMRSparkOperator(
        task_id="search_rollup_{}".format(mode),
        job_name="{} search rollup".format(mode).title(),
        execution_timeout=timedelta(hours=4),
        instance_count=instance_count,
        env=mozetl_envvar("search_rollup", {
            "start_date": "{{ ds_nodash }}",
            "mode": mode,
            "bucket": "net-mozaws-prod-us-west-2-pipeline-analysis",
            "prefix": "spenrose/search/to_vertica",
        }),
        uri="https://raw.githubusercontent.com/mozilla/python_mozetl/master/bin/mozetl-submit.sh",
        dag=dag
    )

    search_rollup.set_upstream(main_summary_sensor)


search_rollup_dag(dag_daily, "daily", instance_count=1)
search_rollup_dag(dag_monthly, "monthly", instance_count=3)
