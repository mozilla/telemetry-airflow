"""
This configures a weekly DAG to run the TAAR Ensemble job off.
"""
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.moz_databricks import MozDatabricksSubmitRunOperator

default_args_weekly = {
    "owner": "frank@mozilla.com",
    "depends_on_past": False,
    "start_date": datetime(2019, 5, 31),
    "email": ["telemetry-alerts@mozilla.com", "frank@mozilla.com"],
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
    "retry_delay": timedelta(minutes=30),
}



dag_weekly = DAG(
    "weekly_main_summary", default_args=default_args_weekly, schedule_interval="@weekly"
)

taar_ensemble = MozDatabricksSubmitRunOperator(
    task_id="taar_ensemble_job",
    job_name="TAAR Ensemble Model",
    owner="vng@mozilla.com",
    email=["vng@mozilla.com", "mlopatka@mozilla.com"],
    execution_timeout=timedelta(hours=11),
    instance_count=5,
    instance_type="i3.2xlarge",
    spot_bid_price_percent=100,
    max_instance_count=60,
    enable_autoscale=True,
    pypi_libs=['mozilla-taar3==0.4.5', 'mozilla-srgutil==0.1.10', 'python-decouple==3.1'],
    env=mozetl_envvar(
        "taar_ensemble",
        {
            "date": "{{ ds_nodash }}",
        },
    ),
    uri="https://raw.githubusercontent.com/mozilla/python_mozetl/master/bin/mozetl-databricks.sh",
    output_visibility="private",
)


taar_ensemble.set_upstream(dag_weekly)
