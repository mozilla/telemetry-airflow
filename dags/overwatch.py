"""
Overwatch
Runs daily at 0700 UTC

Source code is [overwatch repository](https://github.com/mozilla/overwatch-mvp/).
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from operators.gcp_container_operator import GKEPodOperator

docs = """\
This DAG is executes the Overwatch monitoring system.
All alerts related to this DAG can be ignored.

(for more info on dim see: https://github.com/mozilla/overwatch-mvp)
"""


default_args = {
    "owner": "gleonard@mozilla.com",
    "email": [
        "gleonard@mozilla.com",
        "wichan@mozilla.com",
    ],
    "start_date": datetime(2022, 11, 1),
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 0,
}

tags = ["repo/telemetry-airflow", "impact/tier_3", ]
image = "gcr.io/moz-fx-data-airflow-prod-88e0/overwatch:" + Variable.get("overwatch_image_version")


with DAG(
    "overwatch",
    default_args=default_args,
    schedule_interval="0 7 * * *",
    doc_md=docs,
    tags=tags,
) as dag:
    run_analysis = GKEPodOperator(
        task_id="run_analysis",
        name="run_analysis",
        image=image,
        dag=dag,
        env_vars={"SLACK_BOT_TOKEN": Variable.get("overwatch_slack_token")},
        arguments=["run-analysis", "--date={{ ds }}", "./config_files/"],
    )
