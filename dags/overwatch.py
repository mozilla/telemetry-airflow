"""
Overwatch
Runs daily at 0700 UTC

Source code is [overwatch-mvp repository](https://github.com/mozilla/overwatch-mvp/).

This DAG is executes the Overwatch monitoring system.
All alerts related to this DAG can be ignored.

"""

from datetime import datetime

from airflow import DAG
from operators.gcp_container_operator import GKEPodOperator


default_args = {
    "owner": "gleonard@mozilla.com",
    "email": [
        "gleonard@mozilla.com",
        "wichan@mozilla.com",
    ],
    "start_date": datetime(2022, 11, 23),
    "depends_on_past": False,
    "email_on_failure": True,
    "retries": 0,
}

tags = ["repo/telemetry-airflow", "impact/tier_3", ]
image = "gcr.io/moz-fx-data-airflow-prod-88e0/overwatch:{{ var.value.overwatch_image_version }}"


with DAG(
    "overwatch",
    default_args=default_args,
    schedule_interval="0 7 * * *",
    doc_md=__doc__,
    tags=tags,
    catchup=True,
) as dag:
    run_analysis = GKEPodOperator(
        task_id="run_analysis",
        name="run_analysis",
        image=image,
        env_vars={"SLACK_BOT_TOKEN": "{{ var.value.overwatch_slack_token }}"},
        arguments=["run-analysis", "--date={{ ds }}", "./config_files/"],
    )
