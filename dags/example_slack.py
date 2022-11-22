from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

from utils.slack import if_task_fails_alert_slack
from utils.tags import Tag

"""
If getting "channel_not_found" errors, you need to open the slack channel settings, navigate to Integrations,
and add "Airflow-bot" to the Apps section.
"""
tags = [Tag.ImpactTier.tier_3]

"""
default_args_1 = {
    "owner": "hwoo@mozilla.com",
    "email": ["hwoo@mozilla.com"],
    "depends_on_past": False,
    "start_date": datetime(2022, 3, 23),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
    "retry_delay": timedelta(minutes=30),
}

dag1 = DAG("example_slack", default_args=default_args_1, schedule_interval="@daily", tags=tags,)

# The following example shows how to simply post to a slack channel
simple_slack_example = SlackAPIPostOperator(
    task_id="post_hello",
    token=Variable.get("slack_secret_token"),
    text="hello world!",
    channel="#airflow-alerts",
    dag=dag1,
)
"""

# This example shows how to configure a dag's default args callback so it alerts slack on failures using an
# imported utils method
default_args_2 = {
    "owner": "hwoo@mozilla.com",
    "email": ["hwoo@mozilla.com"],
    "depends_on_past": False,
    "start_date": datetime(2022, 3, 23),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 0,
    "retry_delay": timedelta(minutes=30),
    # NOTE: on_failure_callback doesn't trigger until all retries are exhausted
    "on_failure_callback": if_task_fails_alert_slack,
}

dag2 = DAG("example_slack", default_args=default_args_2, schedule_interval="@daily", tags=tags,)

task_with_failed_slack_alerts = BashOperator(
    task_id='fail_task',
    bash_command='exit 1',
    dag=dag2)
