from airflow.models import Variable
from airflow.providers.slack.notifications.slack import SlackNotifier
from airflow.providers.slack.operators.slack import SlackAPIPostOperator

SLACK_CHANNEL = "#airflow-alerts"

DAG_LINK_TEMPLATE = "{{ conf.get('webserver', 'base_url') }}/dags/{{ ti.dag_id }}/grid"
TASK_LINK_TEMPLATE = DAG_LINK_TEMPLATE + "?task_id={{ ti.task_id }}"
TASK_INSTANCE_LINK_TEMPLATE = (
    TASK_LINK_TEMPLATE + "&amp;dag_run_id={{ run_id | urlencode }}"
)


def if_task_fails_alert_slack(context):
    failed_alert = SlackAPIPostOperator(
        task_id="slack_failed",
        channel=SLACK_CHANNEL,
        token=Variable.get("slack_secret_token"),
        text="""
            :red_circle: Task Failed.
            *Task*: {task}
            *Dag*: {dag}
            *Date*: {ds}
            """.format(
            task=context.get("task_instance").task_id,
            dag=context.get("task_instance").dag_id,
            ds=context.get("ds"),
        ),
    )
    return failed_alert.execute(context=context)


class AirflowBotSlackNotifier(SlackNotifier):
    """Slack notifier that will post as @Airflow-bot.

    * @Airflow-bot should be added to the channel being posted to.
    """

    def __init__(self, *, channel: str, text: str, **kwargs):
        super().__init__(
            slack_conn_id="slack_airflow_bot",
            username="Airflow-bot",
            channel=channel,
            text=text,
            **kwargs,
        )


class TaskRetrySlackNotifier(AirflowBotSlackNotifier):
    """@Airflow-bot Slack notifier about task retries, intended to be used as a task retry callback.

    * This notifier uses templates, so it shouldn't be shared between tasks.
    * @Airflow-bot should be added to the channel being posted to.
    """

    def __init__(self, channel: str):
        super().__init__(
            channel=channel,
            text=(
                "⚠️ `{{ ti.task_id }}` in the `{{ ti.dag_id }}` DAG is "
                f"<{TASK_INSTANCE_LINK_TEMPLATE}|retrying>."
            ),
        )


class TaskFailureSlackNotifier(AirflowBotSlackNotifier):
    """@Airflow-bot Slack notifier about task failures, intended to be used as a task failure callback.

    * This notifier uses templates, so it shouldn't be shared between tasks.
    * @Airflow-bot should be added to the channel being posted to.
    """

    def __init__(self, channel: str):
        super().__init__(
            channel=channel,
            text=(
                "🛑 `{{ ti.task_id }}` in the `{{ ti.dag_id }}` DAG "
                f"<{TASK_INSTANCE_LINK_TEMPLATE}|failed>."
            ),
        )
