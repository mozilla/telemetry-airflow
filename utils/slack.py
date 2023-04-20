from airflow.models import Variable
from airflow.providers.slack.operators.slack import SlackAPIPostOperator

SLACK_CHANNEL = "#airflow-alerts"

def if_task_fails_alert_slack(context):
    failed_alert = SlackAPIPostOperator(
        task_id='slack_failed',
        channel=SLACK_CHANNEL,
        token=Variable.get("slack_secret_token"),
        text="""
            :red_circle: Task Failed.
            *Task*: {task}
            *Dag*: {dag}
            *Date*: {ds}
            """.format(
                task=context.get('task_instance').task_id,
                dag=context.get('task_instance').dag_id,
                ds=context.get('ds')
            )
    )
    return failed_alert.execute(context=context)
