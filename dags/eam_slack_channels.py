import datetime

from airflow import DAG
from airflow.providers.cncf.kubernetes.secret import Secret

from operators.gcp_container_operator import GKEPodOperator
from utils.tags import Tag

DOCS = """
### Slack Channels integration
Runs a script in docker image that
 - will archive unused channels
 - delete old archived channels

[docker-etl](https://github.com/mozilla/docker-etl/tree/main/jobs/eam-integrations)

This DAG requires the creation of an Airflow Jira connection.

#### Owner
jmoscon@mozilla.com

"""


def get_airflow_log_link(context):
    import urllib.parse

    dag_run_id = context["dag_run"].run_id
    task_id = context["task_instance"].task_id
    base_url = "http://workflow.telemetry.mozilla.org/dags/"
    base_url += "eam-slack-channels-integration/grid?tab=logs&dag_run_id="
    return base_url + f"{urllib.parse.quote(dag_run_id)}&task_id={task_id}"


def create_jira_ticket(context):
    import json
    import logging

    import requests
    from airflow.providers.atlassian.jira.hooks.jira import JiraHook
    from requests.auth import HTTPBasicAuth

    logger = logging.getLogger(__name__)
    logger.info("Creating Jira ticket ...")

    conn_id = "eam_jira_connection_id"
    conn = JiraHook(
        jira_conn_id=conn_id,
    ).get_connection(conn_id)
    log_url = get_airflow_log_link(context)

    jira_domain = "mozilla-hub-sandbox-721.atlassian.net"
    url = f"https://{jira_domain}/rest/api/3/issue"
    headers = {"Accept": "application/json", "Content-Type": "application/json"}
    auth = HTTPBasicAuth(conn.login, conn.password)
    summary = "Slack Channels Integration - Airflow Task Issue Exception"
    paragraph_text = "Detailed error logging can be found in the link: "
    project_key = "ASP"
    issue_type_id = "10020"  # Issue Type = Bug
    assignee_id = "712020:b999000a-67b1-45ff-8b40-42a5ceeee75b"  # Julio
    payload = json.dumps(
        {
            "fields": {
                "assignee": {"id": assignee_id},
                "project": {"key": project_key},
                "summary": summary,
                "description": {
                    "type": "doc",
                    "version": 1,
                    "content": [
                        {
                            "type": "paragraph",
                            "content": [
                                {
                                    "type": "text",
                                    "text": paragraph_text,
                                },
                                {
                                    "type": "text",
                                    "text": "Mozilla-Telemetry log.",
                                    "marks": [
                                        {
                                            "type": "link",
                                            "attrs": {"href": f"{log_url}"},
                                        }
                                    ],
                                },
                            ],
                        }
                    ],
                },
                "issuetype": {"id": issue_type_id},
            }
        }
    )

    response = requests.post(url, headers=headers, auth=auth, data=payload)
    logger.info(f"response.text={response.text}")
    if response.status_code == 201:
        logger.info("Issue created successfully.")
        return response.json()
    else:
        logger.info(
            f"Failed to create issue. Status code:"
            f"{response.status_code}, Response: {response.text}"
        )
        return None


default_args = {
    "owner": "jmoscon@mozilla.com",
    "emails": ["jmoscon@mozilla.com"],
    "start_date": datetime.datetime(2024, 1, 1),
    "retries": 3,
    # wait 5 min before retry
    "retry_delay": datetime.timedelta(minutes=5),
    "on_failure_callback": create_jira_ticket,
}
tags = [Tag.ImpactTier.tier_3]


SLACK_CHANNEL_TOKEN = Secret(
    deploy_type="env",
    deploy_target="SLACK_CHANNEL_TOKEN",
    secret="airflow-gke-secrets",
    key="SLACK_CHANNEL_TOKEN",
)

slack_service_account = Secret(
    deploy_type="env",
    deploy_target="slack_service_account",
    secret="airflow-gke-secrets",
    key="slack_service_account",
)

with DAG(
    "eam-slack-channels-integration",
    default_args=default_args,
    doc_md=DOCS,
    tags=tags,
    # 10 PM standard time (PST, UTC-8) every day
    schedule_interval="0 6 * * *",
) as dag:
    slack_channels_dag = GKEPodOperator(
        task_id="eam_slack_channels",
        arguments=[
            "python",
            "scripts/slack_channels_integration.py",
            "--level",
            "info",
        ],
        image="gcr.io/moz-fx-data-airflow-prod-88e0/"
        + "eam-integrations_docker_etl:latest",
        gcp_conn_id="google_cloud_airflow_gke",
        secrets=[
            SLACK_CHANNEL_TOKEN,
            slack_service_account,
        ],
    )
