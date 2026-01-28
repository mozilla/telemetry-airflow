import datetime

from airflow import DAG
from airflow.providers.cncf.kubernetes.secret import Secret

from operators.gcp_container_operator import GKEPodOperator
from utils.tags import Tag

DOCS = """
### Workday/docusign integration
Runs a script in docker image that syncs employee data
from Workday to docusign.
It creates a Jira ticket if the task fails.

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
    base_url += "eam-workday-docusign-integration/grid?tab=logs&dag_run_id="
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

    jira_domain = "mozilla-hub.atlassian.net"
    url = f"https://{jira_domain}/rest/api/3/issue"
    headers = {"Accept": "application/json", "Content-Type": "application/json"}
    auth = HTTPBasicAuth(conn.login, conn.password)
    summary = "Workday docusign Integration - Airflow Task Issue Exception"
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


docusign_jwt = Secret(
    deploy_type="env",
    deploy_target="docusign_jwt",
    secret="airflow-gke-secrets",
    key="docusign_jwt",
)
DOCUSIGN_INTEG_WORKDAY_USERNAME = Secret(
    deploy_type="env",
    deploy_target="DOCUSIGN_INTEG_WORKDAY_USERNAME",
    secret="airflow-gke-secrets",
    key="DOCUSIGN_INTEG_WORKDAY_USERNAME",
)
DOCUSIGN_INTEG_WORKDAY_PASSWORD = Secret(
    deploy_type="env",
    deploy_target="DOCUSIGN_INTEG_WORKDAY_PASSWORD",
    secret="airflow-gke-secrets",
    key="DOCUSIGN_INTEG_WORKDAY_PASSWORD",
)

with DAG(
    "eam-workday-docusign-integration",
    default_args=default_args,
    doc_md=DOCS,
    tags=tags,
    # 7:00 PM UTC/12:00 AM PST - weekdays
    schedule_interval="0 7 * * 1-5",
) as dag:
    workday_docusign_dag = GKEPodOperator(
        task_id="eam_workday_docusign",
        arguments=[
            "python",
            "scripts/workday_docusign_integration.py",
            "--level",
            "info",
        ],
        image="us-docker.pkg.dev/moz-fx-data-artifacts-prod/docker-etl/eam-integrations:latest",
        gcp_conn_id="google_cloud_airflow_gke",
        secrets=[
            docusign_jwt,
            DOCUSIGN_INTEG_WORKDAY_USERNAME,
            DOCUSIGN_INTEG_WORKDAY_PASSWORD,
        ],
    )
