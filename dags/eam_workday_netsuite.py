from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.cncf.kubernetes.secret import Secret
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook

from operators.gcp_container_operator import GKEPodOperator
from utils.tags import Tag

DOCS = """
### Workday/Netsuite integration
Runs a script in docker image that syncs employee data
from Workday to Netsuite.
It creates a Jira ticket if the task fails.

[docker-etl](https://github.com/mozilla/docker-etl/tree/main/jobs/eam-integrations)
[telemetry-airflow](https://github.com/mozilla/telemetry-airflow/tree/main/dags/eam_workday_netsuite_integration.py)

This DAG requires the creation of an Airflow Jira connection.

#### Owner
jmoscon@mozilla.com

#### Tags
* impact/tier_3
* repo/telemetry-airflow
* triage/record_only

"""


def get_airflow_log_link(context):
    import urllib.parse

    dag_run_id = context["dag_run"].run_id
    task_id = context["task_instance"].task_id
    base_url = "http://workflow.telemetry.mozilla.org/dags/"
    base_url += "eam-workday-xmatters-integration/grid?tab=logs&dag_run_id="
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
    summary = "Workday Netsuite Integration - Airflow Task Issue Exception"
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
    "start_date": datetime(2024, 1, 1),
    "retries": 3,
    # wait 5 min before retry
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": create_jira_ticket,
}
tags = [Tag.ImpactTier.tier_3, Tag.Triage.record_only, Tag.Repo.airflow]


NETSUITE_INTEG_WORKDAY_USERNAME = Secret(
    deploy_type="env",
    deploy_target="NETSUITE_INTEG_WORKDAY_USERNAME",
    secret="airflow-gke-secrets",
    key="NETSUITE_INTEG_WORKDAY_USERNAME",
)

NETSUITE_INTEG_WORKDAY_PASSWORD = Secret(
    deploy_type="env",
    deploy_target="NETSUITE_INTEG_WORKDAY_PASSWORD",
    secret="airflow-gke-secrets",
    key="NETSUITE_INTEG_WORKDAY_PASSWORD",
)

NETSUITE_INTEG_WORKDAY_LISTING_OF_WORKERS_LINK = Secret(
    deploy_type="env",
    deploy_target="NETSUITE_INTEG_WORKDAY_LISTING_OF_WORKERS_LINK",
    secret="airflow-gke-secrets",
    key="NETSUITE_INTEG_WORKDAY_LISTING_OF_WORKERS_LINK",
)

NETSUITE_INTEG_WORKDAY_INTERNATIONAL_TRANSFER_LINK = Secret(
    deploy_type="env",
    deploy_target="NETSUITE_INTEG_WORKDAY_INTERNATIONAL_TRANSFER_LINK",
    secret="airflow-gke-secrets",
    key="NETSUITE_INTEG_WORKDAY_INTERNATIONAL_TRANSFER_LINK",
)


NETSUITE_INTEG_NETSUITE_CONSUMER_KEY = Secret(
    deploy_type="env",
    deploy_target="NETSUITE_INTEG_NETSUITE_CONSUMER_KEY",
    secret="airflow-gke-secrets",
    key="NETSUITE_INTEG_NETSUITE_CONSUMER_KEY",
)

NETSUITE_INTEG_NETSUITE_CONSUMER_SECRET = Secret(
    deploy_type="env",
    deploy_target="NETSUITE_INTEG_NETSUITE_CONSUMER_SECRET",
    secret="airflow-gke-secrets",
    key="NETSUITE_INTEG_NETSUITE_CONSUMER_SECRET",
)

NETSUITE_INTEG_NETSUITE_TOKEN_ID = Secret(
    deploy_type="env",
    deploy_target="NETSUITE_INTEG_NETSUITE_TOKEN_ID",
    secret="airflow-gke-secrets",
    key="NETSUITE_INTEG_NETSUITE_TOKEN_ID",
)

NETSUITE_INTEG_NETSUITE_TOKEN_SECRET = Secret(
    deploy_type="env",
    deploy_target="NETSUITE_INTEG_NETSUITE_TOKEN_SECRET",
    secret="airflow-gke-secrets",
    key="NETSUITE_INTEG_NETSUITE_TOKEN_SECRET",
)

NETSUITE_INTEG_NETSUITE_TOKEN_OAUTH_REALM = Secret(
    deploy_type="env",
    deploy_target="NETSUITE_INTEG_NETSUITE_TOKEN_OAUTH_REALM",
    secret="airflow-gke-secrets",
    key="NETSUITE_INTEG_NETSUITE_TOKEN_OAUTH_REALM",
)

NETSUITE_INTEG_NETSUITE_HOST = Secret(
    deploy_type="env",
    deploy_target="NETSUITE_INTEG_NETSUITE_HOST",
    secret="airflow-gke-secrets",
    key="NETSUITE_INTEG_NETSUITE_HOST",
)


with DAG(
    "eam-workday-netsuite-integration",
    default_args=default_args,
    doc_md=DOCS,
    tags=tags,
    # 12:00 AM PST - M-F
    schedule_interval="0 8 * * 1-5",
) as dag:
    workday_netsuite_dag = GKEPodOperator(
        task_id="eam_workday_netsuite",
        arguments=["python", "scripts/workday_netsuite_integration.py", "--level", "info"],
        image="gcr.io/moz-fx-data-airflow-prod-88e0/"
        + "eam-integrations_docker_etl:latest",
        gcp_conn_id="google_cloud_airflow_gke",
        secrets=[
            NETSUITE_INTEG_WORKDAY_USERNAME, 
            NETSUITE_INTEG_WORKDAY_PASSWORD,
            NETSUITE_INTEG_WORKDAY_LISTING_OF_WORKERS_LINK,
            NETSUITE_INTEG_WORKDAY_INTERNATIONAL_TRANSFER_LINK,
            NETSUITE_INTEG_NETSUITE_CONSUMER_KEY,
            NETSUITE_INTEG_NETSUITE_CONSUMER_SECRET,
            NETSUITE_INTEG_NETSUITE_TOKEN_ID,
            NETSUITE_INTEG_NETSUITE_TOKEN_SECRET, 
            NETSUITE_INTEG_NETSUITE_TOKEN_OAUTH_REALM,
            NETSUITE_INTEG_NETSUITE_HOST 
        ],
    )
