from datetime import datetime

from airflow import DAG
from airflow.providers.cncf.kubernetes.secret import Secret

from operators.gcp_container_operator import GKEPodOperator
from utils.tags import Tag

DOCS = """
### Workday/XMatters integration
Runs a script in docker image that syncs employee data
from Workday to XMatters.
It creates a Jira ticket if the task fails.

[docker-etl](https://github.com/mozilla/docker-etl/tree/main/jobs/eam-integrations)

This DAG requires the creation of an Airflow Jira connection.

#### Owner
jmoscon@mozilla.com

"""


def on_failure_callback(context):
    from airflow.providers.atlassian.jira.notifications.jira import (
        send_jira_notification,
    )

    exception = context.get("exception")

    send_jira_notification(
        jira_conn_id="eam_jira_connection_id",
        description=f"Workday XMatters Integration \
             Task 1 failed. Exception = {exception}",
        summary="Airflow Task Issue Exception",
        # use this link to find project id and issue type ids :
        # https://mozilla-hub.atlassian.net/rest/api/latest/project/ASP
        project_id=10051,
        issue_type_id=10007,
        labels=["airflow-task-failure"],
    ).notify(context)


default_args = {
    "owner": "jmoscon@mozilla.com",
    "emails": ["jmoscon@mozilla.com"],
    "start_date": datetime(2024, 1, 1),
    "retries": 0,
}
tags = [Tag.ImpactTier.tier_3, Tag.Triage.no_triage]


xmatters_client_id = Secret(
    deploy_type="env",
    deploy_target="XMATTERS_CLIENT_ID",
    secret="airflow-gke-secrets",
    key="XMATTERS_CLIENT_ID",
)
xmatters_username = Secret(
    deploy_type="env",
    deploy_target="XMATTERS_USERNAME",
    secret="airflow-gke-secrets",
    key="XMATTERS_USERNAME",
)
xmatters_password = Secret(
    deploy_type="env",
    deploy_target="XMATTERS_PASSWORD",
    secret="airflow-gke-secrets",
    key="XMATTERS_PASSWORD",
)
xmatters_url = Secret(
    deploy_type="env",
    deploy_target="XMATTERS_URL",
    secret="airflow-gke-secrets",
    key="XMATTERS_URL",
)
xmatters_supervisor_id = Secret(
    deploy_type="env",
    deploy_target="XMATTERS_SUPERVISOR_ID",
    secret="airflow-gke-secrets",
    key="XMATTERS_SUPERVISOR_ID",
)

xmatters_integ_workday_username = Secret(
    deploy_type="env",
    deploy_target="XMATTERS_INTEG_WORKDAY_USERNAME",
    secret="airflow-gke-secrets",
    key="XMATTERS_INTEG_WORKDAY_USERNAME",
)
xmatters_integ_workday_password = Secret(
    deploy_type="env",
    deploy_target="XMATTERS_INTEG_WORKDAY_PASSWORD",
    secret="airflow-gke-secrets",
    key="XMATTERS_INTEG_WORKDAY_PASSWORD",
)

mozgeo_google_api_key = Secret(
    deploy_type="env",
    deploy_target="MOZGEO_GOOGLE_API_KEY",
    secret="airflow-gke-secrets",
    key="MOZGEO_GOOGLE_API_KEY",
)


with DAG(
    "eam-workday-xmatters-integration",
    default_args=default_args,
    doc_md=DOCS,
    on_failure_callback=on_failure_callback,
    tags=tags,
    schedule_interval="@daily",
) as dag:
    workday_xmatters_dag = GKEPodOperator(
        task_id="eam_workday_xmatters",
        arguments=["python", "scripts/workday_xmatters.py", "--level", "info"],
        image="gcr.io/moz-fx-data-airflow-prod-88e0/"
        + "eam-integrations_docker_etl:latest",
        gcp_conn_id="google_cloud_airflow_gke",
        secrets=[
            xmatters_client_id,
            xmatters_username,
            xmatters_password,
            xmatters_url,
            xmatters_supervisor_id,
            xmatters_integ_workday_username,
            xmatters_integ_workday_password,
            mozgeo_google_api_key,
        ],
    )
