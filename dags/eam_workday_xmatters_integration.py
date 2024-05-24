#Jira sandbox https://mozilla-hub-sandbox-721.atlassian.net/jira/software/c/projects/ASP/boards/415

from datetime import datetime
from airflow import DAG

from operators.gcp_container_operator import GKEPodOperator
from utils.tags import Tag
from airflow.operators.bash import BashOperator
from airflow.providers.atlassian.jira.notifications.jira import send_jira_notification
from airflow.operators.docker_operator  import DockerOperator
import logging
logger = logging.getLogger("airflow.task")

DOCS = """
### Workday/XMatters integration
Runs a script in docker image that syncs employee data from Workday to XMatters.
It creates a Jira ticket if the task fails.

[docker-etl](https://github.com/mozilla/docker-etl/tree/main/jobs/eam-integrations)
 
This DAG requires the creation of an Airflow Jira connection.
   
#### Owner
jmoscon@mozilla.com

"""
def on_failure_callback(context):
    exception = context.get('exception')
    
    send_jira_notification(
        jira_conn_id="jira_connection_id",
        description=f"Workday XMatters Integration Task 1 failed. Exception = {exception}",
        summary="Airflow Task Issue Exception",
        #use this link to find project id and issue type ids : https://mozilla-hub.atlassian.net/rest/api/latest/project/ASP
        project_id=10051,
        issue_type_id=10007,
        labels=["airflow-task-failure"],
    ).notify(context)
    
 
default_args = {
 'owner': 'Julio Moscon',
 'emails': ['jmoscon@mozilla.com'],
 'start_date': datetime (2024, 1, 1),
 'retries': 0,
 'on_failure_callback': [on_failure_callback],
}

with DAG(
    "eam-workday-xmatters-integration",
    default_args=default_args,
    doc_md=DOCS,
    schedule_interval="@daily",    
) as dag:
    workday_xmatters_dag = GKEPodOperator(
        task_id="eam_workday_xmatters",
        arguments=[
            "python",
            "scripts/workday_xmatters.py",
            "--level",
            "info"
             
        ],
        image="gcr.io/moz-fx-data-airflow-prod-88e0/eam-integrations_docker_etl:latest",
        gcp_conn_id="google_cloud_airflow_gke",
    )
 