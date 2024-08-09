from datetime import datetime

from airflow import DAG
from airflow.providers.cncf.kubernetes.secret import Secret

from operators.gcp_container_operator import GKEPodOperator
from utils.tags import Tag

DOCS = """
### Bugzilla to BigQuery import

#### Description

Runs a Docker image that fetches bugzilla bugs from
Web Compatibility > Knowledge Base component, as well as their core
bugs dependencies and breakage reports and stores them in BQ.

The container is defined in
[docker-etl](https://github.com/mozilla/docker-etl/tree/main/jobs/webcompat-kb)

*Triage notes*

As long as the most recent DAG run is successful this job doesn't need to be triaged.

#### Owner

kberezina@mozilla.com
"""

default_args = {
    "owner": "kberezina@mozilla.com",
    "email": ["kberezina@mozilla.com", "webcompat-internal@mozilla.org"],
    "depends_on_past": False,
    "start_date": datetime(2023, 9, 26),
    "email_on_failure": True,
}


tags = [
    Tag.ImpactTier.tier_2,
]

every_fifteen_minutes = "*/15 * * * *"

bugzilla_token = Secret(
    deploy_type="env",
    deploy_target="BUGZILLA_API_KEY",
    secret="airflow-gke-secrets",
    key="webcompat_kb_secret__bugzilla_api_key",
)

with DAG(
    "webcompat_kb",
    default_args=default_args,
    max_active_runs=1,
    doc_md=DOCS,
    schedule_interval=every_fifteen_minutes,
    tags=tags,
    catchup=False,
) as dag:
    webcompat_kb_import = GKEPodOperator(
        task_id="webcompat_kb",
        arguments=[
            "python",
            "webcompat_kb/main.py",
            "--bq_project_id",
            "moz-fx-dev-dschubert-wckb",
            "--bq_dataset_id",
            "webcompat_knowledge_base",
        ],
        image="gcr.io/moz-fx-data-airflow-prod-88e0/webcompat-kb_docker_etl:latest",
        dag=dag,
        secrets=[
            bugzilla_token,
        ],
    )
