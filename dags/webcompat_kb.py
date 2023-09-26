from datetime import datetime

from airflow import DAG
from utils.gcp import gke_command
from utils.tags import Tag

DOCS = """
### Bugzilla to BigQuery import

#### Description

Runs a Docker image that fetches bugzilla bugs from
Web Compatibility > Knowledge Base component, as well as their core
bugs dependencies and breakage reports and stores them in BQ.

The container is defined in
[docker-etl](https://github.com/mozilla/docker-etl/tree/main/jobs/webcompat-kb)

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

with DAG(
    "webcompat_kb",
    default_args=default_args,
    doc_md=DOCS,
    schedule_interval=every_fifteen_minutes,
    tags=tags,
    catchup=False,
) as dag:
    webcompat_kb_import = gke_command(
        task_id="webcompat_kb",
        command=[
            "python",
            "webcompat_kb/main.py",
            "--bq_project_id",
            "moz-fx-dev-dschubert-wckb",
            "--bq_dataset_id",
            "webcompat_knowledge_base",
        ],
        docker_image="gcr.io/moz-fx-data-airflow-prod-88e0/webcompat-kb_docker_etl:latest",
        dag=dag,
    )
