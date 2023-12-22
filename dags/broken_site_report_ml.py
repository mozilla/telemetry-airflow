from datetime import datetime

from airflow import DAG

from utils.gcp import gke_command
from utils.tags import Tag

DOCS = """
### ML classification of broken site reports

#### Description

Runs a Docker image that periodically sends a batch of reports
to bugbug http service for classification and stores results in BQ.

The container is defined in
[docker-etl](https://github.com/mozilla/docker-etl/tree/main/jobs/broken-site-report-ml)

#### Owner

kberezina@mozilla.com
"""

default_args = {
    "owner": "kberezina@mozilla.com",
    "email": ["kberezina@mozilla.com", "webcompat-internal@mozilla.org"],
    "depends_on_past": False,
    "start_date": datetime(2023, 12, 21),
    "email_on_failure": True,
}


tags = [
    Tag.ImpactTier.tier_2,
]

every_fifteen_minutes = "*/15 * * * *"

with DAG(
    "broken_site_report_ml",
    default_args=default_args,
    max_active_runs=1,
    doc_md=DOCS,
    schedule_interval=every_fifteen_minutes,
    tags=tags,
    catchup=False,
) as dag:
    broken_site_report_ml = gke_command(
        task_id="broken_site_report_ml",
        command=[
            "python",
            "broken_site_report_ml/main.py",
            "--bq_project_id",
            "moz-fx-dev-dschubert-wckb",
            "--bq_dataset_id",
            "webcompat_user_reports",
        ],
        docker_image="gcr.io/moz-fx-data-airflow-prod-88e0/broken-site-report-ml_docker_etl:latest",
        dag=dag,
    )
