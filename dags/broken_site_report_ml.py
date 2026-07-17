from datetime import datetime

from airflow import DAG

from operators.gcp_container_operator import GKEPodOperator
from utils.tags import Tag

DOCS = """
### ML classification of broken site reports

#### Description

Runs a Docker image that does the following:

1. Translates incoming broken sites reports to English with ML.TRANSLATE.
2. Classifies translated reports as valid/invalid using [bugbug](https://github.com/mozilla/bugbug).
3. Stores translation and classification results in BQ.

The container is defined in
[docker-etl](https://github.com/mozilla/docker-etl/tree/main/jobs/broken-site-report-ml)

*Triage notes*

As long as the most recent DAG run is successful this job doesn't need to be triaged.

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
    broken_site_report_ml = GKEPodOperator(
        task_id="broken_site_report_ml",
        arguments=[
            "python",
            "broken_site_report_ml/main.py",
            "--bq_project_id",
            "moz-fx-dev-dschubert-wckb",
            "--bq_dataset_id",
            "webcompat_user_reports",
        ],
        image="us-docker.pkg.dev/moz-fx-data-artifacts-prod/docker-etl/broken-site-report-ml:latest",
        dag=dag,
    )
