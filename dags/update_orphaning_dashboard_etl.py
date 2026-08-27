"""
Powers https://telemetry.mozilla.org/update-orphaning/.

See the source code in docker-etl for more details:
https://github.com/mozilla/docker-etl/tree/main/jobs/update-orphaning-dashboard.
"""

from datetime import datetime, timedelta

from airflow import DAG

from operators.gcp_container_operator import GKEPodOperator
from utils.constants import DS_WEEKLY
from utils.tags import Tag

default_args = {
    "owner": "akomar@mozilla.com",
    "depends_on_past": False,
    "start_date": datetime(2019, 10, 12),
    "email": [
        "telemetry-alerts@mozilla.com",
        "ahabibi@mozilla.com",
        "aborondo@mozilla.com",
        "akomar@mozilla.com",
    ],
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
}

tags = [Tag.ImpactTier.tier_3]

# run every Monday to maintain compatibility with legacy ATMO schedule
dag = DAG(
    "update_orphaning_dashboard_etl",
    default_args=default_args,
    schedule_interval="0 2 * * MON",
    doc_md=__doc__,
    tags=tags,
)

update_orphaning_dashboard_etl = GKEPodOperator(
        task_id="update_orphaning_dashboard_etl",
        arguments=[
            "--run-date",
            DS_WEEKLY,
            "--output-bucket",
            "moz-fx-data-static-websit-8565-analysis-output",
            "--output-prefix",
            "app-update/data/out-of-date/",
            "--billing-project",
            "mozdata",
        ],
        image="us-docker.pkg.dev/moz-fx-data-artifacts-prod/docker-etl/update-orphaning-dashboard:latest",
        dag=dag,
    )
