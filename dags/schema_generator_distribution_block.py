from datetime import datetime, timedelta

from airflow import DAG

from operators.gcp_container_operator import GKEPodOperator
from utils.tags import Tag

DOCS = """\
# schema_generator_distribution_block

*Triage notes*

Distribution metrics are currently blocked from being added to events and baseline ping schemas
(See [DENG-10606](https://mozilla-hub.atlassian.net/browse/DENG-10606)).
This DAG will fail if there are any distribution metrics defined in any metrics.yaml that
is sent in events or baseline pings.

Look at the logs to find the apps causing the failure and look at the recently added metrics for
the app to find the specific metrics and their owners.  It's possible that the metrics were
erroneously added to these pings.  Resolve by either removing the metrics or moving them
to the metrics ping.
"""

tags = [Tag.ImpactTier.tier_2]

with DAG(
    "schema_generator_distribution_block",
    doc_md=DOCS,
    schedule_interval="0 10 * * *",
    tags=tags,
    default_args={
        "owner": "bewu@mozilla.com",
        "depends_on_past": False,
        "start_date": datetime(2026, 2, 24),
        "email_on_failure": True,
        "email_on_retry": True,
        "retries": 1,
        "retry_delay": timedelta(minutes=30),
    },
) as dag:
    schema_generator_dist_block = GKEPodOperator(
        email=[
            "bewu@mozilla.com",
            "telemetry-alerts@mozilla.com",
        ],
        task_id="dist-block-check",
        image="us-docker.pkg.dev/moz-fx-data-artifacts-prod/mozilla-schema-generator/mozilla-schema-generator:latest",
        dag=dag,
        cmds=["mozilla-schema-generator"],
        arguments=["check-blocked-distribution-metrics"],
    )
