"""
Deploy bigquery etl artifacts.

This DAG is triggered by CircleCI on merges to the `main` branch and by Jenkins after [schemas deploys](https://mozilla-hub.atlassian.net/wiki/spaces/SRE/pages/27920974/BigQuery+shared-prod#BigQuery(shared-prod)-JenkinsJobs).

*Triage notes*

The DAG always re-deploys all bqetl views. So as long as the most recent DAG run
is successful the job can be considered healthy. This means previous failed DAG runs
can be ignored.

`publish_views` doesn't show any logs due to spitting out invalid unicode.

Logs can be viewed in the [GCP logs explorer](https://console.cloud.google.com/logs?project=moz-fx-data-airflow-gke-prod)
with the following query:
```
resource.type="k8s_container"
resource.labels.project_id="moz-fx-data-airflow-gke-prod"
resource.labels.location="us-west1"
resource.labels.cluster_name="workloads-prod-v1"
resource.labels.namespace_name="default"
resource.labels.pod_name=~"publish-views-.+"
severity>=DEFAULT
```
This link leads to a
prepopulated query for the last 12 hours: https://cloudlogging.app.goo.gl/vTs1R7fCMQnLxFtV8

To view logs of a specific task run, replace the pod_name value with the pod name that can be
found at the start of the logs in Airflow, e.g. `INFO - Found matching pod publish-views-abcd1234`.

To find logs for specific datasets/views, add a string to search for to the end of the query.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.utils.trigger_rule import TriggerRule

from operators.gcp_container_operator import GKEPodOperator
from utils.tags import Tag

default_args = {
    "owner": "ascholtz@mozilla.com",
    "email": [
        "ascholtz@mozilla.com",
        "telemetry-alerts@mozilla.com",
    ],
    "depends_on_past": False,
    "start_date": datetime(2022, 12, 6),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
    "retry_delay": timedelta(minutes=30),
}

tags = [Tag.ImpactTier.tier_1]

with DAG(
    "bqetl_artifact_deployment",
    max_active_runs=1,
    default_args=default_args,
    schedule_interval=None,
    doc_md=__doc__,
    tags=tags,
) as dag:
    docker_image = "gcr.io/moz-fx-data-airflow-prod-88e0/bigquery-etl:latest"

    publish_public_udfs = GKEPodOperator(
        task_id="publish_public_udfs",
        arguments=["script/publish_public_udfs"],
        image=docker_image,
    )

    publish_persistent_udfs = GKEPodOperator(
        task_id="publish_persistent_udfs",
        cmds=["bash", "-x", "-c"],
        arguments=[
            "script/publish_persistent_udfs --project-id=moz-fx-data-shared-prod && "
            "script/publish_persistent_udfs --project-id=mozdata"
        ],
        image=docker_image,
    )

    publish_new_tables = GKEPodOperator(
        task_id="publish_new_tables",
        cmds=["bash", "-x", "-c"],
        arguments=[
            "script/bqetl query initialize '*' --skip-existing --project-id=moz-fx-data-shared-prod && "
            "script/bqetl query initialize '*' --skip-existing --project-id=moz-fx-data-experiments && "
            "script/bqetl query initialize '*' --skip-existing --project-id=moz-fx-data-marketing-prod && "
            "script/bqetl query initialize '*' --skip-existing --project-id=data-observability-dev && "
            "script/bqetl query schema update '*' --use-cloud-function=false --ignore-dryrun-skip --project-id=moz-fx-data-shared-prod && "
            "script/bqetl query schema deploy '*' --use-cloud-function=false --force --ignore-dryrun-skip --project-id=moz-fx-data-shared-prod && "
            "script/bqetl query schema update '*' --use-cloud-function=false --ignore-dryrun-skip --project-id=moz-fx-data-experiments && "
            "script/bqetl query schema deploy '*' --use-cloud-function=false --force --ignore-dryrun-skip --project-id=moz-fx-data-experiments && "
            "script/bqetl query schema update '*' --use-cloud-function=false --ignore-dryrun-skip --project-id=moz-fx-data-marketing-prod && "
            "script/bqetl query schema deploy '*' --use-cloud-function=false --force --ignore-dryrun-skip --project-id=moz-fx-data-marketing-prod && "
            "script/bqetl query schema update '*' --use-cloud-function=false --ignore-dryrun-skip --project-id=moz-fx-data-glam-prod-fca7 && "
            "script/bqetl query schema deploy '*' --use-cloud-function=false --force --ignore-dryrun-skip --project-id=moz-fx-data-glam-prod-fca7 && "
            "script/bqetl query schema update '*' --use-cloud-function=false --ignore-dryrun-skip --project-id=data-observability-dev && "
            "script/bqetl query schema deploy '*' --use-cloud-function=false --force --ignore-dryrun-skip --project-id=data-observability-dev"
        ],
        image=docker_image,
    )

    publish_views = GKEPodOperator(
        task_id="publish_views",
        cmds=["bash", "-x", "-c"],
        arguments=[
            "script/bqetl view publish --add-managed-label --skip-authorized --project-id=moz-fx-data-shared-prod && "
            "script/bqetl view publish --add-managed-label --skip-authorized --project-id=moz-fx-data-experiments && "
            "script/bqetl view publish --add-managed-label --skip-authorized --project-id=moz-fx-data-marketing-prod && "
            "script/bqetl view publish --add-managed-label --skip-authorized --project-id=moz-fx-data-glam-prod-fca7 && "
            "script/bqetl view publish --add-managed-label --skip-authorized --project-id=data-observability-dev && "
            "script/bqetl view publish --add-managed-label --skip-authorized --project-id=moz-fx-data-shared-prod --target-project=mozdata --user-facing-only && "
            "script/bqetl view clean --skip-authorized --target-project=moz-fx-data-shared-prod && "
            "script/bqetl view clean --skip-authorized --target-project=moz-fx-data-experiments --project-id=moz-fx-data-experiments && "
            "script/bqetl view clean --skip-authorized --target-project=moz-fx-data-marketing-prod --project-id=moz-fx-data-marketing-prod && "
            "script/bqetl view clean --skip-authorized --target-project=moz-fx-data-glam-prod-fca7 --project-id=moz-fx-data-glam-prod-fca7 && "
            "script/bqetl view clean --skip-authorized --target-project=data-observability-dev --project-id=data-observability-dev && "
            "script/bqetl view clean --skip-authorized --target-project=mozdata --user-facing-only && "
            "script/publish_public_data_views --target-project=moz-fx-data-shared-prod && "
            "script/publish_public_data_views --target-project=mozdata"
        ],
        image=docker_image,
        get_logs=False,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    publish_metadata = GKEPodOperator(
        task_id="publish_metadata",
        cmds=["bash", "-x", "-c"],
        arguments=[
            "script/bqetl metadata publish '*' --project_id=moz-fx-data-shared-prod && "
            "script/bqetl metadata publish '*' --project_id=mozdata && "
            "script/bqetl metadata publish '*' --project_id=moz-fx-data-marketing-prod && "
            "script/bqetl metadata publish '*' --project_id=moz-fx-data-experiments && "
            "script/bqetl metadata publish '*' --project_id=data-observability-dev"
        ],
        image=docker_image,
    )

    publish_views.set_upstream(publish_public_udfs)
    publish_views.set_upstream(publish_persistent_udfs)
    publish_views.set_upstream(publish_new_tables)
    publish_metadata.set_upstream(publish_views)
