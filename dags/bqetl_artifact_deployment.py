"""
Nightly deploy of bigquery etl views.

*Triage notes*

The DAG always re-deploys all bqetl views. So as long as the most recent DAG run
is successful the job can be considered healthy. This means previous failed DAG runs
can be ignored or marked as successful.

`publish_views` doesn't show any logs due to spitting out invalid unicode.
To view relevant errors, either check the [Kubernetes pods](https://console.cloud.google.com/kubernetes/workload/overview?project=moz-fx-data-airflow-gke-prod)
or go to [GCP console and check the query "Project History" tab at the bottom of the page](https://console.cloud.google.com/bigquery?project=moz-fx-data-airflow-gke-prod&ws=!1m0).
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
    default_args=default_args,
    schedule_interval="@daily",
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
            "script/bqetl generate all --use-cloud-function=false && "
            "script/bqetl query initialize '*' --skip-existing --project-id=moz-fx-data-shared-prod && "
            "script/bqetl query initialize '*' --skip-existing --project-id=moz-fx-data-experiments && "
            "script/bqetl query initialize '*' --skip-existing --project-id=moz-fx-data-marketing-prod && "
            "script/bqetl query schema update '*' --use-cloud-function=false --ignore-dryrun-skip --project-id=moz-fx-data-shared-prod && "
            "script/bqetl query schema deploy '*' --use-cloud-function=false --force --ignore-dryrun-skip --project-id=moz-fx-data-shared-prod && "
            "script/bqetl query schema update '*' --use-cloud-function=false --ignore-dryrun-skip --project-id=moz-fx-data-experiments && "
            "script/bqetl query schema deploy '*' --use-cloud-function=false --force --ignore-dryrun-skip --project-id=moz-fx-data-experiments && "
            "script/bqetl query schema update '*' --use-cloud-function=false --ignore-dryrun-skip --project-id=moz-fx-data-marketing-prod && "
            "script/bqetl query schema deploy '*' --use-cloud-function=false --force --ignore-dryrun-skip --project-id=moz-fx-data-marketing-prod && "
            "script/bqetl query schema update '*' --use-cloud-function=false --ignore-dryrun-skip --project-id=moz-fx-data-glam-prod-fca7 && "
            "script/bqetl query schema deploy '*' --use-cloud-function=false --force --ignore-dryrun-skip --project-id=moz-fx-data-glam-prod-fca7"
        ],
        image=docker_image,
    )

    publish_views = GKEPodOperator(
        task_id="publish_views",
        cmds=["bash", "-x", "-c"],
        arguments=[
            "script/bqetl generate all --use-cloud-function=false && "
            "script/bqetl view publish --add-managed-label --skip-authorized --target-project=moz-fx-data-shared-prod && "
            "script/bqetl view publish --add-managed-label --skip-authorized --target-project=moz-fx-data-experiments --project-id=moz-fx-data-experiments && "
            "script/bqetl view publish --add-managed-label --skip-authorized --target-project=moz-fx-data-marketing-prod --project-id=moz-fx-data-marketing-prod && "
            "script/bqetl view publish --add-managed-label --skip-authorized --target-project=mozdata --user-facing-only && "
            "script/bqetl view clean --skip-authorized --target-project=moz-fx-data-shared-prod && "
            "script/bqetl view clean --skip-authorized --target-project=moz-fx-data-experiments --project-id=moz-fx-data-experiments && "
            "script/bqetl view clean --skip-authorized --target-project=moz-fx-data-marketing-prod --project-id=moz-fx-data-marketing-prod && "
            "script/bqetl view clean --skip-authorized --target-project=moz-fx-data-glam-prod-fca7 --project-id=moz-fx-data-glam-prod-fca7 && "
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
            "script/bqetl generate all --use-cloud-function=false && "
            "script/bqetl metadata publish '*' --project_id=moz-fx-data-shared-prod && "
            "script/bqetl metadata publish '*' --project_id=mozdata && "
            "script/bqetl metadata publish '*' --project_id=moz-fx-data-marketing-prod && "
            "script/bqetl metadata publish '*' --project_id=moz-fx-data-experiments"
        ],
        image=docker_image,
    )

    publish_views.set_upstream(publish_public_udfs)
    publish_views.set_upstream(publish_persistent_udfs)
    publish_views.set_upstream(publish_new_tables)
    publish_metadata.set_upstream(publish_views)
