"""
Nightly deploy of bigquery etl views.

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
from airflow.models import DagRun
from airflow.operators.python import ShortCircuitOperator
from airflow.providers.cncf.kubernetes.secret import Secret
from airflow.utils.state import DagRunState
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


def check_for_queued_runs(dag_id: str) -> bool:
    queued_runs = DagRun.find(dag_id=dag_id, state=DagRunState.QUEUED)
    print(f"Found {len(queued_runs)} queued dag runs for {dag_id}")
    return len(queued_runs) == 0

bigeye_api_key_secret = Secret(
    deploy_type="env",
    deploy_target="BIGEYE_API_KEY",
    secret="airflow-gke-secrets",
    key="bqetl_artifact_deployment__bigeye_api_key",
)


with DAG(
    "bqetl_artifact_deployment",
    max_active_runs=1,
    default_args=default_args,
    schedule_interval="@daily",
    doc_md=__doc__,
    tags=tags,
) as dag:
    docker_image = "gcr.io/moz-fx-data-airflow-prod-88e0/bigquery-etl:latest"

    skip_if_queued_runs_exist = ShortCircuitOperator(
        task_id="skip_if_queued_runs_exist",
        ignore_downstream_trigger_rules=True,
        python_callable=check_for_queued_runs,
        op_kwargs={"dag_id": dag.dag_id},
    )

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
            "script/bqetl query initialize '*' --skip-existing --project-id=moz-fx-data-bq-people && "
            "script/bqetl query schema update '*' --use-cloud-function=false --ignore-dryrun-skip --project-id=moz-fx-data-shared-prod && "
            "script/bqetl query schema deploy '*' --use-cloud-function=false --force --ignore-dryrun-skip --project-id=moz-fx-data-shared-prod && "
            "script/bqetl query schema update '*' --use-cloud-function=false --ignore-dryrun-skip --project-id=moz-fx-data-experiments && "
            "script/bqetl query schema deploy '*' --use-cloud-function=false --force --ignore-dryrun-skip --project-id=moz-fx-data-experiments && "
            "script/bqetl query schema update '*' --use-cloud-function=false --ignore-dryrun-skip --project-id=moz-fx-data-marketing-prod && "
            "script/bqetl query schema deploy '*' --use-cloud-function=false --force --ignore-dryrun-skip --project-id=moz-fx-data-marketing-prod && "
            "script/bqetl query schema update '*' --use-cloud-function=false --ignore-dryrun-skip --project-id=moz-fx-data-glam-prod-fca7 && "
            "script/bqetl query schema deploy '*' --use-cloud-function=false --force --ignore-dryrun-skip --project-id=moz-fx-data-glam-prod-fca7 && "
            "script/bqetl query schema update '*' --use-cloud-function=false --ignore-dryrun-skip --project-id=moz-fx-data-bq-people && "
            "script/bqetl query schema deploy '*' --use-cloud-function=false --force --ignore-dryrun-skip --project-id=moz-fx-data-bq-people"
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
            "script/bqetl view publish --add-managed-label --skip-authorized --project-id=moz-fx-data-shared-prod --target-project=mozdata --user-facing-only && "
            "script/bqetl view publish --add-managed-label --skip-authorized --project-id=moz-fx-data-bq-people && "
            "script/bqetl view clean --skip-authorized --target-project=moz-fx-data-shared-prod && "
            "script/bqetl view clean --skip-authorized --target-project=moz-fx-data-experiments --project-id=moz-fx-data-experiments && "
            "script/bqetl view clean --skip-authorized --target-project=moz-fx-data-marketing-prod --project-id=moz-fx-data-marketing-prod && "
            "script/bqetl view clean --skip-authorized --target-project=moz-fx-data-glam-prod-fca7 --project-id=moz-fx-data-glam-prod-fca7 && "
            "script/bqetl view clean --skip-authorized --target-project=mozdata --user-facing-only && "
            "script/bqetl view clean --skip-authorized --target-project=moz-fx-data-bq-people --project-id=moz-fx-data-bq-people && "
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
            "script/bqetl metadata publish '*' --project_id=moz-fx-data-bq-people"
        ],
        image=docker_image,
    )

    publish_bigeye_monitors = GKEPodOperator(
        task_id="publish_bigeye_monitors",
        cmds=["bash", "-x", "-c"],
        arguments=[
            "script/bqetl monitoring deploy '*' --project_id=moz-fx-data-shared-prod"
        ],
        image=docker_image,
        secrets=[bigeye_api_key_secret]
    )

    skip_if_queued_runs_exist.set_downstream(
        [
            publish_public_udfs,
            publish_persistent_udfs,
            publish_new_tables,
        ]
    )
    publish_views.set_upstream(publish_public_udfs)
    publish_views.set_upstream(publish_persistent_udfs)
    publish_views.set_upstream(publish_new_tables)
    publish_metadata.set_upstream(publish_views)
    publish_bigeye_monitors.set_upstream(publish_views)
