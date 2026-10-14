"""
Deploy bigquery etl artifacts.

This DAG is triggered by CircleCI on merges to the `main` branch and by Jenkins after [schemas deploys](https://mozilla-hub.atlassian.net/wiki/spaces/SRE/pages/27920974/BigQuery+shared-prod#BigQuery(shared-prod)-JenkinsJobs).

SQL generation can optionally run during the tasks using the `generate_sql` DAG param, which is used primarily by Jenkins.

*Triage notes*

The DAG always re-deploys all bqetl views. So as long as the most recent DAG run
is successful the job can be considered healthy. This means previous failed DAG runs
can be ignored.

`publish_tables_and_views` runs many steps and produces a lot of output. When a step fails,
the command and the tail of its output are printed under `FAILURE SUMMARY` at the end of
the log and included in the task's exception message and alert email.

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

import textwrap
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import DagRun, Param
from airflow.operators.python import ShortCircuitOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.cncf.kubernetes.secret import Secret
from airflow.utils.state import DagRunState
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from kubernetes.client import models as k8s

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
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

tags = [Tag.ImpactTier.tier_2]

params = {
    "generate_sql": Param(
        default=False,
        type="boolean",
        description="Run sql generation before each publish task",
    ),
}

generate_sql_cmd = (
    "script/bqetl generate all --ignore derived_view_schemas --use-cloud-function=false"
)
# renders generate sql command if params.generate_sql is true, else empty string
generate_sql_cmd_template = (
    "{{ '" + generate_sql_cmd + " && ' if params.generate_sql else '' }}"
)

# `run` tees each step's output to a temp file and, if the step fails, appends the
# command and the tail of its output to a summary. `report_failures` prints the summary at the
# end of the log and writes it to /dev/termination-log, which replaces the default log tail in the
# Airflow exception and alert email. Note that the termination log is limited to 4KB.
run_step_helpers = textwrap.dedent(
    """
    failed=0
    summary=$(mktemp)
    run() {
      echo "+ $*"
      out=$(mktemp)
      "$@" 2>&1 | tee "$out"
      rc=${PIPESTATUS[0]}
      if [ "$rc" -ne 0 ]; then
        failed=1
        {
          echo "FAILED (exit $rc): $*"
          tail -n 12 "$out" | cut -c1-500
          echo
        } >> "$summary"
      fi
      rm -f "$out"
      return "$rc"
    }
    report_failures() {
      if [ "$failed" -ne 0 ]; then
        echo "==================== FAILURE SUMMARY ===================="
        cat "$summary"
        head -c 4000 "$summary" > /dev/termination-log
      fi
    }
    """
)
# SQL generation currently requires ~4 GB of memory.
generate_sql_container_resources = k8s.V1ResourceRequirements(
    requests={
        "memory": "{{ '6Gi' if params.generate_sql else '2Gi' }}",
        "cpu": "{{ '4' if params.generate_sql else '1' }}",
    },
)


def should_run_deployment(dag_id: str, generate_sql: bool) -> bool:
    """
    Run deploys if there are no other queued dag runs or if the generate_sql param is true.

    When used with ShortCircuitOperator, true means run downstream tasks and false means skip.
    """
    queued_runs = DagRun.find(dag_id=dag_id, state=DagRunState.QUEUED)
    print(f"Found {len(queued_runs)} queued dag runs for {dag_id}")
    return len(queued_runs) == 0 or generate_sql


bigeye_api_key_secret = Secret(
    deploy_type="env",
    deploy_target="BIGEYE_API_KEY",
    secret="airflow-gke-restricted-secrets",
    key="bqetl_artifact_deployment__bigeye_api_key",
)


with DAG(
    "bqetl_artifact_deployment",
    max_active_runs=1,
    default_args=default_args,
    schedule_interval=None,
    doc_md=__doc__,
    tags=tags,
    params=params,
    render_template_as_native_obj=True,  # So boolean params get rendered as booleans.
) as dag:
    docker_image = (
        "us-docker.pkg.dev/moz-fx-data-artifacts-prod/private-bigquery-etl/private-bigquery-etl:latest"
    )

    skip_if_queued_runs_exist = ShortCircuitOperator(
        task_id="skip_if_queued_runs_exist",
        ignore_downstream_trigger_rules=True,
        python_callable=should_run_deployment,
        op_kwargs={"dag_id": dag.dag_id, "generate_sql": "{{ params.generate_sql }}"},
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

    with TaskGroup(group_id="publish_static_tables") as publish_static_tables:
        for gcp_project in ("moz-fx-data-shared-prod", "mozdata"):
            GKEPodOperator(
                task_id=f"publish_static_tables_{gcp_project}",
                arguments=[
                    "script/bqetl",
                    "static",
                    "publish",
                    "--project_id",
                    gcp_project,
                ],
                image=docker_image,
            )

    publish_tables_and_views = GKEPodOperator(
        task_id="publish_tables_and_views",
        # No -x since `run` echoes each command and tracing its internals is noisy.
        cmds=["bash", "-c"],
        execution_timeout=timedelta(hours=6),
        arguments=[
            run_step_helpers
            + "{{ 'run " + generate_sql_cmd + " && ' if params.generate_sql else '' }}"
            + textwrap.dedent(
                """
                run script/bqetl generate derived_view_schemas --use-cloud-function=false
                run script/bqetl query initialize '*' --file=materialized_view.sql --skip_existing --project-id=moz-fx-data-shared-prod --project-id=moz-fx-data-experiments --project-id=moz-fx-data-marketing-prod --project-id=moz-fx-data-bq-people
                run script/bqetl deploy '*' --tables --views --use-cloud-function=false --table-skip-existing-schemas --table-force --ignore-dryrun-skip --view-add-managed-label --view-skip-authorized --project-id=moz-fx-data-shared-prod --project-id=moz-fx-data-experiments --project-id=moz-fx-data-marketing-prod --project-id=moz-fx-glam-prod --project-id=moz-fx-data-bq-people
                run script/bqetl view publish --add-managed-label --skip-authorized --project-id=moz-fx-data-shared-prod --target-project=mozdata --user-facing-only &&
                run script/bqetl view clean --skip-authorized --target-project=moz-fx-data-shared-prod &&
                run script/bqetl view clean --skip-authorized --target-project=moz-fx-data-experiments --project-id=moz-fx-data-experiments &&
                run script/bqetl view clean --skip-authorized --target-project=moz-fx-data-marketing-prod --project-id=moz-fx-data-marketing-prod &&
                run script/bqetl view clean --skip-authorized --target-project=moz-fx-glam-prod --project-id=moz-fx-glam-prod &&
                run script/bqetl view clean --skip-authorized --target-project=mozdata --user-facing-only &&
                run script/bqetl view clean --skip-authorized --target-project=moz-fx-data-bq-people --project-id=moz-fx-data-bq-people &&
                run script/publish_public_data_views --target-project=moz-fx-data-shared-prod &&
                run script/publish_public_data_views --target-project=mozdata
                report_failures
                exit $failed
                """
            )
        ],
        image=docker_image,
        container_resources=generate_sql_container_resources,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    publish_metadata = GKEPodOperator(
        task_id="publish_metadata",
        cmds=["bash", "-x", "-c"],
        arguments=[
            generate_sql_cmd_template
            + "script/bqetl metadata publish '*' --project_id=moz-fx-data-shared-prod && "
            "script/bqetl metadata publish '*' --project_id=mozdata && "
            "script/bqetl metadata publish '*' --project_id=moz-fx-data-marketing-prod && "
            "script/bqetl metadata publish '*' --project_id=moz-fx-data-experiments && "
            "script/bqetl metadata publish '*' --project_id=moz-fx-data-bq-people"
        ],
        image=docker_image,
        container_resources=generate_sql_container_resources,
    )

    publish_bigeye_monitors = GKEPodOperator(
        task_id="publish_bigeye_monitors",
        cmds=["bash", "-x", "-c"],
        arguments=[
            "script/bqetl monitoring deploy '*' --project_id=moz-fx-data-shared-prod"
        ],
        image=docker_image,
        secrets=[bigeye_api_key_secret],
    )

    trigger_initialize = TriggerDagRunOperator(
        task_id="trigger_initialize",
        trigger_dag_id="bqetl_artifact_initialize",
        wait_for_completion=False,
        conf={"generate_sql": "{{ params.generate_sql }}"},
    )

    trigger_dryrun = TriggerDagRunOperator(
        task_id="trigger_dryrun",
        trigger_dag_id="bqetl_dryrun",
        wait_for_completion=False,
    )

    # trigger lookml generation
    trigger_looker = TriggerDagRunOperator(
        task_id="trigger_looker", trigger_dag_id="looker", wait_for_completion=False
    )

    skip_if_queued_runs_exist.set_downstream(
        [
            publish_public_udfs,
            publish_persistent_udfs,
            publish_static_tables,
        ]
    )
    publish_tables_and_views.set_upstream(publish_public_udfs)
    publish_tables_and_views.set_upstream(publish_persistent_udfs)
    publish_tables_and_views.set_upstream(publish_static_tables)
    publish_metadata.set_upstream(publish_tables_and_views)
    publish_bigeye_monitors.set_upstream(publish_tables_and_views)
    # trigger dryrun
    # doesn't block downstream tasks
    trigger_dryrun.set_upstream(publish_tables_and_views)
    trigger_looker.set_upstream(publish_tables_and_views)
    trigger_initialize.set_upstream(publish_tables_and_views)
