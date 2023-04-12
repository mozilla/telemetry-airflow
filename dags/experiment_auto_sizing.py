"""
Powers the [auto-sizing](https://github.com/mozilla/auto-sizing) tool
for computing experiment sizing information for various configurations.

*Triage notes*
TBD
"""

from airflow import DAG
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import timedelta, datetime
from operators.gcp_container_operator import GKEPodOperator
from utils.constants import ALLOWED_STATES, FAILED_STATES
from utils.tags import Tag

default_args = {
    "owner": "mwilliams@mozilla.com",
    "email": ["mwilliams@mozilla.com", "ascholtz@mozilla.com", "mbowerman@mozilla.com"],
    "depends_on_past": False,
    "start_date": datetime(2023, 4, 15),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
    "retry_delay": timedelta(minutes=30),
}

tags = [Tag.ImpactTier.tier_1]

with DAG(
        "experiment_auto_sizing",
        default_args=default_args,
        schedule_interval="0 6 * * 0", # 6am every Sunday, after Jetstream
        doc_md=__doc__,
        tags=tags,
) as dag:

    # Built from repo https://github.com/mozilla/auto-sizing
    auto_sizing_image = "gcr.io/moz-fx-data-experiments/auto-sizing:latest"

    auto_sizing_run = GKEPodOperator(
        task_id="auto_sizing_run",
        name="auto_sizing_run",
        image=auto_sizing_image,
        email=default_args["email"],
        arguments=[
            "--log-to-bigquery",
            "run-argo",
            "--bucket=auto-sizing",
            # the Airflow cluster doesn't have Compute Engine API access so pass in IP 
            # and certificate in order for the pod to connect to the Kubernetes cluster
            # running Jetstream/auto-sizing 
            "--cluster-ip={{ var.value.jetstream_cluster_ip }}",
            "--cluster-cert={{ var.value.jetstream_cluster_cert }}"],
        dag=dag,
    )

    wait_for_jetstream = ExternalTaskSensor(
        task_id="wait_for_jetstream",
        external_dag_id="jetstream",
        external_task_id="jetstream_run_config_changed",
        execution_delta=timedelta(hours=2),
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
        email_on_retry=False,
        dag=dag,
    )

    auto_sizing_run.set_upstream(wait_for_jetstream)
