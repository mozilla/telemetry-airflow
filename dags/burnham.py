# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import datetime

from airflow import models
from airflow.contrib.hooks.gcp_api_base_hook import GoogleCloudBaseHook
from operators.bq_sensor import BigQuerySQLSensorOperator
from operators.gcp_container_operator import GKEPodOperator

DEFAULT_GCP_CONN_ID = "google_cloud_derived_datasets"
DEFAULT_GKE_LOCATION = "us-central1-a"
DEFAULT_GKE_CLUSTER_NAME = "bq-load-gke-1"
DEFAULT_GKE_NAMESPACE = "default"

DAG_OWNER = "rpierzina@mozilla.com"
DAG_EMAIL = ["telemetry-alerts@mozilla.com", "rpierzina@mozilla.com"]

BURNHAM_PLATFORM_URL = "https://incoming.telemetry.mozilla.org"


def new_burnham_operator(
    task_id,
    burnham_test_run,
    burnham_test_name,
    burnham_missions,
    burnham_spore_drive=None,
    gcp_conn_id=DEFAULT_GCP_CONN_ID,
    gke_location=DEFAULT_GKE_LOCATION,
    gke_cluster_name=DEFAULT_GKE_CLUSTER_NAME,
    gke_namespace=DEFAULT_GKE_NAMESPACE,
    **kwargs
):
    """Create a new GKEPodOperator that runs the burnham Docker image.

    :param str task_id:                         [Required] ID for the task
    :param str burnham_test_run:                [Required] ID for the test run
    :param str burnham_test_name:               [Required] Name for the test item
    :param List[str] burnham_missions:          [Required] List of mission identifiers

    :param Optional[str] burnham_spore_drive:   Interface for the spore-drive technology
    :param str gcp_conn_id:                     Airflow connection id for GCP access
    :param str gke_location:                    GKE cluster location
    :param str gke_cluster_name:                GKE cluster name
    :param str gke_namespace:                   GKE cluster namespace
    :param str docker_image:                    Docker image to use
    :param Dict[str, Any] kwargs:               Additional kwargs for GKEPodOperator

    :return: GKEPodOperator
    """
    kwargs["name"] = kwargs.get("name", task_id.replace("_", "-"))

    env_vars = {
        "BURNHAM_PLATFORM_URL": BURNHAM_PLATFORM_URL,
        "BURNHAM_TEST_RUN": burnham_test_run,
        "BURNHAM_TEST_NAME": burnham_test_name,
    }

    if burnham_spore_drive is not None:
        env_vars["BURNHAM_SPORE_DRIVE"] = burnham_spore_drive

    # TODO: Update project for Docker image
    return GKEPodOperator(
        task_id=task_id,
        gcp_conn_id=gcp_conn_id,
        project_id=GoogleCloudBaseHook(gcp_conn_id=gcp_conn_id).project_id,
        location=gke_location,
        cluster_name=gke_cluster_name,
        namespace=gke_namespace,
        image="gcr.io/<PROJECT>/burnham:latest",
        image_pull_policy="Always",
        env_vars=env_vars,
        arguments=burnham_missions,
        owner=DAG_OWNER,
        email=DAG_EMAIL,
        **kwargs
    )


default_args = {
    "owner": DAG_OWNER,
    "email": DAG_EMAIL,
    "email_on_failure": True,
    "email_on_retry": True,
    "start_date": datetime.datetime(2020, 6, 15),
    "depends_on_past": False,
    "retries": 0,
}

with models.DAG(
    "burnham", schedule_interval="@daily", default_args=default_args,
) as dag:

    # TODO: Update test information
    burnham_test_run = "TEST RUN"
    burnham_test_name = "TEST NAME"

    client1 = new_burnham_operator(
        task_id="client1",
        burnham_test_run=burnham_test_run,
        burnham_test_name=burnham_test_name,
        burnham_missions=["MISSION G: FIVE WARPS, FOUR JUMPS", "MISSION C: ONE JUMP"],
        burnham_spore_drive="tardigrade",
    )

    client2 = new_burnham_operator(
        task_id="client2",
        burnham_test_run=burnham_test_run,
        burnham_test_name=burnham_test_name,
        burnham_missions=[
            "MISSION A: ONE WARP",
            "MISSION B: TWO WARPS",
            "MISSION D: TWO JUMPS",
            "MISSION E: ONE JUMP, ONE METRIC ERROR",
            "MISSION F: TWO WARPS, ONE JUMP",
            "MISSION G: FIVE WARPS, FOUR JUMPS",
        ],
        burnham_spore_drive="tardigrade-dna",
    )

    client3 = new_burnham_operator(
        task_id="client3",
        burnham_test_run=burnham_test_run,
        burnham_test_name=burnham_test_name,
        burnham_missions=["MISSION A: ONE WARP", "MISSION B: TWO WARPS"],
        burnham_spore_drive=None,
    )

    # TODO: Update SQL for Sensor
    # Wait for 10 pings with this test run ID in live tables
    wait_for_data = BigQuerySQLSensorOperator(
        task_id="wait_for_data",
        sql="TODO",
        bigquery_conn_id=DEFAULT_GCP_CONN_ID,
        use_legacy_sql=False,
    )

    # TODO: Run another GKEPodOperator to verify the data
    # Query for space_travel metrics and one client error
    # expected_metrics = {
    #     "labeled_counter": {
    #         "technology.space_travel": {"spore_drive": 13, "warp_drive": 18}
    #     }
    # }
    verify_data = GKEPodOperator()
    verify_data.set_upstream(wait_for_data)
