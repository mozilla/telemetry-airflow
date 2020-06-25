# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from airflow.contrib.hooks.gcp_api_base_hook import GoogleCloudBaseHook
from operators.bq_sensor import BigQuerySQLSensorOperator
from operators.gcp_container_operator import GKEPodOperator

DEFAULT_GCP_CONN_ID = "google_cloud_derived_datasets"
DEFAULT_GKE_LOCATION = "us-central1-a"
DEFAULT_GKE_CLUSTER_NAME = "bq-load-gke-1"
DEFAULT_GKE_NAMESPACE = "default"

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

    return GKEPodOperator(
        task_id=task_id,
        gcp_conn_id=gcp_conn_id,
        project_id=GoogleCloudBaseHook(gcp_conn_id=gcp_conn_id).project_id,
        location=gke_location,
        cluster_name=gke_cluster_name,
        namespace=gke_namespace,
        image="gcr.io/{{ var.value.gcr_project_id }}/burnham:latest",
        image_pull_policy="Always",
        env_vars=env_vars,
        arguments=burnham_missions,
        **kwargs
    )


def new_burnham_sensor(task_id, sql, gcp_conn_id=DEFAULT_GCP_CONN_ID, **kwargs):
    """Create a new BigQuerySQLSensorOperator that checks for burnham data.

    :param str task_id:                 [Required] ID for the task
    :param str sql:                     [Required] SQL for the sensor

    :param str gcp_conn_id:             Airflow connection id for GCP access
    :param Dict[str, Any] kwargs:       Additional kwargs for BigQuerySQLSensorOperator

    :return: BigQuerySQLSensorOperator
    """
    return BigQuerySQLSensorOperator(
        task_id=task_id,
        sql=sql,
        bigquery_conn_id=gcp_conn_id,
        use_legacy_sql=False,
        **kwargs
    )


def new_burnham_bigquery_operator(
    task_id,
    project_id,
    test_run_information,
    gcp_conn_id=DEFAULT_GCP_CONN_ID,
    gke_location=DEFAULT_GKE_LOCATION,
    gke_cluster_name=DEFAULT_GKE_CLUSTER_NAME,
    gke_namespace=DEFAULT_GKE_NAMESPACE,
    **kwargs
):
    """Create a new GKEPodOperator that runs the burnham-bigquery Docker image.

    :param str task_id:                 [Required] ID for the task
    :param str project_id:              [Required] Project ID where target table lives
    :param str test_run_information:    [Required] JSON-encoded test run information

    :param str gcp_conn_id:             Airflow connection id for GCP access
    :param str gke_location:            GKE cluster location
    :param str gke_cluster_name:        GKE cluster name
    :param str gke_namespace:           GKE cluster namespace
    :param Dict[str, Any] kwargs:       Additional kwargs for GKEPodOperator

    :return: GKEPodOperator
    """
    kwargs["name"] = kwargs.get("name", task_id.replace("_", "-"))

    return GKEPodOperator(
        task_id=task_id,
        gcp_conn_id=gcp_conn_id,
        project_id=GoogleCloudBaseHook(gcp_conn_id=gcp_conn_id).project_id,
        location=gke_location,
        cluster_name=gke_cluster_name,
        namespace=gke_namespace,
        image="gcr.io/{{ var.value.gcr_project_id }}/burnham-bigquery:latest",
        image_pull_policy="Always",
        arguments=[
            "--verbose",
            "--project-id={}".format(project_id),
            "--run={}".format(test_run_information),
        ],
        **kwargs
    )
