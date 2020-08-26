# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import base64
import datetime
import json
import uuid

from airflow import models
from airflow.operators import PythonOperator

from airflow.contrib.hooks.gcp_api_base_hook import GoogleCloudBaseHook
from operators.bq_sensor import BigQuerySQLSensorOperator
from operators.gcp_container_operator import GKEPodOperator

DAG_OWNER = "rpierzina@mozilla.com"
DAG_EMAIL = ["rpierzina@mozilla.com"]

# We use a template for the test run UUID in the DAG. Because we base64 encode
# this query SQL before the template is rendered, we need to use a parameter
# and replace the test run UUID in burnham-bigquery.

# Test scenario test_labeled_counter_metrics: Verify that labeled_counter
# metric values reported by the Glean SDK across several documents from three
# different clients are correct.
TEST_LABELED_COUNTER_METRICS = """
WITH
  numbered AS (
  SELECT
    ROW_NUMBER() OVER (PARTITION BY document_id ORDER BY submission_timestamp) AS _n,
    *
  FROM
    `{project_id}.burnham_live.discovery_v1`
  WHERE
    submission_timestamp > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 3 HOUR)
    AND metrics.uuid.test_run = @burnham_test_run ),
  deduped AS (
  SELECT
    * EXCEPT(_n)
  FROM
    numbered
  WHERE
    _n = 1 )
SELECT
  technology_space_travel.key,
  SUM(technology_space_travel.value) AS value_sum
FROM
  deduped
CROSS JOIN
  UNNEST(metrics.labeled_counter.technology_space_travel) AS technology_space_travel
GROUP BY
  technology_space_travel.key
ORDER BY
  technology_space_travel.key
LIMIT
  20
"""


WANT_TEST_LABELED_COUNTER_METRICS = [
    {"key": "spore_drive", "value_sum": 13},
    {"key": "warp_drive", "value_sum": 18},
]

# Test scenario test_client_ids: Verify that the Glean SDK generated three
# distinct client IDs for three different clients.
TEST_CLIENT_IDS = """
SELECT
  COUNT(DISTINCT client_info.client_id) AS count_client_ids
FROM
  `{project_id}.burnham_live.discovery_v1`
WHERE
  submission_timestamp > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 3 HOUR)
  AND metrics.uuid.test_run = @burnham_test_run
LIMIT
  20
"""
WANT_TEST_CLIENT_IDS = [{"count_client_ids": 3}]

# Test scenario test_experiments: Verify that the Glean SDK correctly reports
# experiment information. The following query counts the number of documents
# submitted from a client which is not enrolled in any experiments, a client
# which is enrolled in the spore_drive experiment on branch tardigrade, and a
# client which is enrolled in the spore_drive experiment on branch
# tardigrade-dna.
TEST_EXPERIMENTS = """
WITH
  numbered AS (
  SELECT
    ROW_NUMBER() OVER (PARTITION BY document_id ORDER BY submission_timestamp) AS _n,
    *
  FROM
    `{project_id}.burnham_live.discovery_v1`
  WHERE
    submission_timestamp > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 3 HOUR)
    AND metrics.uuid.test_run = @burnham_test_run ),
  deduped AS (
  SELECT
    * EXCEPT(_n)
  FROM
    numbered
  WHERE
    _n = 1 ),
  base AS (
  SELECT
    ARRAY(
    SELECT
      CONCAT(key, ':', value.branch) AS key
    FROM
      UNNEST(ping_info.experiments)) AS experiments,
  FROM
    deduped
  LIMIT
    20 ),
  experiment_counts AS (
  SELECT
    experiment,
    COUNT(*) AS document_count
  FROM
    base,
    UNNEST(experiments) AS experiment
  GROUP BY
    experiment),
  no_experiments AS (
  SELECT
    'no_experiments' AS experiment,
    COUNT(*) AS document_count
  FROM
    base
  WHERE
    ARRAY_LENGTH(experiments) = 0 ),
  unioned AS (
  SELECT
    *
  FROM
    experiment_counts
  UNION ALL
  SELECT
    *
  FROM
    no_experiments )
SELECT
  *
FROM
  unioned
ORDER BY
  experiment
"""

WANT_TEST_EXPERIMENTS = [
    {"experiment": "no_experiments", "document_count": 2},
    {"experiment": "spore_drive:tardigrade", "document_count": 2},
    {"experiment": "spore_drive:tardigrade-dna", "document_count": 6},
]

# Test scenario test_glean_error_invalid_value: Verify that the Glean SDK
# correctly reports the number of times a metric was set to an invalid value.
TEST_GLEAN_ERROR_INVALID_VALUE = """
SELECT
  metrics.string.mission_identifier,
  metrics.labeled_counter.glean_error_invalid_value
FROM
  `{project_id}.burnham_live.discovery_v1`
WHERE
  submission_timestamp > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 3 HOUR)
  AND metrics.uuid.test_run = @burnham_test_run
  AND ARRAY_LENGTH(metrics.labeled_counter.glean_error_invalid_value) > 0
ORDER BY
  metrics.string.mission_identifier
LIMIT
  20
"""

WANT_TEST_GLEAN_ERROR_INVALID_VALUE = [
    {
        "mission_identifier": "MISSION E: ONE JUMP, ONE METRIC ERROR",
        "glean_error_invalid_value": [{"key": "mission.status", "value": 1}],
    }
]

SENSOR_TEMPLATE = """
SELECT
  COUNT(*) >= 10
FROM
  `{project_id}.burnham_live.discovery_v1`
WHERE
  submission_timestamp > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 3 HOUR)
  AND metrics.uuid.test_run = "{test_run}"
"""

DEFAULT_GCP_CONN_ID = "google_cloud_derived_datasets"
DEFAULT_GKE_LOCATION = "us-central1-a"
DEFAULT_GKE_CLUSTER_NAME = "bq-load-gke-1"
DEFAULT_GKE_NAMESPACE = "default"

BURNHAM_PLATFORM_URL = "https://incoming.telemetry.mozilla.org"

default_args = {
    "owner": DAG_OWNER,
    "email": DAG_EMAIL,
    "email_on_failure": True,
    "email_on_retry": True,
    "start_date": datetime.datetime(2020, 6, 15),
    "depends_on_past": False,
    "retries": 0,
}


def burnham_run(
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
    :param str burnham_test_run:                [Required] UUID for the test run
    :param str burnham_test_name:               [Required] Name for the test item
    :param List[str] burnham_missions:          [Required] List of mission identifiers

    :param Optional[str] burnham_spore_drive:   Interface for the spore-drive technology
    :param str gcp_conn_id:                     Airflow connection id for GCP access
    :param str gke_location:                    GKE cluster location
    :param str gke_cluster_name:                GKE cluster name
    :param str gke_namespace:                   GKE cluster namespace
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
        image="gcr.io/moz-fx-data-airflow-prod-88e0/burnham:latest",
        image_pull_policy="Always",
        env_vars=env_vars,
        arguments=burnham_missions,
        **kwargs
    )


def burnham_sensor(task_id, sql, gcp_conn_id=DEFAULT_GCP_CONN_ID, **kwargs):
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


def burnham_bigquery_run(
    task_id,
    project_id,
    burnham_test_run,
    burnham_test_scenarios,
    gcp_conn_id=DEFAULT_GCP_CONN_ID,
    gke_location=DEFAULT_GKE_LOCATION,
    gke_cluster_name=DEFAULT_GKE_CLUSTER_NAME,
    gke_namespace=DEFAULT_GKE_NAMESPACE,
    **kwargs
):
    """Create a new GKEPodOperator that runs the burnham-bigquery Docker image.

    :param str task_id:                 [Required] ID for the task
    :param str project_id:              [Required] Project ID where target table lives
    :param str burnham_test_run:        [Required] UUID for the test run
    :param str burnham_test_scenarios:  [Required] Encoded burnham test scenarios

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
        image="gcr.io/moz-fx-data-airflow-prod-88e0/burnham-bigquery:latest",
        image_pull_policy="Always",
        arguments=[
            "-vv",
            "--project-id",
            project_id,
            "--run-id",
            burnham_test_run,
            "--scenarios",
            burnham_test_scenarios,
            "--results-table",
            "burnham_derived.test_results_v1",
            "--log-url",
            "{{ task_instance.log_url }}",
        ],
        **kwargs
    )


with models.DAG(
    "burnham", schedule_interval="@daily", default_args=default_args,
) as dag:

    # Generate a UUID for this test run
    generate_burnham_test_run_uuid = PythonOperator(
        task_id="generate_burnham_test_run_uuid",
        python_callable=lambda: str(uuid.uuid4()),
    )
    burnham_test_run = '{{ task_instance.xcom_pull("generate_burnham_test_run_uuid") }}'

    # We cover multiple test scenarios with pings submitted from the following
    # clients, so they don't submit using a specific test name, but all share
    # the following value.
    burnham_test_name = "test_burnham"

    client1 = burnham_run(
        task_id="client1",
        burnham_test_run=burnham_test_run,
        burnham_test_name=burnham_test_name,
        burnham_missions=["MISSION G: FIVE WARPS, FOUR JUMPS", "MISSION C: ONE JUMP"],
        burnham_spore_drive="tardigrade",
        owner=DAG_OWNER,
        email=DAG_EMAIL,
    )
    client1.set_upstream(generate_burnham_test_run_uuid)

    client2 = burnham_run(
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
        owner=DAG_OWNER,
        email=DAG_EMAIL,
    )
    client2.set_upstream(generate_burnham_test_run_uuid)

    client3 = burnham_run(
        task_id="client3",
        burnham_test_run=burnham_test_run,
        burnham_test_name=burnham_test_name,
        burnham_missions=["MISSION A: ONE WARP", "MISSION B: TWO WARPS"],
        burnham_spore_drive=None,
        owner=DAG_OWNER,
        email=DAG_EMAIL,
    )
    client3.set_upstream(generate_burnham_test_run_uuid)

    project_id = "moz-fx-data-shared-prod"

    wait_for_data = burnham_sensor(
        task_id="wait_for_data",
        sql=SENSOR_TEMPLATE.format(
            project_id=project_id,
            test_run=burnham_test_run,
            test_name=burnham_test_name,
        ),
        timeout=60 * 60 * 1,
    )
    wait_for_data.set_upstream(
        [generate_burnham_test_run_uuid, client1, client2, client3]
    )

    burnham_test_scenarios = [
        {
            "name": "test_labeled_counter_metrics",
            "query": TEST_LABELED_COUNTER_METRICS.format(project_id=project_id),
            "want": WANT_TEST_LABELED_COUNTER_METRICS,
        },
        {
            "name": "test_client_ids",
            "query": TEST_CLIENT_IDS.format(project_id=project_id),
            "want": WANT_TEST_CLIENT_IDS,
        },
        {
            "name": "test_experiments",
            "query": TEST_EXPERIMENTS.format(project_id=project_id),
            "want": WANT_TEST_EXPERIMENTS,
        },
        {
            "name": "test_glean_error_invalid_value",
            "query": TEST_GLEAN_ERROR_INVALID_VALUE.format(project_id=project_id),
            "want": WANT_TEST_GLEAN_ERROR_INVALID_VALUE,
        },
    ]

    json_encoded = json.dumps(burnham_test_scenarios)
    utf_encoded = json_encoded.encode("utf-8")
    b64_encoded = base64.b64encode(utf_encoded).decode("utf-8")

    verify_data = burnham_bigquery_run(
        task_id="verify_data",
        project_id=project_id,
        burnham_test_run=burnham_test_run,
        burnham_test_scenarios=b64_encoded,
        owner=DAG_OWNER,
        email=DAG_EMAIL,
    )
    verify_data.set_upstream(wait_for_data)
