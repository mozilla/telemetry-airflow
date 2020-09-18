# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import base64
import datetime
import json
import uuid

from airflow import models
from airflow.contrib.hooks.gcp_api_base_hook import GoogleCloudBaseHook
from airflow.operators import PythonOperator
from operators.bq_sensor import BigQuerySQLSensorOperator
from operators.gcp_container_operator import GKEPodOperator

DAG_OWNER = "rpierzina@mozilla.com"
DAG_EMAIL = ["glean-team@mozilla.com", "rpierzina@mozilla.com"]

PROJECT_ID = "moz-fx-data-shared-prod"

# We use a template for the test run UUID in the DAG. Because we base64 encode
# this query SQL before the template is rendered, we need to use a parameter
# and replace the test run UUID in burnham-bigquery.

# Live tables are not guaranteed to be deduplicated. To ensure reproducibility,
# we need to deduplicate Glean pings produced by burnham for these tests.
WITH_DEDUPED_TABLE = """
WITH
  numbered AS (
  SELECT
    ROW_NUMBER() OVER (PARTITION BY document_id ORDER BY submission_timestamp) AS _n,
    *
  FROM
    `{project_id}.burnham_live.{table}`
  WHERE
    submission_timestamp BETWEEN TIMESTAMP_SUB(@burnham_execution_date, INTERVAL 1 HOUR)
    AND TIMESTAMP_ADD(@burnham_execution_date, INTERVAL 3 HOUR)
    AND metrics.uuid.test_run = @burnham_test_run ),
  deduped AS (
  SELECT
    * EXCEPT(_n)
  FROM
    numbered
  WHERE
    _n = 1 )"""

WITH_DISCOVERY_V1_DEDUPED = WITH_DEDUPED_TABLE.format(
    project_id=PROJECT_ID, table="discovery_v1"
)

WITH_STARBASE46_V1_DEDUPED = WITH_DEDUPED_TABLE.format(
    project_id=PROJECT_ID, table="starbase46_v1"
)

WITH_SPACE_SHIP_READY_V1_DEDUPED = WITH_DEDUPED_TABLE.format(
    project_id=PROJECT_ID, table="space_ship_ready_v1"
)

# Test scenario test_labeled_counter_metrics: Verify that labeled_counter
# metric values reported by the Glean SDK across several documents from three
# different clients are correct.
TEST_LABELED_COUNTER_METRICS = f"""{WITH_DISCOVERY_V1_DEDUPED}
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
TEST_CLIENT_IDS = f"""
SELECT
  COUNT(DISTINCT client_info.client_id) AS count_client_ids
FROM
  `{PROJECT_ID}.burnham_live.discovery_v1`
WHERE
  submission_timestamp BETWEEN TIMESTAMP_SUB(@burnham_execution_date, INTERVAL 1 HOUR)
  AND TIMESTAMP_ADD(@burnham_execution_date, INTERVAL 3 HOUR)
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
TEST_EXPERIMENTS = f"""{WITH_DISCOVERY_V1_DEDUPED},
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

# Test scenario test_glean_error_invalid_overflow: Verify that the Glean SDK
# correctly reports the number of times a string metric was set to a value that
# exceeds the maximum string length measured in the number of bytes when the
# string is encoded in UTF-8.
TEST_GLEAN_ERROR_INVALID_OVERFLOW = f"""{WITH_DISCOVERY_V1_DEDUPED}
SELECT
  metrics.string.mission_identifier,
  metrics.labeled_counter.glean_error_invalid_overflow
FROM
  deduped
WHERE
  ARRAY_LENGTH(metrics.labeled_counter.glean_error_invalid_overflow) > 0
ORDER BY
  metrics.string.mission_identifier
LIMIT
  20
"""

WANT_TEST_GLEAN_ERROR_INVALID_OVERFLOW = [
    {
        "mission_identifier": "MISSION E: ONE JUMP, ONE METRIC ERROR",
        "glean_error_invalid_overflow": [{"key": "mission.status", "value": 1}],
    }
]

# Test scenario test_starbase46_ping: Verify that the Glean SDK and the
# Data Platform support custom pings using the numbered naming scheme
TEST_STARBASE46_PING = f"""{WITH_STARBASE46_V1_DEDUPED}
SELECT
  COUNT(*) AS count_documents
FROM
  deduped
"""

WANT_TEST_STARBASE46_PING = [{"count_documents": 1}]

# Test scenario test_space_ship_ready_ping: Verify that the Glean SDK and the
# Data Platform support custom pings using the kebab-case naming scheme
TEST_SPACE_SHIP_READY_PING = f"""{WITH_SPACE_SHIP_READY_V1_DEDUPED}
SELECT
  COUNT(*) AS count_documents
FROM
  deduped
"""

WANT_TEST_SPACE_SHIP_READY_PING = [{"count_documents": 3}]

# Sensor templates for the different tables
DISCOVERY_SENSOR_TEMPLATE = """
SELECT
  COUNT(*) >= 10
FROM
  `{project_id}.burnham_live.discovery_v1`
WHERE
  submission_timestamp BETWEEN TIMESTAMP_SUB("{{ execution_date.isoformat() }}", INTERVAL 1 HOUR)
  AND TIMESTAMP_ADD("{{ execution_date.isoformat() }}", INTERVAL 3 HOUR)
  AND metrics.uuid.test_run = "{test_run}"
"""

STARBASE46_SENSOR_TEMPLATE = """
SELECT
  COUNT(*) >= 1
FROM
  `{project_id}.burnham_live.starbase46_v1`
WHERE
  submission_timestamp BETWEEN TIMESTAMP_SUB("{{ execution_date.isoformat() }}", INTERVAL 1 HOUR)
  AND TIMESTAMP_ADD("{{ execution_date.isoformat() }}", INTERVAL 3 HOUR)
  AND metrics.uuid.test_run = "{test_run}"
"""

SPACE_SHIP_READY_SENSOR_TEMPLATE = """
SELECT
  COUNT(*) >= 3
FROM
  `{project_id}.burnham_live.space_ship_ready_v1`
WHERE
  submission_timestamp BETWEEN TIMESTAMP_SUB("{{ execution_date.isoformat() }}", INTERVAL 1 HOUR)
  AND TIMESTAMP_ADD("{{ execution_date.isoformat() }}", INTERVAL 3 HOUR)
  AND metrics.uuid.test_run = "{test_run}"
"""

# GCP and GKE default values
DEFAULT_GCP_CONN_ID = "google_cloud_derived_datasets"
DEFAULT_GKE_LOCATION = "us-central1-a"
DEFAULT_GKE_CLUSTER_NAME = "bq-load-gke-1"
DEFAULT_GKE_NAMESPACE = "default"

BURNHAM_PLATFORM_URL = "https://incoming.telemetry.mozilla.org"

DEFAULT_ARGS = {
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
    **kwargs,
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
        "BURNHAM_VERBOSE": "true",
        "GLEAN_LOG_PINGS": "true",
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
        **kwargs,
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
        **kwargs,
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
    **kwargs,
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
            "--execution-timestamp",
            "{{ execution_date.isoformat() }}",
        ],
        **kwargs,
    )


def encode_test_scenarios(test_scenarios):
    """Encode the given test scenarios as a str.

    :param List[Dict[str, object]] test_scenarios:  [Required] ID for the task
    :return: str
    """
    json_encoded = json.dumps(test_scenarios)
    utf_encoded = json_encoded.encode("utf-8")
    b64_encoded = base64.b64encode(utf_encoded).decode("utf-8")
    return b64_encoded


with models.DAG(
    "burnham", schedule_interval="@daily", default_args=DEFAULT_ARGS,
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

    # Create burnham clients that complete missions and submit pings
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

    # Tasks related to the discovery table
    wait_for_discovery_data = burnham_sensor(
        task_id="wait_for_discovery_data",
        sql=DISCOVERY_SENSOR_TEMPLATE.format(
            project_id=PROJECT_ID,
            test_run=burnham_test_run,
            test_name=burnham_test_name,
        ),
        timeout=60 * 60 * 1,
    )
    wait_for_discovery_data.set_upstream(
        [generate_burnham_test_run_uuid, client1, client2, client3]
    )

    discovery_test_scenarios = [
        {
            "name": "test_labeled_counter_metrics",
            "query": TEST_LABELED_COUNTER_METRICS,
            "want": WANT_TEST_LABELED_COUNTER_METRICS,
        },
        {
            "name": "test_client_ids",
            "query": TEST_CLIENT_IDS,
            "want": WANT_TEST_CLIENT_IDS,
        },
        {
            "name": "test_experiments",
            "query": TEST_EXPERIMENTS,
            "want": WANT_TEST_EXPERIMENTS,
        },
        {
            "name": "test_glean_error_invalid_overflow",
            "query": TEST_GLEAN_ERROR_INVALID_OVERFLOW,
            "want": WANT_TEST_GLEAN_ERROR_INVALID_OVERFLOW,
        },
    ]

    verify_discovery_data = burnham_bigquery_run(
        task_id="verify_discovery_data",
        project_id=PROJECT_ID,
        burnham_test_run=burnham_test_run,
        burnham_test_scenarios=encode_test_scenarios(discovery_test_scenarios),
        owner=DAG_OWNER,
        email=DAG_EMAIL,
    )
    verify_discovery_data.set_upstream(wait_for_discovery_data)

    # Tasks related to the starbase46 table
    wait_for_starbase46_data = burnham_sensor(
        task_id="wait_for_starbase46_data",
        sql=STARBASE46_SENSOR_TEMPLATE.format(
            project_id=PROJECT_ID,
            test_run=burnham_test_run,
            test_name=burnham_test_name,
        ),
        timeout=60 * 60 * 1,
    )
    wait_for_starbase46_data.set_upstream(
        [generate_burnham_test_run_uuid, client1, client2, client3]
    )

    starbase46_test_scenarios = [
        {
            "name": "test_starbase46_ping",
            "query": TEST_STARBASE46_PING,
            "want": WANT_TEST_STARBASE46_PING,
        },
    ]

    verify_starbase46_data = burnham_bigquery_run(
        task_id="verify_starbase46_data",
        project_id=PROJECT_ID,
        burnham_test_run=burnham_test_run,
        burnham_test_scenarios=encode_test_scenarios(starbase46_test_scenarios),
        owner=DAG_OWNER,
        email=DAG_EMAIL,
    )
    verify_starbase46_data.set_upstream(wait_for_starbase46_data)

    # Tasks related to the space_ship_ready table
    wait_for_space_ship_ready_data = burnham_sensor(
        task_id="wait_for_space_ship_ready_data",
        sql=SPACE_SHIP_READY_SENSOR_TEMPLATE.format(
            project_id=PROJECT_ID,
            test_run=burnham_test_run,
            test_name=burnham_test_name,
        ),
        timeout=60 * 60 * 1,
    )
    wait_for_space_ship_ready_data.set_upstream(
        [generate_burnham_test_run_uuid, client1, client2, client3]
    )

    space_ship_ready_test_scenarios = [
        {
            "name": "test_space_ship_ready_ping",
            "query": TEST_SPACE_SHIP_READY_PING,
            "want": WANT_TEST_SPACE_SHIP_READY_PING,
        },
    ]

    verify_space_ship_ready_data = burnham_bigquery_run(
        task_id="verify_space_ship_ready_data",
        project_id=PROJECT_ID,
        burnham_test_run=burnham_test_run,
        burnham_test_scenarios=encode_test_scenarios(space_ship_ready_test_scenarios),
        owner=DAG_OWNER,
        email=DAG_EMAIL,
    )
    verify_space_ship_ready_data.set_upstream(wait_for_space_ship_ready_data)
