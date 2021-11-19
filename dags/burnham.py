# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import base64
import datetime
import json
import logging
import uuid
import time

from dataclasses import dataclass

from airflow import DAG
from airflow.operators.python import PythonOperator
from operators.bq_sensor import BigQuerySQLSensorOperator
from operators.gcp_container_operator import GKEPodOperator

DOCS = """\
# burnham

The burnham project is an end-to-end test suite that aims to automatically
verify that Glean-based products correctly measure, collect, and submit
non-personal information to the GCP-based Data Platform and that the received
telemetry data is then correctly processed, stored to the respective tables
and made available in BigQuery.

See https://github.com/mozilla/burnham
"""

DAG_OWNER = "rpierzina@mozilla.com"
DAG_EMAIL = ["glean-team@mozilla.com", "rpierzina@mozilla.com", "bforehand@mozilla.com"]

PROJECT_ID = "moz-fx-data-shared-prod"

# We cover multiple test scenarios with pings submitted from client1, client2
# and client3. They don't submit pings using a specific test name, but all
# share the following default test name.
DEFAULT_TEST_NAME = "test_burnham"

# We use a template for the test run UUID in the DAG. Because we base64 encode
# this query SQL before the template is rendered, we need to use a parameter
# and replace the test run UUID in burnham-bigquery.

# Live tables are not guaranteed to be deduplicated. To ensure reproducibility,
# we need to deduplicate Glean pings produced by burnham for these tests.
DEDUPED_TABLE = """
  {table}_numbered AS (
  SELECT
    ROW_NUMBER() OVER (PARTITION BY document_id ORDER BY submission_timestamp) AS _n,
    *
  FROM
    `{project_id}.burnham_live.{table}`
  WHERE
    submission_timestamp BETWEEN TIMESTAMP_SUB(@burnham_start_timestamp, INTERVAL 1 HOUR)
    AND TIMESTAMP_ADD(@burnham_start_timestamp, INTERVAL 3 HOUR)
    AND metrics.uuid.test_run = @burnham_test_run ),
  {table}_deduped AS (
  SELECT
    * EXCEPT(_n)
  FROM
    {table}_numbered
  WHERE
    _n = 1 )"""

DISCOVERY_V1_DEDUPED = DEDUPED_TABLE.format(project_id=PROJECT_ID, table="discovery_v1")

STARBASE46_V1_DEDUPED = DEDUPED_TABLE.format(
    project_id=PROJECT_ID, table="starbase46_v1"
)

SPACE_SHIP_READY_V1_DEDUPED = DEDUPED_TABLE.format(
    project_id=PROJECT_ID, table="space_ship_ready_v1"
)

DELETION_REQUEST_V1_DEDUPED = DEDUPED_TABLE.format(
    project_id=PROJECT_ID, table="deletion_request_v1"
)


# Test scenario test_labeled_counter_metrics: Verify that labeled_counter
# metric values reported by the Glean SDK across several documents from three
# different clients are correct.
TEST_LABELED_COUNTER_METRICS = f"""WITH {DISCOVERY_V1_DEDUPED}
SELECT
  technology_space_travel.key,
  SUM(technology_space_travel.value) AS value_sum
FROM
  discovery_v1_deduped
CROSS JOIN
  UNNEST(metrics.labeled_counter.technology_space_travel) AS technology_space_travel
WHERE
  metrics.string.test_name = "{DEFAULT_TEST_NAME}"
GROUP BY
  technology_space_travel.key
ORDER BY
  technology_space_travel.key
LIMIT
  20
"""

WANT_TEST_LABELED_COUNTER_METRICS = [
    {"key": "spore_drive", "value_sum": 25},
    {"key": "warp_drive", "value_sum": 36},
]


# Test scenario test_client_ids: Verify that the Glean SDK generated three
# distinct client IDs for three different clients.
TEST_CLIENT_IDS = f"""
SELECT
  COUNT(DISTINCT client_info.client_id) AS count_client_ids
FROM
  `{PROJECT_ID}.burnham_live.discovery_v1`
WHERE
  submission_timestamp BETWEEN TIMESTAMP_SUB(@burnham_start_timestamp, INTERVAL 1 HOUR)
  AND TIMESTAMP_ADD(@burnham_start_timestamp, INTERVAL 3 HOUR)
  AND metrics.uuid.test_run = @burnham_test_run
  AND metrics.string.test_name = "{DEFAULT_TEST_NAME}"
LIMIT
  20
"""

WANT_TEST_CLIENT_IDS = [{"count_client_ids": 6}]


# Test scenario test_experiments: Verify that the Glean SDK correctly reports
# experiment information. The following query counts the number of documents
# submitted from a client which is not enrolled in any experiments, a client
# which is enrolled in the spore_drive experiment on branch tardigrade, and a
# client which is enrolled in the spore_drive experiment on branch
# tardigrade-dna.
TEST_EXPERIMENTS = f"""WITH {DISCOVERY_V1_DEDUPED},
  base AS (
  SELECT
    ARRAY(
    SELECT
      CONCAT(key, ':', value.branch) AS key
    FROM
      UNNEST(ping_info.experiments)) AS experiments,
  FROM
    discovery_v1_deduped
  WHERE
    metrics.string.test_name = "{DEFAULT_TEST_NAME}"
  LIMIT
    40 ),
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
    {"experiment": "no_experiments", "document_count": 4},
    {"experiment": "spore_drive:tardigrade", "document_count": 4},
    {"experiment": "spore_drive:tardigrade-dna", "document_count": 12},
]


# Test scenario test_glean_error_invalid_overflow: Verify that the Glean SDK
# correctly reports the number of times a string metric was set to a value that
# exceeds the maximum string length measured in the number of bytes when the
# string is encoded in UTF-8.
TEST_GLEAN_ERROR_INVALID_OVERFLOW = f"""WITH {DISCOVERY_V1_DEDUPED}
SELECT
  metrics.string.mission_identifier,
  metrics.labeled_counter.glean_error_invalid_overflow
FROM
  discovery_v1_deduped
WHERE
  ARRAY_LENGTH(metrics.labeled_counter.glean_error_invalid_overflow) > 0
  AND metrics.string.test_name = "{DEFAULT_TEST_NAME}"
ORDER BY
  metrics.string.mission_identifier
LIMIT
  40
"""

WANT_TEST_GLEAN_ERROR_INVALID_OVERFLOW = [
    {
        "mission_identifier": "MISSION E: ONE JUMP, ONE METRIC ERROR",
        "glean_error_invalid_overflow": [{"key": "mission.status", "value": 2}],
    }
]


# Test scenario test_starbase46_ping: Verify that the Glean SDK and the
# Data Platform support custom pings using the numbered naming scheme
TEST_STARBASE46_PING = f"""WITH {STARBASE46_V1_DEDUPED}
SELECT
  COUNT(*) AS count_documents
FROM
  starbase46_v1_deduped
WHERE
  metrics.string.test_name = "{DEFAULT_TEST_NAME}"
"""

WANT_TEST_STARBASE46_PING = [{"count_documents": 2}]


# Test scenario test_space_ship_ready_ping: Verify that the Glean SDK and the
# Data Platform support custom pings using the kebab-case naming scheme
TEST_SPACE_SHIP_READY_PING = f"""WITH {SPACE_SHIP_READY_V1_DEDUPED}
SELECT
  COUNT(*) AS count_documents
FROM
  space_ship_ready_v1_deduped
WHERE
  metrics.string.test_name = "{DEFAULT_TEST_NAME}"
"""

WANT_TEST_SPACE_SHIP_READY_PING = [{"count_documents": 6}]


# Test scenario test_no_ping_after_upload_disabled: Verify that the Glean SDK
# does not upload pings after upload was disabled and resumes to uploading
# pings after it was re-enabled again.
TEST_NO_PING_AFTER_UPLOAD_DISABLED = f"""WITH {DISCOVERY_V1_DEDUPED}
SELECT
  COUNT(*) AS count_documents,
  metrics.string.mission_identifier
FROM
  discovery_v1_deduped
WHERE
  metrics.string.test_name = "test_disable_upload"
GROUP BY
  metrics.string.mission_identifier
ORDER BY
  metrics.string.mission_identifier
LIMIT
  20
"""

WANT_TEST_NO_PING_AFTER_UPLOAD_DISABLED = [
    {"mission_identifier": "MISSION B: TWO WARPS", "count_documents": 2},
    {"mission_identifier": "MISSION C: ONE JUMP", "count_documents": 2},
    {"mission_identifier": "MISSION F: TWO WARPS, ONE JUMP", "count_documents": 2},
]


# Test scenario test_client_ids_after_upload_disabled: Verify that the Glean
# SDK generated a new client ID after upload was disabled.
TEST_CLIENT_IDS_AFTER_UPLOAD_DISABLED = f"""
SELECT
  COUNT(DISTINCT client_info.client_id) AS count_client_ids
FROM
  `{PROJECT_ID}.burnham_live.discovery_v1`
WHERE
  submission_timestamp BETWEEN TIMESTAMP_SUB(@burnham_start_timestamp, INTERVAL 1 HOUR)
  AND TIMESTAMP_ADD(@burnham_start_timestamp, INTERVAL 3 HOUR)
  AND metrics.uuid.test_run = @burnham_test_run
  AND metrics.string.test_name = "test_disable_upload"
LIMIT
  20
"""

WANT_TEST_CLIENT_IDS_AFTER_UPLOAD_DISABLED = [{"count_client_ids": 4}]


# Test scenario test_deletion_request_ping: Verify that the Glean SDK submitted
# a deletion-request ping after upload was disabled.
TEST_DELETION_REQUEST_PING = f"""WITH {DELETION_REQUEST_V1_DEDUPED}
SELECT
  COUNT(*) AS count_documents
FROM
  deletion_request_v1_deduped
WHERE
  metrics.string.test_name = "test_disable_upload"
"""

WANT_TEST_DELETION_REQUEST_PING = [{"count_documents": 2}]

# Test scenario test_deletion_request_ping_client_id: Verify that the Glean SDK
# submitted a deletion-request ping with the expected client ID by joining
# deletion-request records with discovery records. The following query returns
# only the mission.identifier values for discovery pings submitted from burnham
# clients that also submitted a deletion-request ping with the same client ID.
TEST_DELETION_REQUEST_PING_CLIENT_ID = f"""
WITH
  {DISCOVERY_V1_DEDUPED},
  discovery AS (
  SELECT
    client_info.client_id,
    metrics.string.mission_identifier
  FROM
    discovery_v1_deduped
  WHERE
    metrics.string.test_name = "test_disable_upload" ),
  {DELETION_REQUEST_V1_DEDUPED},
  deletion_request AS (
  SELECT
    client_info.client_id
  FROM
    deletion_request_v1_deduped
  WHERE
    metrics.string.test_name = "test_disable_upload")
SELECT
  discovery.mission_identifier
FROM
  discovery
JOIN
  deletion_request
USING
  (client_id)
ORDER BY
  discovery.mission_identifier
"""

WANT_TEST_DELETION_REQUEST_PING_CLIENT_ID = [
    {"mission_identifier": "MISSION B: TWO WARPS"},
    {"mission_identifier": "MISSION B: TWO WARPS"},
    {"mission_identifier": "MISSION C: ONE JUMP"},
    {"mission_identifier": "MISSION C: ONE JUMP"},
]

# Sensor template for the different burnham tables. Note that we use BigQuery
# query parameters in queries for test scenarios, because we need to serialize
# the test scenarios to JSON and b64-encode them to ensure we can safely pass
# this information to the burnham-bigquery Docker container. We can use
# string-formatting here, because Airflow executes the query directly.
SENSOR_TEMPLATE = """
SELECT
  COUNT(*) >= {min_count_rows}
FROM
  `{project_id}.burnham_live.{table}`
WHERE
  submission_timestamp BETWEEN TIMESTAMP_SUB("{start_timestamp}", INTERVAL 1 HOUR)
  AND TIMESTAMP_ADD("{start_timestamp}", INTERVAL 3 HOUR)
  AND metrics.uuid.test_run = "{test_run}"
  AND metrics.string.test_name = "{test_name}"
"""


@dataclass(frozen=True)
class BurnhamDistribution:
    burnham_version: str
    glean_sdk_version: str
    glean_parser_version: str


# GCP and GKE default values
DEFAULT_GCP_CONN_ID = "google_cloud_derived_datasets"
DEFAULT_GCP_PROJECT_ID = "moz-fx-data-derived-datasets"
DEFAULT_GKE_LOCATION = "us-central1-a"
DEFAULT_GKE_CLUSTER_NAME = "bq-load-gke-1"
DEFAULT_GKE_NAMESPACE = "default"

BURNHAM_PLATFORM_URL = "https://incoming.telemetry.mozilla.org"

BURNHAM_DISTRIBUTIONS = {
    "21.0.0": BurnhamDistribution(
        burnham_version="21.0.0",
        glean_sdk_version="41.1.1",
        glean_parser_version="4.0.0",
    )
}

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
    burnham_distribution=None,
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

    if burnham_distribution is not None:
        image_version = burnham_distribution.burnham_version
    else:
        image_version = "latest"

    return GKEPodOperator(
        task_id=task_id,
        gcp_conn_id=gcp_conn_id,
        project_id=DEFAULT_GCP_PROJECT_ID,
        location=gke_location,
        cluster_name=gke_cluster_name,
        namespace=gke_namespace,
        image=f"gcr.io/moz-fx-data-airflow-prod-88e0/burnham:{image_version}",
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
        gcp_conn_id=gcp_conn_id,
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
        project_id=DEFAULT_GCP_PROJECT_ID,
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
            "--start-timestamp",
            "{{ dag_run.start_date.isoformat() }}",
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


def do_sleep(minutes):
    """Sleep for the given number of minutes.

    Writes out an update every minute to give some indication of aliveness.
    """
    logging.info(f"Configured to sleep for {minutes} minutes. Let's begin.")
    for i in range(minutes, 0, -1):
        logging.info(f"{i} minute(s) of sleeping left")
        time.sleep(60)


def sleep_task(minutes, task_id):
    """Return an operator that sleeps for a certain number of minutes.

    :param int    minutes: [Required] Number of minutes to sleep
    :param string task_id: [Required] ID for the task

    :return: PythonOperator
    """
    return PythonOperator(
        task_id=task_id,
        depends_on_past=False,
        python_callable=do_sleep,
        op_kwargs=dict(minutes=minutes),
    )


with DAG(
    "burnham",
    schedule_interval="@daily",
    default_args=DEFAULT_ARGS,
    doc_md=DOCS,
) as dag:

    # Generate a UUID for this test run
    generate_burnham_test_run_uuid = PythonOperator(
        task_id="generate_burnham_test_run_uuid",
        python_callable=lambda: str(uuid.uuid4()),
    )
    burnham_test_run = '{{ task_instance.xcom_pull("generate_burnham_test_run_uuid") }}'

    # This Airflow macro is added to sensors to filter out rows by submission_timestamp
    start_timestamp = "{{ dag_run.start_date.isoformat() }}"

    # Create burnham clients that complete missions and submit pings
    client1 = burnham_run(
        task_id="client1",
        burnham_test_run=burnham_test_run,
        burnham_test_name=DEFAULT_TEST_NAME,
        burnham_missions=["MISSION G: FIVE WARPS, FOUR JUMPS", "MISSION C: ONE JUMP"],
        burnham_spore_drive="tardigrade",
        owner=DAG_OWNER,
        email=DAG_EMAIL,
    )
    client1.set_upstream(generate_burnham_test_run_uuid)

    client2 = burnham_run(
        task_id="client2",
        burnham_test_run=burnham_test_run,
        burnham_test_name=DEFAULT_TEST_NAME,
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
        burnham_test_name=DEFAULT_TEST_NAME,
        burnham_missions=["MISSION A: ONE WARP", "MISSION B: TWO WARPS"],
        burnham_spore_drive=None,
        owner=DAG_OWNER,
        email=DAG_EMAIL,
    )
    client3.set_upstream(generate_burnham_test_run_uuid)

    client4 = burnham_run(
        task_id="client4",
        burnham_test_run=burnham_test_run,
        burnham_test_name="test_disable_upload",
        burnham_missions=[
            "MISSION B: TWO WARPS",
            "MISSION C: ONE JUMP",
            "MISSION H: DISABLE GLEAN UPLOAD",
            "MISSION D: TWO JUMPS",
            "MISSION I: ENABLE GLEAN UPLOAD",
            "MISSION F: TWO WARPS, ONE JUMP",
        ],
        burnham_spore_drive="tardigrade",
        owner=DAG_OWNER,
        email=DAG_EMAIL,
    )
    client4.set_upstream(generate_burnham_test_run_uuid)

    client5 = burnham_run(
        task_id="client5",
        burnham_distribution=BURNHAM_DISTRIBUTIONS["21.0.0"],
        burnham_test_run=burnham_test_run,
        burnham_test_name=DEFAULT_TEST_NAME,
        burnham_missions=["MISSION G: FIVE WARPS, FOUR JUMPS", "MISSION C: ONE JUMP"],
        burnham_spore_drive="tardigrade",
        owner=DAG_OWNER,
        email=DAG_EMAIL,
    )

    client5.set_upstream(generate_burnham_test_run_uuid)

    client6 = burnham_run(
        task_id="client6",
        burnham_distribution=BURNHAM_DISTRIBUTIONS["21.0.0"],
        burnham_test_run=burnham_test_run,
        burnham_test_name=DEFAULT_TEST_NAME,
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

    client6.set_upstream(generate_burnham_test_run_uuid)

    client7 = burnham_run(
        task_id="client7",
        burnham_distribution=BURNHAM_DISTRIBUTIONS["21.0.0"],
        burnham_test_run=burnham_test_run,
        burnham_test_name=DEFAULT_TEST_NAME,
        burnham_missions=["MISSION A: ONE WARP", "MISSION B: TWO WARPS"],
        burnham_spore_drive=None,
        owner=DAG_OWNER,
        email=DAG_EMAIL,
    )

    client7.set_upstream(generate_burnham_test_run_uuid)

    client8 = burnham_run(
        task_id="client8",
        burnham_distribution=BURNHAM_DISTRIBUTIONS["21.0.0"],
        burnham_test_run=burnham_test_run,
        burnham_test_name="test_disable_upload",
        burnham_missions=[
            "MISSION B: TWO WARPS",
            "MISSION C: ONE JUMP",
            "MISSION H: DISABLE GLEAN UPLOAD",
            "MISSION D: TWO JUMPS",
            "MISSION I: ENABLE GLEAN UPLOAD",
            "MISSION F: TWO WARPS, ONE JUMP",
        ],
        burnham_spore_drive="tardigrade",
        owner=DAG_OWNER,
        email=DAG_EMAIL,
    )

    client8.set_upstream(generate_burnham_test_run_uuid)

    # We expect up to 20 minute latency for pings to get loaded to live tables
    # in BigQuery, so we have a task that explicitly sleeps for 20 minutes
    # and make that a dependency for our tasks that need to read the BQ data.
    sleep_20_minutes = sleep_task(minutes=20, task_id="sleep_20_minutes")

    # Tasks related to the discovery table
    wait_for_discovery_data = burnham_sensor(
        task_id="wait_for_discovery_data",
        sql=SENSOR_TEMPLATE.format(
            project_id=PROJECT_ID,
            table="discovery_v1",
            min_count_rows=10,
            start_timestamp=start_timestamp,
            test_run=burnham_test_run,
            test_name=DEFAULT_TEST_NAME,
        ),
        timeout=60 * 60 * 1,
    )
    wait_for_discovery_data.set_upstream(
        [
            client1,
            client2,
            client3,
            client5,
            client6,
            client7,
            sleep_20_minutes,
        ]
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
        sql=SENSOR_TEMPLATE.format(
            project_id=PROJECT_ID,
            table="starbase46_v1",
            min_count_rows=1,
            start_timestamp=start_timestamp,
            test_run=burnham_test_run,
            test_name=DEFAULT_TEST_NAME,
        ),
        timeout=60 * 60 * 1,
    )
    wait_for_starbase46_data.set_upstream(
        [
            client1,
            client2,
            client3,
            client5,
            client6,
            client7,
            sleep_20_minutes,
        ]
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
        sql=SENSOR_TEMPLATE.format(
            project_id=PROJECT_ID,
            table="space_ship_ready_v1",
            min_count_rows=3,
            start_timestamp=start_timestamp,
            test_run=burnham_test_run,
            test_name=DEFAULT_TEST_NAME,
        ),
        timeout=60 * 60 * 1,
    )
    wait_for_space_ship_ready_data.set_upstream(
        [
            client1,
            client2,
            client3,
            client5,
            client6,
            client7,
            sleep_20_minutes,
        ]
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

    wait_for_discovery_data_disable_upload = burnham_sensor(
        task_id="wait_for_discovery_data_disable_upload",
        sql=SENSOR_TEMPLATE.format(
            project_id=PROJECT_ID,
            table="discovery_v1",
            min_count_rows=3,
            start_timestamp=start_timestamp,
            test_run=burnham_test_run,
            test_name="test_disable_upload",
        ),
        timeout=60 * 60 * 1,
    )
    wait_for_discovery_data_disable_upload.set_upstream(
        [client4, client8, sleep_20_minutes]
    )

    discovery_test_scenarios_disable_upload = [
        {
            "name": "test_no_ping_after_upload_disabled",
            "query": TEST_NO_PING_AFTER_UPLOAD_DISABLED,
            "want": WANT_TEST_NO_PING_AFTER_UPLOAD_DISABLED,
        },
        {
            "name": "test_client_ids_after_upload_disabled",
            "query": TEST_CLIENT_IDS_AFTER_UPLOAD_DISABLED,
            "want": WANT_TEST_CLIENT_IDS_AFTER_UPLOAD_DISABLED,
        },
    ]

    verify_discovery_data_disable_upload = burnham_bigquery_run(
        task_id="verify_discovery_data_disable_upload",
        project_id=PROJECT_ID,
        burnham_test_run=burnham_test_run,
        burnham_test_scenarios=encode_test_scenarios(
            discovery_test_scenarios_disable_upload
        ),
        owner=DAG_OWNER,
        email=DAG_EMAIL,
    )
    verify_discovery_data_disable_upload.set_upstream(
        wait_for_discovery_data_disable_upload
    )

    wait_for_deletion_request_data = burnham_sensor(
        task_id="wait_for_deletion_request_data",
        sql=SENSOR_TEMPLATE.format(
            project_id=PROJECT_ID,
            table="deletion_request_v1",
            min_count_rows=1,
            start_timestamp=start_timestamp,
            test_run=burnham_test_run,
            test_name="test_disable_upload",
        ),
        timeout=60 * 60 * 1,
    )
    wait_for_deletion_request_data.set_upstream([client4, client8, sleep_20_minutes])

    deletion_request_test_scenarios = [
        {
            "name": "test_deletion_request_ping",
            "query": TEST_DELETION_REQUEST_PING,
            "want": WANT_TEST_DELETION_REQUEST_PING,
        },
        {
            "name": "test_deletion_request_ping_client_id",
            "query": TEST_DELETION_REQUEST_PING_CLIENT_ID,
            "want": WANT_TEST_DELETION_REQUEST_PING_CLIENT_ID,
        },
    ]

    verify_deletion_request_data = burnham_bigquery_run(
        task_id="verify_deletion_request_data",
        project_id=PROJECT_ID,
        burnham_test_run=burnham_test_run,
        burnham_test_scenarios=encode_test_scenarios(deletion_request_test_scenarios),
        owner=DAG_OWNER,
        email=DAG_EMAIL,
    )

    verify_deletion_request_data.set_upstream(
        [wait_for_discovery_data_disable_upload, wait_for_deletion_request_data]
    )
