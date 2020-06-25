# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import datetime
import json
import uuid

from airflow import models
from utils.burnham import (
    new_burnham_bigquery_operator,
    new_burnham_operator,
    new_burnham_sensor,
)

DAG_OWNER = "rpierzina@mozilla.com"
DAG_EMAIL = ["telemetry-alerts@mozilla.com", "rpierzina@mozilla.com"]

QUERY_TEMPLATE = """
SELECT
  technology_space_travel.key,
  SUM(technology_space_travel.value) AS value_sum
FROM
  `{project_id}.burnham_live.discovery_v1`
CROSS JOIN
  UNNEST(metrics.labeled_counter.technology_space_travel) AS technology_space_travel
WHERE
  submission_timestamp > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 3 HOUR)
  AND metrics.uuid.test_run = "{test_run}"
  AND metrics.string.test_name = "{test_name}"
GROUP BY
  technology_space_travel.key
ORDER BY
  technology_space_travel.key
LIMIT
  {limit}
"""

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

    # Generate a UUID for this test run
    burnham_test_run = str(uuid.uuid4())
    burnham_test_name = "test_labeled_counter_metrics"

    client1 = new_burnham_operator(
        task_id="client1",
        burnham_test_run=burnham_test_run,
        burnham_test_name=burnham_test_name,
        burnham_missions=["MISSION G: FIVE WARPS, FOUR JUMPS", "MISSION C: ONE JUMP"],
        burnham_spore_drive="tardigrade",
        owner=DAG_OWNER,
        email=DAG_EMAIL,
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
        owner=DAG_OWNER,
        email=DAG_EMAIL,
    )

    client3 = new_burnham_operator(
        task_id="client3",
        burnham_test_run=burnham_test_run,
        burnham_test_name=burnham_test_name,
        burnham_missions=["MISSION A: ONE WARP", "MISSION B: TWO WARPS"],
        burnham_spore_drive=None,
        owner=DAG_OWNER,
        email=DAG_EMAIL,
    )

    # TODO: Update SQL for Sensor
    # Wait for 10 pings with this test run ID in live tables
    wait_for_data = new_burnham_sensor(
        task_id="wait_for_data", sql="TODO", timeout=60 * 60 * 1
    )

    project_id = "moz-fx-data-shared-prod"

    test_run_information = {
        "identifier": burnham_test_run,
        "tests": [
            {
                "name": burnham_test_name,
                "query": QUERY_TEMPLATE.format(
                    project_id=project_id,
                    test_run=burnham_test_run,
                    test_name=burnham_test_name,
                    limit=10,
                ),
                "want": [
                    {"key": "spore_drive", "value_sum": "13"},
                    {"key": "warp_drive", "value_sum": "18"},
                ],
            },
        ],
    }

    verify_data = new_burnham_bigquery_operator(
        task_id="verify_data",
        project_id=project_id,
        test_run_information=json.dumps(test_run_information),
        owner=DAG_OWNER,
        email=DAG_EMAIL,
    )
    verify_data.set_upstream(wait_for_data)
