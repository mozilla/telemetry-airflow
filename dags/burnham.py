# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import base64
import datetime
import json
import uuid

from airflow import models
from airflow.operators import PythonOperator
from utils.burnham import burnham_bigquery_run, burnham_run, burnham_sensor

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

SENSOR_TEMPLATE = """
SELECT
  COUNT(*) >= 10
FROM
  `{project_id}.burnham_live.discovery_v1`
WHERE
  submission_timestamp > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 3 HOUR)
  AND metrics.uuid.test_run = "{test_run}"
  AND metrics.string.test_name = "{test_name}"
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
    generate_burnham_test_run_uuid = PythonOperator(
        task_id="generate_burnham_test_run_uuid",
        python_callable=lambda: str(uuid.uuid4()),
    )

    burnham_test_run = '{{ task_instance.xcom_pull("generate_burnham_test_run_uuid") }}'
    burnham_test_name = "test_labeled_counter_metrics"

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
    ]
    json_encoded = json.dumps(burnham_test_scenarios)
    utf_encoded = json_encoded.encode("utf-8")
    b64_encoded = base64.b64encode(utf_encoded)

    verify_data = burnham_bigquery_run(
        task_id="verify_data",
        project_id=project_id,
        burnham_test_run=burnham_test_run,
        burnham_test_scenarios=b64_encoded,
        owner=DAG_OWNER,
        email=DAG_EMAIL,
    )
    verify_data.set_upstream(wait_for_data)
