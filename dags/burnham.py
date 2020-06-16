# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import datetime

from airflow import models
from utils.burnham import new_burnham_operator, new_burnham_sensor

DAG_OWNER = "rpierzina@mozilla.com"
DAG_EMAIL = ["telemetry-alerts@mozilla.com", "rpierzina@mozilla.com"]


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

    # TODO: Run another GKEPodOperator to verify the data
    # Query for space_travel metrics and one client error
    # expected_metrics = {
    #     "labeled_counter": {
    #         "technology.space_travel": {"spore_drive": 13, "warp_drive": 18}
    #     }
    # }
    # verify_data = GKEPodOperator()
    # verify_data.set_upstream(wait_for_data)
