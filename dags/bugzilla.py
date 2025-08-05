from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import requests
import time
import logging

from utils.tags import Tag

docs = """
### Bugzilla
Runs daily scheduled jobs on Bugzilla.
Tags bugs in Bugzilla for the product "Data Platform and Tools" and component "General"
with the whiteboard tag `[dataplatform]` if do not already have a tag. This will trigger a sync to Jira.

#### Owner
ascholtz@mozilla.com
"""

tags = [Tag.ImpactTier.tier_3]

BUGZILLA_URL = "https://bugzilla.mozilla.org/rest"
PRODUCT = "Data Platform and Tools"
COMPONENT = "General"
WHITEBOARD_TAG = "[dataplatform]"
API_KEY_VAR = "BUGZILLA_API_KEY"

default_args = {
    "retries": 1,
    "retry_delay": timedelta(minutes=30),
    "owner": "ascholtz@mozilla.com",
    "email": [
        "telemetry-alerts@mozilla.com",
        "ascholtz@mozilla.com",
    ],
    "depends_on_past": False,
    "start_date": datetime(2025, 8, 3),
    "email_on_failure": True,
    "email_on_retry": True,
}

with DAG(
    "bugzilla",
    default_args=default_args,
    schedule_interval="@daily",
    tags=tags,
    doc_md=docs,
) as dag:

    def fetch_and_tag_bugs():
        api_key = Variable.get(API_KEY_VAR)

        def get_bugs():
            params = {
                "product": PRODUCT,
                "component": COMPONENT,
                "include_fields": "id,whiteboard",
                "status": ["NEW", "ASSIGNED", "REOPENED"],
                "limit": 500,
            }
            response = requests.get(f"{BUGZILLA_URL}/bug", params=params)
            response.raise_for_status()
            return response.json()["bugs"]

        def update_whiteboard(bug_id, new_whiteboard):
            payload = {"whiteboard": new_whiteboard, "api_key": api_key}
            response = requests.put(f"{BUGZILLA_URL}/bug/{bug_id}", data=payload)
            if response.status_code == 200:
                logging.info(f"✔ Bug {bug_id} updated successfully.")
            else:
                logging.error(f"✖ Failed to update bug {bug_id}: {response.text}")

        bugs = get_bugs()
        logging.info(f"Found {len(bugs)} bugs in {PRODUCT} > {COMPONENT}")

        for bug in bugs:
            bug_id = bug["id"]
            whiteboard = bug.get("whiteboard", "")

            if whiteboard.strip() == "":
                new_whiteboard = (whiteboard + " " + WHITEBOARD_TAG).strip()
                logging.info(
                    f"Updating bug {bug_id} with new whiteboard: {new_whiteboard}"
                )
                update_whiteboard(bug_id, new_whiteboard)
                time.sleep(0.5)
            else:
                logging.info(f"Skipping bug {bug_id}: already tagged.")

    tag_bugs_task = PythonOperator(
        task_id="tag_bugs_with_dataplatform", python_callable=fetch_and_tag_bugs
    )
