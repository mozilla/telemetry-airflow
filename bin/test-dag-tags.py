#!/bin/python3
from collections import defaultdict
import subprocess


VALID_TAGS = ("impact/tier_1", "impact/tier_2", "impact/tier_3", "repo/bigquery-etl", "repo/telemetry-airflow",)
REQUIRED_TAG_TYPES = ("impact",)


class DagValidationError(Exception):
    pass


def retrieve_existing_airflow_dags():
    output = subprocess.run(
        "docker-compose exec web mysql -Ns -h db -u root -psecret airflow -e 'SELECT dag_id, name FROM dag_tag;'",
        shell=True,
        capture_output=True
    ).stdout.decode('ascii').strip()

    dags = defaultdict(list)

    for dag in output.replace("\r", "").split("\n"):
        dag_name, tag_name = dag.split("\t")

        dags[dag_name] = dags[dag_name] + [tag_name]

    return dags



if __name__ == "__main__":
    tag_errors = 0

    # bin/run script waits for db and reddis to be up before interacting with webserver container in test-parse script
    dags = retrieve_existing_airflow_dags()

    for dag_id, tags in dags.items():
        tag_categories = [category.split("/")[0] for category in tags]

        if not all(req_tag in tag_categories for req_tag in REQUIRED_TAG_TYPES):
            tag_errors += 1
            print('%s is missing a required tag. Required tags include: %s. Please refer to: https://mozilla.github.io/bigquery-etl/reference/airflow_tags/ for more information.' % (dag_id, REQUIRED_TAG_TYPES))

        if any(tag not in VALID_TAGS for tag in tags):
            tag_errors += 1
            print('DAG: %s contains an invalid tag. Tags specified: %s, valid tags: %s.' % (dag_id, tags, VALID_TAGS))

    if tag_errors:
        raise DagValidationError("DAG tags validation failed.")

    print("DAG tags validation successful.")
