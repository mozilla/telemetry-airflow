#!/bin/python3
from collections import defaultdict
import subprocess


VALID_TAGS = ("impact/tier_1", "impact/tier_2", "impact/tier_3", "repo/bigquery-etl", "repo/telemetry-airflow",)
REQUIRED_TAG_TYPES = ("impact",)


class DagValidationError(Exception):
    pass


def retrieve_existing_airflow_dags_from_db(pswd):
    mask_string = "###"
    shell_cmd = "docker-compose exec web mysql -Ns -h db -u root -p{pswd} airflow -e 'SELECT dag_id, name FROM dag_tag;'"

    print("Executing command: %s" % (shell_cmd.format(pswd=mask_string)))
    cmd_output = subprocess.run(
        shell_cmd.format(pswd=pswd),
        shell=True,
        capture_output=True,
        text=True,
    )
    try:
        cmd_output.check_returncode()
    except subprocess.CalledProcessError as _err:
        print(cmd_output.stdout)
        print(cmd_output.stderr)
        raise _err

    print("Command executed successfully, processing output...")
    processed_cmd_output = cmd_output.stdout.strip().replace("\r", "").split("\n")

    dags = defaultdict(list)

    for dag in processed_cmd_output:
        dag_name, tag_name = dag.split("\t")
        dags[dag_name] = dags[dag_name] + [tag_name]

    print("Number of DAGs found: %s" % (len(dags)))

    return dags


if __name__ == "__main__":
    # Assumes the web and db containers are already running
    tag_errors = 0

    dags = retrieve_existing_airflow_dags_from_db("secret")

    for file_name, tags in dags.items():
        tag_categories = [category.split("/")[0] for category in tags]

        if not all(req_tag in tag_categories for req_tag in REQUIRED_TAG_TYPES):
            tag_errors += 1
            print('%s is missing a required tag. Required tags include: %s. Please refer to: https://mozilla.github.io/bigquery-etl/reference/airflow_tags/ for more information.' % (file_name, REQUIRED_TAG_TYPES))

        if any(tag not in VALID_TAGS for tag in tags):
            tag_errors += 1
            print('DAG file: %s contains an invalid tag. Tags specified: %s, valid tags: %s.' % (file_name, tags, VALID_TAGS))

    if tag_errors:
        raise DagValidationError("DAG tags validation failed.")

    print("DAG tags validation for %s DAGs successful." % (len(dags)))
