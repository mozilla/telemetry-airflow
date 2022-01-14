#!/bin/python3
from collections import defaultdict
import subprocess
from typing import Dict


VALID_TAGS = ("impact/tier_1", "impact/tier_2", "impact/tier_3", "repo/bigquery-etl", "repo/telemetry-airflow",)
REQUIRED_TAG_TYPES = ("impact",)


class DagValidationError(Exception):
    pass


def _execute_shell_cmd(cmd: str, cmd_params: Dict[str, dict] = dict()) -> str:
    mask_string = "###"

    cmd_params_log = {
        key: val["value"] if not val["value"] else mask_string
        for key, val in cmd_params.items()
    }

    print("Executing command: %s" % (cmd.format(**cmd_params_log)))

    cmd_params_formatted = {
        key: val["value"]
        for key, val in cmd_params.items()
    }

    cmd_output = subprocess.run(
        cmd.format(**cmd_params_formatted),
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

    return cmd_output.stdout.strip().replace("\r", "").split("\n")


def get_loaded_airflow_dag_tags_from_db(pswd: str) -> Dict[str, str]:
    shell_cmd = "docker-compose exec web mysql -Ns -h db -u root -p{pswd} airflow -e 'SELECT dag_id, name FROM dag_tag;'"
    cmd_params = {
        "pswd": {
            "value": pswd,
            "is_sensitive": True,
        }
    }

    cmd_output = _execute_shell_cmd(shell_cmd, cmd_params)

    dags = defaultdict(list)

    for dag in cmd_output:
        dag_name, tag_name = dag.split("\t")
        dags[dag_name] = dags[dag_name] + [tag_name]

    print("Number of DAGs with tags found: %s" % (len(dags)))

    return dags


def get_loaded_dags_from_db(pswd) -> int:
    cmd_params = {
        "pswd": {
            "value": pswd,
            "is_sensitive": True,
        }
    }
    return _execute_shell_cmd("docker-compose exec web mysql -Ns -h db -u root -p{pswd} airflow -e 'SELECT dag_id from dag WHERE is_subdag = 0;'", cmd_params)


if __name__ == "__main__":
    db_pass = "secret"
    # Assumes the web and db containers are already running
    tag_errors = 0

    dags = get_loaded_dags_from_db(db_pass)
    num_of_dags = len(dags)

    dags_with_tags = get_loaded_airflow_dag_tags_from_db(db_pass)
    num_of_dags_with_tags = len(dags_with_tags)

    print("Num of DAGs in `dag` table: %s, num of dags with tags: %s" % (num_of_dags, num_of_dags_with_tags))

    if num_of_dags != num_of_dags_with_tags:

        if num_of_dags > num_of_dags_with_tags:
            error_msg = "The following DAGs are missing the tags entirely: %s" % (set(dags).difference(set(dags_with_tags)))
            raise DagValidationError(error_msg)

        elif num_of_dags < num_of_dags_with_tags:
            error_msg = "The following DAGs seem to be missing in the dags table: %s" % (set(dags_with_tags).difference(set(dags)))
            print("WARNING: %s" % error_msg)


    for file_name, tags in dags_with_tags.items():
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
