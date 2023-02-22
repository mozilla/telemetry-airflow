import json
import os
import pathlib
import sys
import warnings
from typing import Dict, Union

import pytest
from airflow.models import DagBag

# get absolute project directory path no matter the environment
PROJECT_DIR = pathlib.Path(__file__).resolve().parent.parent


@pytest.fixture(scope="session")
def get_dag_bag(session_mocker) -> DagBag:
    from airflow.operators.subdag import SubDagOperator

    # Mock _validate_pool, so we don't need an actual provisioned database
    session_mocker.patch.object(
        SubDagOperator,
        "_validate_pool",
        return_value=None,
    )

    # load dev connection and variables
    env_load_variables_from_json(PROJECT_DIR / "resources" / "dev_variables.json")
    env_load_connections_from_json(PROJECT_DIR / "resources" / "dev_connections.json")

    # Replicate Airflow adding dags, plugins folders in system path at runtime
    sys.path.insert(0, str(PROJECT_DIR))
    sys.path.insert(1, str(PROJECT_DIR / "dags"))
    sys.path.insert(2, str(PROJECT_DIR / "plugins"))

    # Supress warnings from loading DAGs
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        dagbag = DagBag(dag_folder=PROJECT_DIR / "dags", include_examples=False)

    return dagbag


def env_load_variables_from_json(path: pathlib.Path) -> None:
    """Load Airflow Variables as environment variables from a JSON file generated
    by running `airflow variables export <filename>.json`. Variable values
    must be `str` or `int`.

    See this link for more information on Airflow Variables as environment variables
    https://airflow.apache.org/docs/apache-airflow/stable/howto/variable.html
    """
    with open(path, "r") as file:
        variables: Dict[str, Union[str, int]] = json.load(file)

    for name, value in variables.items():
        formatted_variable_name = f"AIRFLOW_VAR_{name.upper()}"
        os.environ[formatted_variable_name] = str(value)


def env_load_connections_from_json(path: pathlib.Path) -> None:
    """Load Airflow Connections as environment variables from a JSON file generated
    by running `airflow connections export <filename>.json`. Uses a Connection object
    to ensure correct Connection parsing.

    See this link for more information on Airflow Connections as environment variables
    https://airflow.apache.org/docs/apache-airflow/stable/howto/connection.html
    """
    from airflow.models import Connection

    with open(path, "r") as file:
        connections: Dict[str, Dict] = json.load(file)

    for name, params in connections.items():
        conn_instance = Connection.from_json(value=json.dumps(params), conn_id=name)
        formatted_connection_name = f"AIRFLOW_CONN_{name.upper()}"
        os.environ[formatted_connection_name] = conn_instance.get_uri()
