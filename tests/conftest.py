import pathlib
import sys
import warnings

import pytest
from airflow.models import DagBag
from dotenv import load_dotenv

# get absolute project directory path no matter the environment
PROJECT_DIR = pathlib.Path(__file__).resolve().parent.parent


@pytest.fixture(scope="session")
def get_dag_bag() -> DagBag:
    # load dev connection and variables
    assert load_dotenv(PROJECT_DIR / "resources" / "dev_connections.env")
    assert load_dotenv(PROJECT_DIR / "resources" / "dev_variables.env")

    # Replicate Airflow adding dags, plugins folders in system path at runtime
    sys.path.insert(0, str(PROJECT_DIR))
    sys.path.insert(1, str(PROJECT_DIR / "dags"))
    sys.path.insert(2, str(PROJECT_DIR / "plugins"))

    # Supress warnings from loading DAGs
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        dagbag = DagBag(dag_folder=PROJECT_DIR / "dags", include_examples=False)

    return dagbag
