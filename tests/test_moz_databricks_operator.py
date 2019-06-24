# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import boto3
import pytest
from moto import mock_s3
from plugins.moz_databricks import MozDatabricksSubmitRunOperator

# The environment variables required by the MozDatabricks operator must be available
# at module import because the `os.environ` is accessed in the class scope. These
# variables are used by Airflow to template variables. Monkeypatch occurs after import,
# so the variables are defined in `tox.ini` instead.


@pytest.fixture()
def client():
    """Create a moto generated fixture for s3. Using this fixture will put the function
    under test in the same scope as the @mock_s3 decorator. See
    https://github.com/spulec/moto/issues/620.
    """
    mock_s3().start()
    client = boto3.resource("s3")
    client.create_bucket(Bucket="telemetry-test-bucket")
    client.create_bucket(Bucket="telemetry-airflow")
    yield client
    mock_s3().stop()


@pytest.fixture()
def mock_hook(mocker):
    mock_hook = mocker.patch("airflow.contrib.operators.databricks_operator.DatabricksHook")
    mock_hook_instance = mock_hook.return_value
    mock_hook_instance.submit_run.return_value = 1
    return mock_hook_instance


def test_missing_tbv_or_mozetl_env(mock_hook):
    with pytest.raises(ValueError):
        MozDatabricksSubmitRunOperator(
            job_name="test_databricks", env={}, instance_count=1
        )


def test_mozetl_success(mock_hook, client):
    client.Object("telemetry-test-bucket", "steps/mozetl_runner.py").put(
        Body="raise NotImplementedError"
    )
    operator = MozDatabricksSubmitRunOperator(
        task_id="test_databricks",
        job_name="test_databricks",
        env={"MOZETL_COMMAND": "test"},
        instance_count=1,
    )
    operator.execute(None)
    mock_hook.submit_run.assert_called_once()

    # https://docs.python.org/3.3/library/unittest.mock.html#unittest.mock.Mock.call_args
    # call_args is a tuple where the first element is a list of elements. The first element
    # in `submit_run` is the constructed json blob.
    json = mock_hook.submit_run.call_args[0][0]
    assert json.get("spark_python_task") is not None


def test_tbv_success(mock_hook):
    operator = MozDatabricksSubmitRunOperator(
        task_id="test_databricks",
        job_name="test_databricks",
        env={"TBV_CLASS": "test", "ARTIFACT_URL": "https://test.amazonaws.com/test"},
        instance_count=1,
    )
    operator.execute(None)
    mock_hook.submit_run.assert_called_once()

    json = mock_hook.submit_run.call_args[0][0]
    assert json.get("spark_jar_task") is not None


def test_default_python_version(mock_hook, client):
    # run with default
    operator = MozDatabricksSubmitRunOperator(
        task_id="test_databricks",
        job_name="test_databricks",
        env={"MOZETL_COMMAND": "test"},
        instance_count=1,
    )
    operator.execute(None)
    mock_hook.submit_run.assert_called_once()

    json = mock_hook.submit_run.call_args[0][0]
    assert (
        json["new_cluster"]["spark_env_vars"]["PYSPARK_PYTHON"]
        == "/databricks/python3/bin/python3"
    )

    # run with python 2 specifically
    operator = MozDatabricksSubmitRunOperator(
        task_id="test_databricks",
        job_name="test_databricks",
        env={"MOZETL_COMMAND": "test"},
        python_version=2,
        instance_count=1,
    )
    operator.execute(None)

    json = mock_hook.submit_run.call_args[0][0]
    assert json["new_cluster"]["spark_env_vars"].get("PYSPARK_PYTHON") is None

    with pytest.raises(ValueError):
        MozDatabricksSubmitRunOperator(
            task_id="test_databricks",
            job_name="test_databricks",
            env={"MOZETL_COMMAND": "test"},
            python_version=4,
            instance_count=1,
        ).execute(None)


def test_set_mozetl_path_and_branch(mock_hook, client):
    def mocked_run_submit_args(env):
        MozDatabricksSubmitRunOperator(
            task_id="test_databricks",
            job_name="test_databricks",
            env=env,
            instance_count=1,
        ).execute(None)
        return mock_hook.submit_run.call_args[0][0]

    json = mocked_run_submit_args(
        {
            "MOZETL_COMMAND": "test",
            "MOZETL_GIT_PATH": "https://custom.com/repo.git",
            "MOZETL_GIT_BRANCH": "dev",
        }
    )
    assert (
        json["libraries"][0]["pypi"]["package"] == "git+https://custom.com/repo.git@dev"
    )

    json = mocked_run_submit_args(
        {"MOZETL_COMMAND": "test", "MOZETL_GIT_BRANCH": "dev"}
    )
    assert (
        json["libraries"][0]["pypi"]["package"]
        == "git+https://github.com/mozilla/python_mozetl.git@dev"
    )


def test_mozetl_skips_generates_runner_if_exists(mocker, client):
    client.Object("telemetry-test-bucket", "steps/mozetl_runner.py").put(
        Body="raise NotImplementedError"
    )
    mock_hook = mocker.patch("airflow.contrib.operators.databricks_operator.DatabricksHook")
    mock_runner = mocker.patch("plugins.moz_databricks.generate_runner")

    operator = MozDatabricksSubmitRunOperator(
        task_id="test_databricks",
        job_name="test_databricks",
        env={"MOZETL_COMMAND": "test"},
        instance_count=1,
    )
    operator.execute(None)
    assert mock_hook.called
    assert mock_runner.assert_not_called
    assert (
        operator.json["spark_python_task"]["python_file"]
        == "s3://telemetry-test-bucket/steps/mozetl_runner.py"
    )


def test_mozetl_generates_runner_if_not_exists(mocker, client):
    mock_hook = mocker.patch("airflow.contrib.operators.databricks_operator.DatabricksHook")
    mock_runner = mocker.patch("plugins.moz_databricks.generate_runner")

    operator = MozDatabricksSubmitRunOperator(
        task_id="test_databricks",
        job_name="test_databricks",
        env={"MOZETL_COMMAND": "test"},
        instance_count=1,
    )
    operator.execute(None)
    assert mock_hook.called
    assert mock_runner.called
    assert (
        operator.json["spark_python_task"]["python_file"]
        == "s3://telemetry-test-bucket/steps/mozetl_runner.py"
    )


def test_mozetl_generates_runner_for_external_module(mocker, client):
    client.Object("telemetry-test-bucket", "steps/mozetl_runner.py").put(
        Body="raise NotImplementedError"
    )
    mock_hook = mocker.patch("airflow.contrib.operators.databricks_operator.DatabricksHook")
    mock_runner = mocker.patch("plugins.moz_databricks.generate_runner")

    operator = MozDatabricksSubmitRunOperator(
        task_id="test_databricks",
        job_name="test_databricks",
        env={"MOZETL_COMMAND": "test", "MOZETL_EXTERNAL_MODULE": "custom_module"},
        instance_count=1,
    )
    operator.execute(None)
    assert mock_hook.called
    assert mock_runner.called
    assert (
        operator.json["spark_python_task"]["python_file"]
        == "s3://telemetry-test-bucket/steps/custom_module_runner.py"
    )
