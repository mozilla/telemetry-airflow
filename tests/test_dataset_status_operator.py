# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import pytest
from plugins.statuspage.operator import DatasetStatusOperator
from requests.exceptions import HTTPError
from .test_dataset_status_client import call_args


@pytest.fixture()
def mock_api_keys(monkeypatch):
    monkeypatch.setenv("STATUSPAGE_API_KEY", "test_key")


def test_execute(mocker):
    testing_component_id = 42
    mock_hook = mocker.patch("plugins.statuspage.operator.DatasetStatusHook")
    mock_conn = mock_hook.return_value.get_conn()
    mock_conn.get_or_create.return_value = testing_component_id

    operator = DatasetStatusOperator(
        task_id="test_status",
        name="airflow",
        description="testing status",
        status="operational",
    )
    operator.execute(None)

    mock_conn.get_or_create.assert_called_once_with("airflow", "testing status")
    mock_conn.update.assert_called_once_with(testing_component_id, "operational")


def test_conn_get_or_create_failure(mocker, mock_api_keys):

    mock_req = mocker.patch(
        "plugins.statuspage.dataset_client.StatuspageClient._request"
    )
    mock_req.side_effect = HTTPError("create or fetch component")
    mock_update = mocker.patch(
        "plugins.statuspage.dataset_client.StatuspageClient.update_component"
    )

    with pytest.raises(HTTPError, match="create or fetch component"):
        operator = DatasetStatusOperator(
            task_id="test_status",
            name="airflow",
            description="testing status",
            status="operational",
        )
        operator.execute(None)

    mock_update.assert_not_called()


@pytest.fixture()
def mock_statuspage_init(mocker, mock_api_keys):
    """Mock everything necessary for `DatasetClient.get_or_create`"""

    class MockResponse:
        def json(self):
            return {"id": 42}

    mock_req = mocker.patch(
        "plugins.statuspage.dataset_client.StatuspageClient._request"
    )
    mock_req.return_value = MockResponse()
    mock_id = mocker.patch("plugins.statuspage.dataset_client.StatuspageClient.get_id")
    mock_id.return_value = 43


def test_conn_update_failure(mocker, mock_statuspage_init):
    mock_update = mocker.patch(
        "plugins.statuspage.dataset_client.StatuspageClient.update_component"
    )
    mock_update.side_effect = HTTPError("update component")

    with pytest.raises(HTTPError, match="update component"):
        operator = DatasetStatusOperator(
            task_id="test_status",
            name="airflow",
            description="testing status",
            status="operational",
        )
        operator.execute(None)

    mock_update.assert_called_once()


def test_create_incident(mocker, mock_statuspage_init):
    mock_incident = mocker.patch(
        "plugins.statuspage.dataset_client.StatuspageClient.create_incident"
    )

    operator = DatasetStatusOperator(
        task_id="test_status",
        name="airflow",
        description="testing status",
        status="degraded_performance",
        create_incident=True,
        incident_body="investigating degraded performance",
    )
    operator.execute(None)

    mock_incident.assert_called_once()
    args = call_args(mock_incident)
    assert "airflow" in args.name
    assert args.body == "investigating degraded performance"
    assert args.component_status == "degraded_performance"
