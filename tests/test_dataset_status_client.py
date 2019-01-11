# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import pytest
from collections import namedtuple
from jsonschema.exceptions import ValidationError
from plugins.statuspage.dataset_client import DatasetStatus


@pytest.fixture(autouse=True)
def mock_statuspage_client(monkeypatch):
    def _mock_request(*args, **kwargs):
        class MockResponse:
            def json(self):
                return {"id": 42}

        return MockResponse()

    def _mock_id(*args, **kwargs):
        return 43

    monkeypatch.setattr(
        "plugins.statuspage.dataset_client.StatuspageClient._request", _mock_request
    )
    monkeypatch.setattr(
        "plugins.statuspage.dataset_client.StatuspageClient.get_id", _mock_id
    )


def call_args(mocked_def):
    """Assert that a call argument in a mocked definition is an expected value.

    This is not expected for use with partial or lambda functions.

    :param mocked_def:  A Mock object for a method or definition
    :returns:           A namedtuple with definition parameters
    """
    # callargs is a two-tuple, with the arg dictionary in the second position
    arg_dict = mocked_def.call_args[1]
    CallArguments = namedtuple("CallArguments", arg_dict.keys())
    return CallArguments(**arg_dict)


def test_create_incident_investigation(mocker):
    mocked = mocker.patch(
        "plugins.statuspage.dataset_client.StatuspageClient.create_incident"
    )
    client = DatasetStatus("test_key")
    client.create_incident_investigation("Test Dataset", "test_id")

    mocked.assert_called_once()
    args = call_args(mocked)
    assert args.incident_status == "Investigating"
    assert "Investigating Errors in Test Dataset" in args.name
    assert "Test Dataset is experiencing errors." in args.body


def test_create_incident_investigation_custom_body(mocker):
    mocked = mocker.patch(
        "plugins.statuspage.dataset_client.StatuspageClient.create_incident"
    )
    client = DatasetStatus("test_key")
    client.create_incident_investigation(
        "Test Dataset",
        "test_id",
        body="The dataset has been delayed.",
        component_status="degraded_performance",
    )

    mocked.assert_called_once()
    args = call_args(mocked)
    assert args.body == "The dataset has been delayed."
    assert args.component_status == "degraded_performance"


def test_create_incident_investigation_raises_validation_error():
    client = DatasetStatus("test_key")

    with pytest.raises(ValidationError):
        client.create_incident_investigation(
            "Test Dataset", "test_id", component_status="bogus"
        )
