# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import pytest
from plugins.statuspage.operator import DatasetStatusOperator
from airflow.exceptions import AirflowException


@pytest.fixture
def mock_hook(mocker):
    return mocker.patch("plugins.statuspage.operator.DatasetStatusHook")


def test_execute(mock_hook):
    testing_component_id = 42

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


def test_conn_get_or_create_failure(mock_hook):
    mock_hook.return_value.get_conn().get_or_create.return_value = None

    with pytest.raises(AirflowException, match="create or fetch component"):
        operator = DatasetStatusOperator(
            task_id="test_status",
            name="airflow",
            description="testing status",
            status="operational",
        )
        operator.execute(None)


def test_conn_update_failure(mock_hook):
    mock_hook.return_value.get_conn().update.return_value = None

    with pytest.raises(AirflowException, match="update component"):
        operator = DatasetStatusOperator(
            task_id="test_status",
            name="airflow",
            description="testing status",
            status="operational",
        )
        operator.execute(None)
