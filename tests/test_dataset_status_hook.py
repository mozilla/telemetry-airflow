# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import pytest
from plugins.statuspage.hook import DatasetStatusHook
from airflow.exceptions import AirflowException


API_KEY = "test_key"


def test_hook_reads_environment(monkeypatch):
    monkeypatch.setenv("STATUSPAGE_API_KEY", API_KEY)
    hook = DatasetStatusHook()
    assert hook.api_key == API_KEY


def test_hook_reads_connection(mocker):
    test_connection = "test_statuspage_conn"
    mock_get_conn = mocker.patch("airflow.hooks.base_hook.BaseHook.get_connection")
    mock_get_conn.return_value.password.return_value = API_KEY

    hook = DatasetStatusHook(statuspage_conn_id=test_connection)
    hook.api_key == API_KEY

    mock_get_conn.assert_called_once_with(test_connection)
