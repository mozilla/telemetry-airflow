# Ported from https://github.com/fivetran/airflow-provider-fivetran/blob/main/tests/hooks/test_hooks.py
"""
Unittest module to test Fivetran Hooks.
Requires the unittest, pytest, and requests-mock Python libraries.
Run test:
    python3 -m unittest operators.backport.fivetran.tests.test_hooks.TestFivetranHook
"""

import logging
import os
import pytest
import requests_mock
import unittest
from unittest import mock

# Import Hook
from operators.backport.fivetran.hook import FivetranHook


log = logging.getLogger(__name__)

MOCK_FIVETRAN_RESPONSE_PAYLOAD = {
    "code": "Success",
    "data": {
        "id": "interchangeable_revenge",
        "group_id": "rarer_gradient",
        "service": "google_sheets",
        "service_version": 1,
        "schema": "google_sheets.fivetran_google_sheets_spotify",
        "connected_by": "mournful_shalt",
        "created_at": "2021-03-05T22:58:56.238875Z",
        "succeeded_at": "2021-03-23T20:55:12.670390Z",
        "failed_at": 'null',
        "sync_frequency": 360,
        "schedule_type": "manual",
        "status": {
            "setup_state": "connected",
            "sync_state": "scheduled",
            "update_state": "on_schedule",
            "is_historical_sync": False,
            "tasks": [],
            "warnings": []
        },
        "config": {
            "latest_version": "1",
            "sheet_id": "https://docs.google.com/spreadsheets/d/.../edit#gid=...",
            "named_range": "fivetran_test_range",
            "authorization_method": "User OAuth",
            "service_version": "1",
            "last_synced_changes__utc_": "2021-03-23 20:54"
        }
    }
}


# Mock the `conn_fivetran` Airflow connection (note the `@` after `API_SECRET`)
@mock.patch.dict('os.environ', AIRFLOW_CONN_CONN_FIVETRAN='http://API_KEY:API_SECRET@')
class TestFivetranHook(unittest.TestCase):
    """ 
    Test functions for Fivetran Hook. 
    Mocks responses from Fivetran API.
    """

    @requests_mock.mock()
    def test_get_connector(self, m):

        m.get('https://api.fivetran.com/v1/connectors/interchangeable_revenge',
              json=MOCK_FIVETRAN_RESPONSE_PAYLOAD)

        hook = FivetranHook(
            fivetran_conn_id='conn_fivetran',
        )

        result = hook.get_connector(connector_id='interchangeable_revenge')

        assert result['status']['setup_state'] == 'connected'

    @requests_mock.mock()
    def test_start_fivetran_sync(self, m):

        m.post('https://api.fivetran.com/v1/connectors/interchangeable_revenge/force',
               json=MOCK_FIVETRAN_RESPONSE_PAYLOAD)

        hook = FivetranHook(
            fivetran_conn_id='conn_fivetran',
        )

        result = hook.start_fivetran_sync(connector_id='interchangeable_revenge')

        assert result['code'] == 'Success'


if __name__ == '__main__':
    unittest.main()
