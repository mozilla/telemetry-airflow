# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from statuspage.client import DatasetStatus
from airflow.hooks.base_hook import BaseHook
from airflow.exceptions import AirflowException
import os


class DatasetStatusHook(BaseHook):
    """Create and update the status of a dataset."""

    def __init__(self, api_key=os.environ['STATUSPAGE_API_KEY'], statuspage_conn_id=None):
        """Initialize the client with an API key.

        :param api_key: Statuspage API key
        :param statuspage_conn_id: connection with the API token in the password field
        """
        self.api_key = api_key or self.get_connection(statuspage_conn_id).password
        if not api_key:
            raise AirflowException("Missing an API key for Statuspage")
    
    def get_conn(self):
        return DatasetStatusHook(self.api_key)