# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from airflow.models import BaseOperator
from airflow.exceptions import AirflowException
from statuspage.hook import DatasetStatusHook


class DatasetStatusOperator(BaseOperator):
    def __init__(
        self,
        name,
        description,
        status,
        statuspage_conn_id="statuspage_default",
        **kwargs
    ):
        """Create and update the status of a Data Engineering Dataset.

        :param name: Name of the Statuspage
        :param description: Description of the dataset
        :param status: one of [operational, under_maintenance, degraded_performance, partial_outage, major_outage]
        :param statuspage_conn_id: Airflow connection id for credentials
        """
        super(DatasetStatusOperator, self).__init__(**kwargs)
        self.statuspage_conn_id = statuspage_conn_id
        self.name = name
        self.description = description
        self.status = status

    def execute(self, context):
        conn = DatasetStatusHook(statuspage_conn_id=self.statuspage_conn_id).get_conn()
        comp_id = conn.get_or_create(self.name, self.description)

        if not comp_id:
            raise AirflowException("Failed to create or fetch component")

        self.log.info(
            "Setting status for {} ({}) to {}".format(self.name, comp_id, self.status)
        )

        comp_id = conn.update(comp_id, self.status)
        if not comp_id:
            raise AirflowException("Failed to update component")
