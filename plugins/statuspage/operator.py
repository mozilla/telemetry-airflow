# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from airflow.models import BaseOperator
from statuspage.hook import DatasetStatusHook
from airflow.exceptions import AirflowException


class DatasetStatusOperator(BaseOperator):
    def __init__(self, name, description, status, statuspage_conn_id='statuspage_default', **kwargs):
        super(DatasetStatusOperator, self).__init__(**kwargs)
        self.statuspage_conn_id = statuspage_conn_id
        self.name = name
        self.description = description,
        self.status = status

    def execute(self, context):
        conn = DatasetStatusHook(statuspage_conn_id=self.statuspage_conn_id).get_conn()
        component_id = conn.get_or_create(self.name, self.description)
        if not component_id:
            raise AirflowException("DatasetStatus API failed to create or fetch components")
        self.log.info("Setting status for {} ({}) to {}".format(self.name, component_id, self.status))
        conn.update(component_id, self.status)

