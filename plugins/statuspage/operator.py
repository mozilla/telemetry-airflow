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
        create_incident=False,
        incident_body=None,
        **kwargs
    ):
        """Create and update the status of a Data Engineering Dataset.

        :param name: Name of the Statuspage
        :param description: Description of the dataset
        :param status: one of [operational, under_maintenance, degraded_performance, partial_outage, major_outage]
        :param statuspage_conn_id: Airflow connection id for credentials
        :param create_incident: A flag to enable automated filing of Statuspage incidents
        :param incident_body:   Optional text for describing the incident
        """
        super(DatasetStatusOperator, self).__init__(**kwargs)
        self.statuspage_conn_id = statuspage_conn_id
        self.name = name
        self.description = description
        self.status = status
        self.create_incident = create_incident
        self.incident_body = incident_body

    def execute(self, context):
        conn = DatasetStatusHook(statuspage_conn_id=self.statuspage_conn_id).get_conn()
        comp_id = conn.get_or_create(self.name, self.description)

        self.log.info(
            "Setting status for {} ({}) to {}".format(self.name, comp_id, self.status)
        )

        if self.create_incident:
            incident_id = conn.create_incident_investigation(
                self.name, comp_id, self.incident_body, self.status
            )
            self.log.info("Created incident with id {}".format(incident_id))
        else:
            comp_id = conn.update(comp_id, self.status)
