#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.sensors.base import BaseSensorOperator


class BigQuerySQLSensorOperator(BaseSensorOperator):
    """
    Checks for the existence data in Google Bigquery.

    :param sql: The sql query to run, should return a single row with a
        single value. If that value is coerced to false in some way,
        the sensor continues to wait.
    :type sql: str
    :param gcp_conn_id: The connection ID to use when connecting to
        Google BigQuery.
    :type gcp_conn_id: str
    :param use_legacy_sql: Whether to use BQ legacy SQL
    :type use_legacy_sql: bool
    :param timeout: Time in seconds to wait for the sensor,
        defaults to 1 day.
    :type timeout: int
    """

    template_fields = (*BaseSensorOperator.template_fields, "sql")

    def __init__(
        self,
        sql,
        gcp_conn_id="bigquery_default_conn",
        use_legacy_sql=False,
        timeout=60 * 60 * 24,
        *args,
        **kwargs
    ):
        super().__init__(timeout=timeout, *args, **kwargs)  # noqa: B026
        self.sql = sql
        self.gcp_conn_id = gcp_conn_id
        self.use_legacy_sql = use_legacy_sql
        self.poke_interval = 120
        self.mode = "reschedule"
        self.pool = "DATA_ENG_EXTERNALTASKSENSOR"

    def poke(self, context):
        self.log.info("Running query: %s", self.sql)
        record = self.get_db_hook().get_first(self.sql)
        self.log.info("Resulting Record: %s", record)

        if record:
            return record[0].lower() not in (
                "0",
                "",
                "false",
                "null",
            )
        return False

    def get_db_hook(self):
        return BigQueryHook(
            gcp_conn_id=self.gcp_conn_id, use_legacy_sql=self.use_legacy_sql
        )
