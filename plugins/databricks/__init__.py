"""
A backport of the Databricks hooks and operators from apache/airflow v1.10 stable.

The Mozilla Data Platform currently runs airflow v1.9, which includes a Databricks
operator with a failure mode that can cause duplicate data. The operator intermittenly
fails due to insufficient time between retries, leaving a zombie cluster that continues
to run in the background. This plugin includes an operator that patches this bug and
relieves mitigations around this bug, in particular [1] and [2].

[1]
[2] https://github.com/mozilla/telemetry-airflow/pull/417
[3] https://github.com/mozilla/telemetry-airflow/pull/416
"""

from airflow.plugins_manager import AirflowPlugin
from databricks import databricks_hook as hook, databricks_operator as operator


class DatabricksPlugin(AirflowPlugin):
    name = "databricks"
    hooks = [hook.DatabricksHook]
    operators = [
        operator.DatabricksSubmitRunOperator,
        operator.DatabricksRunNowOperator,
    ]
