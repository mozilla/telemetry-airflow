from airflow.plugins_manager import AirflowPlugin
from databricks import databricks_hook as hook, databricks_operator as operator


class DatasetStatusPlugin(AirflowPlugin):
    name = "dataset_status"
    hooks = [hook.DatabricksHook]
    operators = [
        operator.DatabricksSubmitRunOperator,
        operator.DatabricksRunNowOperator,
    ]
