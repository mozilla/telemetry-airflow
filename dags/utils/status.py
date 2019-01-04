from airflow.operators.dataset_status import DatasetStatusOperator
from airflow.hooks.dataset_status import DatasetStatusHook


def register_status(operator, name, description, on_success=False):
    """Wrap an operator with an external status page.
    
    The default behavior will only set the state of a dataset to partial_outage.

    :param airflow.models.BaseOperator operator: An Airflow operator to set upstream
    :param str name:            The name of the dataset
    :param str description:     A short (1-2 sentence) description of the dataset.
    :param boolean on_success:  Indicator to set the dataset state to operational on success

    :returns: The original airflow operator with downstream dependencies
    """

    kwargs = {"name": name, "description": description, "dag": operator.dag}

    conn = DatasetStatusHook().get_conn()
    conn.get_or_create(name, description)

    if on_success:
        # create and operator on the success case
        success = DatasetStatusOperator(
            trigger_rule="all_success",
            task_id="{}_success".format(operator.task_id),
            status="operational",
            **kwargs
        )
        operator >> success

    # create an operator on the failure case
    failure = DatasetStatusOperator(
        trigger_rule="all_failed",
        task_id="{}_failure".format(operator.task_id),
        status="partial_outage",
        **kwargs
    )

    operator >> failure

    return operator
