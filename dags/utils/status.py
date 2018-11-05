from airflow.operators.dataset_status import DatasetStatusOperator


def register_status(operator, name, description):
    """Wrap an operator with an external status page.
    
    :param operator: An Airflow operator to set upstream
    :type airflow.models.BaseOperator:

    :returns: The original airflow operator with downstream dependenc
    """

    kwargs = {"name": name, "description": description, "dag": operator.dag}

    # create and operator on the success case
    success = DatasetStatusOperator(
        trigger_rule="all_success",
        task_id="{}_success".format(operator.task_id),
        status="operational",
        **kwargs
    )

    # create an operator on the failure case
    failure = DatasetStatusOperator(
        trigger_rule="all_failed",
        task_id="{}_failure".format(operator.task_id),
        status="partial_outage",
        **kwargs
    )

    operator >> success
    operator >> failure

    return operator
