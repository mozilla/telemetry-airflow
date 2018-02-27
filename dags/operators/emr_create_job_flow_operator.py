from airflow.contrib.operators.emr_create_job_flow_operator import EmrCreateJobFlowOperator


class EmrCreateJobFlowOperator(EmrCreateJobFlowOperator):
    """
    Override airflow.contrib.operators.emr_create_job_flow_operator to support
    templating. This has been added upstream but not released and should be
    removed once we upgrade to latest stable release AIRFLOW-713.
    """
    template_fields = ['job_flow_overrides']
