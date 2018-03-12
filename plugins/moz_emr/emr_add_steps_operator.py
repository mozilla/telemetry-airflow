from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator

from moz_emr.moz_emr_mixin import MozEmrMixin


class EmrAddStepsOperator(MozEmrMixin, EmrAddStepsOperator):
    """
    We need templated steps so we can pass in date macros, etc
    """
    template_fields = ['job_flow_id', 'steps']

    """
    Override so we only return one step_id
    """
    def execute(self, context):
        step_ids = super(EmrAddStepsOperator, self).execute(context)
        return step_ids[0]
