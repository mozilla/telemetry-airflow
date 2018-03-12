from airflow.contrib.operators.emr_create_job_flow_operator import EmrCreateJobFlowOperator
from airflow.utils import apply_defaults

from moz_emr.moz_emr_mixin import MozEmrMixin


class EmrCreateJobFlowSelectiveTemplateOperator(MozEmrMixin, EmrCreateJobFlowOperator):
    """
    Unfortunately, the task templater currently throws an exception if a field contains non-strings,
    so we have to separate the fields we want to template from the rest of them
    WARNING: currently, this implementation only supports separating top-level keys since the
    templated dictionary will *overwrite* any duplicate top-level keys.
    """
    template_fields = ['templated_job_flow_overrides']

    @apply_defaults
    def __init__(self,
                 templated_job_flow_overrides=None,
                 *args, **kwargs):
        super(EmrCreateJobFlowSelectiveTemplateOperator, self).__init__(*args, **kwargs)
        self.templated_job_flow_overrides = templated_job_flow_overrides

    def execute(self, context):
        self.job_flow_overrides.update(self.templated_job_flow_overrides)
        return super(EmrCreateJobFlowSelectiveTemplateOperator, self).execute(context)
