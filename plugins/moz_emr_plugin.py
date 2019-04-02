from airflow.plugins_manager import AirflowPlugin

from .moz_emr import EmrAddStepsOperator, EmrCreateJobFlowSelectiveTemplateOperator, \
        MozEmrClusterStartSensor, MozEmrClusterEndSensor


class MozEmr(AirflowPlugin):
    name = "moz_emr"
    operators = [EmrAddStepsOperator,
                 EmrCreateJobFlowSelectiveTemplateOperator,
                 MozEmrClusterStartSensor,
                 MozEmrClusterEndSensor]
