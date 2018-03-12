import logging

from airflow.exceptions import AirflowException
from airflow.contrib.sensors.emr_job_flow_sensor import EmrJobFlowSensor


class MozEmrClusterBaseSensor(EmrJobFlowSensor):
    # Overriding with the code in current airflow master to allow multiple failure states
    def poke(self, context):
        response = self.get_emr_response()

        if not response['ResponseMetadata']['HTTPStatusCode'] == 200:
            logging.info('Bad HTTP response: %s' % response)
            return False

        state = self.state_from_response(response)
        logging.info('Job flow currently %s' % state)

        if state in self.NON_TERMINAL_STATES:
            return False

        if state in self.FAILED_STATE:
            raise AirflowException('EMR job failed')

        return True


# This sensor is meant to be used for spinning up a cluster which will be used for several steps
# The sensor only returns true when it's in "waiting" state, meaning it's ready for another step to
# be added
# Note: 'TERMINATED' is a fatal state since the purpose is to have a cluster ready for additional
# steps in the DAG
class MozEmrClusterStartSensor(MozEmrClusterBaseSensor):
    NON_TERMINAL_STATES = ['STARTING', 'BOOTSTRAPPING', 'RUNNING', 'TERMINATING']
    FAILED_STATE = ['TERMINATED_WITH_ERRORS', 'TERMINATED']


class MozEmrClusterEndSensor(MozEmrClusterBaseSensor):
    NON_TERMINAL_STATES = ['STARTING', 'BOOTSTRAPPING', 'RUNNING', 'WAITING', 'TERMINATING']
    FAILED_STATE = ['TERMINATED_WITH_ERRORS']
