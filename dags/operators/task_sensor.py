import datetime
import os

from sqlalchemy import func

from airflow.exceptions import AirflowException
from airflow.models import DagBag, DagModel, DagRun, TaskInstance
from airflow.operators.sensors import ExternalTaskSensor
from airflow.utils.db import provide_session
from airflow.utils.decorators import apply_defaults
from airflow.utils.state import State


"""
Custom ExternalTaskSensor implementation that also checks for failed states.
Once we update to Airflow 2, this implementation can be deprecated in favour of ExternalTaskSensor.

Based on https://github.com/apache/airflow/blob/v1-10-stable/airflow/sensors/external_task_sensor.py
"""

class ExternalTaskCompletedSensor(ExternalTaskSensor):
    """
    We override __init__ and poke methods to support:
    - Checking external upstream tasks for failed states (e.g. State.FAILED, State.UPSTREAM_FAILED, ...)
        to stop the ExternalTaskSensor from being rescheduled indefinitely.
    - A new parameter `failed_states` has been added to define the states which indicate that the
        external task has failed.

    """

    @apply_defaults
    def __init__(self, failed_states = None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.failed_states = failed_states or [State.FAILED, State.UPSTREAM_FAILED, State.SKIPPED]

    @provide_session
    def poke(self, context, session=None):
        # implementation copied from https://github.com/apache/airflow/blob/v1-10-stable/airflow/sensors/external_task_sensor.py

        if self.execution_delta:
            dttm = context['execution_date'] - self.execution_delta
        elif self.execution_date_fn:
            # Moz specific - _handle_execution_date_fn may not be defined in this context
            raise AirflowException("execution_date_fn is not supported by this sensor.")
        else:
            dttm = context['execution_date']

        dttm_filter = dttm if isinstance(dttm, list) else [dttm]
        serialized_dttm_filter = ','.join(
            [datetime.isoformat() for datetime in dttm_filter])

        self.log.info(
            'Poking for %s.%s on %s ... ',
            self.external_dag_id, self.external_task_id, serialized_dttm_filter
        )

        DM = DagModel
        TI = TaskInstance
        DR = DagRun
        if self.check_existence:
            dag_to_wait = session.query(DM).filter(
                DM.dag_id == self.external_dag_id
            ).first()

            if not dag_to_wait:
                raise AirflowException('The external DAG '
                                       '{} does not exist.'.format(self.external_dag_id))
            else:
                if not os.path.exists(dag_to_wait.fileloc):
                    raise AirflowException('The external DAG '
                                           '{} was deleted.'.format(self.external_dag_id))

            if self.external_task_id:
                refreshed_dag_info = DagBag(dag_to_wait.fileloc).get_dag(self.external_dag_id)
                if not refreshed_dag_info.has_task(self.external_task_id):
                    raise AirflowException('The external task'
                                           '{} in DAG {} does not exist.'.format(self.external_task_id,
                                                                                 self.external_dag_id))
        
        # custom implementation to check for failed tasks
        if self.external_task_id:
            # Moz specific - rename count to count_allowed
            # .count() is inefficient
            count_allowed = session.query(func.count()).filter(
                TI.dag_id == self.external_dag_id,
                TI.task_id == self.external_task_id,
                TI.state.in_(self.allowed_states),
                TI.execution_date.in_(dttm_filter),
            ).scalar()

            # Moz specific - counting failed upstream states
            count_failed = session.query(func.count()).filter(
                TI.dag_id == self.external_dag_id,
                TI.task_id == self.external_task_id,
                TI.state.in_(self.failed_states),
                TI.execution_date.in_(dttm_filter),
            ).scalar()
        else:
            # Moz specific - rename count to count_allowed
            # .count() is inefficient
            count_allowed = session.query(func.count()).filter(
                DR.dag_id == self.external_dag_id,
                DR.state.in_(self.allowed_states),
                DR.execution_date.in_(dttm_filter),
            ).scalar()

            # Moz specific - counting failed upstream states
            count_failed = session.query(func.count()).filter(
                DR.dag_id == self.external_dag_id,
                DR.state.in_(self.failed_states),
                DR.execution_date.in_(dttm_filter),
            ).scalar()

        # Moz specific - set sensor to failed state if external task has faileds
        if count_failed == len(dttm_filter):
            if self.external_task_id:
                raise AirflowException(
                    f'The external task {self.external_task_id} in DAG {self.external_dag_id} failed.'
                )
            else:
                raise AirflowException(f'The external DAG {self.external_dag_id} failed.')

        session.commit()
        return count_allowed == len(dttm_filter)