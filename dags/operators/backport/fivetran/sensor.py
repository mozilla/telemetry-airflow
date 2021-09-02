# Ported from https://github.com/fivetran/airflow-provider-fivetran/blob/main/fivetran_provider/sensors/fivetran.py

from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.utils.decorators import apply_defaults

from operators.backport.fivetran.hook import FivetranHook
from typing import Any


class FivetranSensor(BaseSensorOperator):
    """
    `FivetranSensor` monitors a Fivetran sync job for completion.

    Monitoring with `FivetranSensor` allows you to trigger downstream processes only
    when the Fivetran sync jobs have completed, ensuring data consistency. You can
    use multiple instances of `FivetranSensor` to monitor multiple Fivetran
    connectors. Note, it is possible to monitor a sync that is scheduled and managed
    from Fivetran; in other words, you can use `FivetranSensor` without using
    `FivetranOperator`. If used in this way, your DAG will wait until the sync job
    starts on its Fivetran-controlled schedule and then completes. `FivetranSensor`
    requires that you specify the `connector_id` of the sync job to start. You can
    find `connector_id` in the Settings page of the connector you configured in the
    `Fivetran dashboard <https://fivetran.com/dashboard/connectors>`_.


    :param fivetran_conn_id: `Conn ID` of the Connection to be used to configure
        the hook.
    :type fivetran_conn_id: str
    :param connector_id: ID of the Fivetran connector to sync, found on the
        Connector settings page in the Fivetran Dashboard.
    :type connector_id: str
    :param poke_interval: Time in seconds that the job should wait in
        between each tries
    :type poke_interval: int
    :param fivetran_retry_limit: # of retries when encountering API errors
    :type fivetran_retry_limit: Optional[int]
    :param fivetran_retry_delay: Time to wait before retrying API request
    :type fivetran_retry_delay: int
    """

    # Define which fields get jinjaified
    template_fields = ["connector_id"]

    @apply_defaults
    def __init__(
        self,
        connector_id: str,
        fivetran_conn_id: str = "fivetran",
        poke_interval: int = 60,
        fivetran_retry_limit: int = 3,
        fivetran_retry_delay: int = 1,
        **kwargs: Any
    ) -> None:
        super().__init__(**kwargs)
        self.fivetran_conn_id = fivetran_conn_id
        self.connector_id = connector_id
        self.poke_interval = poke_interval
        self.previous_completed_at = None
        self.fivetran_retry_limit = fivetran_retry_limit
        self.fivetran_retry_delay = fivetran_retry_delay
        self.hook = None

    def _get_hook(self) -> FivetranHook:
        if self.hook is None:
            self.hook = FivetranHook(
                self.fivetran_conn_id,
                retry_limit=self.fivetran_retry_limit,
                retry_delay=self.fivetran_retry_delay,
            )
        return self.hook

    def poke(self, context):
        hook = self._get_hook()
        if self.previous_completed_at is None:
            self.previous_completed_at = hook.get_last_sync(self.connector_id)
        return hook.get_sync_status(self.connector_id, self.previous_completed_at)
