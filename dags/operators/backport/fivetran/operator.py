# Ported from https://github.com/fivetran/airflow-provider-fivetran/blob/main/fivetran_provider/operators/fivetran.py

from airflow.models import BaseOperator, BaseOperatorLink
from airflow.utils.decorators import apply_defaults

from operators.backport.fivetran.hook import FivetranHook
from typing import Optional


class RegistryLink(BaseOperatorLink):
    """Link to Registry"""

    name = "Astronomer Registry"

    # Note that this may break, get_link no longer supports dttm as an arg in v2.3
    # Instead it expects a ti_key. Looks like fivetran hasn't released a
    # new airflow-provider-fivetran>1.1.2 yet.
    # More details at https://github.com/apache/airflow/pull/21798
    def get_link(self, operator, dttm):
        """Get link to registry page."""

        registry_link = (
            "https://registry.astronomer.io/providers/{provider}/modules/{operator}"
        )
        return registry_link.format(provider="fivetran", operator="fivetranoperator")


class FivetranOperator(BaseOperator):
    """
    `FivetranOperator` starts a Fivetran sync job.

    `FivetranOperator` requires that you specify the `connector_id` of the sync job to
    start. You can find `connector_id` in the Settings page of the connector you
    configured in the `Fivetran dashboard <https://fivetran.com/dashboard/connectors>`_.
    Note that when a Fivetran sync job is controlled via an Operator, it is no longer
    run on the schedule as managed by Fivetran. In other words, it is now scheduled only
    from Airflow.

    :param fivetran_conn_id: `Conn ID` of the Connection to be used to configure
        the hook.
    :type fivetran_conn_id: Optional[str]
    :param fivetran_retry_limit: # of retries when encountering API errors
    :type fivetran_retry_limit: Optional[int]
    :param fivetran_retry_delay: Time to wait before retrying API request
    :type fivetran_retry_delay: int
    :param connector_id: ID of the Fivetran connector to sync, found on the
        Connector settings page.
    :type connector_id: str
    """

    operator_extra_links = (RegistryLink(),)

    # Define which fields get jinjaified
    template_fields = ["connector_id"]

    @apply_defaults
    def __init__(
        self,
        connector_id: str,
        run_name: Optional[str] = None,
        timeout_seconds: Optional[int] = None,
        fivetran_conn_id: str = "fivetran",
        fivetran_retry_limit: int = 3,
        fivetran_retry_delay: int = 1,
        poll_frequency: int = 15,
        **kwargs
    ):
        super().__init__(**kwargs)
        self.fivetran_conn_id = fivetran_conn_id
        self.fivetran_retry_limit = fivetran_retry_limit
        self.fivetran_retry_delay = fivetran_retry_delay
        self.connector_id = connector_id
        self.poll_frequency = poll_frequency

    def _get_hook(self) -> FivetranHook:
        return FivetranHook(
            self.fivetran_conn_id,
            retry_limit=self.fivetran_retry_limit,
            retry_delay=self.fivetran_retry_delay,
        )

    def execute(self, context):
        hook = self._get_hook()
        hook.prep_connector(self.connector_id)
        return hook.start_fivetran_sync(self.connector_id)
