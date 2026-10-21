import time

import pendulum
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.context import Context
from google.cloud import monitoring_v3

# Project whose Cloud Monitoring metrics scope includes all the ingestion projects.
METRICS_SCOPE_PROJECT = "moz-fx-data-shared-prod"


class IngestionLatencySensor(BaseSensorOperator):
    """
    Wait until the ingestion latency indicates that all data from the previous UTC day has been loaded into BigQuery.

    The `oldest_unacked_message_age` metrics of all specified PubSub subscriptions are summed to estimate the age of the
    oldest data still in the ingestion pipeline.

    :param list[str] pubsub_subscriptions: PubSub subscriptions, specified as "project/subscription_id".
    :param str gcp_conn_id: The connection ID used to connect to Google Cloud (default is "google_cloud_default").
    """

    def __init__(
        self,
        *,
        pubsub_subscriptions: list[str],
        gcp_conn_id: str = "google_cloud_default",
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.pubsub_subscriptions = pubsub_subscriptions
        self.gcp_conn_id = gcp_conn_id

    def poke(self, context: Context) -> bool:
        metrics_client = monitoring_v3.MetricServiceClient(
            credentials=GoogleBaseHook(gcp_conn_id=self.gcp_conn_id).get_credentials()
        )
        now = int(time.time())
        metrics_lookback_minutes = 30
        metrics_lookback_interval = monitoring_v3.TimeInterval(
            start_time={"seconds": now - (metrics_lookback_minutes * 60)},
            end_time={"seconds": now},
        )

        total_latency = 0
        for pubsub_subscription in self.pubsub_subscriptions:
            project_id, subscription_id = pubsub_subscription.split("/", 1)
            age_series = list(
                metrics_client.list_time_series(
                    name=f"projects/{METRICS_SCOPE_PROJECT}",
                    filter=(
                        'metric.type="pubsub.googleapis.com/subscription/oldest_unacked_message_age"'
                        f' AND resource.labels.project_id="{project_id}"'
                        f' AND resource.labels.subscription_id="{subscription_id}"'
                    ),
                    interval=metrics_lookback_interval,
                    view=monitoring_v3.ListTimeSeriesRequest.TimeSeriesView.FULL,
                )
            )
            if not age_series or not age_series[0].points:
                self.log.warning(
                    f"{pubsub_subscription}: no `oldest_unacked_message_age` data in the last {metrics_lookback_minutes} minutes."
                )
                return False
            # Points are returned newest first.
            age_point = age_series[0].points[0]
            age = age_point.value.int64_value
            age_time = age_point.interval.end_time
            age_staleness = max(0, now - int(age_time.timestamp()))
            self.log.info(
                f"{pubsub_subscription}: `oldest_unacked_message_age` was {age}s at {age_time} ({age_staleness}s stale)."
            )
            total_latency += age + age_staleness

        oldest_data = pendulum.from_timestamp(now - total_latency)
        # Wait an extra 10 minutes in case there's a bit of additional lag or imprecision in the calculated latency.
        oldest_data_threshold = (
            context["data_interval_end"].in_tz("UTC").start_of("day").add(minutes=10)
        )
        yesterday_is_complete = oldest_data >= oldest_data_threshold
        self.log.info(
            f"Total latency is {total_latency}s, so the oldest data is from {oldest_data},"
            f" which {'is' if yesterday_is_complete else 'is not'} on/after {oldest_data_threshold}."
        )
        return yesterday_is_complete
