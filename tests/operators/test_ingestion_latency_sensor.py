from unittest import mock

import pendulum
import pytest
from google.cloud import monitoring_v3

from operators.ingestion_latency_sensor import IngestionLatencySensor

DATA_INTERVAL_END = pendulum.datetime(2026, 10, 20, 1, 0, tz="UTC")
PUBSUB_SUBSCRIPTIONS = ["project-a/sub-1", "project-b/sub-2"]


def age_series(age, seconds_ago=0):
    return monitoring_v3.TimeSeries(
        points=[
            {
                "interval": {
                    "end_time": {
                        "seconds": DATA_INTERVAL_END.int_timestamp - seconds_ago
                    }
                },
                "value": {"int64_value": age},
            }
        ]
    )


def poke(age_series_per_subscription):
    sensor = IngestionLatencySensor(
        task_id="test", pubsub_subscriptions=PUBSUB_SUBSCRIPTIONS
    )
    with (
        mock.patch(
            "operators.ingestion_latency_sensor.time.time",
            return_value=DATA_INTERVAL_END.timestamp(),
        ),
        mock.patch("operators.ingestion_latency_sensor.GoogleBaseHook"),
        mock.patch(
            "operators.ingestion_latency_sensor.monitoring_v3.MetricServiceClient"
        ) as client,
    ):
        client.return_value.list_time_series.side_effect = age_series_per_subscription
        return sensor.poke({"data_interval_end": DATA_INTERVAL_END})


@pytest.mark.parametrize(
    ("age_series_per_subscription", "expected"),
    [
        # 30m + 20m latency = oldest data from 00:10 (exactly the threshold)
        ([[age_series(1800)], [age_series(1200)]], True),
        # 30m + 30m latency = oldest data from 00:00 (before the threshold)
        ([[age_series(1800)], [age_series(1800)]], False),
        # 40m + 30m latency = oldest data from 23:50 yesterday
        ([[age_series(2400)], [age_series(1800)]], False),
        # A 20m-old point with 30m age adjusts to 50m, + 20m = oldest data from 23:50 yesterday
        ([[age_series(1800, seconds_ago=1200)], [age_series(1200)]], False),
        # Missing metric data fails safe.
        ([[age_series(0)], []], False),
    ],
)
def test_poke(age_series_per_subscription, expected):
    assert poke(age_series_per_subscription) is expected
