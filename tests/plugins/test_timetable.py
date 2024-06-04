from unittest import mock

from airflow.timetables.base import DagRunInfo, DataInterval, TimeRestriction
from pendulum import UTC, DateTime, Time

from plugins.timetable import MultiWeekTimetable


def test_manual_interval():
    tt = MultiWeekTimetable(num_weeks=4)
    actual = tt.infer_manual_data_interval(run_after=DateTime(2023, 1, 29))
    expected = DataInterval(start=DateTime(2023, 1, 1), end=DateTime(2023, 1, 29))
    assert actual == expected


def test_first_automated_interval():
    tt = MultiWeekTimetable(num_weeks=4, time=Time(hour=4))
    actual = tt.next_dagrun_info(
        last_automated_data_interval=None,
        restriction=TimeRestriction(
            earliest=DateTime(2023, 1, 1), latest=None, catchup=True
        ),
    )
    expected = DagRunInfo.interval(
        start=DateTime(2023, 1, 1, 4, tzinfo=UTC),
        end=DateTime(2023, 1, 29, 4, tzinfo=UTC),
    )
    assert actual == expected


def test_first_automated_interval_no_catchup():
    tt = MultiWeekTimetable(num_weeks=4)
    with mock.patch.object(
        DateTime, "utcnow", return_value=DateTime(2023, 2, 28, tzinfo=UTC)
    ):
        actual = tt.next_dagrun_info(
            last_automated_data_interval=None,
            restriction=TimeRestriction(
                earliest=DateTime(2023, 1, 1), latest=None, catchup=False
            ),
        )
    expected = DagRunInfo.interval(
        start=DateTime(2023, 1, 29, tzinfo=UTC), end=DateTime(2023, 2, 26, tzinfo=UTC)
    )
    assert actual == expected


def test_next_automated_interval():
    tt = MultiWeekTimetable(num_weeks=4)
    actual = tt.next_dagrun_info(
        last_automated_data_interval=DataInterval(
            start=DateTime(2023, 1, 29, tzinfo=UTC),
            end=DateTime(2023, 2, 26, tzinfo=UTC),
        ),
        restriction=TimeRestriction(
            earliest=DateTime(2023, 1, 1),
            latest=DateTime(2023, 3, 26, tzinfo=UTC),
            catchup=False,
        ),
    )
    expected = DagRunInfo.interval(
        start=DateTime(2023, 2, 26, tzinfo=UTC), end=DateTime(2023, 3, 26, tzinfo=UTC)
    )
    assert actual == expected


def test_last_automated_interval():
    tt = MultiWeekTimetable(num_weeks=4)
    actual = tt.next_dagrun_info(
        last_automated_data_interval=DataInterval(
            start=DateTime(2023, 1, 29, tzinfo=UTC),
            end=DateTime(2023, 2, 26, tzinfo=UTC),
        ),
        restriction=TimeRestriction(
            earliest=DateTime(2023, 1, 1),
            latest=DateTime(2023, 2, 26, tzinfo=UTC),
            catchup=False,
        ),
    )
    assert actual is None
