"""Plugin for alternative timetables that cannot be trivially defined via cron expressions."""

from datetime import timedelta
from typing import Any

from airflow.plugins_manager import AirflowPlugin
from airflow.timetables.base import DagRunInfo, DataInterval, TimeRestriction, Timetable
from pendulum import UTC, DateTime, Time


class MultiWeekTimetable(Timetable):
    def __init__(self, *, num_weeks: int, time: Time = Time.min):
        self.num_weeks = num_weeks
        self.interval_delta = timedelta(days=7 * num_weeks)
        # only enforced for automated data intervals
        self.time = time

    def infer_manual_data_interval(self, run_after: DateTime) -> DataInterval:
        return DataInterval(start=run_after - self.interval_delta, end=run_after)

    def next_dagrun_info(
        self,
        *,
        last_automated_data_interval: DataInterval | None,
        restriction: TimeRestriction,
    ) -> DagRunInfo | None:
        if restriction.earliest is None:  # No start_date specified. Don't schedule.
            return None

        # Find the first run on the regular schedule.
        next_end = (
            DateTime.combine(restriction.earliest, self.time).replace(tzinfo=UTC)
            + self.interval_delta
        )

        max_end = next_end
        if last_automated_data_interval is not None:
            # There was a previous run on the regular schedule.
            # Return the next interval after last_automated_data_interval.end that is
            # aligned with restriction.earliest and self.time
            max_end = last_automated_data_interval.end + self.interval_delta
        elif not restriction.catchup:
            # This is the first ever run on the regular schedule, and catchup is not
            # enabled. Return the last complete interval before now.
            max_end = DateTime.utcnow()
        if next_end < max_end:
            # Return the last complete interval on or before max_end. Use integer
            # division on the number of whole days rather than deal with any corner
            # cases related to leap seconds and partial days.
            skip_intervals = (max_end - next_end).days // self.interval_delta.days
            next_end = next_end + (self.interval_delta * skip_intervals)

        if restriction.latest is not None and next_end > restriction.latest:
            return None  # Over the DAG's scheduled end; don't schedule.
        return DagRunInfo.interval(start=next_end - self.interval_delta, end=next_end)

    def serialize(self) -> dict[str, Any]:
        return {"num_weeks": self.num_weeks, "time": self.time.isoformat()}

    @classmethod
    def deserialize(cls, value: dict[str, Any]) -> Timetable:
        return cls(num_weeks=value["num_weeks"], time=Time.fromisoformat(value["time"]))


class MozillaTimetablePlugin(AirflowPlugin):
    name = "mozilla_timetable_plugin"
    timetables = [MultiWeekTimetable]
