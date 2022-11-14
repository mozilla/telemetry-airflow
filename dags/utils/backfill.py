import dataclasses
import datetime
import re
from typing import Optional, List


@dataclasses.dataclass
class BackfillParams:
    dag_name: str
    start_date: str
    end_date: str
    clear: bool
    dry_run: bool
    task_regex: Optional[str]

    def validate_date_range(self) -> None:
        start_date = datetime.datetime.fromisoformat(self.start_date)
        end_date = datetime.datetime.fromisoformat(self.end_date)
        if start_date > end_date:
            raise ValueError(f"`start_date`={self.start_date} is greater than `end_date`={self.end_date}")

    def validate_regex_pattern(self) -> None:
        if self.task_regex:
            try:
                re.compile(self.task_regex)
            except re.error:
                raise ValueError(f"Invalid regex pattern for `task_regex`={self.task_regex}")

    def generate_backfill_command(self) -> List[str]:
        """Backfill command based off the Airflow plugin implemented by hwoo

        Original implementation in plugins/backfill/main.py

        """
        # Construct the airflow command
        cmd = ['airflow']

        if self.clear:
            cmd.extend(['tasks', 'clear'])

            if self.dry_run:
                # For dry runs we simply time out to avoid zombie procs waiting on user input.
                # The output is what we're interested in
                timeout_list = ['timeout', '60']
                cmd = timeout_list + cmd
            else:
                cmd.append('-y')

            if self.task_regex:
                cmd.extend(['-t', str(self.task_regex)])
        else:
            cmd.extend(['dags', 'backfill', '--donot-pickle'])
            if self.dry_run:
                cmd.append('--dry-run')

            if self.task_regex:
                cmd.extend(['-t', str(self.task_regex)])

        cmd.extend(['-s', str(self.start_date), '-e', str(self.end_date), str(self.dag_name)])

        return cmd
