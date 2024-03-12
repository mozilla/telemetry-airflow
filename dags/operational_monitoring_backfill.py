import datetime

from airflow.decorators import dag, task
from airflow.models.param import Param

from operators.gcp_container_operator import GKEPodOperator
from utils.tags import Tag

docs = """
### operational_monitoring_backfill
Build from telemetry-airflow repo, [dags/operational_monitoring_backfill.py](https://github.com/mozilla/telemetry-airflow/blob/main/dags/operational_monitoring_backfill.py)
Triggers backfills for specifc operational monitoring projects.

#### Owner

ascholtz@mozilla.com
lschiestl@mozilla.com
"""

tags = [Tag.ImpactTier.tier_3, Tag.Triage.no_triage]


@dag(
    dag_id="operational_monitoring_backfill",
    start_date=datetime.datetime(2021, 1, 1, 0, 0),
    schedule_interval=None,
    catchup=False,
    doc_md=docs,
    dagrun_timeout=datetime.timedelta(days=4),
    tags=tags,
    render_template_as_native_obj=True,
    params={
        "slug": Param(
            "slug",
            title="Slug",
            type="string",
            description="[Required] Experimenter slug or slug of OpMon project to (re)run the analysis for",
        ),
        "start_date": Param(
            f"{datetime.date.today()}",
            title="Start Date",
            type="string",
            format="date",
            description="[Required] First date to be backfilled, inclusive",
        ),
        "end_date": Param(
            f"{datetime.date.today()}",
            title="End Date",
            type="string",
            format="date",
            description="[Required] Last date to be backfilled, inclusive",
        ),
        "args": Param(
            None,
            title="Additional Arguments",
            type="string",
            description="[Optional] Additional command line arguments",
        ),
    },
)
def operational_monitoring_backfill_dag():
    @task
    def generate_backfill_arguments(**context):
        cmd = [
            "backfill",
            "--slug",
            context["params"]["slug"],
            "--start-date",
            context["params"]["start_date"],
            "--end_date",
            context["params"]["end_date"],
        ]

        if args := context["params"]["args"]:
            cmd.append(args)

        return cmd

    # Built from repo https://github.com/mozilla/opmon
    opmon_image = "gcr.io/moz-fx-data-experiments/opmon:latest"

    GKEPodOperator(
        task_id="opmon_backfill",
        name="opmon_backfill",
        image=opmon_image,
        arguments=generate_backfill_arguments(),
    )


dag = operational_monitoring_backfill_dag()
