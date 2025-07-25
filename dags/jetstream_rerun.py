import datetime

from airflow.decorators import dag, task
from airflow.models.param import Param

from operators.gcp_container_operator import GKEPodOperator
from utils.tags import Tag

docs = """
### jetstream_rerun
Build from telemetry-airflow repo, [dags/jetstream_rerun.py]
Triggers rerun for Jetstream experiments.

#### Owner
ascholtz@mozilla.com
mwilliams@mozilla.com
"""

tags = [Tag.ImpactTier.tier_3, Tag.Triage.no_triage]

@dag(
    dag_id="jetstream_rerun",
    start_date=datetime.datetime(2025, 1, 1, 0, 0),
    schedule_interval=None,
    catchup=False,
    doc_md=docs,
    dagrun_timeout=datetime.timedelta(days=4),
    tags=tags,
    render_template_as_native_obj=True,
    params={
        "experiment_slug": Param("slug", title="Experiment Slug", type="string", description="Experiment slug to rerun"),
        "recreate_enrollments": Param(False, title="Recreate Enrollments", type="boolean", description="Recreate enrollments option"),
        "statistics_only": Param(False, title="Statistics Only", type="boolean", description="Statistics only option"),
        "analysis_overall": Param(False, title="Analysis Overall", type="boolean", description="Include overall analysis period"),
        "analysis_week": Param(False, title="Analysis Week", type="boolean", description="Include week analysis period"),
        "analysis_day": Param(False, title="Analysis Day", type="boolean", description="Include day analysis period"),
    },
)
def jetstream_rerun_dag():
    @task
    def generate_rerun_arguments(**context):
        cmd = [
            "rerun",
            "--slug", context["params"]["experiment_slug"],
            "--argo",
        ]
        if context["params"].get("recreate_enrollments", False):
            cmd.append("--recreate-enrollments")
        if context["params"].get("statistics_only", False):
            cmd.append("--statistics-only=true")
        # add analysis period flags if True
        if context["params"].get("analysis_overall", False):
            cmd.append("--analysis-periods=overall")
        if context["params"].get("analysis_week", False):
            cmd.append("--analysis-periods=week")
        if context["params"].get("analysis_day", False):
            cmd.append("--analysis-periods=day")
        return cmd

    jetstream_image = "gcr.io/moz-fx-data-experiments/jetstream:latest"

    GKEPodOperator(
        task_id="jetstream_rerun",
        name="jetstream_rerun",
        image=jetstream_image,
        arguments=generate_rerun_arguments(),
    )

dag = jetstream_rerun_dag()
