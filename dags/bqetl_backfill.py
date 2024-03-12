import datetime

from airflow.decorators import dag, task
from airflow.models.param import Param

from operators.gcp_container_operator import GKEPodOperator
from utils.tags import Tag

doc_md = """
# Bqetl Backfill DAG

#### Use with caution: This will overwrite the dates and tables you choose!

#### Some tips/notes:
* Date formats are 2020-03-01
* Try with dryrun=True first
* Read the associated CLI command in bqetl: https://github.com/mozilla/bigquery-etl/blob/7a36416554193c853bb562b34dda6270462acd66/bigquery_etl/cli/query.py#L679

#### Owner
frank@mozilla.com
"""


@dag(
    dag_id="bqetl_backfill",
    schedule_interval=None,
    doc_md=doc_md,
    catchup=False,
    start_date=datetime.datetime(2023, 10, 18),
    dagrun_timeout=datetime.timedelta(days=4),
    tags=[Tag.ImpactTier.tier_3, Tag.Triage.no_triage],
    render_template_as_native_obj=True,
    params={
        "table_name": Param(
            "{dataset}.{table}",
            title="Table Name",
            type="string",
            description="[Required] The table to backfill, must be of the form {dataset}.{table}",
        ),
        "sql_dir": Param(
            "sql",
            title="SQL Directory in bigquery_etl",
            type="string",
            description="[Required] Path to directory which contains queries.",
        ),
        "project_id": Param(
            "moz-fx-data-shared-prod",
            title="GCP Project ID",
            type="string",
            description="[Required] GCP project ID that the query is in (the bqetl directory)",
            enum=[
                "moz-fx-data-shared-prod",
                "moz-fx-data-marketing-prod",
                "moz-fx-data-bq-performance",
                "moz-fx-data-experiments",
                "moz-fx-cjms-prod-f3c7",
                "moz-fx-cjms-nonprod-9a36",
                "glam-fenix-dev",
                "mozfun",
            ],
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
        "dry_run": Param(
            True, title="Dry Run?", type="boolean", description="Dry run the backfill"
        ),
        "exclude": Param(
            [],
            title="Excluded Dates",
            type=["null", "array"],
            description="Dates to exclude from the backfill",
        ),
        "max_rows": Param(
            10,
            title="Max Rows",
            type="number",
            description="Number of rows to return (will be visible in logs)",
            minimum=0,
            maximum=500,
        ),  # Frank made up this max!
        "parallelism": Param(
            8,
            title="Parallelism",
            type="number",
            description="Number of threads to use to run backfills",
            minimum=1,
            maximum=50,
        ),  # Frank made up this max!
        "destination_table": Param(
            None,
            title="Destination Table",
            type=["null", "string"],
            description="(Only set this field if your destination table is different than the project or query!) Destination table name the results are written to. If not set, determines destination table based on query.",
        ),
        "run_checks": Param(
            True,
            title="Run Checks?",
            type="boolean",
            description="Whether to run checks during backfill.",
        ),
    },
)
def bqetl_backfill_dag():
    @task
    def generate_backfill_command(**context):
        """Generate backfill command with arguments."""
        cmd = [
            "bqetl",
            "query",
            "backfill",
            context["params"]["table_name"],
            "--sql_dir",
            context["params"]["sql_dir"],
            "--project_id",
            context["params"]["project_id"],
            "--start_date",
            context["params"]["start_date"],
            "--end_date",
            context["params"]["end_date"],
            "--max_rows",
            str(context["params"]["max_rows"]),
            "--parallelism",
            str(context["params"]["parallelism"]),
        ]

        if destination_table := context["params"]["destination_table"]:
            cmd.append(f"--destination_table={destination_table}")

        if excludes := context["params"]["exclude"]:
            for exclude in excludes:
                cmd.extend(["--exclude", exclude])

        if context["params"]["dry_run"]:
            cmd.append("--dry_run")

        if context["params"]["run_checks"]:
            cmd.append("--checks")
        else:
            cmd.append("--no-checks")

        if not all(isinstance(c, str) for c in cmd):
            raise Exception(
                f"All GKE arguments must be strings! Did you do something surprising to the DAG params?\nArgs: {cmd}"
            )

        print("To run the command locally, execute the following:\r" + " ".join(cmd))

        return cmd

    GKEPodOperator(
        reattach_on_restart=True,
        task_id="bqetl_backfill",
        arguments=generate_backfill_command(),
        image="gcr.io/moz-fx-data-airflow-prod-88e0/bigquery-etl:latest",
        gcp_conn_id="google_cloud_airflow_gke",
    )


dag = bqetl_backfill_dag()
