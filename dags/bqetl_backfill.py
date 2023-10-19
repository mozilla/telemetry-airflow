import datetime
from airflow.decorators import dag, task
from airflow.models.param import Param
from utils.gcp import gke_command
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

def is_true(bool_str: str) -> bool:
    if bool_str == 'True':
        return True
    if bool_str == 'False':
        return False
    return None


@dag(
    dag_id="bqetl_backfill",
    schedule_interval=None,
    doc_md=doc_md,
    catchup=False,
    start_date=datetime.datetime(2023, 10, 18),
    dagrun_timeout=datetime.timedelta(days=4),
    tags=[Tag.ImpactTier.tier_3, Tag.Triage.record_only],
    render_template_as_native_obj=True,
    params={
        "table_name": Param("{dataset}.{table}", type="string", description="[Required] The table to backfill, must be of the form {dataset}.{table}"),
        "sql_dir": Param("sql", type="string", description="[Required] Path to directory which contains queries."),
        "project_id": Param("moz-fx-data-shared-prod", type="string", description="[Required] GCP project ID that the query is in (the bqetl directory)",
                            enum=["moz-fx-data-shared-prod", "moz-fx-data-marketing-prod", "moz-fx-data-bq-performance", "moz-fx-data-experiments", "moz-fx-cjms-prod-f3c7", "moz-fx-cjms-nonprod-9a36", "glam-fenix-dev", "mozfun"]),
        "start_date": Param(f"{datetime.date.today()}", type="string", format="date", description="[Required] First date to be backfilled, inclusive"),
        "end_date": Param(f"{datetime.date.today()}", type="string", format="date", description="[Required] Last date to be backfilled, inclusive"),
        "dry_run": Param(True, type="boolean", description="Dry run the backfill"),
        "exclude": Param([], type=["null", "array"], description="Dates to exclude from the backfill"),
        "max_rows": Param(10, type="number", description="Number of rows to return (will be visible in logs)", minimum=0, maximum=500), # Frank made up this max!
        "parallelism": Param(8, type="number", description="Number of threads to use to run backfills", minimum=1, maximum=50), # Frank made up this max!
        "no_partition": Param(False, type="boolean", description="(WARNING: Will overwrite table!) Disable writing results to a partition. Overwrites entire destination table."),
        "destination_table": Param(None, type=["null", "string"], description="(Only set this field if your destination table is different than the project or query!) Destination table name the results are written to. If not set, determines destination table based on query."),
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
            "--sql_dir", context["params"]["sql_dir"],
            "--project_id", context["params"]["project_id"],
            "--start_date", context["params"]["start_date"],
            "--end_date", context["params"]["end_date"],
            "--max_rows", str(context["params"]["max_rows"]),
            "--parallelism", str(context["params"]["parallelism"]),
        ]

        if destination_table := context["params"]["destination_table"]:
            cmd.append(f"--destination_table={destination_table}")

        if excludes := context["params"]["exclude"]:
            for exclude in excludes:
                cmd.extend(["--exclude", exclude])

        if context["params"]["dry_run"]:
            cmd.append("--dry_run")

        if context["params"]["no_partition"]:
            cmd.append("--no_partition")

        if not all((isinstance(c, str) for c in cmd)):
            raise Exception(f"All GKE arguments must be strings! Did you do something surprising to the DAG params?\nArgs: {cmd}")

        print("To run the command locally, execute the following:\r" + " ".join(cmd))

        return cmd

    run_backfill = gke_command(
        reattach_on_restart=True,
        task_id="bqetl_backfill",
        command = "{{ " + "task_instance.xcom_pull(task_ids='generate_backfill_command')" + " }}",
        docker_image="gcr.io/moz-fx-data-airflow-prod-88e0/bigquery-etl:latest",
        gcp_conn_id='google_cloud_gke_sandbox',
        gke_project_id='moz-fx-data-gke-sandbox',
        gke_location='us-west1',
        gke_cluster_name='fbertsch-gke-sandbox-2',
    )

    generate_backfill_command() >> run_backfill


dag = bqetl_backfill_dag()
