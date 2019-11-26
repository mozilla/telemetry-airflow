from datetime import datetime, timedelta

from airflow import DAG
from airflow.contrib.hooks.gcp_api_base_hook import GoogleCloudBaseHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.moz_databricks import MozDatabricksSubmitRunOperator
from airflow.operators.subdag_operator import SubDagOperator
from utils.dataproc import copy_artifacts_dev, moz_dataproc_pyspark_runner
from utils.mozetl import mozetl_envvar

default_args = {
    "owner": "wbeard@mozilla.com",
    "depends_on_past": False,
    "start_date": datetime(2019, 2, 22),
    "email": [
        "telemetry-alerts@mozilla.com",
        "wbeard@mozilla.com",
        "amiyaguchi@mozilla.com",
    ],
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
    "retry_delay": timedelta(minutes=30),
}

# run on the 8th hour of the 1st day of the month so it runs after clients_daily
dag = DAG("bgbb_fit", default_args=default_args, schedule_interval="0 8 1 * *")

clients_daily_v6_dummy = DummyOperator(
    task_id="clients_daily_v6_dummy",
    job_name="A placeholder for the implicit clients daily dependency",
    dag=dag,
)

bgbb_fit = MozDatabricksSubmitRunOperator(
    task_id="bgbb_fit",
    job_name="Fit parameters for a BGBB model to determine active profiles",
    execution_timeout=timedelta(hours=2),
    instance_count=3,
    release_label="6.1.x-scala2.11",
    env=mozetl_envvar(
        "bgbb_fit",
        {
            "submission-date": "{{ next_ds }}",
            "model-win": "90",
            "start-params": "[0.387, 0.912, 0.102, 1.504]",
            "sample-ids": "[42]",
            "sample-fraction": "1.0",
            "penalizer-coef": "0.01",
            "bucket": "{{ task.__class__.private_output_bucket }}",
            "prefix": "bgbb/params/v1",
        },
        dev_options={"model-win": "30"},
        other={
            "MOZETL_GIT_PATH": "https://github.com/wcbeard/bgbb_airflow.git",
            "MOZETL_EXTERNAL_MODULE": "bgbb_airflow",
        },
    ),
    dag=dag,
)

clients_daily_v6_dummy >> bgbb_fit


subdag_args = default_args.copy()
subdag_args["retries"] = 0

task_id = "bgbb_fit_dataproc"
gcp_conn = GoogleCloudBaseHook("google_cloud_airflow_dataproc")
keyfile = json.loads(gcp_conn.extras["extra__google_cloud_platform__keyfile_dict"])
project_id = keyfile["project_id"]

is_dev = os.environ.get("DEPLOY_ENVIRONMENT") == "dev"
client_email = (
    keyfile["client_email"]
    if is_dev
    else "dataproc-runner-prod@airflow-dataproc.iam.gserviceaccount.com"
)
artifact_bucket = (
    "{}-dataproc-artifacts".format(project_id)
    if is_dev
    else "moz-fx-data-prod-airflow-dataproc-artifacts"
)
storage_bucket = (
    "{}-dataproc-scratch".format(project_id)
    if is_dev
    else "moz-fx-data-prod-dataproc-scratch"
)
output_bucket = artifact_bucket if is_dev else "moz-fx-data-derived-datasets-parquet"


bgbb_fit_dataproc = SubDagOperator(
    task_id=task_id,
    dag=dag,
    subdag=moz_dataproc_pyspark_runner(
        parent_dag_name=dag.dag_id,
        dag_name=task_id,
        job_name="bgbb_fit_dataproc",
        cluster_name="bgbb-fit-{{ ds_nodash }}",
        idle_delete_ttl="600",
        num_workers=10,
        worker_machine_type="n1-standard-8",
        init_actions_uris=[
            "gs://dataproc-initialization-actions/python/pip-install.sh"
        ],
        additional_properties={
            "spark:spark.jars": "gs://spark-lib/bigquery/spark-bigquery-latest.jar"
        },
        additional_metadata={
            "PIP_PACKAGES": "git+https://github.com/wcbeard/bgbb_airflow.git@bigquery"
        },
        python_driver_code="gs://{}/jobs/bgbb_runner.py".format(artifact_bucket),
        py_args=[
            "bgbb_fit",
            "--submission-date",
            "{{ next_ds }}",
            "--model-win",
            "90",
            "--start-params",
            "[0.387, 0.912, 0.102, 1.504]",
            "--sample-ids",
            "[42]",
            "--sample-fraction",
            "1.0",
            "--penalizer-coef",
            "0.01",
            "--source",
            "bigquery",
            "--view-materialization-project",
            project_id if is_dev else "moz-fx-data-shared-prod",
            "--view-materialization-dataset",
            "analysis",
            "--bucket-protocol",
            "gs",
            "--bucket",
            output_bucket,
            "--prefix",
            "bgbb/params/v1",
        ],
        gcp_conn_id=gcp_conn.gcp_conn_id,
        service_account=client_email,
        artifact_bucket=artifact_bucket,
        storage_bucket=storage_bucket,
        default_args=subdag_args,
    ),
)

# copy over artifacts if we're running in dev
if is_dev:
    copy_to_dev = copy_artifacts_dev(dag, project_id, artifact_bucket, storage_bucket)
    copy_to_dev.set_downstream(bgbb_fit_dataproc)
