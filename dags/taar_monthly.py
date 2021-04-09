"""
This configures a monthly DAG to delete TAAR opt-out profiles.
"""
from airflow import DAG
from datetime import datetime, timedelta
from airflow.models import Variable
from airflow.operators.sensors import ExternalTaskSensor

from operators.gcp_container_operator import GKEPodOperator  # noqa

TAAR_BIGTABLE_INSTANCE_ID = Variable.get("taar_bigtable_instance_id")
TAAR_ETL_STORAGE_BUCKET = Variable.get("taar_etl_storage_bucket")
TAAR_PROFILE_PROJECT_ID = Variable.get("taar_gcp_project_id")
TAAR_DATAFLOW_SUBNETWORK = Variable.get("taar_dataflow_subnetwork")

# This uses a circleci built docker image from github.com/mozilla/taar_gcp_etl
TAAR_ETL_CONTAINER_IMAGE = (
    "gcr.io/moz-fx-data-airflow-prod-88e0/taar_gcp_etl:0.6.2"
)

# should be in sync with shredder DAG
SCHEDULE_DAYS = 28
DELETE_DAYS = 29


default_args_monthly = {
    "owner": "epavlov@mozilla.com",
    "email": [
        "anatal@mozilla.com",
        "mlopatka@mozilla.com",
        "hwoo@mozilla.com",
        "epavlov@mozilla.com",
        "telemetry-alerts@mozilla.com"
    ],
    "depends_on_past": False,
    # should be in sync with shredder DAG
    "start_date": datetime(2020, 4, 7),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 1,
    "retry_delay": timedelta(minutes=60),
}

taar_monthly = DAG(
    "taar_weekly", default_args=default_args_monthly, schedule_interval=timedelta(days=SCHEDULE_DAYS)
)

wait_shredder = ExternalTaskSensor(
    task_id="wait_for_shredder",
    external_dag_id="shredder",
    external_task_id="shredder-flat-rate",
    execution_delta=timedelta(hours=1),
    mode="reschedule",
    pool="DATA_ENG_EXTERNALTASKSENSOR",
    email_on_retry=False,
    dag=taar_monthly)

delete_optout = GKEPodOperator(
    task_id="delete_opt_out_users_from_bigtable",
    name="delete_opt_out_users_from_bigtable",
    image=TAAR_ETL_CONTAINER_IMAGE,
    # Due to the nature of the container run, we set get_logs to False,
    # To avoid urllib3.exceptions.ProtocolError: 'Connection broken: IncompleteRead(0 bytes read)' errors
    # Where the pod continues to run, but airflow loses its connection and sets the status to Failed
    # See: https://github.com/mozilla/telemetry-airflow/issues/844
    get_logs=False,
    arguments=[
        "-m",
        "taar_etl.taar_profile_bigtable",
        "--iso-date={{ ds_nodash }}",
        "--gcp-project=%s" % TAAR_PROFILE_PROJECT_ID,
        "--avro-gcs-bucket=%s" % TAAR_ETL_STORAGE_BUCKET,
        "--subnetwork=%s" % TAAR_DATAFLOW_SUBNETWORK,
        "--bigtable-instance-id=%s" % TAAR_BIGTABLE_INSTANCE_ID,
        "--bigtable-delete-opt-out",
        "--delete-opt-out-days=%s" % DELETE_DAYS],
    dag=taar_monthly
)

wait_shredder >> delete_optout
