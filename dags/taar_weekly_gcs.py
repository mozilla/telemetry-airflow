"""
This configures a weekly DAG to run the TAAR Ensemble job off.
"""
from airflow import DAG
from airflow.contrib.hooks.aws_hook import AwsHook
from datetime import datetime, timedelta
from airflow.operators.subdag_operator import SubDagOperator
from airflow.models import Variable

from operators.gcp_container_operator import GKEPodOperator  # noqa
from utils.dataproc import moz_dataproc_pyspark_runner

taar_aws_conn_id = "airflow_taar_rw_s3"
taar_aws_access_key, taar_aws_secret_key, session = AwsHook(
    taar_aws_conn_id
).get_credentials()
taar_ensemble_cluster_name = "dataproc-taar-ensemble"
taar_gcpdataproc_conn_id = "google_cloud_airflow_dataproc"

TAAR_BIGTABLE_INSTANCE_ID = Variable.get("taar_bigtable_instance_id")

TAAR_ETL_TMP_STORAGE_BUCKET = Variable.get("taar_etl_storage_bucket")
TAAR_ETL_MODEL_STORAGE_BUCKET = Variable.get("taar_etl_model_storage_bucket")

TAAR_PROFILE_PROJECT_ID = Variable.get("taar_gcp_project_id")
TAAR_DATAFLOW_SUBNETWORK = Variable.get("taar_dataflow_subnetwork")

# This uses a circleci built docker image from github.com/mozilla/taar_gcp_etl
TAAR_ETL_CONTAINER_IMAGE = (
    "gcr.io/moz-fx-data-airflow-prod-88e0/taar_gcp_etl:0.4.7"
)


default_args_weekly = {
    "owner": "vng@mozilla.com",
    "email": [
        "vng@mozilla.com",
        "mlopatka@mozilla.com",
        "telemetry-alerts@mozilla.com",
    ],
    "depends_on_past": False,
    "start_date": datetime(2020, 4, 4),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 0,
    "retry_delay": timedelta(minutes=60),
}


taar_weekly = DAG(
    "taar_weekly_gcs", default_args=default_args_weekly, schedule_interval="@weekly"
)


def wipe_gcs_files():
    # This just makes sure we clear the bucket.
    return [
        "bash",
        "-c",
        # gsutil returns an error if you try to delete 0 files
        # so we need to ignore errors here
        """/google-cloud-sdk/bin/gsutil -m rm gs://%s/* || /bin/true"""
        % TAAR_ETL_TMP_STORAGE_BUCKET,
    ]


def taar_profile_common_args():
    return [
        "-m",
        "taar_etl.taar_profile_bigtable",
        "--iso-date={{ ds_nodash }}",
        "--gcp-project=%s" % TAAR_PROFILE_PROJECT_ID,
        "--avro-gcs-bucket=%s" % TAAR_ETL_TMP_STORAGE_BUCKET,
        "--bigtable-instance-id=%s" % TAAR_BIGTABLE_INSTANCE_ID,
        "--sample-rate=1.0",
        "--subnetwork=%s" % TAAR_DATAFLOW_SUBNETWORK,
    ]


wipe_gcs_bucket = GKEPodOperator(
    owner="vng@mozilla.com",
    email=["vng@mozilla.com", "mlopatka@mozilla.com",],
    task_id="wipe_taar_gcs_bucket",
    name="wipe_taar_gcs_bucket",
    image="google/cloud-sdk:242.0.0-alpine",
    arguments=wipe_gcs_files(),
    location="us-central1-a",
    cluster_name="bq-load-gke-1",
    dag=taar_weekly,
)

dump_bq_to_tmp_table = GKEPodOperator(
    owner="vng@mozilla.com",
    email=["vng@mozilla.com", "mlopatka@mozilla.com",],
    task_id="dump_bq_to_tmp_table",
    name="dump_bq_to_tmp_table",
    image=TAAR_ETL_CONTAINER_IMAGE,
    arguments=taar_profile_common_args() + ["--fill-bq",],
    dag=taar_weekly,
)

extract_bq_tmp_to_gcs_avro = GKEPodOperator(
    owner="vng@mozilla.com",
    email=["vng@mozilla.com", "mlopatka@mozilla.com",],
    task_id="extract_bq_tmp_to_gcs_avro",
    name="extract_bq_tmp_to_gcs_avro",
    image=TAAR_ETL_CONTAINER_IMAGE,
    arguments=taar_profile_common_args() + ["--bq-to-gcs",],
    dag=taar_weekly,
)

dataflow_import_avro_to_bigtable = GKEPodOperator(
    owner="vng@mozilla.com",
    email=["vng@mozilla.com", "mlopatka@mozilla.com",],
    task_id="dataflow_import_avro_to_bigtable",
    name="dataflow_import_avro_to_bigtable",
    image=TAAR_ETL_CONTAINER_IMAGE,
    # Due to the nature of the container run, we set get_logs to False,
    # To avoid urllib3.exceptions.ProtocolError: 'Connection broken: IncompleteRead(0 bytes read)' errors
    # Where the pod continues to run, but airflow loses its connection and sets the status to Failed
    # See: https://github.com/mozilla/telemetry-airflow/issues/844
    get_logs=False,
    arguments=taar_profile_common_args() + ["--gcs-to-bigtable",],
    dag=taar_weekly,
)

wipe_gcs_bucket_cleanup = GKEPodOperator(
    owner="vng@mozilla.com",
    email=["vng@mozilla.com", "mlopatka@mozilla.com",],
    task_id="wipe_gcs_bucket_cleanup",
    name="wipe_taar_gcs_bucket",
    location="us-central1-a",
    cluster_name="bq-load-gke-1",
    image="google/cloud-sdk:242.0.0-alpine",
    arguments=wipe_gcs_files(),
    dag=taar_weekly,
)

wipe_bigquery_tmp_table = GKEPodOperator(
    owner="vng@mozilla.com",
    email=["vng@mozilla.com", "mlopatka@mozilla.com",],
    task_id="wipe_bigquery_tmp_table",
    name="wipe_bigquery_tmp_table",
    image=TAAR_ETL_CONTAINER_IMAGE,
    arguments=taar_profile_common_args() + ["--wipe-bigquery-tmp-table",],
    dag=taar_weekly,
)


# This job should complete in approximately 30 minutes given
# 35 x n1-standard-8 workers and 2 SSDs per node.
taar_ensemble = SubDagOperator(
    task_id="taar_ensemble",
    subdag=moz_dataproc_pyspark_runner(
        parent_dag_name=taar_weekly.dag_id,
        dag_name="taar_ensemble",
        default_args=default_args_weekly,
        cluster_name=taar_ensemble_cluster_name,
        job_name="TAAR_ensemble",
        python_driver_code="gs://moz-fx-data-prod-airflow-dataproc-artifacts/jobs/taar_ensemble.py",
        additional_properties={
            "spark:spark.jars": "gs://spark-lib/bigquery/spark-bigquery-latest.jar",
            "spark:spark.jars.packages": "org.apache.spark:spark-avro_2.11:2.4.4",
            "spark:spark.python.profile": "true",
        },
        num_workers=35,
        worker_machine_type="n1-standard-8",
        master_machine_type="n1-standard-8",
        init_actions_uris=[
            "gs://moz-fx-data-prod-airflow-dataproc-artifacts/jobs/pip-install.sh"
        ],
        additional_metadata={
            "PIP_PACKAGES": "mozilla-taar3==0.4.12 mozilla-srgutil==0.2.1 python-decouple==3.1 click==7.0 boto3==1.7.71 dockerflow==2018.4.0"
        },
        optional_components=["ANACONDA", "JUPYTER"],
        py_args=[
            "--date",
            "{{ ds_nodash }}",
            "--gcs_model_bucket",
            TAAR_ETL_MODEL_STORAGE_BUCKET,
            "--sample_rate",
            "0.005",
        ],
        gcp_conn_id=taar_gcpdataproc_conn_id,
        master_disk_type="pd-ssd",
        worker_disk_type="pd-ssd",
        master_disk_size=1024,
        worker_disk_size=1024,
        master_num_local_ssds=2,
        worker_num_local_ssds=2,
    ),
    dag=taar_weekly,
)


wipe_gcs_bucket >> dump_bq_to_tmp_table
dump_bq_to_tmp_table >> extract_bq_tmp_to_gcs_avro
extract_bq_tmp_to_gcs_avro >> dataflow_import_avro_to_bigtable
dataflow_import_avro_to_bigtable >> wipe_gcs_bucket_cleanup
wipe_gcs_bucket_cleanup >> wipe_bigquery_tmp_table
wipe_bigquery_tmp_table >> taar_ensemble
