"""
Daily data exports used by TAAR.

Source code is in [mozilla/telemetry-batch-view](https://github.com/mozilla/telemetry-batch-view/blob/main/src/main/scala/com/mozilla/telemetry/ml/AddonRecommender.scala).

For context, see https://github.com/mozilla/taar
"""


from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.subdag_operator import SubDagOperator
from airflow.models import Variable
from itertools import chain

from operators.gcp_container_operator import GKEPodOperator  # noqa
from utils.constants import ALLOWED_STATES, FAILED_STATES
from utils.dataproc import (
    moz_dataproc_pyspark_runner,
    moz_dataproc_jar_runner,
)
from utils.tags import Tag

TAAR_ETL_STORAGE_BUCKET = Variable.get("taar_etl_storage_bucket")
TAAR_ETL_MODEL_STORAGE_BUCKET = Variable.get("taar_etl_model_storage_bucket")

# This uses a circleci built docker image from github.com/mozilla/taar_gcp_etl
TAAR_ETL_CONTAINER_IMAGE = "gcr.io/moz-fx-data-airflow-prod-88e0/taar_gcp_etl:0.6.5"


# Dataproc connection to GCP
taar_gcpdataproc_conn_id = "google_cloud_airflow_dataproc"
taar_gcpdataproc_project_id = "airflow-dataproc"

taar_aws_conn_id = "airflow_taar_rw_s3"
taar_aws_access_key, taar_aws_secret_key, session = AwsBaseHook(
    aws_conn_id=taar_aws_conn_id, client_type='s3').get_credentials()
taarlite_cluster_name = "dataproc-taarlite-guidguid"
taar_locale_cluster_name = "dataproc-taar-locale"
taar_similarity_cluster_name = "dataproc-taar-similarity"


default_args = {
    "owner": "epavlov@mozilla.com",
    "depends_on_past": False,
    "start_date": datetime(2019, 10, 7),
    "email": ["telemetry-alerts@mozilla.com", "hwoo@mozilla.com",
              "epavlov@mozilla.com"],
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 0,
    "retry_delay": timedelta(minutes=30),
}

tags = [Tag.ImpactTier.tier_2]

with DAG("taar_daily", default_args=default_args, schedule_interval="0 4 * * *", doc_md=__doc__, tags=tags,) as dag:
    amodump = GKEPodOperator(
        task_id="taar_amodump",
        name="taar-amodump",
        # This uses a circleci built docker image from github.com/mozilla/taar_gcp_etl
        image=TAAR_ETL_CONTAINER_IMAGE,
        arguments=["-m", "taar_etl.taar_amodump", "--date", "{{ ds_nodash }}",
                "--gcs-bucket", TAAR_ETL_MODEL_STORAGE_BUCKET],
    )

    amowhitelist = GKEPodOperator(
        task_id="taar_amowhitelist",
        name="taar-amowhitelist",
        # This uses a circleci built docker image from github.com/mozilla/taar_gcp_etl
        image=TAAR_ETL_CONTAINER_IMAGE,
        # We are extracting addons from the AMO server's APIs which don't
        # support date based queries, so no date parameter is required
        # here.
        arguments=["-m", "taar_etl.taar_amowhitelist",
                "--gcs-bucket", TAAR_ETL_MODEL_STORAGE_BUCKET],
    )

    editorial_whitelist = GKEPodOperator(
        task_id="taar_update_whitelist",
        name="taar-update-whitelist",
        # This uses a circleci built docker image from github.com/mozilla/taar_gcp_etl
        image=TAAR_ETL_CONTAINER_IMAGE,
        arguments=["-m", "taar_etl.taar_update_whitelist", "--date", "{{ ds_nodash }}",
                "--bucket", TAAR_ETL_MODEL_STORAGE_BUCKET],
    )

    wait_for_clients_daily_export = ExternalTaskSensor(
        task_id="wait_for_clients_daily_export",
        external_dag_id="parquet_export",
        external_task_id="clients_daily_export",
        execution_delta=timedelta(hours=1),
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
        email_on_retry=False,)

    wait_for_clients_last_seen = ExternalTaskSensor(
        task_id="wait_for_clients_last_seen",
        external_dag_id="bqetl_main_summary",
        external_task_id="telemetry_derived__clients_last_seen__v1",
        execution_delta=timedelta(hours=2),
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        pool="DATA_ENG_EXTERNALTASKSENSOR",
        email_on_retry=False,
    )

    taar_locale = SubDagOperator(
        task_id="taar_locale",
        subdag=moz_dataproc_pyspark_runner(
            parent_dag_name=dag.dag_id,
            dag_name="taar_locale",
            default_args=default_args,
            cluster_name=taar_locale_cluster_name,
            job_name="TAAR_Locale",
            python_driver_code="gs://moz-fx-data-prod-airflow-dataproc-artifacts/jobs/taar_locale.py",
            # GCS bucket for testing is located in `cfr-personalization-experiment` project
            # python_driver_code="gs://taar_models/tmp/jobs/taar_locale.py",
            num_workers=12,
            py_args=[
                "--date",
                "{{ ds_nodash }}",
                "--bucket",
                TAAR_ETL_MODEL_STORAGE_BUCKET,
                "--prefix",
                "taar/locale",
            ],
            gcp_conn_id=taar_gcpdataproc_conn_id,
            project_id=taar_gcpdataproc_project_id
        ),
    )

    taar_similarity = SubDagOperator(
        task_id="taar_similarity",
        subdag=moz_dataproc_pyspark_runner(
            parent_dag_name=dag.dag_id,
            dag_name="taar_similarity",
            default_args=default_args,
            cluster_name=taar_similarity_cluster_name,
            job_name="TAAR_similarity",
            python_driver_code="gs://moz-fx-data-prod-airflow-dataproc-artifacts/jobs/taar_similarity.py",
            # GCS bucket for testing is located in `cfr-personalization-experiment` project
            # python_driver_code="gs://taar_models/tmp/jobs/taar_similarity.py",
            init_actions_uris=["gs://dataproc-initialization-actions/python/pip-install.sh"],
            additional_metadata={'PIP_PACKAGES': "google-cloud-bigquery==1.20.0 google-cloud-storage==1.19.1"},
            additional_properties={"spark:spark.jars.packages": "org.apache.spark:spark-avro_2.11:2.4.3",
                                "spark:spark.jars":"gs://spark-lib/bigquery/spark-bigquery-latest.jar"},
            num_workers=8,
            master_machine_type='n1-standard-8',
            worker_machine_type="n1-highmem-32",
            py_args=[
                "--date", "{{ ds }}",
                "--bucket", TAAR_ETL_MODEL_STORAGE_BUCKET,
                "--prefix", "taar/similarity"
            ],
            gcp_conn_id=taar_gcpdataproc_conn_id,
            project_id=taar_gcpdataproc_project_id,
            master_disk_type="pd-ssd",
            worker_disk_type="pd-ssd",
            master_disk_size=1024,
            worker_disk_size=1024,
            master_num_local_ssds=2,
            worker_num_local_ssds=2,
        ),
    )

    taar_collaborative_recommender = SubDagOperator(
        task_id="addon_recommender",
        subdag=moz_dataproc_jar_runner(
            parent_dag_name=dag.dag_id,
            dag_name="addon_recommender",
            job_name="Train_the_Collaborative_Addon_Recommender",
            main_class="com.mozilla.telemetry.ml.AddonRecommender",
            jar_urls=[
                # GCS bucket for testing is located in `cfr-personalization-experiment` project
                # 'gs://taar_models/tmp/telemetry-batch-view-1.2.jar'
                # we should move artifacts to GCS eventually
                "https://s3-us-west-2.amazonaws.com/net-mozaws-data-us-west-2-ops-ci-artifacts"
                "/mozilla/telemetry-batch-view/main/telemetry-batch-view.jar",
            ],
            jar_args=[
            "train",
            "--runDate={{ds_nodash}}",
            "--inputTable=gs://moz-fx-data-derived-datasets-parquet/clients_daily/v6",
            f"--privateBucket=gs://{TAAR_ETL_MODEL_STORAGE_BUCKET}",
            f"--checkpointDir=gs://{TAAR_ETL_STORAGE_BUCKET}/spark-checkpoints"
            ],
            cluster_name="addon-recommender-{{ds_nodash}}",
            image_version="1.3",
            worker_machine_type="n1-standard-8",
            num_workers=20,
            optional_components=[],
            install_component_gateway=False,
            init_actions_uris=[],
            aws_conn_id=taar_aws_conn_id,
            gcp_conn_id=taar_gcpdataproc_conn_id,
            project_id=taar_gcpdataproc_project_id,
            default_args=default_args
        ),
    )

    taar_lite = SubDagOperator(
        task_id="taar_lite",
        subdag=moz_dataproc_pyspark_runner(
            parent_dag_name=dag.dag_id,
            dag_name="taar_lite",
            default_args=default_args,
            cluster_name=taarlite_cluster_name,
            job_name="TAAR_Lite_GUID_GUID",
            init_actions_uris=["gs://dataproc-initialization-actions/python/pip-install.sh"],
            additional_metadata={
                'PIP_PACKAGES': "google-cloud-bigquery==1.20.0 google-cloud-storage==1.19.1"},
            additional_properties={"spark:spark.jars.packages": "org.apache.spark:spark-avro_2.11:2.4.3",
                                "spark:spark.jars": "gs://spark-lib/bigquery/spark-bigquery-latest.jar"},
            python_driver_code="gs://moz-fx-data-prod-airflow-dataproc-artifacts/jobs/taar_lite_guidguid.py",
            # GCS bucket for testing is located in `cfr-personalization-experiment` project
            # python_driver_code="gs://taar_models/tmp/jobs/taar_lite_guidguid.py",
            num_workers=8,
            py_args=[
                "--date", "{{ ds }}",
                "--bucket", TAAR_ETL_MODEL_STORAGE_BUCKET,
                "--prefix", "taar/lite"
            ],
            gcp_conn_id=taar_gcpdataproc_conn_id,
            project_id=taar_gcpdataproc_project_id,
        ),
    )



    taar_lite_guidranking = GKEPodOperator(
        task_id="taar_lite_guidranking",
        name="taar_lite_guidranking",
        # This uses a circleci built docker image from github.com/mozilla/taar_gcp_etl
        image=TAAR_ETL_CONTAINER_IMAGE,
        arguments=["-m", "taar_etl.taar_lite_guid_ranking",
                "--date", "{{ macros.ds_add(ds, -1) }}",
                "--prefix", "taar/lite",
                "--bucket", TAAR_ETL_MODEL_STORAGE_BUCKET],
    )


    amodump >> amowhitelist
    amodump >> editorial_whitelist

    taar_tasks = [
        taar_similarity,
        taar_locale,
        taar_collaborative_recommender,
        taar_lite,
        taar_lite_guidranking,
    ]

    wait_for_clients_daily_export >> taar_tasks
    wait_for_clients_last_seen >> taar_tasks
