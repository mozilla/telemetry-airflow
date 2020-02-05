from datetime import datetime, timedelta

from airflow import DAG
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.contrib.hooks.gcp_api_base_hook import GoogleCloudBaseHook
from airflow.operators.sensors import ExternalTaskSensor
from airflow.operators.subdag_operator import SubDagOperator
from itertools import chain

from operators.gcp_container_operator import GKEPodOperator  # noqa
from utils.dataproc import (
    moz_dataproc_pyspark_runner,
    moz_dataproc_jar_runner,
)

gke_cluster_name = "bq-load-gke-1"

# Defined in Airflow's UI -> Admin -> Connections
gcp_conn_id = "google_cloud_derived_datasets"
connection = GoogleCloudBaseHook(gcp_conn_id=gcp_conn_id)

# Dataproc connection to GCP
gcpdataproc_conn_id = "google_cloud_airflow_dataproc"


taar_aws_conn_id = "airflow_taar_rw_s3"
taar_aws_access_key, taar_aws_secret_key, session = AwsHook(taar_aws_conn_id).get_credentials()
taarlite_cluster_name = "dataproc-taarlite-guidguid"
taar_locale_cluster_name = "dataproc-taar-locale"
taar_similarity_cluster_name = "dataproc-taar-similarity"
taar_gcpdataproc_conn_id = "google_cloud_airflow_dataproc"
taar_dynamo_cluster_name = "dataproc-taar-dynamo"

default_args = {
    "owner": "vng@mozilla.com",
    "depends_on_past": False,
    "start_date": datetime(2019, 10, 7),
    "email": ["telemetry-alerts@mozilla.com", "amiyaguchi@mozilla.com"],
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 3,
    "retry_delay": timedelta(minutes=30),
}

dag = DAG("taar_daily", default_args=default_args, schedule_interval="0 1 * * *")

amodump = GKEPodOperator(
    task_id="taar_amodump",
    gcp_conn_id=gcp_conn_id,
    project_id=connection.project_id,
    location="us-central1-a",
    cluster_name=gke_cluster_name,
    name="taar-amodump",
    namespace="default",
    # This uses a circleci built docker image from github.com/mozilla/taar_gcp_etl
    image="gcr.io/moz-fx-data-airflow-prod-88e0/taar_gcp_etl:0.1",
    owner="vng@mozilla.com",
    email=["mlopatka@mozilla.com", "vng@mozilla.com", "hwoo@mozilla.com"],
    arguments=["-m", "taar_etl.taar_amodump", "--date", "{{ ds_nodash }}"],
    env_vars={
        "AWS_ACCESS_KEY_ID": taar_aws_access_key,
        "AWS_SECRET_ACCESS_KEY": taar_aws_secret_key,
    },
    dag=dag,
)

amowhitelist = GKEPodOperator(
    task_id="taar_amowhitelist",
    gcp_conn_id=gcp_conn_id,
    project_id=connection.project_id,
    location="us-central1-a",
    cluster_name=gke_cluster_name,
    name="taar-amowhitelist",
    namespace="default",
    # This uses a circleci built docker image from github.com/mozilla/taar_gcp_etl
    image="gcr.io/moz-fx-data-airflow-prod-88e0/taar_gcp_etl:0.1",
    owner="vng@mozilla.com",
    email=["mlopatka@mozilla.com", "vng@mozilla.com", "hwoo@mozilla.com"],
    # We are extracting addons from the AMO server's APIs which don't
    # support date based queries, so no date parameter is required
    # here.
    arguments=["-m", "taar_etl.taar_amowhitelist"],
    env_vars={
        "AWS_ACCESS_KEY_ID": taar_aws_access_key,
        "AWS_SECRET_ACCESS_KEY": taar_aws_secret_key,
    },
    dag=dag,
)

editorial_whitelist = GKEPodOperator(
    task_id="taar_update_whitelist",
    gcp_conn_id=gcp_conn_id,
    project_id=connection.project_id,
    location="us-central1-a",
    cluster_name=gke_cluster_name,
    name="taar-update-whitelist",
    namespace="default",
    # This uses a circleci built docker image from github.com/mozilla/taar_gcp_etl
    image="gcr.io/moz-fx-data-airflow-prod-88e0/taar_gcp_etl:0.1",
    owner="vng@mozilla.com",
    email=["mlopatka@mozilla.com", "vng@mozilla.com", "hwoo@mozilla.com"],
    arguments=["-m", "taar_etl.taar_update_whitelist", "--date", "{{ ds_nodash }}"],
    env_vars={
        "AWS_ACCESS_KEY_ID": taar_aws_access_key,
        "AWS_SECRET_ACCESS_KEY": taar_aws_secret_key,
    },
    dag=dag,
)

wait_for_clients_daily_export = ExternalTaskSensor(
    task_id="wait_for_clients_daily_export",
    external_dag_id="main_summary",
    external_task_id="clients_daily_export",
    dag=dag)

wait_for_main_summary_export = ExternalTaskSensor(
    task_id="wait_for_main_summary_export",
    external_dag_id="main_summary",
    external_task_id="main_summary_export",
    dag=dag)

taar_dynamo_job = SubDagOperator(
    task_id="taar_dynamo_job",
    subdag=moz_dataproc_pyspark_runner(
        parent_dag_name=dag.dag_id,
        dag_name="taar_dynamo_job",
        default_args=default_args,
        master_machine_type='n1-standard-32',
        worker_machine_type='n1-standard-32',
        cluster_name=taar_dynamo_cluster_name,
        job_name="TAAR_Dynamo",
        python_driver_code="gs://moz-fx-data-prod-airflow-dataproc-artifacts/jobs/taar_dynamo.py",
        num_workers=12,
        py_args=[
            "--date",
            "{{ ds_nodash }}",
            "--aws_access_key_id",
            taar_aws_access_key,
            "--aws_secret_access_key",
            taar_aws_secret_key,
        ],
        aws_conn_id=taar_aws_conn_id,
        gcp_conn_id=taar_gcpdataproc_conn_id,
        master_disk_type='pd-ssd',
        worker_disk_type='pd-ssd',
    ),
    dag=dag,
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
        num_workers=12,
        py_args=[
            "--date",
            "{{ ds_nodash }}",
            "--aws_access_key_id",
            taar_aws_access_key,
            "--aws_secret_access_key",
            taar_aws_secret_key,
            "--bucket",
            "telemetry-private-analysis-2",
            "--prefix",
            "taar/locale/",
        ],
        aws_conn_id=taar_aws_conn_id,
        gcp_conn_id=taar_gcpdataproc_conn_id,
    ),
    dag=dag,
)

taar_similarity_args = default_args.copy()
taar_similarity_args["email"] = ["vng@mozilla.com", "mlopatka@mozilla.com", "akomar@mozilla.com"]
taar_similarity = SubDagOperator(
    task_id="taar_similarity",
    subdag=moz_dataproc_pyspark_runner(
        parent_dag_name=dag.dag_id,
        dag_name="taar_similarity",
        default_args=taar_similarity_args,
        cluster_name=taar_similarity_cluster_name,
        job_name="TAAR_similarity",
        python_driver_code="gs://moz-fx-data-prod-airflow-dataproc-artifacts/jobs/taar_similarity.py",
        num_workers=4,
        worker_machine_type="n1-highmem-96",
        master_machine_type='n1-standard-8',
        py_args=[
            "--date", "{{ ds_nodash }}",
            "--bucket", "telemetry-private-analysis-2",
            "--prefix", "taar/similarity/",
            "--aws_access_key_id", taar_aws_access_key,
            "--aws_secret_access_key", taar_aws_secret_key,
        ],
        aws_conn_id=taar_aws_conn_id,
        gcp_conn_id=taar_gcpdataproc_conn_id,
    ),
    dag=dag,
)

taar_collaborative_recommender = SubDagOperator(
    task_id="addon_recommender",
    subdag=moz_dataproc_jar_runner(
        parent_dag_name=dag.dag_id,
        dag_name="addon_recommender",
        job_name="Train_the_Collaborative_Addon_Recommender",
        main_class="com.mozilla.telemetry.ml.AddonRecommender",
        jar_urls=[
            "https://s3-us-west-2.amazonaws.com/net-mozaws-data-us-west-2-ops-ci-artifacts"
            "/mozilla/telemetry-batch-view/master/telemetry-batch-view.jar",
        ],
        jar_args=[
          "train",
          "--runDate={{ds_nodash}}",
          "--inputTable=gs://moz-fx-data-derived-datasets-parquet/clients_daily/v6",
          "--privateBucket=s3a://telemetry-parquet",
          "--publicBucket=s3a://telemetry-public-analysis-2",
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
        default_args={
            key: value
            for key, value in chain(default_args.items(), [
                ("owner", "mlopatka@mozilla.com"),
                ("email", ["telemetry-alerts@mozilla.com", "mlopatka@mozilla.com", "vng@mozilla.com"]),
            ])
        },
    ),
    dag=dag,
)

taar_lite = SubDagOperator(
    task_id="taar_lite",
    subdag=moz_dataproc_pyspark_runner(
        parent_dag_name=dag.dag_id,
        dag_name="taar_lite",
        default_args=default_args,
        cluster_name=taarlite_cluster_name,
        job_name="TAAR_Lite_GUID_GUID",
        python_driver_code="gs://moz-fx-data-prod-airflow-dataproc-artifacts/jobs/taar_lite_guidguid.py",
        num_workers=8,
        py_args=[
            "--date",
            "{{ ds_nodash }}",
            "--aws_access_key_id",
            taar_aws_access_key,
            "--aws_secret_access_key",
            taar_aws_secret_key,
        ],
        aws_conn_id=taar_aws_conn_id,
        gcp_conn_id=taar_gcpdataproc_conn_id,
    ),
    dag=dag,
)


amodump >> amowhitelist
amodump >> editorial_whitelist

wait_for_main_summary_export >> taar_dynamo_job
wait_for_clients_daily_export >> taar_similarity
wait_for_clients_daily_export >> taar_locale
wait_for_clients_daily_export >> taar_collaborative_recommender
wait_for_clients_daily_export >> taar_lite
