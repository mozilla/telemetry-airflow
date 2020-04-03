"""
This configures a weekly DAG to run the TAAR Ensemble job off.
"""
from airflow import DAG
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.operators.sensors import ExternalTaskSensor
from datetime import datetime, timedelta
from airflow.operators.moz_databricks import MozDatabricksSubmitRunOperator
from airflow.operators.subdag_operator import SubDagOperator
from utils.dataproc import moz_dataproc_pyspark_runner
from utils.mozetl import mozetl_envvar

taar_aws_conn_id = "airflow_taar_rw_s3"
taar_aws_access_key, taar_aws_secret_key, session = AwsHook(
    taar_aws_conn_id
).get_credentials()
taar_ensemble_cluster_name = "dataproc-taar-ensemble"
taar_gcpdataproc_conn_id = "google_cloud_airflow_dataproc"

default_args_weekly = {
    "owner": "vng@mozilla.com",
    "depends_on_past": False,
    "start_date": datetime(2020, 4, 4),
    "email": ["telemetry-alerts@mozilla.com"],
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
    "retry_delay": timedelta(minutes=60),
}


taar_weekly = DAG(
    "taar_weekly", default_args=default_args_weekly, schedule_interval="@weekly"
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
            "spark:spark.hadoop.fs.s3a.access.key": taar_aws_access_key,
            "spark:spark.hadoop.fs.s3a.secret.key": taar_aws_secret_key,
            "spark:spark.jars.packages": "org.apache.spark:spark-avro_2.11:2.4.4",
            "spark:spark.python.profile": "true",
        },
        num_workers=35,
        worker_machine_type="n1-standard-8",
        master_machine_type="n1-standard-8",
        init_actions_uris=["gs://moz-fx-data-prod-airflow-dataproc-artifacts/pip-install.sh"],
        additional_metadata={
            "PIP_PACKAGES": "mozilla-taar3==0.4.12 mozilla-srgutil==0.2.1 python-decouple==3.1 click==7.0 boto3==1.7.71 dockerflow==2018.4.0"
        },
        optional_components=["ANACONDA", "JUPYTER"],
        py_args=[
            "--date",
            "{{ ds_nodash }}",
            "--aws_access_key_id",
            taar_aws_access_key,
            "--aws_secret_access_key",
            taar_aws_secret_key,
            "--sample_rate",
            "0.005",
        ],
        aws_conn_id=taar_aws_conn_id,
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
