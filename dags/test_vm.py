"""A testing script for VM workflows in Airflow.

Containers are an common tool when working with job workflows in Airflow. For
example, we run the `mozilla/bigquery-etl:latest` image on a shared Google
Kubernetes Engine (GKE) cluster. Workloads no longer need to be run on Airflow
cluster and can be offloaded elsewhere.

A VM can perform the same functionality, at the minor cost of VM startup for
each command. Starting VMs in Google Compute Engine (GCE) is surprisingly fast.
An SSH connection can be established in less than 30 seconds, and a container
can be fetched from a registry in another 30 seconds.

The following will create a VM environment that assumes permissions from the
parent project. The VM can run a published docker image on DockerHub or Google
Container Registry (GCR).
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.sensors import ExternalTaskSensor
from operators.gce_container_subdag_operator import GceContainerSubDagOperator
from operators.gcloud_bash_operator import GCloudBashOperator

default_args = {
    "owner": "amiyaguchi@mozilla.com",
    "email": ["amiyaguchi@mozilla.com"],
    "depends_on_past": False,
    "start_date": datetime(2020, 5, 27),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
    "retry_delay": timedelta(minutes=30),
}

TEST_CONN_ID = "google_cloud_derived_datasets"

with DAG("test_vm", default_args=default_args, schedule_interval="0 1 * * *") as dag:
    dag >> BashOperator(task_id="gcloud_info", bash_command="gcloud info", dag=dag)
    dag >> GCloudBashOperator(
        task_id="verify_gcs_writable",
        conn_id=TEST_CONN_ID,
        bash_command="""
            bucket="gs://airflow-test-vm-dag-test-bucket-$RANDOM"
            gsutil mb $bucket
            gsutil ls $bucket
            gsutil rm -r $bucket
        """,
        dag=dag,
    )
    dag >> GceContainerSubDagOperator(
        task_id="verify_prio_processor_unit_test",
        default_args=default_args,
        conn_id=TEST_CONN_ID,
        image="mozilla/prio-processor:latest",
        dag=dag,
    )
