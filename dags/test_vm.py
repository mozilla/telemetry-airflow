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
import json
import os
import shutil
import tempfile
from datetime import datetime, timedelta

from airflow import DAG
from airflow.contrib.hooks.gcp_api_base_hook import GoogleCloudBaseHook
from airflow.operators.bash_operator import BashOperator
from airflow.operators.sensors import ExternalTaskSensor
from operators.gcp_container_operator import GKEPodOperator

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


def gcloud_bash_operator(dag, task_id, conn_id, command):
    conn = GoogleCloudBaseHook(conn_id)
    keyfile = json.loads(
        conn.extras["extra__google_cloud_platform__keyfile_dict"].encode()
    )
    # project of the service credentials
    project_id = keyfile["project_id"]

    tmp_dir = tempfile.mkdtemp(prefix="airflow-gcloud-cmd")
    tmp_credentials = os.path.join(tmp_dir, "keyfile.json")
    tmp_command = os.path.join(tmp_dir, "command.sh")

    with open(tmp_credentials, "w") as fp:
        json.dump(keyfile, fp)
    with open(tmp_command, "w") as fp:
        fp.write(command)

    return BashOperator(
        task_id=task_id,
        bash_command="""
        set -e
        echo "Running command: $(cat ${TMP_COMMAND})"
        gcloud auth activate-service-account --key-file ${GOOGLE_APPLICATION_CREDENTIALS}
        gcloud config set project ${PROJECT_ID}
        set -x
        gcloud config get-value project
        source ${TMP_COMMAND}
        """,
        env={
            "GOOGLE_APPLICATION_CREDENTIALS": tmp_credentials,
            # https://github.com/GoogleCloudPlatform/gsutil/issues/236
            "CLOUDSDK_PYTHON": "python",
            "PROJECT_ID": project_id,
            # hack to avoid airflow confusing this with a jinja template: https://stackoverflow.com/a/42198617
            "TMP_COMMAND": tmp_command + " ",
        },
        dag=dag,
    )
    shutil.rmtree(tmp_dir)


def gce_container_subdag(
    parent_dag_name,
    child_dag_name,
    conn_id,
    container,
    image=None,
    env_vars={},
    **kwargs
):
    pass


with DAG("test_vm", default_args=default_args, schedule_interval="0 1 * * *") as dag:
    dag >> gcloud_bash_operator(
        dag=dag,
        task_id="verify_gcs_writable",
        conn_id="google_cloud_derived_datasets",
        command="""
            bucket="gs://airflow-test-vm-dag-test-bucket-$RANDOM"
            gsutil mb $bucket
            gsutil ls $bucket
            gsutil rm -r $bucket
        """,
    )
    # >> gce_container_subdag(
    #     dag=dag,
    #     task_id="verify_prio_processor_unit_test",
    #     conn_id="google_cloud_derived_datasets",
    #     image="mozilla/prio-processor:latest",
    # )
