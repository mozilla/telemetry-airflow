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
import re
import json
import logging
import os
import shutil
import tempfile
from datetime import datetime, timedelta

from airflow import DAG
from airflow.contrib.hooks.gcp_api_base_hook import GoogleCloudBaseHook
from airflow.operators.bash_operator import BashOperator
from airflow.operators.sensors import ExternalTaskSensor
from airflow.operators.subdag_operator import SubDagOperator
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


def merge_dict(a, b):
    x = a.copy()
    for key, value in b.items():
        if key in x:
            logging.warning("{} is over written".format(key))
        x[key] = value
    return x


def gcloud_bash_operator(dag, task_id, conn_id, command, env_vars={}, **kwargs):
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
        env=merge_dict(
            env_vars,
            {
                "GOOGLE_APPLICATION_CREDENTIALS": tmp_credentials,
                # https://github.com/GoogleCloudPlatform/gsutil/issues/236
                "CLOUDSDK_PYTHON": "python",
                "PROJECT_ID": project_id,
                # hack to avoid airflow confusing this with a jinja template: https://stackoverflow.com/a/42198617
                "TMP_COMMAND": tmp_command + " ",
            },
        ),
        dag=dag,
        **kwargs
    )
    shutil.rmtree(tmp_dir)


def gce_container_subdag(
    parent_dag_name,
    child_dag_name,
    default_args,
    conn_id,
    task_id,
    image=None,
    env_vars={},
    arguments=[],
    zone="us-west1-b",
    **kwargs
):
    # Create a valid cluster id based on the instances reference. This value must
    # be passed as an environment variable so airflow can properly template variables.
    # see: https://cloud.google.com/compute/docs/reference/rest/v1/instances
    cluster_id = "-".join(re.split(r"[^a-zA-Z0-9']", task_id))
    assert re.match(r"^[a-z]([-a-z0-9]*[a-z0-9])?$", cluster_id), (
        "invalid cluster_id: " + cluster_id
    )
    cluster_id += "-{{ ds_nodash }}"

    # NOTE: set the number of retries to 0, because retries are useless in this subdag
    args = default_args.copy()
    args["retries"] = 0

    with DAG("{}.{}".format(parent_dag_name, child_dag_name), default_args=args) as dag:
        start_op = gcloud_bash_operator(
            dag=dag,
            task_id="gcloud_compute_instances_create",
            conn_id=conn_id,
            command="""
                # https://cloud.google.com/sdk/gcloud/reference/compute/instances/create#INSTANCE_NAMES
                # https://cloud.google.com/container-optimized-os/docs/how-to/create-configure-instance#list-images
                gcloud compute instances create ${CLUSTER_ID} \
                    --zone ${ZONE} \
                    --image-project=cos-cloud \
                    --image-family=cos-stable
            """,
            env_vars=dict(CLUSTER_ID=cluster_id, ZONE=zone),
        )
        # use the same trick as gcloud_bash_operator to copy over the command over
        tmp_dir = tempfile.mkdtemp(prefix="airflow-gce-container")
        tmp_command = os.path.join(tmp_dir, "command.sh")
        with open(tmp_command, "w") as fp:
            fp.write(
                """
                #!/bin/bash
                # TODO envvars
                echo $TEST_VARIABLE
                docker run {image}
            """.format(
                    image=image
                )
            )
        scp_op = gcloud_bash_operator(
            dag=dag,
            task_id="gcloud_compute_scp",
            conn_id=conn_id,
            command="""
                gcloud compute scp \
                    $TMP_DOCKER_SH $CLUSTER_ID:/tmp/command.sh \
                    --zone=$ZONE
            """,
            env_vars=dict(
                CLUSTER_ID=cluster_id, ZONE=zone, TMP_DOCKER_SH=tmp_command + " "
            ),
        )
        container_op = gcloud_bash_operator(
            dag=dag,
            task_id=task_id,
            conn_id=conn_id,
            command="""
                gcloud compute ssh ${CLUSTER_ID} \
                    --zone=${ZONE} \
                    --command="bash -x -c 'source /tmp/command.sh'"
            """,
            env_vars=dict(CLUSTER_ID=cluster_id, ZONE=zone, IMAGE=image),
        )

        delete_op = gcloud_bash_operator(
            dag=dag,
            task_id="gcloud_compute_instances_delete",
            conn_id=conn_id,
            command="""
                gcloud compute instances delete ${CLUSTER_ID} --zone=${ZONE}
            """,
            env_vars=dict(CLUSTER_ID=cluster_id, ZONE=zone),
            trigger_rule="all_done",
        )
        dag >> start_op >> scp_op >> container_op >> delete_op
        return dag


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
    dag >> SubDagOperator(
        subdag=gce_container_subdag(
            parent_dag_name=dag.dag_id,
            child_dag_name="test_container_subdag",
            default_args=default_args,
            task_id="verify_prio_processor_unit_test",
            conn_id="google_cloud_derived_datasets",
            image="mozilla/prio-processor:latest",
        ),
        task_id="test_container_subdag",
    )
