import os
import re
import tempfile

from airflow import DAG
from airflow.operators.subdag_operator import SubDagOperator

from operators.gcloud_bash_operator import GCloudBashOperator


class GceContainerSubDagOperator(SubDagOperator):
    def __init__(self, task_id, dag=None, default_args=None, **kwargs):
        super(GceContainerSubDagOperator, self).__init__(
            task_id=task_id,
            dag=dag,
            subdag=self.gce_container_subdag(
                dag=dag, default_args=default_args, task_id=task_id, **kwargs
            ),
        )

    @property
    def task_type(self):
        return "SubDagOperator"

    @staticmethod
    def gce_container_subdag(
        dag,
        default_args,
        conn_id,
        task_id,
        image=None,
        env_vars={},
        # TODO: cmds: list[str] override entrypoint
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

        with DAG("{}.{}".format(dag.dag_id, task_id), default_args=args) as dag:
            start_op = GCloudBashOperator(
                task_id="gcloud_compute_instances_create",
                conn_id=conn_id,
                bash_command="""
                    # https://cloud.google.com/sdk/gcloud/reference/compute/instances/create#INSTANCE_NAMES
                    # https://cloud.google.com/container-optimized-os/docs/how-to/create-configure-instance#list-images
                    gcloud compute instances create ${CLUSTER_ID} \
                        --zone ${ZONE} \
                        --image-project=cos-cloud \
                        --image-family=cos-stable
                """,
                env=dict(CLUSTER_ID=cluster_id, ZONE=zone),
            )
            # use the same trick as gcloud_bash_operator to copy over the command over
            tmp_dir = tempfile.mkdtemp(prefix="airflow-gce-container")
            tmp_command = os.path.join(tmp_dir, "command.sh")

            with open(tmp_command, "w") as fp:
                fp.write(
                    "#!/bin/bash\n"
                    + "docker run \\\n"
                    + "\t".join(
                        "-e {}={}\\\n".format(k, v) for k, v in env_vars.items()
                    )
                    + "\t"
                    + image
                    + "\\\n\t\t"
                    + " ".join(arguments)
                )
            scp_op = GCloudBashOperator(
                task_id="gcloud_compute_scp",
                conn_id=conn_id,
                bash_command="""
                    cat $TMP_DOCKER_SH
                    gcloud compute scp \
                        --zone=$ZONE \
                        $TMP_DOCKER_SH $CLUSTER_ID:/tmp/command.sh
                """,
                env=dict(
                    CLUSTER_ID=cluster_id, ZONE=zone, TMP_DOCKER_SH=tmp_command + " "
                ),
            )
            container_op = GCloudBashOperator(
                task_id=task_id,
                conn_id=conn_id,
                bash_command="""
                    gcloud compute ssh ${CLUSTER_ID} \
                        --zone=${ZONE} \
                        --command="bash -x -c 'source /tmp/command.sh'"
                """,
                env=dict(CLUSTER_ID=cluster_id, ZONE=zone, IMAGE=image),
            )

            delete_op = GCloudBashOperator(
                task_id="gcloud_compute_instances_delete",
                conn_id=conn_id,
                bash_command="""
                    gcloud compute instances delete ${CLUSTER_ID} --zone=${ZONE}
                """,
                env=dict(CLUSTER_ID=cluster_id, ZONE=zone),
                trigger_rule="all_done",
            )
            dag >> start_op >> scp_op >> container_op >> delete_op
            return dag
