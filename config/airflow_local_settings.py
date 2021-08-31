from copy import deepcopy

from kubernetes.client import models as k8s
from kubernetes.client.models import V1Container, V1Pod

STATE_COLORS = {
    "queued": "gray",
    "running": "lime",
    "success": "#0000FF",
    "failed": "red",
    "up_for_retry": "gold",
    "up_for_reschedule": "turquoise",
    "upstream_failed": "orange",
    "skipped": "pink",
    "scheduled": "tan",
}


def pod_mutation_hook(pod: V1Pod):
    """Modify all Kubernetes pod definitions when run with the pod operator.

    Changes to this function will require a cluster restart to pick up.
    Functionality here can be moved closer to the pod definition in Airflow 2.x.

    https://github.com/kubernetes-client/python/blob/master/kubernetes/docs/V1Pod.md
    """

    # Check that we're running a prio-processor job, and spin up a side-car
    # container for proxying gcs buckets. All other jobs will be unaffected.
    # This whole mutation would be unnecessary if there were a long-lived minio
    # service available to the pod's network.
    if pod.metadata.labels["job-kind"] == "prio-processor":
        pod.spec.share_process_namespace = True
        # there is only one container within the pod, so lets append a few more

        # Add a new container to the spec to run minio. We will run a gcs
        # gateway and proxy all traffic through it. This allows the container to
        # use the mc tool and s3a spark adapter and makes it cloud-provider
        # agnostic. See https://github.com/mozilla/prio-processor/pull/119 for
        # the reason behind the pinned image.
        minio_container = deepcopy(pod.spec.containers[0])
        minio_container.image = "minio/minio:RELEASE.2021-06-17T00-10-46Z"
        minio_container.args = ["gateway", "gcs", "$(PROJECT_ID)"]
        minio_container.name = "minio"
        pod.spec.containers.append(minio_container)

        # Search for a new process named `minio-done` and kill the minio
        # container above. This can be done using `exec -a minio-done sleep 10`
        # which will will create a process available in the shared namespace for
        # 10 seconds. We use a ubuntu image so we can utilize pidof and pkill.
        pkill_container = deepcopy(pod.spec.containers[0])
        pkill_container.image = "ubuntu:focal"
        pkill_container.args = [
            "bash",
            "-c",
            "until pidof minio-done; do sleep 1; done; pkill -SIGINT -f minio",
        ]
        pkill_container.name = "reaper"
        pod.spec.containers.append(pkill_container)
