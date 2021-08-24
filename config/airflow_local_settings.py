from kubernetes.client.models import V1Pod, V1Container
from kubernetes.client import models as k8s
from copy import deepcopy

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
    """https://github.com/kubernetes-client/python/blob/master/kubernetes/docs/V1Pod.md"""

    # check that we're running a prio-processor job, and spin up a side-car
    # container for proxying gcs buckets.
    if pod.metadata.labels["job-kind"] == "prio-processor":
        pod.spec.share_process_namespace = True
        # there is only one container within the pod, so lets append a few more

        # for proxying gcs resource consistently across platforms
        minio_container = deepcopy(pod.spec.containers[0])
        minio_container.image = "minio/minio:RELEASE.2021-06-17T00-10-46Z"
        minio_container.args = ["gateway", "gcs", "$(PROJECT_ID)"]
        minio_container.name = "minio"
        pod.spec.containers.append(minio_container)

        pkill_container = deepcopy(pod.spec.containers[0])
        pkill_container.image = "ubuntu:focal"
        pkill_container.args = [
            "bash",
            "-c",
            "while !pidof minio-done; do sleep 1; done; pkill -SIGINT -f minio; pkill -SIGINT -f minio-done",
        ]
        pkill_container.name = "reaper"
        pod.spec.containers.append(pkill_container)
