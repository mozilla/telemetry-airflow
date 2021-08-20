from kubernetes.client.models import V1Pod, V1Container
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
        # there is only one container within the pod
        minio_container = deepcopy(pod.spec.containers[0])
        minio_container.image = "minio/minio:RELEASE.2021-06-17T00-10-46Z"
        minio_container.args = ["gateway", "gcs", "$(PROJECT_ID)"]
        minio_container.name = "minio"
        pod.spec.containers.append(minio_container)