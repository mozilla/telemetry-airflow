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

        # also create a volume for communication...
        volume = k8s.V1Volume(
            name="comm-volume", empty_dir=k8s.V1EmptyDirVolumeSource()
        )

        volume_mount = k8s.V1VolumeMount(
            name="comm-volume", mount_path="/mnt/comm", sub_path=None, read_only=False
        )

        pod.spec.volumes = [volume]
        pod.spec.containers[0].volume_mounts = [volume_mount]

        # for proxying gcs resource consistently across platforms
        minio_container = deepcopy(pod.spec.containers[0])
        minio_container.image = "minio/minio:RELEASE.2021-06-17T00-10-46Z"
        minio_container.args = ["gateway", "gcs", "$(PROJECT_ID)"]
        minio_container.name = "minio"
        pod.spec.containers.append(minio_container)

        pkill_container = deepcopy(pod.spec.containers[0])
        pkill_container.image = "ubuntu:focal"
        pkill_container.args = [
            "-c",
            "sleep 10 && ls /mnt/comm && ps && echo test",
        ]
        pkill_container.name = "reaper"
        pod.spec.containers.append(pkill_container)
