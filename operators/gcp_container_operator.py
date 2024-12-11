import kubernetes.client as k8s
from airflow.providers.cncf.kubernetes.callbacks import KubernetesPodOperatorCallback
from airflow.providers.google.cloud.operators.kubernetes_engine import (
    GKEStartPodOperator as UpstreamGKEPodOperator,
)


class GKEPodOperatorCallbacks(KubernetesPodOperatorCallback):
    @staticmethod
    def on_pod_completion(
        *, pod: k8s.V1Pod, client: k8s.CoreV1Api, mode: str, **kwargs
    ) -> None:
        # Allow eviction of completed pods so they don't prevent the cluster from scaling down.
        pod_patch = k8s.V1Pod(
            metadata=k8s.V1ObjectMeta(
                annotations={"cluster-autoscaler.kubernetes.io/safe-to-evict": "true"}
            )
        )
        client.patch_namespaced_pod(
            pod.metadata.name, pod.metadata.namespace, pod_patch
        )


class GKEPodOperator(UpstreamGKEPodOperator):
    """
    Based off GKEStartPodOperator.

    - In 1.10.x this inherited from upstream GKEPodOperator, rather than GKEStartPodOperator(v2)
    - In 1.10.x we needed to override the execute and helper methods to set an environment
    variable for authentication to work (CLOUDSDK_AUTH_CREDENTIAL_FILE_OVERRIDE). Fixed in v2
    - We will keep this class and call the upstream GkeStartPodOperator now, because
    numerous places in our code references it still
    - Overrides init to default image_pull_policy=Always, in_cluster=False,
    do_xcom_push=False and GKE params
    - Defaults reattach_on_restart=False to address a 1.10.12 regression where GkePodOperators
        reruns will simply attach to an existing pod and not perform any new work.
    - Hard sets reattach_on_restart=False when do_xcom_push=True to address an error
        Retrying a failed task with do_xcom_push=True causes airflow to reattach to the pod
        eventually causing a 'Handshake status 500 Internal Server Error'. Logs will indicate
        'found a running pod with ... different try_number. Will attach to this pod and monitor
        instead of starting new one'.

    """

    def __init__(
        self,
        image_pull_policy="Always",
        in_cluster=False,
        do_xcom_push=False,
        reattach_on_restart=False,
        # Defined in Airflow's UI -> Admin -> Connections
        gcp_conn_id="google_cloud_airflow_gke",
        project_id="moz-fx-data-airflow-gke-prod",
        location="us-west1",
        cluster_name="workloads-prod-v1",
        namespace="default",
        *args,
        **kwargs,
    ):
        # Hard set reattach_on_restart = False when do_xcom_push is enabled.
        if do_xcom_push:
            reattach_on_restart = False

        super().__init__(
            *args,
            image_pull_policy=image_pull_policy,
            in_cluster=in_cluster,
            do_xcom_push=do_xcom_push,
            reattach_on_restart=reattach_on_restart,
            gcp_conn_id=gcp_conn_id,
            project_id=project_id,
            location=location,
            cluster_name=cluster_name,
            namespace=namespace,
            callbacks=GKEPodOperatorCallbacks,
            **kwargs,
        )
