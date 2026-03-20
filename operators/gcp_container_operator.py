import logging
from typing import Optional

import kubernetes.client as k8s
from airflow.exceptions import AirflowException
from airflow.providers.cncf.kubernetes.operators.pod import (
    KubernetesPodOperatorCallback,
    OnFinishAction,
    PodPhase,
)
from airflow.providers.google.cloud.operators.kubernetes_engine import (
    GKEStartPodOperator as UpstreamGKEPodOperator,
    KubernetesEnginePodLink,
)
from airflow.utils.context import Context


logger = logging.getLogger(__name__)


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

    # Disable the templating setting for the `on_finish_action` field because that
    # mangles the field's enum value into an incompatible string representation.
    template_fields = tuple(
        set(UpstreamGKEPodOperator.template_fields) - {"on_finish_action"}
    )

    def __init__(
        self,
        image_pull_policy="Always",
        in_cluster=False,
        startup_timeout_seconds=240,
        do_xcom_push=False,
        reattach_on_restart=False,
        # Delete succeeded pods to avoid an upstream operator bug where re-running successful tasks
        # with `reattach_on_restart` enabled doesn't actually start a new pod if a pod from a
        # previous successful try exists (https://mozilla-hub.atlassian.net/browse/DENG-8670).
        on_finish_action=OnFinishAction.DELETE_SUCCEEDED_POD.value,
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
            startup_timeout_seconds=startup_timeout_seconds,
            do_xcom_push=do_xcom_push,
            reattach_on_restart=reattach_on_restart,
            on_finish_action=on_finish_action,
            gcp_conn_id=gcp_conn_id,
            project_id=project_id,
            location=location,
            cluster_name=cluster_name,
            namespace=namespace,
            callbacks=GKEPodOperatorCallbacks,
            **kwargs,
        )

    def get_or_create_pod(
        self, pod_request_obj: k8s.V1Pod, context: Context
    ) -> k8s.V1Pod:
        """Set GKE pod link during pod creation.

        Workaround for https://github.com/apache/airflow/issues/46658
        This is meant for apache-airflow-providers-google==14.0.0
        """
        pod = super().get_or_create_pod(pod_request_obj, context)
        self.pod = pod
        KubernetesEnginePodLink.persist(context=context, task_instance=self)
        return pod

    def process_pod_deletion(self, pod: k8s.V1Pod, *, reraise=True):
        if pod is None:
            return

        # Since we don't set on_finish_action="delete_pod" the pod may be left running if the task
        # was stopped for a reason other than the pod succeeding/failing, like a pod startup timeout
        # or a task execution timeout (this could be considered a bug in the Kubernetes provider).
        # As a workaround we delete the pod during cleanup if it's still running.
        try:
            remote_pod: k8s.V1Pod = self.client.read_namespaced_pod(
                pod.metadata.name, pod.metadata.namespace
            )
            if (
                remote_pod.status.phase not in PodPhase.terminal_states
                and self.on_finish_action != OnFinishAction.DELETE_POD
            ):
                logger.info(
                    f"Deleting {remote_pod.status.phase.lower()} pod: {pod.metadata.name}"
                )
                self.pod_manager.delete_pod(remote_pod)
            else:
                super().process_pod_deletion(pod, reraise=reraise)
        except Exception as e:
            if isinstance(e, k8s.ApiException) and e.status == 404:
                # Ignore "404 Not Found" errors.
                logger.warning(f'Pod "{pod.metadata.name}" not found.')
            elif reraise:
                raise
            else:
                logger.exception(e)

    def cleanup(self, pod: k8s.V1Pod, remote_pod: k8s.V1Pod):
        """Include pod logs in the failure exception for email alerts."""
        # Fetch tail logs before super().cleanup() which may delete the pod depending on OnFinishAction
        pod_phase = (
            remote_pod.status.phase
            if remote_pod and hasattr(remote_pod, "status")
            else None
        )
        tail_log_lines = 20
        tail_logs = (
            self._get_tail_logs(remote_pod, tail_log_lines)
            if pod_phase != PodPhase.SUCCEEDED
            else None
        )
        try:
            super().cleanup(pod, remote_pod)
        except AirflowException as e:
            if tail_logs:
                raise AirflowException(
                    "\n".join(
                        [
                            f"Last {tail_log_lines} lines of pod logs:",
                            tail_logs,
                            "",
                            str(e),
                        ]
                    )
                ) from e
            raise

    def _get_tail_logs(self, pod: k8s.V1Pod, tail_lines: int) -> Optional[str]:
        """Fetch the last tail_lines lines of logs from the pod's container."""
        if pod is None:
            return None
        try:
            log_consumer = self.pod_manager.read_pod_logs(
                pod=pod,
                container_name=self.base_container_name,
                tail_lines=tail_lines,
                follow=False,
            )
            lines = [
                raw_line.decode("utf-8", errors="backslashreplace").rstrip()
                for raw_line in log_consumer
            ]
            return "\n".join(lines) if lines else None
        except Exception:
            logger.info("Failed to fetch tail logs for failure email", exc_info=True)
            return None
