from airflow.providers.google.cloud.operators.kubernetes_engine import GKEStartPodOperator as UpstreamGKEPodOperator


class GKEPodOperator(UpstreamGKEPodOperator):
    """
    - This will now have the same defaults as GkeNatPodOperator pointing to the newer GKE cluster
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
        instead of starting new one'

    """
    def __init__(self,
                 image_pull_policy='Always',
                 in_cluster=False,
                 do_xcom_push=False,
                 reattach_on_restart=False,
                 # Defined in Airflow's UI -> Admin -> Connections
                 gcp_conn_id='google_cloud_airflow_gke',
                 project_id='moz-fx-data-airflow-gke-prod',
                 location='us-west1',
                 cluster_name='workloads-prod-v1',
                 namespace='default',
                 *args,
                 **kwargs):

        # Hard set reattach_on_restart = False when do_xcom_push is enabled.
        if do_xcom_push:
            reattach_on_restart = False

        # GKE node pool autoscaling is failing to scale down when completed pods exist on the node
        # in Completed states, due to the pod not being replicated. E.g. behind an rc or deployment.
        annotations = {'cluster-autoscaler.kubernetes.io/safe-to-evict': 'true'}

        super(GKEPodOperator, self).__init__(
            image_pull_policy=image_pull_policy,
            in_cluster=in_cluster,
            do_xcom_push=do_xcom_push,
            reattach_on_restart=reattach_on_restart,
            annotations=annotations,
            gcp_conn_id=gcp_conn_id,
            project_id=project_id,
            location=location,
            cluster_name=cluster_name,
            namespace=namespace,
            *args,
            **kwargs)


class GKENatPodOperator(UpstreamGKEPodOperator):
    """
    This class is similar to the one defined above but has defaults to use a different GKE Cluster
    With a NAT
    """
    def __init__(self,
                 image_pull_policy='Always',
                 in_cluster=False,
                 do_xcom_push=False,
                 reattach_on_restart=False,
                 # Defined in Airflow's UI -> Admin -> Connections
                 gcp_conn_id='google_cloud_airflow_gke',
                 project_id='moz-fx-data-airflow-gke-prod',
                 location='us-west1',
                 cluster_name='workloads-prod-v1',
                 namespace='default',
                 *args,
                 **kwargs):

        # Hard set reattach_on_restart = False when do_xcom_push is enabled.
        if do_xcom_push:
            reattach_on_restart = False

        # GKE node pool autoscaling is failing to scale down when completed pods exist on the node
        # in Completed states, due to the pod not being replicated. E.g. behind an rc or deployment.
        annotations = {'cluster-autoscaler.kubernetes.io/safe-to-evict': 'true'}

        super(GKENatPodOperator, self).__init__(
            image_pull_policy=image_pull_policy,
            in_cluster=in_cluster,
            do_xcom_push=do_xcom_push,
            reattach_on_restart=reattach_on_restart,
            annotations=annotations,
            gcp_conn_id=gcp_conn_id,
            project_id=project_id,
            location=location,
            cluster_name=cluster_name,
            namespace=namespace,
            *args,
            **kwargs)
