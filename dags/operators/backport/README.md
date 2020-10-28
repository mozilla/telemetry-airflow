### Using kube_client.py from 1.10.2
We used to include the airflow/contrib/kubernetes/kube_client.py from 1.10.2
because the 1.10.7 kube_client.py has some configuration issues when
trying to push xcom from gkepodoperator. if do_push_xcom is set to False,
the upstream GkePodOperator works fine.

### As of 1.10.12 I've removed the backported 1.10.7 gcp_container_operator,
kubernetes_pod_operator, and the 1.10.2 kube_client
