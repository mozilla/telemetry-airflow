### Using kube_client.py from 1.10.2
We include the airflow/contrib/kubernetes/kube_client.py from 1.10.2
because the 1.10.7 kube_client.py has some configuration issues when
trying to push xcom from gkepodoperator. if do_push_xcom is set to False,
the upstream GkePodOperator works fine.
