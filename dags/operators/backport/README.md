### Using kube_client.py from 1.10.2
We used to include the airflow/contrib/kubernetes/kube_client.py from 1.10.2
because the 1.10.7 kube_client.py has some configuration issues when
trying to push xcom from gkepodoperator. if do_push_xcom is set to False,
the upstream GkePodOperator works fine.

### As of 1.10.12 I've removed the backported 1.10.7 gcp_container_operator,
kubernetes_pod_operator, and the 1.10.2 kube_client


### Fivetran operator backported from 2.0+
Fivetran provides an [operator and sensor](https://github.com/fivetran/airflow-provider-fivetran)
for integrating with the Fivetran API for Airflow version 2.0+. This was backported for
Airflow 1.10.15, and then our backport was removed after upgrading to Airflow 2.3.3.

### For 2.1.0 I've removed bigquery_operator_1_10_2.py, in favor of the new
google provider code.
