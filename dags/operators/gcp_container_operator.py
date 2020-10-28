import os
import subprocess
import tempfile

from google.auth.environment_vars import CREDENTIALS

from airflow import AirflowException

from airflow.contrib.hooks.gcp_container_hook import GKEClusterHook

from airflow.contrib.operators.gcp_container_operator import GKEPodOperator as UpstreamGKEPodOperator

KUBE_CONFIG_ENV_VAR = "KUBECONFIG"
GCLOUD_APP_CRED = "CLOUDSDK_AUTH_CREDENTIAL_FILE_OVERRIDE"

# Note: In the next version of airflow this will change.
# This module is deprecated. Please use `airflow.providers.google.cloud.operators.kubernetes_engine`.

class GKEPodOperator(UpstreamGKEPodOperator):
    """
    We override execute and _set_env_from_extras methods to support:

    - `CLOUDSDK_AUTH_CREDENTIAL_FILE_OVERRIDE` environment variable that is
        set to the path of the Service Account JSON key file. This is neccesary
        for gcloud to operate.

    - Adjust when NamedTemporaryFile file descriptor is closed.

    - Preserve XCOM result when do_xcom_push is True.

    - Override init to default image_pull_policy=Always, in_cluster=False, do_xcom_push=False and GKE params

    - set reattach_on_restart=False when do_xcom_push=True to address an error (details below)

    """
    def __init__(self,
                 image_pull_policy='Always',
                 in_cluster=False,
                 do_xcom_push=False,
                 # Defined in Airflow's UI -> Admin -> Connections
                 gcp_conn_id='google_cloud_derived_datasets',
                 project_id='moz-fx-data-derived-datasets',
                 location='us-central1-a',
                 cluster_name='bq-load-gke-1',
                 namespace='default',
                 *args,
                 **kwargs):

        """
        Retrying a failed task with do_xcom_push=True causes airflow to reattach to the pod
        eventually causing a 'Handshake status 500 Internal Server Error'. Logs will indicate
        'found a running pod with ... different try_number. Will attach to this pod and monitor
        instead of starting new one'
        """
        reattach_on_restart = False if do_xcom_push else True

        super(GKEPodOperator, self).__init__(
            image_pull_policy=image_pull_policy,
            in_cluster=in_cluster,
            do_xcom_push=do_xcom_push,
            reattach_on_restart=reattach_on_restart,
            gcp_conn_id=gcp_conn_id,
            project_id=project_id,
            location=location,
            cluster_name=cluster_name,
            namespace=namespace,
            *args,
            **kwargs)

    def execute(self, context):
        # We can remove this override once upgraded to 2.0.  https://issues.apache.org/jira/browse/AIRFLOW-4072

        # Moz specific - Commented out key_file references (Jason fixed auth behaviour with 1.10.2)
        # key_file = None

        # If gcp_conn_id is not specified gcloud will use the default
        # service account credentials.
        if self.gcp_conn_id:
            from airflow.hooks.base_hook import BaseHook
            # extras is a deserialized json object
            extras = BaseHook.get_connection(self.gcp_conn_id).extra_dejson
            self._set_env_from_extras(extras=extras) # Moz specific since func no longer returns value

        # Write config to a temp file and set the environment variable to point to it.
        # This is to avoid race conditions of reading/writing a single file
        with tempfile.NamedTemporaryFile() as conf_file:
            os.environ[KUBE_CONFIG_ENV_VAR] = conf_file.name
            # Attempt to get/update credentials
            # We call gcloud directly instead of using google-cloud-python api
            # because there is no way to write kubernetes config to a file, which is
            # required by KubernetesPodOperator.
            # The gcloud command looks at the env variable `KUBECONFIG` for where to save
            # the kubernetes config file.
            subprocess.check_call(
                ["gcloud", "container", "clusters", "get-credentials",
                 self.cluster_name,
                 "--zone", self.location,
                 "--project", self.project_id])

            # if key_file: # Moz specific commented out
            #    key_file.close() # Moz specific commented out

            # Tell `KubernetesPodOperator` where the config file is located
            self.config_file = os.environ[KUBE_CONFIG_ENV_VAR]
            result = super(UpstreamGKEPodOperator, self).execute(context) # Moz specific
            if self.do_xcom_push: # Moz specific
                return result # Moz specific


    def _set_env_from_extras(self, extras):
        """
        Sets the environment variable `GOOGLE_APPLICATION_CREDENTIALS` and
        `CLOUDSDK_AUTH_CREDENTIAL_FILE_OVERRIDE`with either:

        - The path to the keyfile from the specified connection id
        - A generated file's path if the user specified JSON in the connection id. The
            file is assumed to be deleted after the process dies due to how mkstemp()
            works.

        The environment variable is used inside the gcloud command to determine correct
        service account to use.
        """
        key_path = self._get_field(extras, 'key_path', False)
        keyfile_json_str = self._get_field(extras, 'keyfile_dict', False)

        if not key_path and not keyfile_json_str:
            self.log.info('Using gcloud with application default credentials.')
        elif key_path:
            os.environ[CREDENTIALS] = key_path
            os.environ[GCLOUD_APP_CRED] = key_path
            return None
        else:
            # Write service account JSON to secure file for gcloud to reference
            service_key = tempfile.NamedTemporaryFile(delete=False)
            service_key.write(keyfile_json_str.encode('utf-8'))
            os.environ[CREDENTIALS] = service_key.name
            os.environ[GCLOUD_APP_CRED] = service_key.name
            # Return file object to have a pointer to close after use,
            # thus deleting from file system.
            service_key.close() # Moz specific instead of return service_key
