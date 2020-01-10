import os
import subprocess
import tempfile

from airflow import AirflowException

from airflow.contrib.hooks.gcp_container_hook import GKEClusterHook

from airflow.contrib.operators.gcp_container_operator import GKEPodOperator as UpstreamGKEPodOperator, GKEClusterCreateOperator, GKEClusterDeleteOperator


KUBE_CONFIG_ENV_VAR = "KUBECONFIG"
G_APP_CRED = "GOOGLE_APPLICATION_CREDENTIALS"
GCLOUD_APP_CRED = "CLOUDSDK_AUTH_CREDENTIAL_FILE_OVERRIDE"


class GKEPodOperator(UpstreamGKEPodOperator):
    """
    We override execute and _set_env_from_extras methods to support:

    - `CLOUDSDK_AUTH_CREDENTIAL_FILE_OVERRIDE` environment variable that is
        set to the path of the Service Account JSON key file. This is neccesary
        for gcloud to operate.

    - Adjust when NamedTemporaryFile file descriptor is closed.

    - Preserve XCOM result when xcom_push is True.

    """
    def __init__(self, image_pull_policy='Always', *args,  **kwargs):
        super(GKEPodOperator, self).__init__(image_pull_policy=image_pull_policy, *args, **kwargs)

    def execute(self, context):
        # If gcp_conn_id is not specified gcloud will use the default
        # service account credentials.
        if self.gcp_conn_id:
            from airflow.hooks.base_hook import BaseHook
            # extras is a deserialized json object
            extras = BaseHook.get_connection(self.gcp_conn_id).extra_dejson
            self._set_env_from_extras(extras=extras)

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

            # Tell `KubernetesPodOperator` where the config file is located
            self.config_file = os.environ[KUBE_CONFIG_ENV_VAR]
            result = super(UpstreamGKEPodOperator, self).execute(context)
            if self.xcom_push:
                return result

    def _set_env_from_extras(self, extras):
        """
        Sets the environment variable `GOOGLE_APPLICATION_CREDENTIALS` and
        `CLOUDSDK_AUTH_CREDENTIAL_FILE_OVERRIDE` with either:

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
            os.environ[G_APP_CRED] = key_path
            os.environ[GCLOUD_APP_CRED] = key_path
        else:
            # Write service account JSON to secure file for gcloud to reference
            service_key = tempfile.NamedTemporaryFile(delete=False)
            service_key.write(keyfile_json_str)
            os.environ[G_APP_CRED] = service_key.name
            os.environ[GCLOUD_APP_CRED] = service_key.name
            service_key.close()

class GKEClusterCreateOperator(GKEClusterCreateOperator):
    """
    We override methods to fix https://issues.apache.org/jira/browse/AIRFLOW-4518
    The GKEClusterHook constructor call wasn't updated to reflect a code change

    """
    def _check_input(self):
        if all([self.gcp_conn_id, self.project_id, self.location, self.body]):
            if isinstance(self.body, dict) and 'name' in self.body:
                # Don't throw error
                return
            # If not dict, then must
            elif self.body.name and self.body.initial_node_count:
                return

        self.log.error(
            'One of (gcp_conn_id, project_id, location, body, body[\'name\'], '
            'body[\'initial_node_count\']) is missing or incorrect')
        raise AirflowException('Operator has incorrect or missing input.')

    def execute(self, context):
        self._check_input()
        hook = GKEClusterHook(gcp_conn_id=self.gcp_conn_id, location=self.location)
        create_op = hook.create_cluster(cluster=self.body)
        return create_op

class GKEClusterDeleteOperator(GKEClusterDeleteOperator):
    """
    We override methods to fix https://issues.apache.org/jira/browse/AIRFLOW-4518
    The GKEClusterHook constructor call wasn't updated to reflect a code change

    """
    def _check_input(self):
        if not all([self.gcp_conn_id, self.project_id, self.name, self.location]):
            self.log.error(
                'One of (gcp_conn_id, project_id, name, location) is missing or incorrect')
            raise AirflowException('Operator has incorrect or missing input.')


    def execute(self, context):
        self._check_input()
        hook = GKEClusterHook(gcp_conn_id=self.gcp_conn_id, location=self.location)
        delete_result = hook.delete_cluster(name=self.name)
        return delete_result
