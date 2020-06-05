import re
import json
import logging
import os
import shutil
import tempfile
import textwrap

from airflow.contrib.hooks.gcp_api_base_hook import GoogleCloudBaseHook
from airflow.operators.bash_operator import BashOperator


class GCloudBashOperator(BashOperator):
    def __init__(self, conn_id, *args, **kwargs):
        super(GCloudBashOperator, self).__init__(*args, **kwargs)
        self.conn = GoogleCloudBaseHook(conn_id)

    def execute(self, context):
        keyfile = json.loads(
            self.conn.extras["extra__google_cloud_platform__keyfile_dict"].encode()
        )
        # project of the service credentials
        project_id = keyfile["project_id"]

        tmp_dir = tempfile.mkdtemp(prefix="airflow-gcloud-cmd")

        # write credentials to a file
        tmp_credentials = os.path.join(tmp_dir, "keyfile.json")
        with open(tmp_credentials, "w") as fp:
            json.dump(keyfile, fp)

        self.bash_command = (
            textwrap.dedent(
                """
                set -e
                echo "Running command: $(cat $TMP_COMMAND)"
                gcloud auth activate-service-account --key-file $GOOGLE_APPLICATION_CREDENTIALS
                gcloud config set project $PROJECT_ID
                gcloud info
                set -x
                gcloud config get-value project
            """
            ).strip()
            + "\n"
            + textwrap.dedent(self.bash_command)
        )

        self.env = self._merge_dict(
            os.environ,
            self._merge_dict(
                self.env or {},
                {
                    "GOOGLE_APPLICATION_CREDENTIALS": tmp_credentials,
                    "PROJECT_ID": project_id,
                    # hack to avoid airflow confusing this with a jinja template
                    # https://stackoverflow.com/a/42198617
                    # set temporary config location instead of a shared one
                    # https://stackoverflow.com/a/48343135
                    "CLOUDSDK_CONFIG": tmp_dir + " ",
                },
            ),
        )
        try:
            super(GCloudBashOperator, self).execute(context)
        finally:
            shutil.rmtree(tmp_dir)

    def _merge_dict(self, a, b):
        x = a.copy()
        for key, value in b.items():
            if key in x:
                logging.warning("{} is over written".format(key))
            x[key] = value
        return x
