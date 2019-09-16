#!/bin/bash

set -exo pipefail

# Logs will be available on the dataproc nodes at /var/log/dataproc-initialization-script-X.log
# or via the GCP Dataproc UI

ARTIFACTS_BUCKET=gs://moz-fx-data-prod-airflow-dataproc-artifacts

ROLE=$(/usr/share/google/get_metadata_value attributes/dataproc-role)
if [[ "${ROLE}" == 'Master' ]]; then
  # You can put any master-specific logic here
  echo "Running dataproc_init.sh on master..."
fi

gsutil cp $ARTIFACTS_BUCKET/jars/* /usr/lib/spark/jars/

# Install spark packages
# See https://github.com/mozilla/telemetry-spark-packages-assembly
TSPA_VERSION=v1.0.0
TSPA_GS_PATH=$ARTIFACTS_BUCKET/mozilla/telemetry-spark-packages-assembly/$TSPA_VERSION/telemetry-spark-packages-assembly.jar
TSPA_JAR=/usr/lib/spark/jars/telemetry-spark-packages-assembly.jar
gsutil cp $TSPA_GS_PATH $TSPA_JAR

# Install python packages
PIP_REQUIREMENTS_FILE=/tmp/requirements.txt
gsutil cp $ARTIFACTS_BUCKET/bootstrap/python-requirements.txt $PIP_REQUIREMENTS_FILE
/opt/conda/default/bin/pip install --upgrade pip
/opt/conda/default/bin/pip install -r $PIP_REQUIREMENTS_FILE
