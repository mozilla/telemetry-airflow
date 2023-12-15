FROM apache/airflow:slim-2.7.3-python3.10

ARG PROJECT_DIR="/opt/airflow"

ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH="$PYTHONPATH:$PROJECT_DIR"
ENV AIRFLOW_HOME=$PROJECT_DIR

USER root

# This is a temporary workaround for MySQL invalid GPG Key errors
# raised when attempting to update and install OS apps.
# The fix removed mysql repo from the app sources. In our case
# this is okay since we use Postgres.
# Related Github issue: https://github.com/apache/airflow/issues/36231
# Temporary fixed grabbed from this specific comment:
# https://github.com/apache/airflow/issues/36231#issuecomment-1857798655
RUN rm /etc/apt/sources.list.d/mysql.list

RUN apt-get update \
  && apt-get install -y --no-install-recommends build-essential

# Legacy docker image dependencies to be reviewed
RUN apt-get install -y --no-install-recommends \
    lsb-release gnupg curl && \
    CLOUD_SDK_REPO="cloud-sdk-$(lsb_release -c -s)" && \
    echo "deb http://packages.cloud.google.com/apt $CLOUD_SDK_REPO main" | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list && \
    curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key add - && \
    apt-get update -y && apt-get install google-cloud-sdk -y && apt-get install google-cloud-sdk-gke-gcloud-auth-plugin && \
    apt-get remove -y lsb-release gnupg

RUN apt-get autoremove -yqq --purge && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

USER airflow

COPY requirements.txt /
RUN pip install --no-cache-dir -r /requirements.txt

WORKDIR $PROJECT_DIR

# deploylib expects /app/version.json, copy the file if it exists
COPY *version.json /app/version.json

COPY . .
