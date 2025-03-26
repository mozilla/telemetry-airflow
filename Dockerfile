FROM apache/airflow:slim-2.10.5-python3.11

ARG PROJECT_DIR="/opt/airflow"

ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH="$PYTHONPATH:$PROJECT_DIR"
ENV AIRFLOW_HOME=$PROJECT_DIR

USER root

RUN apt-get update \
  && apt-get install -y --no-install-recommends build-essential

# Legacy docker image dependencies to be reviewed
RUN apt-get install -y --no-install-recommends \
    lsb-release gnupg curl && \
    CLOUD_SDK_REPO="cloud-sdk-$(lsb_release -c -s)" && \
    echo "deb https://packages.cloud.google.com/apt $CLOUD_SDK_REPO main" | tee -a /etc/apt/sources.list.d/google-cloud-cli.list && \
    curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key add - && \
    apt-get update -y && apt-get install google-cloud-cli -y && apt-get install google-cloud-cli-gke-gcloud-auth-plugin && \
    apt-get remove -y lsb-release gnupg

RUN apt-get autoremove -yqq --purge && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

USER airflow

COPY requirements.txt /
RUN pip install --no-cache-dir -r /requirements.txt
COPY requirements-override.txt /
RUN pip install --no-cache-dir -r /requirements-override.txt --upgrade

WORKDIR $PROJECT_DIR

COPY . .
