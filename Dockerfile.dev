FROM python:2.7-slim
MAINTAINER Jannis Leidel <jezdez@mozilla.com>

# add a non-privileged user for installing and running the application
RUN mkdir /app && \
    chown 10001:10001 /app && \
    groupadd --gid 10001 app && \
    useradd --no-create-home --uid 10001 --gid 10001 --home-dir /app app

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        apt-transport-https build-essential curl git libpq-dev python-dev \
        default-libmysqlclient-dev gettext sqlite3 libffi-dev libsasl2-dev \
        lsb-release gnupg vim && \
    CLOUD_SDK_REPO="cloud-sdk-$(lsb_release -c -s)" && \
    echo "deb http://packages.cloud.google.com/apt $CLOUD_SDK_REPO main" | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list && \
    curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key add - && \
    apt-get update -y && apt-get install google-cloud-sdk -y && \
    apt-get remove -y lsb-release gnupg && \
    apt-get autoremove -y && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

# Install Python dependencies
COPY requirements.txt /tmp/
# Switch to /tmp to install dependencies outside home dir
WORKDIR /tmp

RUN pip install --upgrade pip
RUN export SLUGIFY_USES_TEXT_UNIDECODE=yes && pip install --no-cache-dir -r requirements.txt

# Switch back to home directory
WORKDIR /app

USER 10001

ENV PYTHONUNBUFFERED=1 \
    # DEV_USERNAME= \
    AWS_REGION=us-west-2 \
    # AWS_ACCESS_KEY_ID= \
    # AWS_SECRET_ACCESS_KEY= \
    SPARK_BUCKET=telemetry-spark-emr-2 \
    AIRFLOW_BUCKET=telemetry-airflow \
    PRIVATE_OUTPUT_BUCKET=telemetry-test-bucket \
    PUBLIC_OUTPUT_BUCKET=telemetry-test-bucket \
    EMR_KEY_NAME=20161025-dataops-dev \
    EMR_FLOW_ROLE=telemetry-spark-cloudformation-TelemetrySparkInstanceProfile-1SATUBVEXG7E3 \
    EMR_SERVICE_ROLE=EMR_DevRole \
    EMR_INSTANCE_TYPE=c3.4xlarge \
    PORT=8000 \
    DEPLOY_ENVIRONMENT=dev \
    DEVELOPMENT=1 \
    DEPLOY_TAG=master \
    ARTIFACTS_BUCKET=net-mozaws-data-us-west-2-ops-ci-artifacts \
    DATABRICKS_DEFAULT_IAM=arn:aws:iam::144996185633:instance-profile/databricks-ec2

# Airflow configuration can be set here using the following format:
# $AIRFLOW__{SECTION}__{KEY}
# See also: https://airflow.apache.org/configuration.html

ENV AIRFLOW_HOME=/app \
    AIRFLOW_AUTHENTICATE=False \
    AIRFLOW_AUTH_BACKEND=airflow.contrib.auth.backends.password_auth \
    AIRFLOW_BROKER_URL=redis://redis:6379/0 \
    AIRFLOW_RESULT_URL=redis://redis:6379/0 \
    AIRFLOW_FLOWER_PORT="5555" \
    AIRFLOW_DATABASE_URL=mysql://root:secret@db:3306/airflow \
    AIRFLOW__CORE__FERNET_KEY="0000000000000000000000000000000000000000000=" \
    AIRFLOW_SECRET_KEY="000000000000000000000000000000000000000000=" \
    # AIRFLOW_SMTP_HOST= \
    # AIRFLOW_SMTP_USER= \
    # AIRFLOW_SMTP_PASSWORD= \
    AIRFLOW_SMTP_FROM=telemetry-alerts@airflow.dev.mozaws.net \
    AIRFLOW__SCHEDULER__CATCHUP_BY_DEFAULT=False \
    AIRFLOW_EMAIL_BACKEND="airflow.macros.log_email_backend.log_email_backend" \
    URL="http://localhost:8000" \
    WEBSERVER_USE_RBAC="False"

EXPOSE $PORT

# Using /bin/bash as the entrypoint works around some volume mount issues on Windows
# where volume-mounted files do not have execute bits set.
# https://github.com/docker/compose/issues/2301#issuecomment-154450785 has additional background.
ENTRYPOINT ["/bin/bash", "/app/bin/run"]

CMD ["web"]
