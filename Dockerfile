FROM python:2-slim
MAINTAINER Mozilla Telemetry

ENV PYTHONUNBUFFERED 1

ENV AWS_REGION us-west-2
# ENV AWS_ACCESS_KEY_ID
# ENV AWS_SECRET_ACCESS_KEY

ENV SPARK_BUCKET telemetry-spark-emr-2
ENV AIRFLOW_BUCKET telemetry-test-bucket
ENV PRIVATE_OUTPUT_BUCKET telemetry-test-bucket
ENV PUBLIC_OUTPUT_BUCKET telemetry-test-bucket

ENV EMR_KEY_NAME mozilla_vitillo
ENV EMR_FLOW_ROLE telemetry-spark-cloudformation-TelemetrySparkInstanceProfile-1SATUBVEXG7E3
ENV EMR_SERVICE_ROLE EMR_DefaultRole
ENV EMR_INSTANCE_TYPE c3.4xlarge

ENV AIRFLOW_HOME /app
ENV AIRFLOW_AUTHENTICATE False
ENV AIRFLOW_BROKER_URL redis://redis:6379/0
ENV AIRFLOW_RESULT_URL ${AIRFLOW_BROKER_URL}
ENV AIRFLOW_FLOWER_PORT 5555
ENV AIRFLOW_DATABASE_URL postgres://postgres@db/postgres
ENV AIRFLOW_FERNET_KEY "VDRN7HAYDw36BTFbLPibEgJ7q2Dzn-dVGwtbu8iKwUg"
ENV AIRFLOW_SECRET_KEY "3Pxs_qg7J6TvRuAIIpu4E2EK_8sHlOYsxZbB-o82mcg"
# ENV AIRFLOW_SMTP_HOST
# ENV AIRFLOW_SMTP_USER
# ENV AIRFLOW_SMTP_PASSWORD
ENV AIRFLOW_SMTP_FROM telemetry-alerts@airflow.dev.mozaws.net

ENV PORT 8000

EXPOSE 8000

# add a non-privileged user for installing and running the application
RUN mkdir /app && \
    chown 10001:10001 /app && \
    chmod g+w /app && \
    groupadd --gid 10001 app && \
    useradd --uid 10001 --gid 10001 --home /app app

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        apt-transport-https build-essential curl git libpq-dev \
        postgresql-client gettext sqlite3 libffi-dev libsasl2-dev && \
    apt-get autoremove -y && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

# Switch to /tmp to install dependencies outside home dir
WORKDIR /tmp

# Install Python dependencies
COPY requirements.txt /tmp/
RUN pip install --upgrade --no-cache-dir -r requirements.txt

# Switch back to home directory
WORKDIR /app

COPY . /app

RUN chown -R 10001:10001 /app && \
    chmod -R g+w /app

USER 10001
