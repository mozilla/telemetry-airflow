FROM python:2-slim
MAINTAINER Jannis Leidel <jezdez@mozilla.com>

# add a non-privileged user for installing and running the application
RUN mkdir /app && \
    chown 10001:10001 /app && \
    groupadd --gid 10001 app && \
    useradd --no-create-home --uid 10001 --gid 10001 --home-dir /app app

# python-slim base image has missing directories required for psql install
RUN mkdir -p /usr/share/man/man1
RUN mkdir -p /usr/share/man/man7

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        apt-transport-https build-essential curl git libpq-dev \
        postgresql-client gettext sqlite3 libffi-dev libsasl2-dev && \
    apt-get autoremove -y && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

# Install Python dependencies
COPY requirements.txt /tmp/
# Switch to /tmp to install dependencies outside home dir
WORKDIR /tmp

RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

# Switch back to home directory
WORKDIR /app

COPY . /app

RUN chown -R 10001:10001 /app

USER 10001

ENV PYTHONUNBUFFERED=1 \
    # AWS_REGION= \
    # AWS_ACCESS_KEY_ID= \
    # AWS_SECRET_ACCESS_KEY= \
    # SPARK_BUCKET= \
    # AIRFLOW_BUCKET= \
    # PRIVATE_OUTPUT_BUCKET= \
    # PUBLIC_OUTPUT_BUCKET= \
    # EMR_KEY_NAME= \
    # EMR_FLOW_ROLE= \
    # EMR_SERVICE_ROLE= \
    # EMR_INSTANCE_TYPE= \
    # DEPLOY_ENVIRONMENT = \
    # DEPLOY_TAG = \
    # ARTIFACTS_BUCKET = \
    # DATABRICKS_DEFAULT_IAM \
    PORT=8000

ENV AIRFLOW_HOME=/app \
    # AIRFLOW_AUTHENTICATE= \
    # AIRFLOW_AUTH_BACKEND= \
    # AIRFLOW_BROKER_URL= \
    # AIRFLOW_RESULT_URL= \
    # AIRFLOW_FLOWER_PORT= \
    # AIRFLOW_DATABASE_URL= \
    # AIRFLOW__CORE__FERNET_KEY= \
    # AIRFLOW_SECRET_KEY= \
    # AIRFLOW_SMTP_HOST= \
    # AIRFLOW_SMTP_USER= \
    # AIRFLOW_SMTP_PASSWORD= \
    # AIRFLOW_SMTP_FROM= \
    AIRFLOW_EMAIL_BACKEND="airflow.utils.email.send_email_smtp"

EXPOSE $PORT

# Using /bin/bash as the entrypoint works around some volume mount issues on Windows
# where volume-mounted files do not have execute bits set.
# https://github.com/docker/compose/issues/2301#issuecomment-154450785 has additional background.
ENTRYPOINT ["/bin/bash", "/app/bin/run"]

CMD ["web"]
