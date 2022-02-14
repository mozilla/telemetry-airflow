# Use buster image because the default bullseye image has updated coreutils that require a newer
# linux kernel than provided by CircleCI, per
# https://forums.docker.com/t/multiple-projects-stopped-building-on-docker-hub-operation-not-permitted/92570/6
# and https://forums.docker.com/t/multiple-projects-stopped-building-on-docker-hub-operation-not-permitted/92570/11
FROM python:3.7-slim-buster
MAINTAINER Harold Woo <hwoo@mozilla.com>

# Due to AIRFLOW-6854, Python 3.7 is chosen as the base python version.

# add a non-privileged user for installing and running the application
RUN mkdir /app && \
    chown 10001:10001 /app && \
    groupadd --gid 10001 app && \
    useradd --no-create-home --uid 10001 --gid 10001 --home-dir /app app

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        apt-transport-https build-essential curl git libpq-dev python-dev \
        default-libmysqlclient-dev gettext sqlite3 libffi-dev libsasl2-dev \
        lsb-release gnupg vim screen procps default-mysql-client && \
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

COPY . /app

RUN chown -R 10001:10001 /app

USER 10001

ENV PYTHONUNBUFFERED=1 \
    PORT=8000
    # AWS_ACCESS_KEY_ID= \
    # AWS_SECRET_ACCESS_KEY= \
    # DEPLOY_ENVIRONMENT =

ENV AIRFLOW_HOME=/app \
    AIRFLOW_EMAIL_BACKEND="airflow.utils.email.send_email_smtp"
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


EXPOSE $PORT

# Using /bin/bash as the entrypoint works around some volume mount issues on Windows
# where volume-mounted files do not have execute bits set.
# https://github.com/docker/compose/issues/2301#issuecomment-154450785 has additional background.
ENTRYPOINT ["/bin/bash", "/app/bin/run"]

CMD ["web"]
