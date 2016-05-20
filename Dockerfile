# AUTHOR: Roberto Vitillo
# DESCRIPTION: Mozilla's Airflow container
# BUILD: docker build --rm -t vitillo/telemetry-airflow
# SOURCE: https://github.com/vitillo/telemetry-airflow

FROM puckel/docker-airflow
MAINTAINER vitillo

USER root
RUN apt-get update -yqq && \
    apt-get install -yqq python-pip && \
    pip install boto3

ADD ansible/files/airflow/airflow.cfg ${AIRFLOW_HOME}/airflow.cfg
ADD ansible/files/airflow/entrypoint.sh ${AIRFLOW_HOME}/entrypoint.sh

USER airflow
ADD dags/ /usr/local/airflow/dags/
# ENTRYPOINT ["/bin/sh", "-c"]