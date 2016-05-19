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

USER airflow
ADD dags/ /usr/local/airflow/dags/
# ENTRYPOINT ["/bin/sh", "-c"]