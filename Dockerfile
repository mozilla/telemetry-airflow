# AUTHOR: Roberto Vitillo
# DESCRIPTION: Mozilla's Airflow container
# BUILD: docker build --rm -t mozdata/telemetry-airflow
# SOURCE: https://github.com/mozilla/telemetry-airflow

FROM puckel/docker-airflow:1.7.1.3
MAINTAINER vitillo

USER root
RUN apt-get update -yqq && \
    apt-get install -yqq python-pip python-mysqldb && \
    pip install boto3 && \
    pip install airflow[async] && \
    pip install airflow[password] && \
    pip install retrying

ADD ansible/files/airflow/airflow.cfg ${AIRFLOW_HOME}/airflow.cfg
ADD ansible/files/airflow/entrypoint.sh ${AIRFLOW_HOME}/entrypoint.sh
ADD ansible/files/airflow/replace_env.py ${AIRFLOW_HOME}/replace_env.py
RUN chown airflow:airflow ${AIRFLOW_HOME}/airflow.cfg

USER airflow
ADD dags/ /usr/local/airflow/dags/
