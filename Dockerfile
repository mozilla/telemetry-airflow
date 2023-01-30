FROM apache/airflow:slim-2.3.3-python3.8

ARG PROJECT_DIR="/opt/airflow"

ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH="$PYTHONPATH:$PROJECT_DIR"
ENV AIRFLOW_HOME=$PROJECT_DIR

USER root
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         build-essential default-libmysqlclient-dev \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

USER airflow

COPY requirements.txt /
RUN pip install --no-cache-dir -r /requirements.txt

WORKDIR $PROJECT_DIR
COPY . .
