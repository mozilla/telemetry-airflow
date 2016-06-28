#!/usr/bin/env bash

CMD="airflow"
TRY_LOOP="10"
MYSQL_HOST=${DB_URI:-mysql}
MYSQL_PORT="3306"
RABBITMQ_HOST="rabbitmq"
RABBITMQ_CREDS="airflow:airflow"
FERNET_KEY=$(python -c "from cryptography.fernet import Fernet; FERNET_KEY = Fernet.generate_key().decode(); print FERNET_KEY")

# Generate Fernet key
sed -i "s/{FERNET_KEY}/${FERNET_KEY}/" $AIRFLOW_HOME/airflow.cfg

# Wait for RabbitMQ
j=0
while ! curl -sI -u $RABBITMQ_CREDS http://$RABBITMQ_HOST:15672/api/whoami |grep '200 OK'; do
  j=`expr $j + 1`
  if [ $j -ge $TRY_LOOP ]; then
    echo "$(date) - $RABBITMQ_HOST still not reachable, giving up"
    exit 1
  fi
  echo "$(date) - waiting for RabbitMQ... $j/$TRY_LOOP"
  sleep 5
done

sed -i "s~{DB_URI}~${DB_URI}~" $AIRFLOW_HOME/airflow.cfg
sed -i "s~{DB_USER}~${DB_USER}~" $AIRFLOW_HOME/airflow.cfg
sed -i "s~{DB_PASSWORD}~${DB_PASSWORD}~" $AIRFLOW_HOME/airflow.cfg
sed -i "s~{AIRFLOW_ENABLE_AUTH}~${AIRFLOW_ENABLE_AUTH}~" $AIRFLOW_HOME/airflow.cfg
sed -i "s~{SMTP_HOST}~${SMTP_HOST}~" $AIRFLOW_HOME/airflow.cfg
sed -i "s~{SMTP_USER}~${SMTP_USER}~" $AIRFLOW_HOME/airflow.cfg
sed -i "s~{SMTP_PASSWORD}~${SMTP_PASSWORD}~" $AIRFLOW_HOME/airflow.cfg

i=0
while ! nc $MYSQL_HOST $MYSQL_PORT >/dev/null 2>&1 < /dev/null; do
  i=`expr $i + 1`
  if [ $i -ge $TRY_LOOP ]; then
    echo "$(date) - ${MYSQL_HOST}:${MYSQL_PORT} still not reachable, giving up"
    exit 1
  fi
  echo "$(date) - waiting for ${MYSQL_HOST}:${MYSQL_PORT}... $i/$TRY_LOOP"
  sleep 5
done

if [ "$1" = "webserver" ]; then
  echo "Initialize database..."
  $CMD initdb
  $CMD upgradedb
fi

eval $CMD "${@:-$COMMAND}"
