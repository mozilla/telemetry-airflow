#!/usr/bin/env bash

CMD="airflow"
TRY_LOOP="10"
MYSQL_HOST=${DB_URI:-mysql}
MYSQL_PORT="3306"
RABBITMQ_HOST="rabbitmq"
RABBITMQ_CREDS="airflow:airflow"

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

# Generate Fernet key for replacement below
export FERNET_KEY=$(python -c "from cryptography.fernet import Fernet; FERNET_KEY = Fernet.generate_key().decode(); print FERNET_KEY")

# Replace environment vars in airflow config file.
python $AIRFLOW_HOME/replace_env.py $AIRFLOW_HOME/airflow.cfg

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

sleep 5

if [[ "$COMMAND" == "scheduler"* ]]; then
  # Work around scheduler hangs, see bug 1286825.
  # Run the scheduler inside a retry loop.
  while echo "Running"; do
    eval $CMD "${@:-$COMMAND}"
    echo "Scheduler exited with code $?.  Respawning.." >&2
    date >> /tmp/airflow_scheduler_errors.txt
    sleep 1
  done
else
  eval $CMD "${@:-$COMMAND}"
fi
