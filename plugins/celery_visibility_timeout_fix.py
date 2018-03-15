from airflow.executors.celery_executor import app

# We need to override visibility_timeout in a redis-backed celery queue since the default
# is one hour, meaning sensors are marked as failed after one hour
app.conf.BROKER_TRANSPORT_OPTIONS = {'visibility_timeout': 10 * 60 * 60}
