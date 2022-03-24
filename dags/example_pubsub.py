from airflow import DAG
from datetime import datetime, timedelta
from utils.tags import Tag

from airflow.providers.google.cloud.operators.pubsub import PubSubPublishMessageOperator

default_args = {
    "owner": "hwoo@mozilla.com",
    "email": ["hwoo@mozilla.com"],
    "depends_on_past": False,
    "start_date": datetime(2022, 3, 23),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
    "retry_delay": timedelta(minutes=30),
}

m1 = {'data': b'Hello, World!',
      'attributes': {'type': 'greeting'}
     }
m2 = {'data': b'Knock, knock'}
m3 = {'attributes': {'foo': ''}}

tags = [Tag.ImpactTier.tier_3]
dag = DAG("example_pubsub", default_args=default_args, schedule_interval="@daily", tags=tags,)

publish_task = PubSubPublishMessageOperator(
    task_id="publish_to_pubsub",
    project_id="moz-fx-data-airflow-prod-88e0",
    topic="airflow-glam-triggers",
    gcp_conn_id="google_cloud_airflow_pubsub",
    messages=[m1, m2, m3],
    dag=dag,
)
