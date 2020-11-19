import datetime

from airflow import DAG
from utils.gcp import gke_command

default_args = {
    "owner": "bewu@mozilla.com",
    "start_date": datetime.datetime(2020, 10, 31, 0, 0),
    "email": ["bewu@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=1800),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
}

GA_PROPERTIES = [
    ("65789850", "mozilla.org"),
    ("66602784", "blog.mozilla.org"),
    ("65912487", "support.mozilla.org"),
    ("180612539", "monitor.firefox.com"),
    ("220432379", "vpn.mozilla.org"),
    ("65887927", "hacks.mozilla.org"),
    ("66726481", "developer.mozilla.org"),
]

with DAG(
    "copy_ga_sessions",
    default_args=default_args,
    schedule_interval="0 1 * * *",
) as dag:
    for property_id, property_name in GA_PROPERTIES:
        commands = [
            "--start-date", "{{ macros.ds_add(ds, -2) }}",
            "--src-project", "ga-mozilla-org-prod-001",
            "--dst-project", "moz-fx-data-marketing-prod",
            "--overwrite",
            property_id
          ]

        copy_ga_sessions = gke_command(
            task_id=f"copy_ga_sessions_{property_name}",
            command=commands,
            docker_image="mozilla/bigquery-etl:latest",
            gcp_conn_id="google_cloud_derived_datasets",
            dag=dag,
        )
