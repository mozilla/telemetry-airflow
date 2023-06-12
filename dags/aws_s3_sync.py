from airflow import DAG
from datetime import timedelta, datetime
from operators.gcp_container_operator import GKEPodOperator
from utils.tags import Tag

DOCS = """
Temporary DAG to synchronize S3 with GCS for CDN backing https://analysis-output.telemetry.mozilla.org
See https://mozilla-hub.atlassian.net/browse/DSRE-951 for details.
"""

default_args = {
    "owner": "whd@mozilla.com",
    "depends_on_past": False,
    "start_date": datetime(2023, 6, 12),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
    "retry_delay": timedelta(minutes=30),
}

tags = [Tag.ImpactTier.tier_3]

prefixes = ["public-data-report"]

with DAG(
    "aws_s3_sync",
    doc_md=DOCS,
    default_args=default_args,
    schedule_interval="@daily",
    tags=tags,
) as dag:

    aws_conn_id = "aws_dev_telemetry_public_analysis_2_rw"
    aws_access_key, aws_secret_key, _ = AwsBaseHook(
        aws_conn_id=aws_conn_id, client_type="s3"
    ).get_credentials()

    docker_image = "google/cloud-sdk:slim"
    s3_bucket = "telemetry-public-analysis-2"
    # stage for testing
    gcs_bucket = "moz-fx-data-static-websit-f7e0-analysis-output"

    for prefix in prefixes:
        noslash = prefix.replace("/", "-")
        GKEPodOperator(
            task_id=f"aws_s3_sync_{noslash.replace('-', '_')}",
            name=f"aws-s3-sync-{noslash}",
            image=docker_image,
            arguments=[
                "/google-cloud-sdk/bin/gsutil",
                "-m",
                "rsync",
                "-d",
                "-r",
                f"s3://{s3_bucket}/{prefix}/",
                f"gs://{gcs_bucket}/{prefix}/",
            ],
            env_vars={
                "AWS_ACCESS_KEY_ID": aws_access_key,
                "AWS_SECRET_ACCESS_KEY": aws_secret_key,
            },
            dag=dag,
        )
