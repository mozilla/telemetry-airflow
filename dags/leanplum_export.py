from airflow import DAG
from datetime import datetime, timedelta
from utils import leanplum


default_args = {
    "owner": "frank@mozilla.com",
    "email": [
        "bewu@mozilla.com",
        "frank@mozilla.com",
    ],
    "depends_on_past": False,
    "start_date": datetime(2019, 10, 10),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
    "retry_delay": timedelta(minutes=30),
}

# tuples of (app_name, s3_prefix)
LEANPLUM_APPS = [
    (
        "firefox_android_release",
        "firefox_android",
        "{{ var.value.LEANPLUM_FIREFOX_ANDROID_RELEASE_APP_ID }}",
        "{{ var.value.LEANPLUM_FIREFOX_ANDROID_RELEASE_CLIENT_KEY }}"
    ),
    (
        "firefox_android_beta",
        "firefox_android_beta",
        "{{ var.value.LEANPLUM_FIREFOX_ANDROID_BETA_APP_ID }}",
        "{{ var.value.LEANPLUM_FIREFOX_ANDROID_BETA_CLIENT_KEY }}"
    ),
    (
        "firefox_android_nightly",
        "firefox_android_nightly",
        "{{ var.value.LEANPLUM_FIREFOX_ANDROID_NIGHTLY_APP_ID }}",
        "{{ var.value.LEANPLUM_FIREFOX_ANDROID_NIGHTLY_CLIENT_KEY }}"
    ),
    (
        "firefox_ios_release",
        "firefox_ios",
        "{{ var.value.LEANPLUM_FIREFOX_IOS_RELEASE_APP_ID }}",
        "{{ var.value.LEANPLUM_FIREFOX_IOS_RELEASE_CLIENT_KEY }}"
    ),
]

with DAG(
        "leanplum_export",
        default_args=default_args,
        schedule_interval="0 2/4 * * *",
        max_active_runs=1,
) as dag:

    for app_name, s3_bucket, app_id_var, client_key_var in LEANPLUM_APPS:
        data_export = leanplum.export(
            task_id=f"{app_name}_export",
            bq_project="moz-fx-data-shared-prod",
            s3_prefix=s3_bucket,
            bq_dataset_id=f"{app_name}_external",
            table_prefix="leanplum",
            version="2",
            dag=dag,
        )

        message_export = leanplum.get_messages(
            task_id=f"{app_name}_messages",
            app_id=app_id_var,
            client_key=client_key_var,
            bq_project="moz-fx-data-shared-prod",
            bq_dataset_id=f"{app_name}_external",
            table_prefix="leanplum",
            version="2",
            dag=dag,
        )

        message_export.set_upstream(data_export)
