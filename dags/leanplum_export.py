from airflow import DAG
from datetime import datetime, timedelta
from utils import leanplum


default_args = {
    'owner': 'frank@mozilla.com',
    "email": [
        "bewu@mozilla.com",
        "frank@mozilla.com",
    ],
    'depends_on_past': False,
    'start_date': datetime(2019, 10, 10),
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=30),
}


with DAG('leanplum_export',
         default_args=default_args,
         schedule_interval='@daily') as dag:

    fennec_release_export = leanplum.export(
        task_id='fennec_release_export',
        bq_project='moz-fx-data-shared-prod',
        s3_prefix='firefox_android',
        bq_dataset_id='firefox_android_release_external',
        table_prefix='leanplum',
        version='2',
        dag=dag
    )

    firefox_ios_release_export = leanplum.export(
        task_id='firefox_ios_release_export',
        bq_project='moz-fx-data-shared-prod',
        s3_prefix='firefox_ios',
        bq_dataset_id='firefox_ios_release_external',
        table_prefix='leanplum',
        version='2',
        dag=dag
    )

    firefox_android_beta_export = leanplum.export(
        task_id='firefox_android_beta_export',
        bq_project='moz-fx-data-shared-prod',
        s3_prefix='firefox_android_beta',
        bq_dataset_id='firefox_android_beta_external',
        table_prefix='leanplum',
        version='2',
        dag=dag
    )

    firefox_android_nightly_export = leanplum.export(
        task_id='firefox_android_nightly_export',
        bq_project='moz-fx-data-shared-prod',
        s3_prefix='firefox_android_nightly',
        bq_dataset_id='firefox_android_nightly_external',
        table_prefix='leanplum',
        version='2',
        dag=dag
    )
