from airflow import DAG
from airflow.models import Variable
from datetime import datetime, timedelta
from utils import leanplum


default_args = {
    'owner': 'frank@mozilla.com',
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

    fennec_nightly_export = leanplum.export(
        task_id='fennec_nightly_export',
        leanplum_app_id=Variable.get('LEANPLUM_FENNEC_NIGHTLY_APP_ID'),
        leanplum_client_key=Variable.get('LEANPLUM_FENNEC_NIGHTLY_CLIENT_KEY'),
        bq_project='moz-fx-data-shared-prod',
        gcs_prefix='firefox/android/nightly',
        bq_dataset_id='firefox_android_nightly_external',
        table_prefix='leanplum',
        dag=dag
    )

    fennec_beta_export = leanplum.export(
        task_id='fennec_beta_export',
        leanplum_app_id=Variable.get('LEANPLUM_FENNEC_BETA_APP_ID'),
        leanplum_client_key=Variable.get('LEANPLUM_FENNEC_BETA_CLIENT_KEY'),
        bq_project='moz-fx-data-shared-prod',
        gcs_prefix='firefox/android/beta',
        bq_dataset_id='firefox_android_beta_external',
        table_prefix='leanplum',
        dag=dag
    )

    fennec_release_export = leanplum.export(
        task_id='fennec_release_export',
        leanplum_app_id=Variable.get('LEANPLUM_FENNEC_RELEASE_APP_ID'),
        leanplum_client_key=Variable.get('LEANPLUM_FENNEC_RELEASE_CLIENT_KEY'),
        bq_project='moz-fx-data-shared-prod',
        gcs_prefix='firefox/android/release',
        bq_dataset_id='firefox_android_release_external',
        table_prefix='leanplum',
        dag=dag
    )

    firefox_preview_export = leanplum.export(
        task_id='firefox_preview_export',
        leanplum_app_id=Variable.get('LEANPLUM_FIREFOX_PREVIEW_APP_ID'),
        leanplum_client_key=Variable.get('LEANPLUM_FIREFOX_PREVIEW_CLIENT_KEY'),
        bq_project='moz-fx-data-shared-prod',
        gcs_prefix='firefox-preview/android/release',
        bq_dataset_id='firefox_preview_external',
        table_prefix='leanplum',
        dag=dag
    )

    firefox_ios_release_export = leanplum.export(
        task_id='firefox_ios_release_export',
        leanplum_app_id=Variable.get('LEANPLUM_FIREFOX_IOS_RELEASE_APP_ID'),
        leanplum_client_key=Variable.get('LEANPLUM_FIREFOX_IOS_RELEASE_CLIENT_KEY'),
        bq_project='moz-fx-data-shared-prod',
        gcs_prefix='firefox/ios/release',
        bq_dataset_id='firefox_ios_release_external',
        table_prefix='leanplum',
        dag=dag
    )
