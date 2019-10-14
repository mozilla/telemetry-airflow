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

    app_id = Variable.get('LEANPLUM_FENNEC_NIGHTLY_APP_ID')
    client_key = Variable.get('LEANPLUM_FENNEC_NIGHTLY_CLIENT_KEY')

    fennec_nightly_export = leanplum.export(
        task_id='fennec_nightly_export',
        leanplum_app_id=app_id,
        leanplum_client_key=client_key,
        gcs_bucket='moz-fx-data-leanplum-export',
        gcs_prefix='firefox-android-dev',
        bq_dataset_id='firefox_android_dev_leanplum',
        dag=dag
    )
