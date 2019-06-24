from datetime import datetime, timedelta

from airflow import DAG

from airflow.contrib.hooks.gcp_api_base_hook import GoogleCloudBaseHook

from operators.gcp_container_operator import GKEPodOperator



DEFAULT_ARGS = {
    'owner': 'hwoo@mozilla.com',
    'depends_on_past': False,
    'start_date': datetime(2019, 4, 17),
    'end_date': datetime(2019, 4, 17),
    'email': ['hwoo@mozilla.com', 'dataops+alerts@mozilla.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=15),
}

gcp_conn_id = "google_cloud_derived_datasets"
connection = GoogleCloudBaseHook(gcp_conn_id=gcp_conn_id)

blp_dag = DAG(
    'adi_dim_backfill',
    default_args=DEFAULT_ARGS,
    dagrun_timeout=timedelta(hours=1),
    schedule_interval='0 0 * * *'
)

load_args = [
    'bq',
    '--location=US',
    'load',
    '--source_format=CSV',
    '--skip_leading_rows=1',
    '--replace',
    '--field_delimiter=,',
    'blpadi.adi_dim_backfill${{ ds_nodash }}',
    'gs://dp2-stage-vertica/copy_adi_dimensional_by_date/{{ ds }}.csv',
]

insert_args = [
    'bq',
    '--location=US',
    'query',
    '--replace',
    '--destination_table',
    'moz-fx-data-derived-datasets:blpadi.adi_dimensional_by_date${{ ds_nodash }}',
    '--use_legacy_sql=false',
    "select tot_requests_on_date, _year_quarter, bl_date, product, v_prod_major, prod_os, v_prod_os, channel, locale, continent_code, cntry_code, distro_name, distro_version from blpadi.adi_dim_backfill where bl_date = '{{ ds }}'",
]

load_bq_to_tmp_tbl = GKEPodOperator(
    task_id='bq_load_tmp_tbl',
    gcp_conn_id=gcp_conn_id,
    project_id=connection.project_id,
    location='us-central1-a',
    cluster_name='bq-load-gke-1',
    name='bq-load-tmp-tbl',
    namespace='default',
    image='google/cloud-sdk:242.0.0-alpine',
    arguments=load_args,
    dag=blp_dag
)

select_insert_into_final_table = GKEPodOperator(
    task_id='bigquery_insert_final_table',
    gcp_conn_id=gcp_conn_id,
    project_id='moz-fx-data-derived-datasets',
    location='us-central1-a',
    cluster_name='bq-load-gke-1',
    name='bq-query-insert-final-tbl',
    namespace='default',
    image='google/cloud-sdk:242.0.0-alpine',
    arguments=insert_args,
    dag=blp_dag
)

load_bq_to_tmp_tbl.set_downstream(select_insert_into_final_table)
