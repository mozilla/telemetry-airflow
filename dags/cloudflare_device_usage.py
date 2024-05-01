#Load libraries
import requests
import json
import pandas as pd
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow.providers.google.cloud.operators.gcs import GCSDeleteObjectsOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from utils.tags import Tag

#Get the auth token from an Airflow variable
auth_token = Variable.get('cloudflare_auth_token')

#Define DOC string
DOCS = """Pulls device usage data from the Cloudflare API; Owner: kwindau@mozilla.com
Note: Each run pulls data for the date 3 days prior"""

default_args = {
    "owner": "kwindau@mozilla.com",
    "email": ["kwindau@mozilla.com"],
    "depends_on_past": False,
    "start_date": datetime(2024, 4, 21),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
    "retry_delay": timedelta(minutes=30)
}

TAGS = [Tag.ImpactTier.tier_3, Tag.Repo.airflow]

#Configurations
device_usg_configs = {"timeout_limit": 2000,
                    "locations": ["ALL","BE","BG","CA","CZ","DE","DK","EE","ES","FI","FR",
                                      "GB","HR","IE","IT","CY","LV","LT","LU","HU",
                                      "MT","MX","NL","AT","PL","PT","RO","SI","SK","US","SE","GR"],
                    "bucket": "gs://moz-fx-data-prod-external-data/",
                    "results_stg_gcs_fpth": "gs://moz-fx-data-prod-external-data/cloudflare/device_usage/RESULTS_STAGING/%s_results.csv",
                    "results_archive_gcs_fpath": "gs://moz-fx-data-prod-external-data/cloudflare/device_usage/RESULTS_ARCHIVE/%s_results.csv",
                    "errors_stg_gcs_fpth": "gs://moz-fx-data-prod-external-data/cloudflare/device_usage/ERRORS_STAGING/%s_errors.csv",
                    "errors_archive_gcs_fpath": "gs://moz-fx-data-prod-external-data/cloudflare/device_usage/ERRORS_ARCHIVE/%s_errors.csv",
                    "gcp_conn_id": "google_cloud_gke_sandbox"}



def generate_device_type_timeseries_api_call(strt_dt, end_dt, agg_int, location):
    """ Inputs: Start Date in YYYY-MM-DD format, End Date in YYYY-MM-DD format, and desired agg_interval; Returns API URL """
    if location == 'ALL':
        device_usage_api_url = "https://api.cloudflare.com/client/v4/radar/http/timeseries/device_type?name=human&botClass=LIKELY_HUMAN&dateStart=%sT00:00:00.000Z&dateEnd=%sT00:00:00.000Z&name=bot&botClass=LIKELY_AUTOMATED&dateStart=%sT00:00:00.000Z&dateEnd=%sT00:00:00.000Z&format=json&aggInterval=%s" % (strt_dt, end_dt, strt_dt, end_dt, agg_int)
    else:
        device_usage_api_url = "https://api.cloudflare.com/client/v4/radar/http/timeseries/device_type?name=human&botClass=LIKELY_HUMAN&dateStart=%sT00:00:00.000Z&dateEnd=%sT00:00:00.000Z&location=%s&name=bot&botClass=LIKELY_AUTOMATED&dateStart=%sT00:00:00.000Z&dateEnd=%sT00:00:00.000Z&location=%s&format=json&aggInterval=%s" % (strt_dt, end_dt, location, strt_dt, end_dt, location, agg_int)
    return device_usage_api_url


def parse_device_type_timeseries_response_human(result):
    """ Takes the response JSON and returns parsed information"""
    ### HUMAN
    human_timestamps = result['human']['timestamps']
    human_desktop = result['human']['desktop']
    human_mobile = result['human']['mobile']
    human_other = result['human']['other']
    return human_timestamps, human_desktop, human_mobile, human_other


def parse_device_type_timeseries_response_bot(result):
    """ Takes the response JSON and returns parsed information"""
    ### BOT 
    bot_timestamps = result['bot']['timestamps']
    bot_desktop = result['bot']['desktop']
    bot_mobile = result['bot']['mobile']
    bot_other = result['bot']['other']
    return bot_timestamps, bot_desktop, bot_mobile, bot_other


#Generate the result dataframe
def make_device_usage_result_df(user_type, desktop, mobile, other, timestamps, last_upd, norm, conf, agg_interval, location):
    """ User type = string, desktop, mobile, other, timestamps, arrays, """
    return pd.DataFrame({'Timestamp': timestamps,
                'UserType': [user_type] * len(timestamps),
                'Location': [location] * len(timestamps),
                'DesktopUsagePct': desktop,
                'MobileUsagePct': mobile,
                'OtherUsagePct': other,
                'ConfLevel': [conf]* len(timestamps),
                'AggInterval': [agg_interval] * len(timestamps),
                'NormalizationType': [norm] * len(timestamps),
                'LastUpdated': [last_upd] * len(timestamps)
                })

def get_device_usage_data(**kwargs):
    """ Call API and retrieve device usage data and save both errors & results to GCS"""
    #Calculate start date and end date
    logical_dag_dt = kwargs.get('ds')
    logical_dag_dt_as_date = datetime.strptime(logical_dag_dt, '%Y-%m-%d').date()
    start_date = logical_dag_dt_as_date - timedelta(days=4)
    end_date = start_date + timedelta(days=1)
    print('Start Date: ', start_date)
    print('End Date: ', end_date)

    #Configure request headers
    bearer_string = 'Bearer %s' % auth_token
    headers = {'Authorization': bearer_string}

    #Initialize the empty results & errors dataframe



    for loc in device_usg_configs['locations']:
        return 'loc: '+loc





    #LOAD RESULTS & ERRORS TO STAGING GCS
    result_fpath = device_usg_configs["bucket"] + device_usg_configs["results_stg_gcs_fpth"] % start_date
    print('Writing results to: ', result_fpath)

    error_fpath = device_usg_configs["bucket"] + device_usg_configs["errors_stg_gcs_fpth"] % start_date
    print('Writing errors to: ', error_fpath)
#Calculate start date and end date from the DAG run date


#Define DAG
with DAG(
    "cloudflare_device_usage",
    default_args=default_args,
    catchup=False,
    doc_md=DOCS,
    schedule_interval="0 5 * * *",
    tags=TAGS,
) as dag:

    #Define OS usage task
    get_device_usage_data = PythonOperator(task_id="get_device_usage_data",
                                python_callable=get_device_usage_data,
                                execution_timeout=timedelta(minutes=55)) 
    
    #Load the results from GCS to a temporary staging table in BQ (overwritten each run)
    load_results_to_bq_stg = GCSToBigQueryOperator(task_id = "load_results_to_bq_stg",
                                               bucket = device_usg_configs["bucket"],
                                               destination_project_dataset_table = "moz-fx-data-shared-prod.cloudflare_derived.device_results_stg",
                                               source_format = 'CSV',
                                               compression='NONE',
                                               create_disposition="CREATE_IF_NEEDED",
                                               skip_leading_rows=1,
                                               write_disposition = "WRITE_TRUNCATE",
                                               gcp_conn_id = device_usg_configs["gcp_conn_id"],
                                               allow_jagged_rows = False)
    
    #Load the errors from GCS to a temporary staging table in BQ (overwritten each run)
    load_errors_to_bq_stg = GCSToBigQueryOperator(task_id="load_errors_to_bq_stg",
                                                bucket= device_usg_configs["bucket"],
                                               destination_project_dataset_table = "moz-fx-data-shared-prod.cloudflare_derived.device_errors_stg",
                                               source_format = 'CSV',
                                               compression='NONE',
                                               create_disposition="CREATE_IF_NEEDED",
                                               skip_leading_rows=1,
                                               write_disposition="WRITE_TRUNCATE",
                                               gcp_conn_id= device_usg_configs["gcp_conn_id"],
                                               allow_jagged_rows = False)

    #Run a query to process data from staging and insert it into the production gold table
    load_results_to_bq_gold = BigQueryInsertJobOperator(task_id="load_results_to_bq_gold")

    load_errors_to_bq_gold = BigQueryInsertJobOperator(task_id="load_errors_to_bq_gold",
                                                       configuration={
                                                           "query": "load_cf_device_usg_errors_from_stg_to_gld.sql",
                                                            "destinationTable": {'projectId': 'moz-fx-data-shared-prod',
                                                                                 'datasetId': 'cloudflare_derived',
                                                                                 'tableId': 'os_usage_errors_v1'},
                                                            "createDisposition": "CREATE_NEVER",
                                                            "writeDisposition": "WRITE_APPEND"
                                                           },
                                                         project_id="moz-fx-data-shared-prod",
                                                        gcp_conn_id = os_usg_configs["gcp_conn_id"])

    #Archive the result files by moving them out of staging path and into archive path
    archive_results = GCSToGCSOperator(task_id="archive_results",
                                       source_bucket = device_usg_configs["bucket"],
                                       source_object = device_usg_configs["results_stg_gcs_fpth"] % '{{ ds }}', 
                                       destination_bucket = device_usg_configs["bucket"],
                                       destination_object = device_usg_configs["results_archive_gcs_fpath"] % '{{ ds }}',
                                       gcp_conn_id=device_usg_configs["gcp_conn_id"], 
                                       exact_match = True)
    
    #Archive the error files by moving them out of staging path and into archive path
    archive_errors = GCSToGCSOperator(task_id="archive_errors",
                                      source_bucket = device_usg_configs["bucket"],
                                       source_object = device_usg_configs["errors_staging_gcs_fpath"] % '{{ ds }}',
                                       destination_bucket = device_usg_configs["bucket"],
                                       destination_object = device_usg_configs["errors_archive_gcs_fpath"] % '{{ ds }}',
                                       gcp_conn_id=device_usg_configs["gcp_conn_id"],
                                       exact_match = True)
    
    #Delete the result file from the staging path
    del_results_from_gcs_stg = GCSDeleteObjectsOperator(task_id="del_results_from_gcs_stg",
                                                        bucket_name = device_usg_configs["bucket"],
                                                        objects = [ device_usg_configs["results_stg_gcs_fpth"] % '{{ ds }}' ],
                                                        gcp_conn_id=device_usg_configs["gcp_conn_id"])
    
    #Delete the error file from the staging path
    del_errors_from_gcs_stg = GCSDeleteObjectsOperator(task_id="del_errors_from_gcs_stg",
                                                       bucket_name = device_usg_configs["bucket"],
                                                        objects = [ device_usg_configs["errors_stg_gcs_fpth"] % '{{ ds }}' ],
                                                        gcp_conn_id=device_usg_configs["gcp_conn_id"])

    run_device_qa_checks = EmptyOperator(task_id="run_device_qa_checks")

get_device_usage_data >> load_results_to_bq_stg >> load_results_to_bq_gold >> archive_results >> del_results_from_gcs_stg
get_device_usage_data >> load_errors_to_bq_stg >> load_errors_to_bq_gold >> archive_errors >> del_errors_from_gcs_stg
[del_results_from_gcs_stg , del_errors_from_gcs_stg] >> run_device_qa_checks