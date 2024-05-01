#Load libraries
import requests
import json
from utils.tags import Tag
import pandas as pd
from utils.cloudflare import * 
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.models import Variable
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow.providers.google.cloud.operators.gcs import GCSDeleteObjectsOperator

#Get the auth token from an Airflow variable
auth_token = Variable.get('cloudflare_auth_token')

#Define DOC string
DOCS = """
### Pulls OS usage data from the Cloudflare API 

#### Owner
kwindau@mozilla.com
"""

default_args = {
    "owner": "kwindau@mozilla.com",
    "email": ["kwindau@mozilla.com"],
    "depends_on_past": False,
    "start_date": datetime(2024, 4, 21),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
    "retry_delay": timedelta(minutes=30),
}

TAGS = [Tag.ImpactTier.tier_3, Tag.Repo.airflow]

#Configurations
os_usg_configs = {"timeout_limit": 2000,
                 "device_types":  ["DESKTOP", "MOBILE", "OTHER", "ALL"],
                "locations": ["ALL","BE","BG","CA","CZ","DE","DK","EE","ES","FI","FR",
                            "GB","HR","IE","IT","CY","LV","LT","LU","HU","MT","MX",
                            "NL","AT","PL","PT","RO","SI","SK","US","SE","GR"],
                "results_stg_gcs_fpth": "gs://moz-fx-data-prod-external-data/cloudflare/os_usage/RESULTS_STAGING/%s_results.csv",
                "results_archive_gcs_fpth": "gs://moz-fx-data-prod-external-data/cloudflare/os_usage/RESULTS_ARCHIVE/%s_results.csv",
                "errors_stg_gcs_fpth": "gs://moz-fx-data-prod-external-data/cloudflare/os_usage/ERRORS_STAGING/%s_errors.csv",
                "errors_archive_gcs_fpth": "gs://moz-fx-data-prod-external-data/cloudflare/os_usage/ERRORS_ARCHIVE/%s_errors.csv",
                "gcp_conn_id": "google_cloud_gke_sandbox"}

#Configure request headers
bearer_string = 'Bearer %s' % auth_token
headers = {'Authorization': bearer_string}

def generate_os_timeseries_api_call(strt_dt, end_dt, agg_int, location, device_type):
    """ Inputs: Start Date in YYYY-MM-DD format, End Date in YYYY-MM-DD format, and desired agg_interval; Returns API URL """
    if location == 'ALL' and device_type == 'ALL':
        os_usage_api_url = "https://api.cloudflare.com/client/v4/radar/http/timeseries_groups/os?dateStart=%sT00:00:00.000Z&dateEnd=%sT00:00:00.000Z&format=json&aggInterval=%s" % (strt_dt, end_dt, agg_int)
    elif location != 'ALL' and device_type == 'ALL':
        os_usage_api_url = "https://api.cloudflare.com/client/v4/radar/http/timeseries_groups/os?dateStart=%sT00:00:00.000Z&dateEnd=%sT00:00:00.000Z&location=%s&format=json&aggInterval=%s" % (strt_dt, end_dt, location, agg_int)
    elif location == 'ALL' and device_type != 'ALL':
        os_usage_api_url = "https://api.cloudflare.com/client/v4/radar/http/timeseries_groups/os?dateStart=%sT00:00:00.000Z&dateEnd=%sT00:00:00.000Z&deviceType=%s&format=json&aggInterval=%s" % (strt_dt, end_dt, device_type, agg_int)
    else:
        os_usage_api_url = "https://api.cloudflare.com/client/v4/radar/http/timeseries_groups/os?dateStart=%sT00:00:00.000Z&dateEnd=%sT00:00:00.000Z&location=%s&deviceType=%s&format=json&aggInterval=%s" % (strt_dt, end_dt, location, device_type, agg_int)
    return os_usage_api_url


###NOTE - this function should be used in device & OS but not browser
def get_timeseries_api_call_date_ranges(start_date, end_date, max_days_interval): 
    """ Input start date, end date as string in YYYY-MM-DD format, max days interval as int, and returns a dataframe of intervals to use"""

    #Initialize arrays we will later use to make the dataframe
    sd_array = []
    ed_array = []

    #Convert the start date and end date to date objects
    start_dt = datetime.strptime(start_date, '%Y-%m-%d').date()
    end_dt = datetime.strptime(end_date, '%Y-%m-%d').date()

    #Calculate # of days between start date and end date
    days_between_start_dt_and_end_dt = (end_dt - start_dt).days

    #Divide the # of days by how the max # of days you want in each API request
    nbr_iterations = int(days_between_start_dt_and_end_dt/max_days_interval) - 1

    #Initialize the current start date with the start date
    current_start_dt = start_dt

    #For each iteration
    for i in range(nbr_iterations):        
        #Append current start dt 
        sd_array.append(str(current_start_dt))
        #Calculate end date
        current_end_date = current_start_dt + timedelta(days=max_days_interval)
        if current_end_date <= end_dt:
            ed_array.append(str(current_end_date))
        else: 
            ed_array.append(str(end_dt))

        #Update the current start date to be 1 day after the last end date
        current_start_dt = current_end_date + timedelta(days = 1)

    dates_df = pd.DataFrame({"Start_Date": sd_array,
                             "End_Date": ed_array})
    return dates_df

def get_os_usage_data(**kwargs):




    #LOAD RESULTS & ERRORS TO STAGING GCS

#Calculate start date and end date from the DAG run date


#Define DAG
with DAG(
    "cloudflare_os_usage",
    default_args=default_args,
    catchup=False,
    doc_md=DOCS,
    schedule_interval="0 5 * * *",
    tags=TAGS,
) as dag:

    #Define OS usage task
    get_data = EmptyOperator(task_id="get_os_usage_data")

    load_results_to_bq = EmptyOperator(task_id="load_results_to_bq")
    load_errors_to_bq = EmptyOperator(task_id="load_errors_to_bq")

    archive_results = GCSToGCSOperator(task_id="archive_results",
                                       source_bucket = os_usg_configs["bucket"],
                                       source_object = os_usg_configs["results_stg_gcs_fpth"] % '{{ ds }}', 
                                       destination_bucket = os_usg_configs["bucket"],
                                       destination_object = os_usg_configs["results_archive_gcs_fpth"] % '{{ ds }}',
                                       gcp_conn_id=os_usg_configs["gcp_conn_id"], 
                                       exact_match = True)
    
    archive_errors = GCSToGCSOperator(task_id="archive_errors",
                                      source_bucket = os_usg_configs["bucket"],
                                       source_object = os_usg_configs["errors_stg_gcs_fpth"] % '{{ ds }}',
                                       destination_bucket = os_usg_configs["bucket"],
                                       destination_object = os_usg_configs["errors_archive_gcs_fpth"] % '{{ ds }}',
                                       gcp_conn_id=os_usg_configs["gcp_conn_id"],
                                       exact_match = True)

    del_results_from_gcs_stg = GCSDeleteObjectsOperator(task_id="del_results_from_gcs_stg",
                                                        bucket_name = os_usg_configs["bucket"],
                                                        objects = [ os_usg_configs["results_stg_gcs_fpth"] % '{{ ds }}' ],
                                                        prefix=None,
                                                        gcp_conn_id=os_usg_configs["gcp_conn_id"])
    
    del_errors_from_gcs_stg = GCSDeleteObjectsOperator(task_id="del_errors_from_gcs_stg",
                                                       bucket_name = os_usg_configs["bucket"],
                                                        objects = [ os_usg_configs["errors_stg_gcs_fpth"] % '{{ ds }}' ],
                                                        prefix=None,
                                                        gcp_conn_id=os_usg_configs["gcp_conn_id"])
    
    run_os_qa_checks = EmptyOperator(task_id="run_os_qa_checks")

get_data >> load_results_to_bq >> archive_results >> del_results_from_gcs_stg
get_data >> load_errors_to_bq >> archive_errors >> del_errors_from_gcs_stg

[del_results_from_gcs_stg,del_errors_from_gcs_stg] >> run_os_qa_checks