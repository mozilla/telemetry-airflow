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

#Define DOC string
DOCS = """
### Pulls device usage data from the Cloudflare API 

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
device_usage_configs = {"timeout_limit": 2000,
                        "locations": ["ALL","BE","BG","CA","CZ","DE","DK","EE","ES","FI","FR",
                                      "GB","HR","IE","IT","CY","LV","LT","LU","HU",
                                      "MT","MX","NL","AT","PL","PT","RO","SI","SK","US","SE","GR"],
                        "results_staging_gcs_fpath": "gs://moz-fx-data-prod-external-data/cloudflare/device_usage/RESULTS_STAGING/%s_results.csv",
                        "results_archive_gcs_fpath": "gs://moz-fx-data-prod-external-data/cloudflare/device_usage/RESULTS_ARCHIVE/%s_results.csv",
                        "errors_staging_gcs_fpath": "gs://moz-fx-data-prod-external-data/cloudflare/device_usage/ERRORS_STAGING/%s_errors.csv",
                        "errors_archive_gcs_fpath": "gs://moz-fx-data-prod-external-data/cloudflare/device_usage/ERRORS_ARCHIVE/%s_errors.csv"}

auth_token = Variable.get('cloudflare_auth_token')

#Configure request headers
bearer_string = 'Bearer %s' % auth_token
headers = {'Authorization': bearer_string}

#Define generate device API URL
#### PART 1 - FUNCTIONS FOR GENERATING URL TO HIT 
def generate_device_type_timeseries_api_call(strt_dt, end_dt, agg_int, location):
    """ Inputs: Start Date in YYYY-MM-DD format, End Date in YYYY-MM-DD format, and desired agg_interval; Returns API URL """
    if location == 'ALL':
        device_usage_api_url = "https://api.cloudflare.com/client/v4/radar/http/timeseries/device_type?name=human&botClass=LIKELY_HUMAN&dateStart=%sT00:00:00.000Z&dateEnd=%sT00:00:00.000Z&name=bot&botClass=LIKELY_AUTOMATED&dateStart=%sT00:00:00.000Z&dateEnd=%sT00:00:00.000Z&format=json&aggInterval=%s" % (strt_dt, end_dt, strt_dt, end_dt, agg_int)
    else:
        device_usage_api_url = "https://api.cloudflare.com/client/v4/radar/http/timeseries/device_type?name=human&botClass=LIKELY_HUMAN&dateStart=%sT00:00:00.000Z&dateEnd=%sT00:00:00.000Z&location=%s&name=bot&botClass=LIKELY_AUTOMATED&dateStart=%sT00:00:00.000Z&dateEnd=%sT00:00:00.000Z&location=%s&format=json&aggInterval=%s" % (strt_dt, end_dt, location, strt_dt, end_dt, location, agg_int)
    return device_usage_api_url

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
    for loc in device_usage_configs['locations']:
        return 'loc: '+loc

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
    get_data = PythonOperator(task_id="get_device_usage_data",
                                python_callable=get_device_usage_data,
                                #op_args=[device_type, location],
                                execution_timeout=timedelta(minutes=20))
    load_results_to_bq = EmptyOperator(task_id="load_results_to_bq")
    load_errors_to_bq = EmptyOperator(task_id="load_errors_to_bq")

    archive_results = EmptyOperator(task_id="archive_results")
    archive_errors = EmptyOperator(task_id="archive_errors")

    del_results_from_gcs_stg = EmptyOperator(task_id="delete_results_from_gcs_stg")
    del_errors_from_gcs_stg = EmptyOperator(task_id="delete_errors_from_gcs_stg")

    run_device_qa_checks = EmptyOperator(task_id="run_device_qa_checks")

get_data >> load_results_to_bq >> archive_results >> del_results_from_gcs_stg
get_data >> load_errors_to_bq >> archive_errors >> del_errors_from_gcs_stg

[del_results_from_gcs_stg , del_errors_from_gcs_stg] >> run_device_qa_checks
