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

#Define DOC string
DOCS = """
### Pulls browser usage data from the Cloudflare API 
Note - each execution runs for the time period 4 days prior

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
browser_usage_configs = {"timeout_limit": 2000,
                         "device_types": ["DESKTOP", "MOBILE", "OTHER", "ALL"],
                        "operating_systems": ["ALL", "WINDOWS", "MACOSX", "IOS",
                                             "ANDROID", "CHROMEOS", "LINUX", "SMART_TV"],
                        "locations": ["ALL","BE","BG","CA","CZ","DE","DK","EE","ES",
                                      "FI","FR","GB","HR","IE","IT","CY","LV","LT",
                                      "LU","HU","MT","MX","NL","AT","PL","PT","RO",
                                      "SI","SK","US","SE","GR"],
                        "user_types": ["ALL"]
                        }

auth_token = '' #pull from secret manager

#Configure request headers
bearer_string = 'Bearer %s' % auth_token
headers = {'Authorization': bearer_string}

#Define function to pull browser data from the cloudflare API
def get_browser_data(**kwargs):
    #Calculate start date and end date
    logical_dag_dt = kwargs.get('ds')
    logical_dag_dt_as_date = datetime.strptime(logical_dag_dt, '%Y-%m-%d').date()
    start_date = logical_dag_dt_as_date - timedelta(days=4)
    #Calculate the end date to be the start_date + 1
    end_date = start_date + timedelta(days=1)
    print('start_date: ', start_date)
    print('end_date: ', end_date)

    #Loop through the combinations
    """ Pull browser data for each combination of the configs from the Cloudflare API """
    for device_type in browser_usage_configs['device_types']:
        for loc in browser_usage_configs['locations']:
            for os in browser_usage_configs['operating_systems']:
                for user_type in browser_usage_configs["user_types"]:
                    print('device type: ', device_type)
                    print('location: ', loc)
                    print('os: ', os)
                    print('user_type: ', user_type)
                    
                    #Initialize the empty results and errors dataframes
                    browser_results_df = initialize_browser_results_df()
                    browser_errors_df = initialize_browser_errors_df()
                        
                    #Generate the URL
                    browser_usage_api_url = generate_browser_api_call(start_date, end_date, device_type, loc, os, user_type)
                    print('browser_usage_api_url: ', browser_usage_api_url) #TEMP FOR TESTING

                    #Make the API call
                    response = requests.get(browser_usage_api_url, headers=headers, timeout = browser_usage_configs['timeout_limit'])
                    response_json = json.loads(response.text)

                    #if the response was successful, get the result
                    if response_json['success'] is True:
                        #Save the results to GCS
                        print('we will parse the result and save to results folder in GCS')
                        
                    else:
                        #Save the errors to GCS
                        print('we will parse the result and save to errors folder in GCS')

    #Make this eventually print # of success vs # of errors
    return None

#Define DAG
with DAG(
    "cloudflare_browser_usage",
    default_args=default_args,
    catchup=False,
    doc_md=DOCS,
    schedule_interval="0 5 * * *",
    tags=TAGS,
) as dag:

    #get_browser_usage_data = EmptyOperator(task_id="get_browser_usage_data")
    get_browser_usage_data = PythonOperator(task_id="get_browser_usage_data",
                                python_callable=get_browser_data,
                                op_args=[], ##how to add start date
                                execution_timeout=timedelta(minutes=20))
    
    

    load_browser_usage_data_to_gcs = EmptyOperator(task_id="load_browser_usage_data_to_gcs")
    load_browser_usage_results_to_bq = EmptyOperator(task_id="load_browser_usage_results_to_bq")
    load_browser_usage_errors_to_bq = EmptyOperator(task_id="load_browser_usage_errors_to_bq")
    run_browser_qa_checks = EmptyOperator(task_id="run_browser_qa_checks")


get_browser_usage_data >> load_browser_usage_data_to_gcs >> load_browser_usage_results_to_bq 
load_browser_usage_results_to_bq >> load_browser_usage_errors_to_bq >> run_browser_qa_checks