#Load libraries
import requests
import json
from utils.tags import Tag
import pandas as pd
from utils.cloudflare import * 
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator


#Define DOC string
DOCS = """
### Pulls browser usage data from the Cloudflare API 

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
def get_browser_data():
    #Calculate start date and end date
    start_date = "{{ ds }}"
    end_date = "{{ next_ds }}"
    print('start date')
    print(start_date)
    print('end_date')
    print(end_date)
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

                    #Initialize the results dataframe
                    final_result_df = pd.DataFrame({'StartTime': [],
                                        'EndTime': [],
                                        'DeviceType': [] ,
                                        'Location': [] ,
                                        'UserType': [],
                                        'Browser': [],
                                        'OperatingSystem': [],
                                        'PercentShare': [],
                                        'ConfLevel': [],
                                        'Normalization': [],
                                        'LastUpdated': []})

                    #Initialize the errors dataframe
                    final_errors_df = pd.DataFrame({'StartTime': [],
                                    'EndTime': [],
                                    'Location': [],
                                    'UserType': [],
                                    'DeviceType': [],
                                    'OperatingSystem': []})

                #Generate the URL ## FIX BELOW - add user type to the function
                browser_usage_api_url = generate_browser_api_call(start_date, end_date, device_type, loc, os, user_type)

                #Make the API call

                #Save the results to GCS

                #Save the errors to GCS


    return browser_usage_api_url #Temp for testing

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
                                #op_args=[device_type, location],
                                execution_timeout=timedelta(minutes=20))
    
    

    load_browser_usage_data_to_gcs = EmptyOperator(task_id="load_browser_usage_data_to_gcs")
    load_browser_usage_results_to_bq = EmptyOperator(task_id="load_browser_usage_results_to_bq")
    load_browser_usage_errors_to_bq = EmptyOperator(task_id="load_browser_usage_errors_to_bq")
    run_browser_qa_checks = EmptyOperator(task_id="run_browser_qa_checks")


get_browser_usage_data >> load_browser_usage_data_to_gcs >> load_browser_usage_results_to_bq 
load_browser_usage_results_to_bq >> load_browser_usage_errors_to_bq >> run_browser_qa_checks