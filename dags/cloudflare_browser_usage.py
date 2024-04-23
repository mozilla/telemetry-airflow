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
                                      "SI","SK","US","SE","GR"]}

auth_token = '' #pull from secret manager

#Configure request headers
bearer_string = 'Bearer %s' % auth_token
headers = {'Authorization': bearer_string}

#Calculate start date and end date from the DAG run date
def get_browser_data():
    """ Pull browser data for each combination of the configs from the Cloudflare API """
    for device_type in browser_usage_configs['device_types']:
        for location in browser_usage_configs['locations']:
            for os in browser_usage_configs['operating_systems']:
                print('device type: ', device_type)
                print('location: ', location)
                print('os: ', os)

                #Generate the URL call

                #Make the API call

                #Save the results to GCS

    combo = "device_type: "+device_type+" loc"+location+" os: "+os
    return combo

#Define DAG
with DAG(
    "cloudflare_browser_usage",
    default_args=default_args,
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
    load_browser_usage_data_to_bq = EmptyOperator(task_id="load_browser_usage_data_to_bq")
    run_browser_qa_checks = EmptyOperator(task_id="run_browser_qa_checks")


get_browser_usage_data >> load_browser_usage_data_to_gcs >> load_browser_usage_data_to_bq >> run_browser_qa_checks
