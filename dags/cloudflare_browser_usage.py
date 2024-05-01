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
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow.providers.google.cloud.operators.gcs import GCSDeleteObjectsOperator

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
brwsr_usg_configs = {"timeout_limit": 2000,
                    "device_types": ["DESKTOP"], 
                    "operating_systems": ["ALL"], 
                    "locations": ["US"], 
                    "user_types": ["ALL"],
                    "bucket": "gs://moz-fx-data-prod-external-data/",
                    "results_staging_gcs_fpath": "cloudflare/browser_usage/RESULTS_STAGING/%s_results.csv",
                    "results_archive_gcs_fpath": "cloudflare/browser_usage/RESULTS_ARCHIVE/%s_results.csv",
                    "errors_staging_gcs_fpath": "cloudflare/browser_usage/ERRORS_STAGING/%s_errors.csv",
                    "errors_archive_gcs_fpath": "cloudflare/browser_usage/ERRORS_ARCHIVE/%s_errors.csv",
                    "gcp_conn_id": "google_cloud_gke_sandbox"}

#Define function to pull browser data from the cloudflare API
def get_browser_data(**kwargs):
    """ Pull browser data for each combination of the configs from the Cloudflare API, always runs with a lag of 4 days"""
    #Calculate start date and end date
    logical_dag_dt = kwargs.get('ds')
    logical_dag_dt_as_date = datetime.strptime(logical_dag_dt, '%Y-%m-%d').date()
    start_date = logical_dag_dt_as_date - timedelta(days=4)
    end_date = start_date + timedelta(days=1)
    print('Start Date: ', start_date)
    print('End Date: ', end_date)

    #Configure request headers
    auth_token = '' #TO DO pull from secret manager
    bearer_string = 'Bearer %s' % auth_token
    headers = {'Authorization': bearer_string}

    #Initialize the empty results and errors dataframes
    browser_results_df = initialize_browser_results_df()
    browser_errors_df = initialize_browser_errors_df()

    #Loop through the combinations
    for device_type in brwsr_usg_configs['device_types']:
        for loc in brwsr_usg_configs['locations']:
            for os in brwsr_usg_configs['operating_systems']:
                for user_type in brwsr_usg_configs["user_types"]:
                    print('device type: ', device_type)
                    print('location: ', loc)
                    print('os: ', os)
                    print('user_type: ', user_type)
                        
                    #Generate the URL & call the API
                    browser_usage_api_url = generate_browser_api_call(start_date, end_date, device_type, loc, os, user_type)
                    response = requests.get(browser_usage_api_url, headers=headers, timeout = brwsr_usg_configs['timeout_limit'])
                    response_json = json.loads(response.text)

                    #TEMP FOR TESTING
                    print('response_json')
                    print(response_json)
                    #TEMP FOR TESTING

                    #if the response was successful, get the result and append it to the results dataframe
                    if response_json['success'] is True:
                        #Save the results to GCS
                        result = response_json['result']
                        confidence_level, normalization, last_updated =  parse_response_metadata(result)
                        startTime, endTime = parse_browser_usg_start_and_end_time(result)

                        data = result['top_0']

                        browser_lst = []
                        browser_share_lst = []

                        for browser in data:
                            browser_lst.append(browser['name'])
                            browser_share_lst.append(browser['value'])

                        new_browser_results_df = pd.DataFrame({'StartTime': [startTime]* len(browser_lst),
                                            'EndTime': [endTime] * len(browser_lst),
                                            'DeviceType': [device_type] * len(browser_lst) ,
                                            'Location': [loc] * len(browser_lst) ,
                                            'UserType': [user_type] * len(browser_lst),
                                            'Browser': browser_lst,
                                            'OperatingSystem': [os] * len(browser_lst),
                                            'PercentShare': browser_share_lst,
                                            'ConfLevel': [confidence_level] * len(browser_lst),
                                            'Normalization': [normalization] * len(browser_lst),
                                            'LastUpdated': [last_updated] * len(browser_lst)})
                        browser_results_df = pd.concat([browser_results_df, new_browser_results_df])

                    #If there were errors, save them to the errors dataframe
                    else:
                        new_browser_error_df = pd.DataFrame({'StartTime': [start_date],
                                                            'EndTime': [end_date],
                                                            'Location': [loc],
                                                            'UserType': [user_type],
                                                            'DeviceType': [device_type],
                                                            'OperatingSystem': [os]})
                        browser_errors_df = pd.concat([browser_errors_df,new_browser_error_df])

    #LOAD RESULTS & ERRORS TO STAGING GCS
    result_fpath = brwsr_usg_configs["bucket"] + brwsr_usg_configs["results_staging_gcs_fpath"] % start_date
    print('Writing results to: ', result_fpath)

    error_fpath = brwsr_usg_configs["bucket"] + brwsr_usg_configs["errors_staging_gcs_fpath"] % start_date
    print('Writing errors to: ', error_fpath)

    browser_results_df.to_csv(result_fpath, index=False)
    browser_errors_df.to_csv(error_fpath, index=False)

    #Return a summary to the console
    len_results = str(len(browser_results_df))
    len_errors = str(len(browser_errors_df))
    result_summary = "# Result Rows: %s; # of Error Rows: %s" % (len_results, len_errors)
    return result_summary

#Define DAG
with DAG(
    "cloudflare_browser_usage",
    default_args=default_args,
    catchup=False,
    doc_md=DOCS,
    schedule_interval="0 5 * * *", #Daily at 5am
    tags=TAGS,
) as dag:

    #Call the API and save the results to GCS
    get_data = PythonOperator(task_id="get_browser_usage_data",
                                python_callable=get_browser_data,
                                execution_timeout=timedelta(minutes=20))
    
    #Load the results in GCS to the BQ tables
    load_results_to_bq = EmptyOperator(task_id="load_results_to_bq")
    load_errors_to_bq = EmptyOperator(task_id="load_errors_to_bq")

    #Archive the result files by moving them out of staging and into archive
    archive_results = GCSToGCSOperator(task_id="archive_results",
                                       source_bucket = brwsr_usg_configs["bucket"],
                                       source_object = brwsr_usg_configs["results_staging_gcs_fpath"] % '{{ ds }}', 
                                       destination_bucket = brwsr_usg_configs["bucket"],
                                       destination_object = brwsr_usg_configs["results_archive_gcs_fpath"] % '{{ ds }}',
                                       gcp_conn_id=brwsr_usg_configs["gcp_conn_id"], 
                                       exact_match = True)
    
    #Archive the error files by moving them out of staging and into archive
    archive_errors = GCSToGCSOperator(task_id="archive_errors",
                                      source_bucket = brwsr_usg_configs["bucket"],
                                       source_object = brwsr_usg_configs["errors_staging_gcs_fpath"] % '{{ ds }}',
                                       destination_bucket = brwsr_usg_configs["bucket"],
                                       destination_object = brwsr_usg_configs["errors_archive_gcs_fpath"] % '{{ ds }}',
                                       gcp_conn_id=brwsr_usg_configs["gcp_conn_id"],
                                       exact_match = True)
    #Delete the results from staging GCS 
    del_results_from_gcs_stg = GCSDeleteObjectsOperator(task_id="del_results_from_gcs_stg",
                                                        bucket_name = brwsr_usg_configs["bucket"],
                                                        objects = [ brwsr_usg_configs["results_staging_gcs_fpath"] % '{{ ds }}' ],
                                                        prefix=None,
                                                        gcp_conn_id=brwsr_usg_configs["gcp_conn_id"])
    
    #Delete the errors from staging GCS 
    del_errors_from_gcs_stg = GCSDeleteObjectsOperator(task_id="del_errors_from_gcs_stg",
                                                       bucket_name = brwsr_usg_configs["bucket"],
                                                        objects = [ brwsr_usg_configs["errors_staging_gcs_fpath"] % '{{ ds }}' ],
                                                        prefix=None,
                                                        gcp_conn_id=brwsr_usg_configs["gcp_conn_id"])

    #Run browser QA checks - make sure there is only 1 row per primary key, error if PKs have more than 1 row
    run_browser_qa_checks = EmptyOperator(task_id="run_browser_qa_checks")


get_data >> load_results_to_bq >> archive_results >> del_results_from_gcs_stg
get_data >> load_errors_to_bq >> archive_errors >> del_errors_from_gcs_stg
[ del_results_from_gcs_stg, del_errors_from_gcs_stg] >> run_browser_qa_checks
