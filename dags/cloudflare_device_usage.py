#Load libraries
import requests
import json
from utils.tags import Tag
import pandas as pd
from utils.cloudflare import * 
from airflow import DAG
from airflow.operators.empty import EmptyOperator


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
                                      "MT","MX","NL","AT","PL","PT","RO","SI","SK","US","SE","GR"]}

#Calculate start date and end date from the DAG run date


#Define DAG
with DAG(
    "cloudflare_device_usage",
    default_args=default_args,
    doc_md=DOCS,
    schedule_interval="0 5 * * *",
    tags=TAGS,
) as dag:

    #Define OS usage task
    get_device_usage_data = EmptyOperator(task_id="get_device_usage_data")
    load_device_usage_data_to_gcs = EmptyOperator(task_id="load_device_usage_data_to_gcs")
    load_device_usage_data_to_bq = EmptyOperator(task_id="load_device_usage_data_to_bq")
    run_device_qa_checks = EmptyOperator(task_id="run_device_qa_checks")
get_device_usage_data >> load_device_usage_data_to_gcs >> load_device_usage_data_to_bq >> run_device_qa_checks
