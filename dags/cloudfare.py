#Load libraries
import time
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

from utils.cloudfare.cloudfare_functions import *

#Define docstring
DOCS = """
### extract_cloudfare_market_share_data
Calls cloudfare APIs and loads external market share data into BigQuery
Owner: kwindau@mozilla.com
"""

end_date = '{{ macros.ds_format(ds, "%Y-%m-%d") }}'
#start_date = # end_date - 7 #FIX LATER

##FIX BELOW 
def retrieve_cloudfare_browser_usg_data():
    #???
    return None

def retrieve_cloudfare_device_usg_data():
    #???
    return None

def retrieve_cloudfare_os_usg_data():
    #???
    return None

#Set default arguments
DEFAULT_ARGS = {
    "owner": "kwindau@mozilla.com",
    "start_date": datetime(2024, 3, 23, 0, 0),
    "end_date": None,
    "email": ["kwindau@mozilla.com", "telemetry-alerts@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": timedelta(seconds=1800),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
}

#Set impact level tag
TAGS = ["impact/tier_2"]

#Define the DAG 
with DAG(
    dag_id="extract_cloudfare_market_share_data",
    default_args=DEFAULT_ARGS,
    doc_md=DOCS,
    schedule_interval="5 5 * * 1", #Run weekly on Mondays at 5:05AM
    tags=TAGS,
    catchup=False,
    max_active_runs=1,  
) as dag:
        
    get_browser_usage_data = PythonOperator(
        task_id="get_browser_usage_data",
        python_callable=retrieve_cloudfare_browser_usg_data,
        op_args=[],
        execution_timeout=timedelta(hours=1)
    )

    get_device_usage_data = PythonOperator(
        task_id="get_device_usage_data",
        python_callable=retrieve_cloudfare_device_usg_data,
        op_args=[],
        execution_timeout=timedelta(hours=1)
    )

    get_os_usage_data = PythonOperator(
        task_id="get_os_usage_data",
        python_callable=retrieve_cloudfare_os_usg_data,
        op_args=[],
        execution_timeout=timedelta(hours=1)
    )

    get_browser_usage_data >> get_device_usage_data >> get_os_usage_data 