# Load libraries
import json
from datetime import datetime, timedelta

import pandas as pd
import requests
from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.operators.gcs import GCSDeleteObjectsOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator

from utils.tags import Tag

# Load auth token
auth_token = Variable.get("cloudflare_auth_token", default_var="abc")

# Define DOC string
DOCS = """Pulls browser usage data from the Cloudflare API; Owner: kwindau@mozilla.com
Note: Each run pulls data for the date 4 days prior"""

default_args = {
    "owner": "kwindau@mozilla.com",
    "email": ["kwindau@mozilla.com"],
    "depends_on_past": False,
    "start_date": datetime(2024, 5, 3),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
    "retry_delay": timedelta(minutes=30),
}

TAGS = [Tag.ImpactTier.tier_3, Tag.Repo.airflow]

# Configurations
brwsr_usg_configs = {
    "timeout_limit": 2000,
    "device_types": ["DESKTOP", "MOBILE", "OTHER", "ALL"],
    "operating_systems": [
        "ALL",
        "WINDOWS",
        "MACOSX",
        "IOS",
        "ANDROID",
        "CHROMEOS",
        "LINUX",
        "SMART_TV",
    ],
    "locations": [
        "ALL",
        "BE",
        "BG",
        "CA",
        "CZ",
        "DE",
        "DK",
        "EE",
        "ES",
        "FI",
        "FR",
        "GB",
        "HR",
        "IE",
        "IT",
        "CY",
        "LV",
        "LT",
        "LU",
        "HU",
        "MT",
        "MX",
        "NL",
        "AT",
        "PL",
        "PT",
        "RO",
        "SI",
        "SK",
        "US",
        "SE",
        "GR",
    ],
    "user_types": ["ALL"],
    "bucket": "gs://moz-fx-data-prod-external-data/",
    "results_stg_gcs_fpth": "cloudflare/browser_usage/RESULTS_STAGING/%s_results.csv",
    "results_archive_gcs_fpath": "cloudflare/browser_usage/RESULTS_ARCHIVE/%s_results.csv",
    "errors_stg_gcs_fpth": "cloudflare/browser_usage/ERRORS_STAGING/%s_errors.csv",
    "errors_archive_gcs_fpath": "cloudflare/browser_usage/ERRORS_ARCHIVE/%s_errors.csv",
    "gcp_project_id": "moz-fx-data-shared-prod",
    "gcp_conn_id": "google_cloud_airflow_dataproc",
}


# Function to generate API call based on configs passed
def generate_browser_api_call(
    strt_dt, end_dt, device_type, location, op_system, user_typ
):
    """Create the API url based on the input parameters."""
    user_type_string = "" if user_typ == "ALL" else f"&botClass={0}" % (user_typ)
    location_string = "" if location == "ALL" else f"&location={0}" % (location)
    op_system_string = "" if op_system == "ALL" else f"&os={0}" % (op_system)
    device_type_string = (
        "" if device_type == "ALL" else f"&deviceType={0}" % (device_type)
    )
    browser_api_url = (
        f"https://api.cloudflare.com/client/v4/radar/http/top/browsers?dateStart={0}T00:00:00.000Z&dateEnd={1}T00:00:00.000Z{2}{3}{4}{5}&format=json"
        % (
            strt_dt,
            end_dt,
            device_type_string,
            location_string,
            op_system_string,
            user_type_string,
        )
    )
    return browser_api_url


# Define function to pull browser data from the cloudflare API
def get_browser_data(**kwargs):
    """Pull browser data for each combination of the configs from the Cloudflare API, always runs with a lag of 4 days."""
    # Calculate start date and end date
    logical_dag_dt = kwargs.get("ds")
    logical_dag_dt_as_date = datetime.strptime(logical_dag_dt, "%Y-%m-%d").date()
    start_date = logical_dag_dt_as_date - timedelta(days=4)
    end_date = start_date + timedelta(days=1)
    print("Start Date: ", start_date)
    print("End Date: ", end_date)

    # Configure request headers
    bearer_string = f"Bearer {0}" % (auth_token)
    headers = {"Authorization": bearer_string}

    # Initialize the empty results and errors dataframes
    browser_results_df = pd.DataFrame(
        {
            "StartTime": [],
            "EndTime": [],
            "DeviceType": [],
            "Location": [],
            "UserType": [],
            "Browser": [],
            "OperatingSystem": [],
            "PercentShare": [],
            "ConfLevel": [],
            "Normalization": [],
            "LastUpdated": [],
        }
    )

    browser_errors_df = pd.DataFrame(
        {
            "StartTime": [],
            "EndTime": [],
            "Location": [],
            "UserType": [],
            "DeviceType": [],
            "OperatingSystem": [],
        }
    )

    # Loop through the combinations
    for device_type in brwsr_usg_configs["device_types"]:
        for loc in brwsr_usg_configs["locations"]:
            for os in brwsr_usg_configs["operating_systems"]:
                for user_type in brwsr_usg_configs["user_types"]:
                    curr_combo = (
                        f"Device Type: {0}, Location: {1}, OS: {2}, User Type: {3}"
                        % (device_type, loc, os, user_type)
                    )
                    print(curr_combo)

                    # Generate the URL & call the API
                    brwsr_usg_api_url = generate_browser_api_call(
                        start_date, end_date, device_type, loc, os, user_type
                    )
                    response = requests.get(
                        brwsr_usg_api_url,
                        headers=headers,
                        timeout=brwsr_usg_configs["timeout_limit"],
                    )
                    response_json = json.loads(response.text)

                    # if the response was successful, get the result and append it to the results dataframe
                    if response_json["success"] is True:
                        # Save the results to GCS
                        result = response_json["result"]
                        confidence_level = result["meta"]["confidenceInfo"]["level"]
                        normalization = result["meta"]["normalization"]
                        last_updated = result["meta"]["lastUpdated"]
                        startTime = result["meta"]["dateRange"][0]["startTime"]
                        endTime = result["meta"]["dateRange"][0]["endTime"]
                        data = result["top_0"]
                        browser_lst = []
                        browser_share_lst = []

                        for browser in data:
                            browser_lst.append(browser["name"])
                            browser_share_lst.append(browser["value"])

                        new_browser_results_df = pd.DataFrame(
                            {
                                "StartTime": [startTime] * len(browser_lst),
                                "EndTime": [endTime] * len(browser_lst),
                                "DeviceType": [device_type] * len(browser_lst),
                                "Location": [loc] * len(browser_lst),
                                "UserType": [user_type] * len(browser_lst),
                                "Browser": browser_lst,
                                "OperatingSystem": [os] * len(browser_lst),
                                "PercentShare": browser_share_lst,
                                "ConfLevel": [confidence_level] * len(browser_lst),
                                "Normalization": [normalization] * len(browser_lst),
                                "LastUpdated": [last_updated] * len(browser_lst),
                            }
                        )
                        browser_results_df = pd.concat(
                            [browser_results_df, new_browser_results_df]
                        )

                    # If there were errors, save them to the errors dataframe
                    else:
                        new_browser_error_df = pd.DataFrame(
                            {
                                "StartTime": [start_date],
                                "EndTime": [end_date],
                                "Location": [loc],
                                "UserType": [user_type],
                                "DeviceType": [device_type],
                                "OperatingSystem": [os],
                            }
                        )
                        browser_errors_df = pd.concat(
                            [browser_errors_df, new_browser_error_df]
                        )

    # LOAD RESULTS & ERRORS TO STAGING GCS
    result_fpath = (
        brwsr_usg_configs["bucket"]
        + brwsr_usg_configs["results_stg_gcs_fpth"] % logical_dag_dt
    )
    error_fpath = (
        brwsr_usg_configs["bucket"]
        + brwsr_usg_configs["errors_stg_gcs_fpth"] % logical_dag_dt
    )
    browser_results_df.to_csv(result_fpath, index=False)
    browser_errors_df.to_csv(error_fpath, index=False)
    print("Wrote errors to: ", error_fpath)
    print("Wrote results to: ", result_fpath)

    # Return a summary to the console
    len_results = str(len(browser_results_df))
    len_errors = str(len(browser_errors_df))
    result_summary = f"# Result Rows: {0}; # of Error Rows: {1}" % (
        len_results,
        len_errors,
    )
    return result_summary


del_any_existing_browser_gold_results_for_date = """DELETE FROM `moz-fx-data-shared-prod.cloudflare_derived.browser_usage_v1`
WHERE dte = {{ ds }} """

del_any_existing_browser_gold_errors_for_date = """DELETE FROM `moz-fx-data-shared-prod.cloudflare_derived.browser_usage_errors_v1`
WHERE dte = {{ ds }} """

browser_usg_stg_to_gold_query = """ INSERT INTO `moz-fx-data-shared-prod.cloudflare_derived.browser_usage_v1`
SELECT
CAST(StartTime as date) AS dte,
device_type,
location,
user_type,
browser,
operating_system.
percent_share,
normalization,
last_updated_ts
FROM `moz-fx-data-shared-prod.cloudflare_derived.browser_results_stg` """

browser_usg_errors_stg_to_gold_query = """ INSERT INTO `moz-fx-data-shared-prod.cloudflare_derived.browser_usage_errors_v1`
SELECT
CAST(StartTime as date) AS dte,
location,
user_type,
device_type,
operating_system
FROM `moz-fx-data-shared-prod.cloudflare_derived.browser_errors_stg`  """


# Define DAG
with DAG(
    "cloudflare_browser_usage",
    default_args=default_args,
    catchup=False,
    doc_md=DOCS,
    schedule_interval="0 5 * * *",  # Daily at 5am
    tags=TAGS,
) as dag:
    # Call the API and save the results to GCS
    get_browser_usage_data = PythonOperator(
        task_id="get_browser_usage_data",
        python_callable=get_browser_data,
        execution_timeout=timedelta(minutes=55),
    )

    # Load the results from GCS to a temporary staging table in BQ (overwritten each run)
    load_results_to_bq_stg = GCSToBigQueryOperator(
        task_id="load_results_to_bq_stg",
        bucket=brwsr_usg_configs["bucket"],
        destination_project_dataset_table="moz-fx-data-shared-prod.cloudflare_derived.browser_results_stg",
        schema_fields=[
            {"name": "StartTime", "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "EndTime", "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "DeviceType", "type": "STRING", "mode": "NULLABLE"},
            {"name": "Location", "type": "STRING", "mode": "NULLABLE"},
            {"name": "UserType", "type": "STRING", "mode": "NULLABLE"},
            {"name": "Browser", "type": "STRING", "mode": "NULLABLE"},
            {"name": "OperatingSystem", "type": "STRING", "mode": "NULLABLE"},
            {"name": "PercentShare", "type": "NUMERIC", "mode": "NULLABLE"},
            {"name": "ConfLevel", "type": "STRING", "mode": "NULLABLE"},
            {"name": "Normalization", "type": "STRING", "mode": "NULLABLE"},
            {"name": "LastUpdated", "type": "TIMESTAMP", "mode": "NULLABLE"},
        ],
        source_format="CSV",
        source_objects=brwsr_usg_configs["bucket"]
        + brwsr_usg_configs["results_stg_gcs_fpth"] % "{{ ds }}",
        compression="NONE",
        create_disposition="CREATE_IF_NEEDED",
        skip_leading_rows=1,
        write_disposition="WRITE_TRUNCATE",
        gcp_conn_id=brwsr_usg_configs["gcp_conn_id"],
        allow_jagged_rows=False,
    )

    # Load the errors from GCS to a temporary staging table in BQ (overwritten each run)
    load_errors_to_bq_stg = GCSToBigQueryOperator(
        task_id="load_errors_to_bq_stg",
        bucket=brwsr_usg_configs["bucket"],
        destination_project_dataset_table="moz-fx-data-shared-prod.cloudflare_derived.browser_errors_stg",
        schema_fields=[
            {"name": "StartTime", "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "EndTime", "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "Location", "type": "STRING", "mode": "NULLABLE"},
            {"name": "UserType", "type": "STRING", "mode": "NULLABLE"},
            {"name": "DeviceType", "type": "STRING", "mode": "NULLABLE"},
            {"name": "OperatingSystem", "type": "STRING", "mode": "NULLABLE"},
        ],
        source_format="CSV",
        source_objects=brwsr_usg_configs["bucket"]
        + brwsr_usg_configs["errors_stg_gcs_fpth"] % "{{ ds }}",
        compression="NONE",
        create_disposition="CREATE_IF_NEEDED",
        skip_leading_rows=1,
        write_disposition="WRITE_TRUNCATE",
        gcp_conn_id=brwsr_usg_configs["gcp_conn_id"],
        allow_jagged_rows=False,
    )

    # This will delete anything if the DAG is ever run multiple times for the same date
    delete_bq_gold_res_for_date_if_any = BigQueryInsertJobOperator(
        task_id="delete_bq_gold_res_for_date_if_any",
        configuration={
            "query": del_any_existing_browser_gold_results_for_date,
            "useLegacySql": False,
        },
        project_id="moz-fx-data-shared-prod",
        gcp_conn_id=brwsr_usg_configs["gcp_conn_id"],
    )

    # This will delete anything if the DAG is ever run multiple times for the same date
    delete_bq_gold_err_for_date_if_any = BigQueryInsertJobOperator(
        task_id="delete_bq_gold_err_for_date_if_any",
        configuration={
            "query": del_any_existing_browser_gold_errors_for_date,
            "useLegacySql": False,
        },
        project_id="moz-fx-data-shared-prod",
        gcp_conn_id=brwsr_usg_configs["gcp_conn_id"],
    )

    # Run a query to process data from staging and insert it into the production gold table
    load_results_to_bq_gold = BigQueryInsertJobOperator(
        task_id="load_results_to_bq_gold",
        configuration={
            "query": browser_usg_stg_to_gold_query,
            "destinationTable": {
                "projectId": "moz-fx-data-shared-prod",
                "datasetId": "cloudflare_derived",
                "tableId": "browser_usage_v1",
            },
            "createDisposition": "CREATE_NEVER",
            "writeDisposition": "WRITE_APPEND",
        },
        project_id=brwsr_usg_configs["gcp_project_id"],
        gcp_conn_id=brwsr_usg_configs["gcp_conn_id"],
    )

    load_errors_to_bq_gold = BigQueryInsertJobOperator(
        task_id="load_errors_to_bq_gold",
        configuration={
            "query": browser_usg_errors_stg_to_gold_query,
            "destinationTable": {
                "projectId": "moz-fx-data-shared-prod",
                "datasetId": "cloudflare_derived",
                "tableId": "browser_usage_errors_v1",
            },
            "createDisposition": "CREATE_NEVER",
            "writeDisposition": "WRITE_APPEND",
        },
        project_id=brwsr_usg_configs["gcp_project_id"],
        gcp_conn_id=brwsr_usg_configs["gcp_conn_id"],
    )

    # Archive the result files by moving them out of staging path and into archive path
    archive_results = GCSToGCSOperator(
        task_id="archive_results",
        source_bucket=brwsr_usg_configs["bucket"],
        source_object=brwsr_usg_configs["results_stg_gcs_fpth"] % "{{ ds }}",
        destination_bucket=brwsr_usg_configs["bucket"],
        destination_object=brwsr_usg_configs["results_archive_gcs_fpath"] % "{{ ds }}",
        gcp_conn_id=brwsr_usg_configs["gcp_conn_id"],
        exact_match=True,
    )

    # Archive the error files by moving them out of staging path and into archive path
    archive_errors = GCSToGCSOperator(
        task_id="archive_errors",
        source_bucket=brwsr_usg_configs["bucket"],
        source_object=brwsr_usg_configs["errors_stg_gcs_fpth"] % "{{ ds }}",
        destination_bucket=brwsr_usg_configs["bucket"],
        destination_object=brwsr_usg_configs["errors_archive_gcs_fpath"] % "{{ ds }}",
        gcp_conn_id=brwsr_usg_configs["gcp_conn_id"],
        exact_match=True,
    )

    # Delete the results from staging GCS
    del_results_from_gcs_stg = GCSDeleteObjectsOperator(
        task_id="del_results_from_gcs_stg",
        bucket_name=brwsr_usg_configs["bucket"],
        objects=[brwsr_usg_configs["results_stg_gcs_fpth"] % "{{ ds }}"],
        gcp_conn_id=brwsr_usg_configs["gcp_conn_id"],
    )

    # Delete the errors from staging GCS
    del_errors_from_gcs_stg = GCSDeleteObjectsOperator(
        task_id="del_errors_from_gcs_stg",
        bucket_name=brwsr_usg_configs["bucket"],
        objects=[brwsr_usg_configs["errors_stg_gcs_fpth"] % "{{ ds }}"],
        gcp_conn_id=brwsr_usg_configs["gcp_conn_id"],
    )

    # Run browser QA checks - make sure there is only 1 row per primary key, error if PKs have more than 1 row
    run_browser_qa_checks = EmptyOperator(task_id="run_browser_qa_checks")


(
    get_browser_usage_data
    >> load_results_to_bq_stg
    >> delete_bq_gold_res_for_date_if_any
    >> load_results_to_bq_gold
    >> archive_results
    >> del_results_from_gcs_stg
)
(
    get_browser_usage_data
    >> load_errors_to_bq_stg
    >> delete_bq_gold_err_for_date_if_any
    >> load_errors_to_bq_gold
    >> archive_errors
    >> del_errors_from_gcs_stg
)
[del_results_from_gcs_stg, del_errors_from_gcs_stg] >> run_browser_qa_checks
