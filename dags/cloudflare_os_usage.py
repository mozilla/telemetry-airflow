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

# Get the auth token from an Airflow variable
auth_token = Variable.get("cloudflare_auth_token", default_var="abc")

# Define DOC string
DOCS = """Pulls OS usage data from the Cloudflare API; Owner: kwindau@mozilla.com
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
os_usg_configs = {
    "timeout_limit": 2000,
    "device_types": ["DESKTOP", "MOBILE", "OTHER", "ALL"],
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
    "bucket": "gs://moz-fx-data-prod-external-data/",
    "results_stg_gcs_fpth": "gs://moz-fx-data-prod-external-data/cloudflare/os_usage/RESULTS_STAGING/%s_results.csv",
    "results_archive_gcs_fpth": "gs://moz-fx-data-prod-external-data/cloudflare/os_usage/RESULTS_ARCHIVE/%s_results.csv",
    "errors_stg_gcs_fpth": "gs://moz-fx-data-prod-external-data/cloudflare/os_usage/ERRORS_STAGING/%s_errors.csv",
    "errors_archive_gcs_fpth": "gs://moz-fx-data-prod-external-data/cloudflare/os_usage/ERRORS_ARCHIVE/%s_errors.csv",
    "gcp_project_id": "moz-fx-data-shared-prod",
    "gcp_conn_id": "google_cloud_airflow_dataproc",
}


# Function to configure the API URL
def generate_os_timeseries_api_call(strt_dt, end_dt, agg_int, location, device_type):
    """Generate the API call for Operating System Usage Data."""
    if location == "ALL" and device_type == "ALL":
        os_usage_api_url = f"https://api.cloudflare.com/client/v4/radar/http/timeseries_groups/os?dateStart={strt_dt}T00:00:00.000Z&dateEnd={end_dt}T00:00:00.000Z&format=json&aggInterval={agg_int}"
    elif location != "ALL" and device_type == "ALL":
        os_usage_api_url = f"https://api.cloudflare.com/client/v4/radar/http/timeseries_groups/os?dateStart={strt_dt}T00:00:00.000Z&dateEnd={strt_dt}T00:00:00.000Z&location={location}&format=json&aggInterval={agg_int}"
    elif location == "ALL" and device_type != "ALL":
        os_usage_api_url = f"https://api.cloudflare.com/client/v4/radar/http/timeseries_groups/os?dateStart={strt_dt}T00:00:00.000Z&dateEnd={end_dt}T00:00:00.000Z&deviceType={device_type}&format=json&aggInterval={agg_int}"
    else:
        os_usage_api_url = f"https://api.cloudflare.com/client/v4/radar/http/timeseries_groups/os?dateStart={strt_dt}T00:00:00.000Z&dateEnd={end_dt}T00:00:00.000Z&location={location}&deviceType={device_type}&format=json&aggInterval={agg_int}"
    return os_usage_api_url


def get_os_usage_data(**kwargs):
    """Pull OS usage data from the Cloudflare API and save errors & results to GCS."""
    # Calculate start date and end date
    logical_dag_dt = kwargs.get("ds")
    logical_dag_dt_as_date = datetime.strptime(logical_dag_dt, "%Y-%m-%d").date()
    start_date = logical_dag_dt_as_date - timedelta(days=4)
    end_date = start_date + timedelta(days=1)
    print("Start Date: ", start_date)
    print("End Date: ", end_date)

    # Configure request headers
    bearer_string = f"Bearer {auth_token}"
    headers = {"Authorization": bearer_string}

    # Initialize the empty results & errors dataframe
    result_df = pd.DataFrame(
        {
            "Timestamps": [],
            "OS": [],
            "Location": [],
            "DeviceType": [],
            "Share": [],
            "ConfidenceLevel": [],
            "AggrInterval": [],
            "Normalization": [],
            "LastUpdatedTS": [],
        }
    )

    # Initialize an errors dataframe
    errors_df = pd.DataFrame(
        {"StartTime": [], "EndTime": [], "Location": [], "DeviceType": []}
    )

    # Go through all combinations, submit API requests
    for device_type in os_usg_configs["device_types"]:
        for loc in os_usg_configs["locations"]:
            print("Device Type: ", device_type)
            print("Loc: ", loc)

            # Generate the URL with given parameters
            os_usage_api_url = generate_os_timeseries_api_call(
                start_date, end_date, "1d", loc, device_type
            )

            # Call the API and save the response as JSON
            response = requests.get(
                os_usage_api_url,
                headers=headers,
                timeout=os_usg_configs["timeout_limit"],
            )
            response_json = json.loads(response.text)

            # If response was successful, get the result
            if response_json["success"] is True:
                result = response_json["result"]
                # Parse metadata
                conf_lvl = result["meta"]["confidenceInfo"]["level"]
                aggr_intvl = result["meta"]["aggInterval"]
                nrmlztn = result["meta"]["normalization"]
                lst_upd = result["meta"]["lastUpdated"]
                data_dict = result["serie_0"]

                for key, val in data_dict.items():
                    new_result_df = pd.DataFrame(
                        {
                            "Timestamps": data_dict["timestamps"],
                            "OS": [key] * len(val),
                            "Location": [loc] * len(val),
                            "DeviceType": [device_type] * len(val),
                            "Share": val,
                            "ConfidenceLevel": [conf_lvl] * len(val),
                            "AggrInterval": [aggr_intvl] * len(val),
                            "Normalization": [nrmlztn] * len(val),
                            "LastUpdatedTS": [lst_upd] * len(val),
                        }
                    )
                    result_df = pd.concat([result_df, new_result_df])

            # If response was not successful, get the errors
            else:
                # errors = response_json["errors"]  # Maybe add to capture, right now not using this
                new_errors_df = pd.DataFrame(
                    {
                        "StartTime": [start_date],
                        "EndTime": [end_date],
                        "Location": [loc],
                        "DeviceType": [device_type],
                    }
                )
                errors_df = pd.concat([errors_df, new_errors_df])

    result_fpath = (
        os_usg_configs["bucket"]
        + os_usg_configs["results_stg_gcs_fpth"] % logical_dag_dt
    )
    errors_fpath = (
        os_usg_configs["bucket"]
        + os_usg_configs["errors_stg_gcs_fpth"] % logical_dag_dt
    )

    result_df.to_csv(result_fpath, index=False)
    errors_df.to_csv(errors_fpath, index=False)
    print("Wrote errors to: ", errors_fpath)
    print("Wrote results to: ", result_fpath)

    # Write a summary to the logs
    len_results = str(len(result_df))
    len_errors = str(len(errors_df))
    result_summary = f"# Result Rows: {len_results}; # of Error Rows: {len_errors}"
    return result_summary


del_any_existing_op_sys_gold_results_for_date = """DELETE FROM `moz-fx-data-shared-prod.cloudflare_derived.os_usage_v1`
WHERE dte = {{ ds }} """

del_any_existing_op_sys_gold_errors_for_date = """DELETE FROM `moz-fx-data-shared-prod.cloudflare_derived.os_usage_errors_v1`
WHERE dte = {{ ds }} """

os_usage_stg_to_gold_query = """INSERT INTO `moz-fx-data-shared-prod.cloudflare_derived.os_usage_v1`
SELECT
CAST(Timestamps AS DATE) AS dte,
OS AS os,
Location AS location,
DeviceType AS device_type,
Share AS os_share,
Normalization AS normalization_type,
CAST(LastUpdatedTS as TIMESTAMP) AS last_updated_ts
FROM `moz-fx-data-shared-prod.cloudflare_derived.os_results_stg`"""

os_usage_errors_stg_to_gold_query = """INSERT INTO `moz-fx-data-shared-prod.cloudflare_derived.os_usage_errors_v1`
SELECT
CAST(StartTime AS DATE) AS dte,
Location AS location,
DeviceType AS device_type
FROM `moz-fx-data-shared-prod.cloudflare_derived.os_errors_stg`  """

# Define DAG
with DAG(
    "cloudflare_os_usage",
    default_args=default_args,
    catchup=False,
    doc_md=DOCS,
    schedule_interval="0 5 * * *",
    tags=TAGS,
) as dag:

    # Define OS usage task
    get_data = PythonOperator(
        task_id="get_os_usage_data",
        python_callable=get_os_usage_data,
        execution_timeout=timedelta(minutes=55),
    )

    load_results_to_bq_stg = GCSToBigQueryOperator(
        task_id="load_results_to_bq_stg",
        bucket=os_usg_configs["bucket"],
        destination_project_dataset_table="moz-fx-data-shared-prod.cloudflare_derived.os_results_stg",
        schema_fields=[
            {"name": "Timestamps", "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "OS", "type": "STRING", "mode": "NULLABLE"},
            {"name": "Location", "type": "STRING", "mode": "NULLABLE"},
            {"name": "DeviceType", "type": "STRING", "mode": "NULLABLE"},
            {"name": "Share", "type": "NUMERIC", "mode": "NULLABLE"},
            {"name": "ConfidenceLevel", "type": "STRING", "mode": "NULLABLE"},
            {"name": "AggrInterval", "type": "STRING", "mode": "NULLABLE"},
            {"name": "Normalization", "type": "STRING", "mode": "NULLABLE"},
            {"name": "LastUpdatedTS", "type": "TIMESTAMP", "mode": "NULLABLE"},
        ],
        source_format="CSV",
        source_objects=os_usg_configs["bucket"]
        + os_usg_configs["results_stg_gcs_fpth"] % "{{ ds }}",
        compression="NONE",
        create_disposition="CREATE_IF_NEEDED",
        skip_leading_rows=1,
        write_disposition="WRITE_TRUNCATE",
        gcp_conn_id=os_usg_configs["gcp_conn_id"],
        allow_jagged_rows=False,
    )

    load_errors_to_bq_stg = GCSToBigQueryOperator(
        task_id="load_errors_to_bq_stg",
        bucket=os_usg_configs["bucket"],
        destination_project_dataset_table="moz-fx-data-shared-prod.cloudflare_derived.os_errors_stg",
        schema_fields=[
            {"name": "StartTime", "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "EndTime", "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "Location", "type": "STRING", "mode": "NULLABLE"},
            {"name": "DeviceType", "type": "STRING", "mode": "NULLABLE"},
        ],
        source_format="CSV",
        source_objects=os_usg_configs["bucket"]
        + os_usg_configs["errors_stg_gcs_fpth"] % "{{ ds }}",
        compression="NONE",
        create_disposition="CREATE_IF_NEEDED",
        skip_leading_rows=1,
        write_disposition="WRITE_TRUNCATE",
        gcp_conn_id=os_usg_configs["gcp_conn_id"],
        allow_jagged_rows=False,
    )

    # This will delete anything if the DAG is ever run multiple times for the same date
    delete_bq_gold_res_for_date_if_any = BigQueryInsertJobOperator(
        task_id="delete_bq_gold_res_for_date_if_any",
        configuration={
            "query": del_any_existing_op_sys_gold_results_for_date,
            "useLegacySql": False,
        },
        project_id="moz-fx-data-shared-prod",
        gcp_conn_id=os_usg_configs["gcp_conn_id"],
    )

    # This will delete anything if the DAG is ever run multiple times for the same date
    delete_bq_gold_err_for_date_if_any = BigQueryInsertJobOperator(
        task_id="delete_bq_gold_err_for_date_if_any",
        configuration={
            "query": del_any_existing_op_sys_gold_errors_for_date,
            "useLegacySql": False,
        },
        project_id="moz-fx-data-shared-prod",
        gcp_conn_id=os_usg_configs["gcp_conn_id"],
    )

    load_results_to_bq_gold = BigQueryInsertJobOperator(
        task_id="load_results_to_bq_gold",
        configuration={
            "query": os_usage_stg_to_gold_query,
            "destinationTable": {
                "projectId": "moz-fx-data-shared-prod",
                "datasetId": "cloudflare_derived",
                "tableId": "os_usage_v1",
            },
            "createDisposition": "CREATE_NEVER",
            "writeDisposition": "WRITE_APPEND",
        },
        project_id="moz-fx-data-shared-prod",
        gcp_conn_id=os_usg_configs["gcp_conn_id"],
    )

    load_errors_to_bq_gold = BigQueryInsertJobOperator(
        task_id="load_errors_to_bq_gold",
        configuration={
            "query": os_usage_errors_stg_to_gold_query,
            "destinationTable": {
                "projectId": "moz-fx-data-shared-prod",
                "datasetId": "cloudflare_derived",
                "tableId": "os_usage_errors_v1",
            },
            "createDisposition": "CREATE_NEVER",
            "writeDisposition": "WRITE_APPEND",
        },
        project_id="moz-fx-data-shared-prod",
        gcp_conn_id=os_usg_configs["gcp_conn_id"],
    )

    # Copy the result files from staging path into archive path after they are processed
    archive_results = GCSToGCSOperator(
        task_id="archive_results",
        source_bucket=os_usg_configs["bucket"],
        source_object=os_usg_configs["results_stg_gcs_fpth"] % "{{ ds }}",
        destination_bucket=os_usg_configs["bucket"],
        destination_object=os_usg_configs["results_archive_gcs_fpth"] % "{{ ds }}",
        gcp_conn_id=os_usg_configs["gcp_conn_id"],
        exact_match=True,
    )

    # Copy the error files from staging path into archive path after they are processed
    archive_errors = GCSToGCSOperator(
        task_id="archive_errors",
        source_bucket=os_usg_configs["bucket"],
        source_object=os_usg_configs["errors_stg_gcs_fpth"] % "{{ ds }}",
        destination_bucket=os_usg_configs["bucket"],
        destination_object=os_usg_configs["errors_archive_gcs_fpth"] % "{{ ds }}",
        gcp_conn_id=os_usg_configs["gcp_conn_id"],
        exact_match=True,
    )

    # Delete the result file from the staging path
    del_results_from_gcs_stg = GCSDeleteObjectsOperator(
        task_id="del_results_from_gcs_stg",
        bucket_name=os_usg_configs["bucket"],
        objects=[os_usg_configs["results_stg_gcs_fpth"] % "{{ ds }}"],
        gcp_conn_id=os_usg_configs["gcp_conn_id"],
    )

    # Delete the error file from the staging path
    del_errors_from_gcs_stg = GCSDeleteObjectsOperator(
        task_id="del_errors_from_gcs_stg",
        bucket_name=os_usg_configs["bucket"],
        objects=[os_usg_configs["errors_stg_gcs_fpth"] % "{{ ds }}"],
        gcp_conn_id=os_usg_configs["gcp_conn_id"],
    )
    # Run QA checks
    run_os_qa_checks = EmptyOperator(task_id="run_os_qa_checks")

(
    get_data
    >> load_results_to_bq_stg
    >> delete_bq_gold_res_for_date_if_any
    >> load_results_to_bq_gold
    >> archive_results
    >> del_results_from_gcs_stg
)
(
    get_data
    >> load_errors_to_bq_stg
    >> delete_bq_gold_err_for_date_if_any
    >> load_errors_to_bq_gold
    >> archive_errors
    >> del_errors_from_gcs_stg
)
[del_results_from_gcs_stg, del_errors_from_gcs_stg] >> run_os_qa_checks
