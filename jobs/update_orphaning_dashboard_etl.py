#!/usr/bin/env python
# coding: utf-8

import argparse
import boto3
import datetime as dt
import json
import re
from urllib.request import urlopen

from pyspark.sql.functions import udf
from pyspark.sql.session import SparkSession
from pyspark.sql.types import *
from pyspark.sql.types import *
from pyspark.sql import functions as F

from google.cloud import bigquery

"""
This job generates JSON data files used by the Firefox Application Update Out Of Date dashboard:
https://telemetry.mozilla.org/update-orphaning/

It was adapted from an ATMO notebook: https://github.com/mozilla/mozilla-reports/blob/80c9bd6468877e1df1f48208c7c99266bedb9896/projects/app_update_out_of_date.kp/orig_src/app_update_out_of_date.ipynb
"""
# Get the time when this job started.
start_time = dt.datetime.now()
print("Start: " + str(start_time.strftime("%Y-%m-%d %H:%M:%S")))

spark = SparkSession.builder.appName('app-update-out-of-date').getOrCreate()
sqlContext = spark

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--run-date", '-d', required=True,
        help="Date (ds_nodash in airflow) of processing run. E.g. 2019-08-01.",
    )
    parser.add_argument(
        "--gcs-bucket", required=True,
        help="GCS bucket for intermediate aggregation table dump and results.",
    )
    parser.add_argument(
        "--gcs-prefix", required=True,
        help="GCS prefix for intermediate aggregation table dump and results.",
    )
    parser.add_argument("--s3-output-bucket", required=True)
    parser.add_argument("--s3-output-path", required=True)
    parser.add_argument("--aws-access-key-id", required=True)
    parser.add_argument("--aws-secret-access-key", required=True)
    return parser.parse_args()

args = parse_args()
target_date = args.run_date


# Uncomment out the following two lines and adjust |today_str| as necessary to run manually without uploading the json.
today_str = target_date

channel_to_process = "release"
min_version = 42
up_to_date_releases = 2
weeks_of_subsession_data = 12
min_update_ping_count = 4
min_subsession_hours = 2
min_subsession_seconds = min_subsession_hours * 60 * 60


today = dt.datetime.strptime(today_str, "%Y%m%d").date()

# MON = 0, SAT = 5, SUN = 6 -> SUN = 0, MON = 1, SAT = 6
day_index = (today.weekday() + 1) % 7
# Filename used to save the report's JSON
report_filename = (today - dt.timedelta(day_index)).strftime("%Y%m%d")
# Maximum report date which is the previous Saturday
max_report_date = today - dt.timedelta(7 + day_index - 6)
# Suffix of the longitudinal datasource name to use
longitudinal_suffix = max_report_date.strftime("%Y%m%d")
# String used in the SQL queries to limit records to the maximum report date.
# Since the queries use less than this is the day after the previous Saturday.
max_report_date_sql = (max_report_date + dt.timedelta(days=1)).strftime("%Y-%m-%d")
# The Sunday prior to the last Saturday
min_report_date = max_report_date - dt.timedelta(days=6)
# String used in the SQL queries to limit records to the minimum report date
# Since the queries use greater than this is six days prior to the previous Saturday.
min_report_date_sql = min_report_date.strftime("%Y-%m-%d")
# Date used to limit records to the number of weeks specified by
# weeks_of_subsession_data prior to the maximum report date
min_subsession_date = max_report_date - dt.timedelta(weeks=weeks_of_subsession_data)
# Date used to compute the latest version from firefox_history_major_releases.json
latest_ver_date_str = (max_report_date - dt.timedelta(days=7)).strftime("%Y-%m-%d")

print("max_report_date     : " + max_report_date.strftime("%Y%m%d"))
print("max_report_date_sql : " + max_report_date_sql)
print("min_report_date     : " + min_report_date.strftime("%Y%m%d"))
print("min_report_date_sql : " + min_report_date_sql)
print("min_subsession_date : " + min_subsession_date.strftime("%Y%m%d"))
print("report_filename     : " + report_filename)
print("latest_ver_date_str : " + latest_ver_date_str)

##################################### BEGIN LONGITUDINAL SHIM ##########################################################
GCS_TABLE_DUMP_PATH = f"gs://{args.gcs_bucket}/{args.gcs_prefix}/longitudinal-shim-dump/"
AGGREGATION_TABLE_PROJECT = "moz-fx-data-derived-datasets"
AGGREGATION_TABLE_DATASET = "analysis"
AGGREGATION_TABLE_NAME = "out_of_date_longitudinal_shim"

def longitudinal_shim_aggregate(date_from, date_to, destination_project, destination_dataset, destination_table):
    """Aggregate selected metrics to destination table.

       Until full GCP backfill is completed, we have to union `telemetry.main` table with
       `moz-fx-data-shared-prod.static.main_1pct_backfill` in the query. The latter contains a sample of main pings
       having sample_id=42 from '2019-01-01' to '2019-09-01'."""

    bq = bigquery.Client()

    longitudinal_shim_sql = f"""
    WITH
        main_sample_1pct AS (
        SELECT
            submission_timestamp,
            client_id,
            struct(cast(environment.build.version as STRING) as version, environment.build.application_name as application_name) as build,
            struct(struct(environment.settings.update.channel as channel, environment.settings.update.enabled as enabled) as update) as settings,
            payload.info.session_length,
            payload.info.profile_subsession_counter,
            environment.settings.update.enabled,
            payload.info.subsession_start_date,
            payload.info.subsession_length,
            payload.histograms.update_check_code_notify,
            payload.keyed_histograms.update_check_extended_error_notify,
            payload.histograms.update_check_no_update_notify,
            payload.histograms.update_not_pref_update_auto_notify,
            payload.histograms.update_ping_count_notify,
            payload.histograms.update_unable_to_apply_notify,
            payload.histograms.update_download_code_partial,
            payload.histograms.update_download_code_complete,
            payload.histograms.update_state_code_partial_stage,
            payload.histograms.update_state_code_complete_stage,
            payload.histograms.update_state_code_unknown_stage,
            payload.histograms.update_state_code_partial_startup,
            payload.histograms.update_state_code_complete_startup,
            payload.histograms.update_state_code_unknown_startup,
            payload.histograms.update_status_error_code_complete_startup,
            payload.histograms.update_status_error_code_partial_startup,
            payload.histograms.update_status_error_code_unknown_startup,
            payload.histograms.update_status_error_code_complete_stage,
            payload.histograms.update_status_error_code_partial_stage,
            payload.histograms.update_status_error_code_unknown_stage
        FROM
            `moz-fx-data-shared-prod.telemetry.main`
        WHERE
            sample_id = 42
            AND environment.build.version IS NOT NULL )
    SELECT
        client_id,
        struct(array_agg(build.version order by subsession_start_date desc, profile_subsession_counter desc limit 1000) as version, array_agg(build.application_name order by subsession_start_date desc, profile_subsession_counter desc limit 1000) as application_name) as build,
        struct(struct(array_agg(struct(settings.update.channel) order by subsession_start_date desc, profile_subsession_counter desc limit 1000) as channel, array_agg(struct(settings.update.enabled) order by subsession_start_date desc, profile_subsession_counter desc limit 1000) as enabled) as update) as settings,
        array_agg(session_length ignore nulls order by subsession_start_date desc, profile_subsession_counter desc limit 1000) as session_length,
        array_agg(enabled ignore nulls order by subsession_start_date desc, profile_subsession_counter desc limit 1000) as enabled,
        array_agg(subsession_start_date ignore nulls order by subsession_start_date desc, profile_subsession_counter desc limit 1000) as subsession_start_date,
        array_agg(subsession_length ignore nulls order by subsession_start_date desc, profile_subsession_counter desc limit 1000) as subsession_length,
        array_agg(struct(update_check_code_notify) order by subsession_start_date desc, profile_subsession_counter desc limit 1000) as update_check_code_notify,
        array_agg(struct(update_check_extended_error_notify) order by subsession_start_date desc, profile_subsession_counter desc limit 1000) as update_check_extended_error_notify,
        array_agg(struct(update_check_no_update_notify) order by subsession_start_date desc, profile_subsession_counter desc limit 1000) as update_check_no_update_notify,
        array_agg(struct(update_not_pref_update_auto_notify) order by subsession_start_date desc, profile_subsession_counter desc limit 1000) as update_not_pref_update_auto_notify,
        array_agg(struct(update_ping_count_notify) order by subsession_start_date desc, profile_subsession_counter desc limit 1000) as update_ping_count_notify,
        array_agg(struct(update_unable_to_apply_notify) order by subsession_start_date desc, profile_subsession_counter desc limit 1000) as update_unable_to_apply_notify,
        array_agg(struct(update_download_code_partial) order by subsession_start_date desc, profile_subsession_counter desc limit 1000) as update_download_code_partial,
        array_agg(struct(update_download_code_complete) order by subsession_start_date desc, profile_subsession_counter desc limit 1000) as update_download_code_complete,
        array_agg(struct(update_state_code_partial_stage) order by subsession_start_date desc, profile_subsession_counter desc limit 1000) as update_state_code_partial_stage,
        array_agg(struct(update_state_code_complete_stage) order by subsession_start_date desc, profile_subsession_counter desc limit 1000) as update_state_code_complete_stage,
        array_agg(struct(update_state_code_unknown_stage) order by subsession_start_date desc, profile_subsession_counter desc limit 1000) as update_state_code_unknown_stage,
        array_agg(struct(update_state_code_partial_startup) order by subsession_start_date desc, profile_subsession_counter desc limit 1000) as update_state_code_partial_startup,
        array_agg(struct(update_state_code_complete_startup) order by subsession_start_date desc, profile_subsession_counter desc limit 1000) as update_state_code_complete_startup,
        array_agg(struct(update_state_code_unknown_startup) order by subsession_start_date desc, profile_subsession_counter desc limit 1000) as update_state_code_unknown_startup,
        array_agg(struct(update_status_error_code_complete_startup) order by subsession_start_date desc, profile_subsession_counter desc limit 1000) as update_status_error_code_complete_startup,
        array_agg(struct(update_status_error_code_partial_startup) order by subsession_start_date desc, profile_subsession_counter desc limit 1000) as update_status_error_code_partial_startup,
        array_agg(struct(update_status_error_code_unknown_startup) order by subsession_start_date desc, profile_subsession_counter desc limit 1000) as update_status_error_code_unknown_startup,
        array_agg(struct(update_status_error_code_complete_stage) order by subsession_start_date desc, profile_subsession_counter desc limit 1000) as update_status_error_code_complete_stage,
        array_agg(struct(update_status_error_code_partial_stage) order by subsession_start_date desc, profile_subsession_counter desc limit 1000) as update_status_error_code_partial_stage,
        array_agg(struct(update_status_error_code_unknown_stage) order by subsession_start_date desc, profile_subsession_counter desc limit 1000) as update_status_error_code_unknown_stage
    FROM
        main_sample_1pct
    WHERE
        DATE(submission_timestamp)>='{date_from}'
        AND DATE(submission_timestamp)<='{date_to}'
    GROUP BY
        client_id
    """

    dataset_id = destination_dataset
    table_ref = bq.dataset(dataset_id, project=destination_project).table(destination_table)
    job_config = bigquery.QueryJobConfig()
    job_config.destination = table_ref
    job_config.write_disposition = 'WRITE_TRUNCATE'

    print("Query is: " + longitudinal_shim_sql)
    print("Starting BigQuery aggregation to destination table " + str(table_ref) + "...")
    query_job = bq.query(longitudinal_shim_sql, job_config=job_config)
    # Wait for query execution
    query_job.result()
    print("BigQuery aggregation finished")


def longitudinal_shim_transform(project, dataset, table):
    """Transform preaggregated dataset to match legacy Longitudinal schema and make it available as
     `longitudinal_shim` in SQL context.
    """
    # BigQuery Storage API can't handle the schema and queries (`out_of_date_details_sql`) we have here
    # As a workaround, we'll load AVRO-exported dump to Spark
    print("Exporting BigQuery table as AVRO...")
    destination_uri = GCS_TABLE_DUMP_PATH + "*"

    job_config = bigquery.ExtractJobConfig()
    job_config.destination_format = "AVRO"

    bq = bigquery.Client()
    table_ref = bq.dataset(dataset, project=project).table(table)

    extract_job = bq.extract_table(
        table_ref,
        destination_uri,
        location="US",
        job_config=job_config
    )  
    extract_job.result()  # Waits for job to complete.

    print(
        "Exported {} to {}".format(table_ref, destination_uri)
    )

    longitudinal_shim_raw_from_avro=spark.read.format("avro").load(GCS_TABLE_DUMP_PATH)

    def flatten_settings_update(settings):
        channel_list = list(map(lambda x: x[0], settings["update"]["channel"]))
        enabled_list = list(map(lambda x: x[0], settings["update"]["enabled"]))
        return {"update": {"channel": channel_list, "enabled": enabled_list}}

    return_struct_type = StructType([StructField("update", StructType([StructField("channel", ArrayType(StringType())), StructField("enabled", ArrayType(BooleanType()))]))])
    udf_flatten_settings_update = udf(flatten_settings_update, return_struct_type)

    def fix_schema(raw_longitudinal_df):
        with_settings_fixed = raw_longitudinal_df.withColumn("settings_fixed", udf_flatten_settings_update("settings")).drop("settings").withColumnRenamed("settings_fixed", "settings")
        return with_settings_fixed



    longitudinal_shim_raw_from_avro_fixed = fix_schema(longitudinal_shim_raw_from_avro)




    def merge_keyed_count_histograms(histograms, histogram_name):
        res = {}
        n_hist = len(histograms)

        for i, histogram_struct in enumerate(histograms):
            histogram_array = histogram_struct[histogram_name]
            if histogram_array:
                for key, count_histogram_string in histogram_array:
                    if key not in res:
                        res[key] = [0]*n_hist
                    res[key][i]=json.loads(count_histogram_string)['values'].get('0', 0)

        return res

    merge_keyed_count_histograms_udf = F.udf(merge_keyed_count_histograms, MapType(StringType(), ArrayType(IntegerType())))

    def merge_keyed_count_histogram_col(df, col_name):
        return df.withColumn(col_name+"_merged", merge_keyed_count_histograms_udf(col_name, F.lit(col_name))).drop(col_name).withColumnRenamed(col_name+"_merged", col_name)

    def merge_enumerated_histograms(histograms, histogram_name, n_values):
        res = []
        all_null = True # needed to maintain compatibility with Longitudinal
        for histogram_string_struct in histograms:
            compacted_histogram = [0]*(int(n_values)+1)
            histogram_string = histogram_string_struct[histogram_name]
            if histogram_string:
                all_null = False
                values = json.loads(histogram_string).get('values', {})
                for key, value in values.items():
                    compacted_histogram[int(key)] = value
            res.append(compacted_histogram)
        if all_null:
            res = None
        return res
    merge_enumerated_histograms_udf = F.udf(merge_enumerated_histograms, ArrayType(ArrayType(IntegerType())))
    def merge_enumerated_histogram_col(df, col_name, n_values):
        return df.withColumn(col_name+"_merged", merge_enumerated_histograms_udf(col_name, F.lit(col_name), F.lit(n_values))).drop(col_name).withColumnRenamed(col_name+"_merged", col_name)
    def merge_enumerated_histogram_columns(df, cols_n_values):
        for col_name, n_values in cols_n_values:
            df = merge_enumerated_histogram_col(df, col_name, n_values)
        return df


    def merge_count_histograms(histograms, histogram_name):
        res = []
        all_null = True # needed to maintain compatibility with Longitudinal
        for histogram_string_struct in histograms:
            histogram_string = histogram_string_struct[histogram_name]
            if histogram_string:
                all_null = False
                histogram_value = json.loads(histogram_string)['values'].get('0', 0)
                res.append(histogram_value)
            else:
                res.append(0)
        if all_null:
            res = None
        return res
    merge_count_histograms_udf = F.udf(merge_count_histograms, ArrayType(IntegerType()))
    def merge_count_histogram_columns(df, cols):
        for col_name in cols:
            df = df.withColumn(col_name+"_merged", merge_count_histograms_udf(col_name, F.lit(col_name))).drop(col_name).withColumnRenamed(col_name+"_merged", col_name)
        return df


    with_keyed_count_histograms_merged = merge_keyed_count_histogram_col(longitudinal_shim_raw_from_avro_fixed, "update_check_extended_error_notify")

    with_enumerated_histograms_merged = merge_enumerated_histogram_columns(with_keyed_count_histograms_merged, [["update_check_code_notify", 50], ["update_download_code_partial", 50], ["update_download_code_complete", 50], ["update_state_code_partial_stage", 20], ["update_state_code_complete_stage", 20],["update_state_code_unknown_stage", 20],["update_state_code_partial_startup", 20],["update_state_code_complete_startup", 20],["update_state_code_unknown_startup", 20],["update_status_error_code_complete_startup", 100],["update_status_error_code_partial_startup", 100],["update_status_error_code_unknown_startup", 100],["update_status_error_code_complete_stage", 100],["update_status_error_code_partial_stage", 100],["update_status_error_code_unknown_stage", 100]])

    with_count_histograms_merged = merge_count_histogram_columns(with_enumerated_histograms_merged, ["update_check_no_update_notify","update_not_pref_update_auto_notify","update_ping_count_notify","update_unable_to_apply_notify"])

    longitudinal_shim_df = with_count_histograms_merged

    longitudinal_shim_df.registerTempTable("longitudinal_shim")


aggregation_to = max_report_date.strftime("%Y-%m-%d")
aggregation_from = (max_report_date - dt.timedelta(days=6*31)).strftime("%Y-%m-%d")

longitudinal_shim_aggregate(date_from=aggregation_from, date_to=aggregation_to,
                                destination_project=AGGREGATION_TABLE_PROJECT, 
                                destination_dataset=AGGREGATION_TABLE_DATASET, destination_table=AGGREGATION_TABLE_NAME)

longitudinal_shim_transform(project=AGGREGATION_TABLE_PROJECT, 
                                dataset=AGGREGATION_TABLE_DATASET, table=AGGREGATION_TABLE_NAME)

####################################### END LONGITUDINAL SHIM ##########################################################

# Get the latest Firefox version available based on the date.
def latest_version_on_date(date, major_releases):
    latest_date = u"1900-01-01"
    latest_ver = 0
    for version, release_date in major_releases.items():
        version_int = int(version.split(".")[0])
        if release_date <= date and release_date >= latest_date and version_int >= latest_ver:
            latest_date = release_date
            latest_ver = version_int

    return latest_ver

major_releases_json = urlopen("https://product-details.mozilla.org/1.0/firefox_history_major_releases.json").read()
major_releases = json.loads(major_releases_json)
latest_version = latest_version_on_date(latest_ver_date_str, major_releases)
earliest_up_to_date_version = str(latest_version - up_to_date_releases)

print("Latest Version: " + str(latest_version))


# Create a dictionary to store the general settings that will be written to a JSON file.
report_details_dict = {"latestVersion": latest_version,
                       "upToDateReleases": up_to_date_releases,
                       "minReportDate": min_report_date.strftime("%Y-%m-%d"),
                       "maxReportDate": max_report_date.strftime("%Y-%m-%d"),
                       "weeksOfSubsessionData": weeks_of_subsession_data,
                       "minSubsessionDate": min_subsession_date.strftime("%Y-%m-%d"),
                       "minSubsessionHours": min_subsession_hours,
                       "minSubsessionSeconds": min_subsession_seconds,
                       "minUpdatePingCount": min_update_ping_count}
report_details_dict


# Create the common SQL FROM clause.
# Note: using the parquet is as fast as using 'FROM longitudinal_vYYYMMDD'
# and it allows the query to go further back in time.

#longitudinal_from_sql = ("FROM longitudinal_v{} ").format(longitudinal_suffix)
# longitudinal_from_sql = ("FROM parquet.`s3://telemetry-parquet/longitudinal/v{}` ").format(longitudinal_suffix)
longitudinal_from_sql = ("FROM longitudinal_shim ")
longitudinal_from_sql


# Create the common build.version SQL WHERE clause.
build_version_where_sql = "(build.version[0] RLIKE '^[0-9]{2,3}\.0[\.0-9]*$' OR build.version[0] = '50.1.0')"
build_version_where_sql


# Create the remaining common SQL WHERE clause.
common_where_sql = (""
    "build.application_name[0] = 'Firefox' AND "
    "DATEDIFF(SUBSTR(subsession_start_date[0], 0, 10), '{}') >= 0 AND "
    "DATEDIFF(SUBSTR(subsession_start_date[0], 0, 10), '{}') < 0 AND "
    "settings.update.channel[0] = '{}'"
"").format(min_report_date_sql,
           max_report_date_sql,
           channel_to_process)
common_where_sql


# Create the SQL for the summary query.
summary_sql = (""
"SELECT "
    "COUNT(CASE WHEN build.version[0] >= '{}.' AND build.version[0] < '{}.' THEN 1 END) AS versionUpToDate, "
    "COUNT(CASE WHEN build.version[0] < '{}.' AND build.version[0] >= '{}.' THEN 1 END) AS versionOutOfDate, "
    "COUNT(CASE WHEN build.version[0] < '{}.' THEN 1 END) AS versionTooLow, "
    "COUNT(CASE WHEN build.version[0] > '{}.' THEN 1 END) AS versionTooHigh, "
    "COUNT(CASE WHEN NOT build.version[0] > '0' THEN 1 END) AS versionMissing "
"{} "
"WHERE "
    "{} AND "
    "{}"
"").format(str(latest_version - up_to_date_releases),
           str(latest_version + 2),
           str(latest_version - up_to_date_releases),
           str(min_version),
           str(min_version),
           str(latest_version + 2),
           longitudinal_from_sql,
           common_where_sql,
           build_version_where_sql)
summary_sql


# Run the summary SQL query.
summaryDF = sqlContext.sql(summary_sql)


# Create a dictionary to store the results from the summary query that will be written to a JSON file.
summary_dict = summaryDF.first().asDict()
summary_dict


# Create the SQL for the out of date details query.
# Only query for the columns and the records that are used to optimize
# for speed. Adding update_state_code_partial_stage and
# update_state_code_complete_stage increased the time it takes this
# notebook to run by 50 seconds when using 4 clusters.

# Creating a temporary table of the data after the filters have been
# applied and joining it with the original datasource to include
# other columns doesn't appear to speed up the process but it doesn't
# appear to slow it down either so all columns of interest are in this
# query.

out_of_date_details_sql = (""
"SELECT "
    "client_id, "
    "build.version, "
    "session_length, "
    "settings.update.enabled, "
    "subsession_start_date, "
    "subsession_length, "
    "update_check_code_notify, "
    "update_check_extended_error_notify, "
    "update_check_no_update_notify, "
    "update_not_pref_update_auto_notify, "
    "update_ping_count_notify, "
    "update_unable_to_apply_notify, "
    "update_download_code_partial, "
    "update_download_code_complete, "
    "update_state_code_partial_stage, "
    "update_state_code_complete_stage, "
    "update_state_code_unknown_stage, "
    "update_state_code_partial_startup, "
    "update_state_code_complete_startup, "
    "update_state_code_unknown_startup, "
    "update_status_error_code_complete_startup, "
    "update_status_error_code_partial_startup, "
    "update_status_error_code_unknown_startup, "
    "update_status_error_code_complete_stage, "
    "update_status_error_code_partial_stage, "
    "update_status_error_code_unknown_stage "
"{}"
"WHERE "
    "{} AND "
    "{} AND "
    "build.version[0] < '{}.' AND "
    "build.version[0] >= '{}.'"
"").format(longitudinal_from_sql,
           common_where_sql,
           build_version_where_sql,
           str(latest_version - up_to_date_releases),
           str(min_version))
out_of_date_details_sql


# Run the out of date details SQL query.
out_of_date_details_df = sqlContext.sql(out_of_date_details_sql)


# Create the RDD used to further restrict which clients are out of date
# to focus on clients that are of concern and potentially of concern.
out_of_date_details_rdd = out_of_date_details_df.rdd.cache()


# ### The next several cells are to find the clients that are "out of date, potentially of concern" so they can be excluded from the "out of date, of concern" clients.

# Create an RDD of out of date telemetry pings that have and don't have
# a previous telemetry ping with a version that is up to date along
# with a dictionary of the count of True and False.
def has_out_of_date_max_version_mapper(d):
    ping = d
    index = 0
    while (index < len(ping.version)):
        if ((ping.version[index] == "50.1.0" or
             p.match(ping.version[index])) and
            ping.version[index] > earliest_up_to_date_version):
            return False, ping
        index += 1

    return True, ping

# RegEx for a valid release versions except for 50.1.0 which is handled separately.
p = re.compile('^[0-9]{2,3}\\.0[\\.0-9]*$')

has_out_of_date_max_version_rdd = out_of_date_details_rdd.map(has_out_of_date_max_version_mapper).cache()
has_out_of_date_max_version_dict = has_out_of_date_max_version_rdd.countByKey()
has_out_of_date_max_version_dict


# Create an RDD of the telemetry pings that have a previous telemetry ping
# with a version that is up to date.
has_out_of_date_max_version_true_rdd = has_out_of_date_max_version_rdd.filter(lambda p: p[0] == True).values().cache()


# Create an RDD of out of date telemetry pings that have and have not
# sent an update telemtry ping for any version of Firefox along with a
# dictionary of the count of True and False.
def has_update_ping_mapper(d):
    ping = d
    if (ping.update_ping_count_notify is not None and
        (ping.update_check_code_notify is not None or
         ping.update_check_no_update_notify is not None)):
        return True, ping

    return False, ping

has_update_ping_rdd = has_out_of_date_max_version_true_rdd.map(has_update_ping_mapper).cache()
has_update_ping_dict = has_update_ping_rdd.countByKey()
has_update_ping_dict


# Create an RDD of the telemetry pings that have an update telemtry
# ping for any version of Firefox.
has_update_ping_true_rdd = has_update_ping_rdd.filter(lambda p: p[0] == True).values().cache()


# Create an RDD of out of date telemetry pings that have and have not
# ran this version of Firefox for more than the amount of seconds as
# specified by min_subsession_seconds along with a dictionary of the
# count of True and False.
def has_min_subsession_length_mapper(d):
    ping = d
    seconds = 0
    index = 0
    current_version = ping.version[0]
    while (seconds < min_subsession_seconds and
           index < len(ping.subsession_start_date) and
           index < len(ping.version) and
           ping.version[index] == current_version):
        try:
            date = dt.datetime.strptime(ping.subsession_start_date[index][:10],
                                        "%Y-%m-%d").date()
            if date < min_subsession_date:
                return False, ping

            seconds += ping.subsession_length[index]
            index += 1
        except: # catch *all* exceptions
            index += 1

    if seconds >= min_subsession_seconds:
        return True, ping

    return False, ping

has_min_subsession_length_rdd = has_update_ping_true_rdd.map(has_min_subsession_length_mapper).cache()
has_min_subsession_length_dict = has_min_subsession_length_rdd.countByKey()
has_min_subsession_length_dict


# Create an RDD of the telemetry pings that have ran this version of
# Firefox for more than the amount of seconds as specified by
# min_subsession_seconds.
has_min_subsession_length_true_rdd = has_min_subsession_length_rdd.filter(lambda p: p[0] == True).values().cache()


# Create an RDD of out of date telemetry pings that have and have not
# sent the minimum number of update pings as specified by
# min_update_ping_count for this version of Firefox along with a
# dictionary of the count of True and False.
def has_min_update_ping_count_mapper(d):
    ping = d
    index = 0
    update_ping_count_total = 0
    current_version = ping.version[0]
    while (update_ping_count_total < min_update_ping_count and
           index < len(ping.update_ping_count_notify) and
           index < len(ping.version) and
           ping.version[index] == current_version):

        pingCount = ping.update_ping_count_notify[index]
        # Is this an update ping or just a placeholder for the telemetry ping?
        if pingCount > 0:
            try:
                date = dt.datetime.strptime(ping.subsession_start_date[index][:10],
                                            "%Y-%m-%d").date()
                if date < min_subsession_date:
                    return False, ping

            except: # catch *all* exceptions
                index += 1
                continue

            # Is there also a valid update check code or no update telemetry ping?
            if (ping.update_check_code_notify is not None and
                len(ping.update_check_code_notify) > index):
                for code_value in ping.update_check_code_notify[index]:
                    if code_value > 0:
                        update_ping_count_total += pingCount
                        index += 1
                        continue

            if (ping.update_check_no_update_notify is not None and
                len(ping.update_check_no_update_notify) > index and
                ping.update_check_no_update_notify[index] > 0):
                update_ping_count_total += pingCount

        index += 1

    if update_ping_count_total < min_update_ping_count:
        return False, ping

    return True, ping

has_min_update_ping_count_rdd = has_min_subsession_length_true_rdd.map(has_min_update_ping_count_mapper).cache()
has_min_update_ping_count_dict = has_min_update_ping_count_rdd.countByKey()
has_min_update_ping_count_dict


# Create an RDD of the telemetry pings that have sent the minimum
# number of update pings as specified by min_update_ping_count.
has_min_update_ping_count_true_rdd = has_min_update_ping_count_rdd.filter(lambda p: p[0] == True).values().cache()


# Create an RDD of out of date telemetry pings that are supported and
# are not supported based on whether they have not received or have
# received the unsupported update xml for the last update check along
# with a dictionary of the count of True and False.
def is_supported_mapper(d):
    ping = d
    index = 0
    update_ping_count_total = 0
    current_version = ping.version[0]
    while (update_ping_count_total < min_update_ping_count and
           index < len(ping.update_ping_count_notify) and
           index < len(ping.version) and
           ping.version[index] == current_version):
        pingCount = ping.update_ping_count_notify[index]
        # Is this an update ping or just a placeholder for the telemetry ping?
        if pingCount > 0:
            # Is there also a valid update check code or no update telemetry ping?
            if (ping.update_check_code_notify is not None and
                len(ping.update_check_code_notify) > index and
                ping.update_check_code_notify[index][28] > 0):
                return False, ping

        index += 1
        
    return True, ping

is_supported_rdd = has_min_update_ping_count_true_rdd.map(is_supported_mapper).cache()
is_supported_dict = is_supported_rdd.countByKey()
is_supported_dict


# Create an RDD of the telemetry pings that are supported based on
# whether they have not received or have received the unsupported
# update xml for the last update check.
is_supported_true_rdd = is_supported_rdd.filter(lambda p: p[0] == True).values().cache()


# Create an RDD of out of date telemetry pings that have and don't have
# the ability to apply an update along with a dictionary of the count 
# of True and False.
def is_able_to_apply_mapper(d):
    ping = d
    index = 0
    current_version = ping.version[0]
    while (index < len(ping.update_ping_count_notify) and
           index < len(ping.version) and
           ping.version[index] == current_version):
        if ping.update_ping_count_notify[index] > 0:
            # Only check the last value for update_unable_to_apply_notify
            # to determine if the client is unable to apply.
            if (ping.update_unable_to_apply_notify is not None and
                ping.update_unable_to_apply_notify[index] > 0):
                return False, ping

            return True, ping

        index += 1

    raise ValueError("Missing update unable to apply value!")

is_able_to_apply_rdd = is_supported_true_rdd.map(is_able_to_apply_mapper).cache()
is_able_to_apply_dict = is_able_to_apply_rdd.countByKey()
is_able_to_apply_dict


# Create an RDD of the telemetry pings that have the ability to apply
# an update.
is_able_to_apply_true_rdd = is_able_to_apply_rdd.filter(lambda p: p[0] == True).values().cache()


# Create an RDD of out of date telemetry pings that have and don't have
# application update enabled via policy along with a dictionary of the
# count of True and False.
def has_update_enabled_mapper(d):
    ping = d
    index = 0
    current_version = ping.version[0]
    while (index < len(ping.update_ping_count_notify) and
           index < len(ping.version) and
           ping.version[index] == current_version):
        if ping.update_ping_count_notify[index] > 0:
            # If there is an update ping and settings.update.enabled has a value
            # for the same telemetry submission then use the value of
            # settings.update.enabled to determine whether app update is enabled.
            # This isn't 100% accurate because the update ping and the value for
            # settings.update.enabled are gathered at different times but it is
            # accurate enough for this report.
            if (ping.enabled is not None and
                ping.enabled[index] is False):
                return False, ping

            return True, ping

        index += 1

    raise ValueError("Missing update enabled value!")

has_update_enabled_rdd = is_able_to_apply_true_rdd.map(has_update_enabled_mapper).cache()
has_update_enabled_dict = has_update_enabled_rdd.countByKey()
has_update_enabled_dict


# ### The next several cells categorize the clients that are "out of date, of concern".

# Create a reference to the dictionary which will be written to the
# JSON that populates the web page data. This way the reference in the
# web page never changes. A reference is all that is needed since the
# dictionary is not modified.
of_concern_dict = has_update_enabled_dict


# Create an RDD of the telemetry pings that have the
# application.update.enabled preference set to True.
# 
# This RDD is created from the last "out of date, potentially of concern"
# RDD and it is named of_concern_true_rdd to simplify the addition of new code
# without having to modify consumers of the RDD.
of_concern_true_rdd = has_update_enabled_rdd.filter(lambda p: p[0] == True).values().cache()


# Create an RDD of out of date, of concern telemetry ping client
# versions along with a dictionary of the count of each version.
def by_version_mapper(d):
    ping = d
    return ping.version[0], ping

of_concern_by_version_rdd = of_concern_true_rdd.map(by_version_mapper)
of_concern_by_version_dict = of_concern_by_version_rdd.countByKey()
of_concern_by_version_dict


# Create an RDD of out of date, of concern telemetry ping update check
# codes along with a dictionary of the count of each update check code.
def check_code_notify_mapper(d):
    ping = d
    index = 0
    current_version = ping.version[0]
    while (index < len(ping.update_ping_count_notify) and
           index < len(ping.version) and
           ping.version[index] == current_version):
        if (ping.update_ping_count_notify[index] > 0 and
            ping.update_check_code_notify is not None):
            code_index = 0
            for code_value in ping.update_check_code_notify[index]:
                if code_value > 0:
                    return code_index, ping
                code_index += 1

            if (ping.update_check_no_update_notify is not None and
                ping.update_check_no_update_notify[index] > 0):
                return 0, ping

        index += 1

    return -1, ping

check_code_notify_of_concern_rdd = of_concern_true_rdd.map(check_code_notify_mapper)
check_code_notify_of_concern_dict = check_code_notify_of_concern_rdd.countByKey()
check_code_notify_of_concern_dict


# Create an RDD of out of date, of concern telemetry pings that had a
# general failure for the update check. The general failure codes are:
# * CHK_GENERAL_ERROR_PROMPT: 22
# * CHK_GENERAL_ERROR_SILENT: 23
check_code_notify_general_error_of_concern_rdd =     check_code_notify_of_concern_rdd.filter(lambda p: p[0] == 22 or p[0] == 23).values().cache()


# Create an RDD of out of date, of concern telemetry ping update check
# extended error values for the clients that had a general failure for
# the update check along with a dictionary of the count of the error
# values.
def check_ex_error_notify_mapper(d):
    ping = d
    current_version = ping.version[0]
    for index, version in enumerate(ping.version):
        if (ping.update_ping_count_notify[index] > 0 and
            ping.update_check_extended_error_notify is not None):
            for key_name in ping.update_check_extended_error_notify:
                if ping.update_check_extended_error_notify[key_name][index] > 0:
                    if version == current_version:
                        key_name = key_name[17:]
                        if len(key_name) == 4:
                            key_name = key_name[1:]
                        return int(key_name), ping
                    return -1, ping

    return -2, ping

check_ex_error_notify_of_concern_rdd = check_code_notify_general_error_of_concern_rdd.map(check_ex_error_notify_mapper)
check_ex_error_notify_of_concern_dict = check_ex_error_notify_of_concern_rdd.countByKey()
check_ex_error_notify_of_concern_dict


# Create an RDD of out of date, of concern telemetry ping update
# download codes along with a dictionary of the count of the codes.
def download_code_mapper(d):
    ping = d
    current_version = ping.version[0]
    for index, version in enumerate(ping.version):
        if ping.update_download_code_partial is not None:
            code_index = 0
            for code_value in ping.update_download_code_partial[index]:
                if code_value > 0:
                    if version == current_version:
                        return code_index, ping
                    return -1, ping
                code_index += 1

        if ping.update_download_code_complete is not None:
            code_index = 0
            for code_value in ping.update_download_code_complete[index]:
                if code_value > 0:
                    if version == current_version:
                        return code_index, ping
                    return -1, ping
                code_index += 1

    return -2, ping

download_code_of_concern_rdd = of_concern_true_rdd.map(download_code_mapper)
download_code_of_concern_dict = download_code_of_concern_rdd.countByKey()
download_code_of_concern_dict


# Create an RDD of out of date, of concern telemetry ping staged update
# state codes along with a dictionary of the count of the codes.
def state_code_stage_mapper(d):
    ping = d
    current_version = ping.version[0]
    for index, version in enumerate(ping.version):
        if ping.update_state_code_partial_stage is not None:
            code_index = 0
            for code_value in ping.update_state_code_partial_stage[index]:
                if code_value > 0:
                    if version == current_version:
                        return code_index, ping
                    return -1, ping
                code_index += 1

        if ping.update_state_code_complete_stage is not None:
            code_index = 0
            for code_value in ping.update_state_code_complete_stage[index]:
                if code_value > 0:
                    if version == current_version:
                        return code_index, ping
                    return -1, ping
                code_index += 1

        if ping.update_state_code_unknown_stage is not None:
            code_index = 0
            for code_value in ping.update_state_code_unknown_stage[index]:
                if code_value > 0:
                    if version == current_version:
                        return code_index, ping
                    return -1, ping
                code_index += 1

    return -2, ping

state_code_stage_of_concern_rdd = of_concern_true_rdd.map(state_code_stage_mapper).cache()
state_code_stage_of_concern_dict = state_code_stage_of_concern_rdd.countByKey()
state_code_stage_of_concern_dict


# Create an RDD of out of date, of concern telemetry pings that failed
# to stage an update.
# * STATE_FAILED: 12
state_code_stage_failed_of_concern_rdd =     state_code_stage_of_concern_rdd.filter(lambda p: p[0] == 12).values().cache()


# Create an RDD of out of date, of concern telemetry ping staged update
# state failure codes along with a dictionary of the count of the codes.
def state_failure_code_stage_mapper(d):
    ping = d
    current_version = ping.version[0]
    for index, version in enumerate(ping.version):
        if ping.update_status_error_code_partial_stage is not None:
            code_index = 0
            for code_value in ping.update_status_error_code_partial_stage[index]:
                if code_value > 0:
                    if version == current_version:
                        return code_index, ping
                    return -1, ping
                code_index += 1

        if ping.update_status_error_code_complete_stage is not None:
            code_index = 0
            for code_value in ping.update_status_error_code_complete_stage[index]:
                if code_value > 0:
                    if version == current_version:
                        return code_index, ping
                    return -1, ping
                code_index += 1

        if ping.update_status_error_code_unknown_stage is not None:
            code_index = 0
            for code_value in ping.update_status_error_code_unknown_stage[index]:
                if code_value > 0:
                    if version == current_version:
                        return code_index, ping
                    return -1, ping
                code_index += 1

    return -2, ping

state_failure_code_stage_of_concern_rdd = state_code_stage_failed_of_concern_rdd.map(state_failure_code_stage_mapper)
state_failure_code_stage_of_concern_dict = state_failure_code_stage_of_concern_rdd.countByKey()
state_failure_code_stage_of_concern_dict


# Create an RDD of out of date, of concern telemetry ping startup
# update state codes along with a dictionary of the count of the codes.
def state_code_startup_mapper(d):
    ping = d
    current_version = ping.version[0]
    for index, version in enumerate(ping.version):
        if ping.update_state_code_partial_startup is not None:
            code_index = 0
            for code_value in ping.update_state_code_partial_startup[index]:
                if code_value > 0:
                    if version == current_version:
                        return code_index, ping
                    return -1, ping
                code_index += 1

        if ping.update_state_code_complete_startup is not None:
            code_index = 0
            for code_value in ping.update_state_code_complete_startup[index]:
                if code_value > 0:
                    if version == current_version:
                        return code_index, ping
                    return -1, ping
                code_index += 1

        if ping.update_state_code_unknown_startup is not None:
            code_index = 0
            for code_value in ping.update_state_code_unknown_startup[index]:
                if code_value > 0:
                    if version == current_version:
                        return code_index, ping
                    return -1, ping
                code_index += 1

    return -2, ping

state_code_startup_of_concern_rdd = of_concern_true_rdd.map(state_code_startup_mapper).cache()
state_code_startup_of_concern_dict = state_code_startup_of_concern_rdd.countByKey()
state_code_startup_of_concern_dict


# Create an RDD of the telemetry pings that have ping startup update state code equal to 12.
state_code_startup_failed_of_concern_rdd =     state_code_startup_of_concern_rdd.filter(lambda p: p[0] == 12).values().cache()


# Create an RDD of out of date, of concern telemetry ping startup
# update state failure codes along with a dictionary of the count of the
# codes.
def state_failure_code_startup_mapper(d):
    ping = d
    current_version = ping.version[0]
    for index, version in enumerate(ping.version):
        if ping.update_status_error_code_partial_startup is not None:
            code_index = 0
            for code_value in ping.update_status_error_code_partial_startup[index]:
                if code_value > 0:
                    if version == current_version:
                        return code_index, ping
                    return -1, ping
                code_index += 1

        if ping.update_status_error_code_complete_startup is not None:
            code_index = 0
            for code_value in ping.update_status_error_code_complete_startup[index]:
                if code_value > 0:
                    if version == current_version:
                        return code_index, ping
                    return -1, ping
                code_index += 1

        if ping.update_status_error_code_unknown_startup is not None:
            code_index = 0
            for code_value in ping.update_status_error_code_unknown_startup[index]:
                if code_value > 0:
                    if version == current_version:
                        return code_index, ping
                    return -1, ping
                code_index += 1

    return -2, ping

state_failure_code_startup_of_concern_rdd = state_code_startup_failed_of_concern_rdd.map(state_failure_code_startup_mapper)
state_failure_code_startup_of_concern_dict = state_failure_code_startup_of_concern_rdd.countByKey()
state_failure_code_startup_of_concern_dict


# Create an RDD of out of date, of concern telemetry pings that have
# and have not received only no updates available during the update
# check for their current version of Firefox along with a dictionary
# of the count of values.
def has_only_no_update_found_mapper(d):
    ping = d
    if ping.update_check_no_update_notify is None:
        return False, ping

    current_version = ping.version[0]
    for index, version in enumerate(ping.version):
        if current_version != version:
            return True, ping

        if ping.update_ping_count_notify[index] > 0:
            # If there is an update ping and update_check_no_update_notify
            # has a value equal to 0 then the update check returned a
            # value other than no update found. This could be improved by
            # checking the check value for error conditions and ignoring
            # those codes and ignoring the check below for those cases.
            if (ping.update_check_no_update_notify[index] == 0):
                return False, ping

    return True, ping

has_only_no_update_found_rdd = of_concern_true_rdd.map(has_only_no_update_found_mapper).cache()
has_only_no_update_found_dict = has_only_no_update_found_rdd.countByKey()
has_only_no_update_found_dict


# Create an RDD of the telemetry pings that have not received only no updates
# available during the update check for their current version of Firefox.
has_only_no_update_found_false_rdd = has_only_no_update_found_rdd.filter(lambda p: p[0] == False).values().cache()


# Create an RDD of out of date, of concern telemetry pings that have and
# don't have any update download pings for their current version of
# Firefox along with a dictionary of the count of the values.
def has_no_download_code_mapper(d):
    ping = d
    current_version = ping.version[0]
    for index, version in enumerate(ping.version):
        if current_version != version:
            return True, ping

        if ping.update_download_code_partial is not None:
            for code_value in ping.update_download_code_partial[index]:
                if code_value > 0:
                    return False, ping

        if ping.update_download_code_complete is not None:
            for code_value in ping.update_download_code_complete[index]:
                if code_value > 0:
                    return False, ping

    return True, ping

has_no_download_code_rdd = has_only_no_update_found_false_rdd.map(has_no_download_code_mapper).cache()
has_no_download_code_dict = has_no_download_code_rdd.countByKey()
has_no_download_code_dict


# Create an RDD of the telemetry pings that don't have any update
# download pings for their current version of Firefox.
has_no_download_code_false_rdd = has_no_download_code_rdd.filter(lambda p: p[0] == False).values().cache()


# Create an RDD of out of date, of concern telemetry pings that have and
# don't have an update failure state for their current version of
# Firefox along with a dictionary of the count of the values.
def has_update_apply_failure_mapper(d):
    ping = d
    current_version = ping.version[0]
    for index, version in enumerate(ping.version):
        if current_version != version:
            return False, ping

        if (ping.update_state_code_partial_startup is not None and
            ping.update_state_code_partial_startup[index][12] > 0):
            return True, ping

        if (ping.update_state_code_complete_startup is not None and
            ping.update_state_code_complete_startup[index][12] > 0):
            return True, ping

    return False, ping

has_update_apply_failure_rdd = has_no_download_code_false_rdd.map(has_update_apply_failure_mapper)
has_update_apply_failure_dict = has_update_apply_failure_rdd.countByKey()
has_update_apply_failure_dict


# Create a reference to the dictionary which will be written to the
# JSON that populates the web page data. This way the reference in the
# web page never changes. A reference is all that is needed since the
# dictionary is not modified.
of_concern_categorized_dict = has_update_apply_failure_dict


# Create the JSON that will be written to a file for the report.
results_dict = {"reportDetails": report_details_dict,
                "summary": summary_dict,
                "hasOutOfDateMaxVersion": has_out_of_date_max_version_dict,
                "hasUpdatePing": has_update_ping_dict,
                "hasMinSubsessionLength": has_min_subsession_length_dict,
                "hasMinUpdatePingCount": has_min_update_ping_count_dict,
                "isSupported": is_supported_dict,
                "isAbleToApply": is_able_to_apply_dict,
                "hasUpdateEnabled": has_update_enabled_dict,
                "ofConcern": of_concern_dict,
                "hasOnlyNoUpdateFound": has_only_no_update_found_dict,
                "hasNoDownloadCode": has_no_download_code_dict,
                "hasUpdateApplyFailure": has_update_apply_failure_dict,
                "ofConcernCategorized": of_concern_categorized_dict,
                "ofConcernByVersion": of_concern_by_version_dict,
                "checkCodeNotifyOfConcern": check_code_notify_of_concern_dict,
                "checkExErrorNotifyOfConcern": check_ex_error_notify_of_concern_dict,
                "downloadCodeOfConcern": download_code_of_concern_dict,
                "stateCodeStageOfConcern": state_code_stage_of_concern_dict,
                "stateFailureCodeStageOfConcern": state_failure_code_stage_of_concern_dict,
                "stateCodeStartupOfConcern": state_code_startup_of_concern_dict,
                "stateFailureCodeStartupOfConcern": state_failure_code_startup_of_concern_dict}
results_json = json.dumps(results_dict, ensure_ascii=False)
# This job was previously running on Python2, which serialized boolean keys in a different way
# We're uppercasing booleans here to keep the output compatible with the dashboard
results_json = results_json.replace('"true"', '"True"').replace('"false"', '"False"')

# Save the output to be uploaded automatically once the job completes.
# The file will be stored at:
# * https://analysis-output.telemetry.mozilla.org/app-update/data/out-of-date/FILENAME
    
bucket = args.s3_output_bucket #"telemetry-public-analysis-2"
path = args.s3_output_path #"app-update/data/out-of-date/"
timestamped_s3_key = path + report_filename + ".json"
client = boto3.client('s3', 'us-west-2',
    aws_access_key_id=args.aws_access_key_id,
    aws_secret_access_key=args.aws_secret_access_key
)
client.put_object(Body=results_json, Bucket=bucket, Key=timestamped_s3_key)
print(f"Output file saved to: {bucket}/{timestamped_s3_key}")


# Get the time when this job ended.
end_time = dt.datetime.now()
print("End: " + str(end_time.strftime("%Y-%m-%d %H:%M:%S")))


# Get the elapsed time it took to run this job.
elapsed_time = end_time - start_time
print("Elapsed Seconds: " + str(int(elapsed_time.total_seconds())))
