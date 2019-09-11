#!/usr/bin/env python

import argparse
import json
import logging
import tempfile
import urllib2

from datetime import datetime as dt, timedelta, date
from os import environ

from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.types import *

"""
Adapted from:
https://github.com/mozilla-services/data-pipeline/blob/0c94d328f243338d21bae360547c300ac1b82b12/reports/socorro_import/ImportCrashData.ipynb

Original source json data (not ndjson):
s3://crashstats-telemetry-crashes-prod-us-west-2/v1/crash_report

Original destination parquet data:
s3://telemetry-parquet/socorro_crash/v2/crash_date=20190801

This job now reads json from GCS, and writes parquet to GCS.

"""

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


def parse_args():
    parser = argparse.ArgumentParser(
        description="Write json socorro crash reports to parquet.")
    parser.add_argument(
        "--date", '-d', required=True,
        help="Date (ds_nodash in airflow) of data to process. E.g. 20190801.",
    )
    parser.add_argument(
        "--source-gcs-path", required=True,
        help="The source gcs path, without the date folder prefix. E.g. gs://moz-fx-data-prod-socorro/v1/crash_report",
    )
    parser.add_argument(
        "--dest-gcs-path", required=True,
        help="The destination gcs path, without version and date folder prefixes. E.g. gs://moz-fx-data-prod-socorro/socorro_crash_parquet",
    )
    parser.add_argument(
        "--num-partitions", type=int, default=10,
        help="Number of partitions to use when rewriting json to parquet.",
    )
    return parser.parse_args()

# We create the pyspark datatype for representing the crash data in spark. This is a slightly modified version of peterbe/crash-report-struct-code.
def create_struct(schema):
    """ Take a JSON schema and return a pyspark StructType of equivalent structure. """
    
    replace_definitions(schema, schema['definitions'])
    assert '$ref' not in str(schema), 're-write didnt work'
    
    struct = StructType()
    for row in get_rows(schema):
        struct.add(row)

    return struct


def replace_definitions(schema, definitions):
    """ Replace references in the JSON schema with their definitions."""

    if 'properties' in schema:
        for prop, meta in schema['properties'].items():
            replace_definitions(meta, definitions)
    elif 'items' in schema:
        if '$ref' in schema['items']:
            ref = schema['items']['$ref'].split('/')[-1]
            schema['items'] = definitions[ref]
            replace_definitions(schema['items'], definitions)
        else:
            replace_definitions(schema['items'], definitions)
    elif '$ref' in str(schema):
        err_msg = "Reference not found for schema: {}".format(str(schema))
        log.error(err_msg)
        raise ValueError(err_msg)


def get_rows(schema):
    """ Map the fields in a JSON schema to corresponding data structures in pyspark."""
    
    if 'properties' not in schema:
        err_msg = "Invalid JSON schema: properties field is missing."
        log.error(err_msg)
        raise ValueError(err_msg)
        
    for prop in sorted(schema['properties']):
        meta = schema['properties'][prop]
        if 'string' in meta['type']:
            logging.debug("{!r} allows the type to be String AND Integer".format(prop))
            yield StructField(prop, StringType(), 'null' in meta['type'])
        elif 'integer' in meta['type']:
            yield StructField(prop, IntegerType(), 'null' in meta['type'])
        elif 'boolean' in meta['type']:
            yield StructField(prop, BooleanType(), 'null' in meta['type'])
        elif meta['type'] == 'array' and 'items' not in meta:
            # Assuming strings in the array
            yield StructField(prop, ArrayType(StringType(), False), True)
        elif meta['type'] == 'array' and 'items' in meta:
            struct = StructType()
            for row in get_rows(meta['items']):
                struct.add(row)
            yield StructField(prop, ArrayType(struct), True)
        elif meta['type'] == 'object':
            struct = StructType()
            for row in get_rows(meta):
                struct.add(row)
            yield StructField(prop, struct, True)
        else:
            err_msg = "Invalid JSON schema: {}".format(str(meta)[:100])
            log.error(err_msg)
            raise ValueError(err_msg)


# First fetch from the primary source in s3 as per bug 1312006. We fall back to the github location if this is not available.
def fetch_schema():
    """ Fetch the crash data schema from an s3 location or github location. This
    returns the corresponding JSON schema in a python dictionary. """
    
    region = "us-west-2"
    bucket = "crashstats-telemetry-crashes-prod-us-west-2"
    key = "crash_report.json"
    fallback_url = "https://raw.githubusercontent.com/mozilla/socorro/master/socorro/schemas/crash_report.json"

    try:
        log.info("Fetching latest crash data schema from s3://{}/{}".format(bucket, key))

        # Use spark to pull schema file instead of boto since the dataproc hadoop configs only work with spark.
        # Note: only do this on small json files, since collect will bring the file onto the driver
        json_obj = spark.read.json("s3a://{}/{}".format(bucket, key), multiLine=True).toJSON().collect()
        resp = json.loads(json_obj[0])
    except Exception, e:
        log.warning(("Could not fetch schema from s3://{}/{}: {}\n"
                     "Fetching crash data schema from {}")
                    .format(bucket, key, e, fallback_url))
        resp = urllib2.urlopen(fallback_url)

    return resp


# Read crash data as json, convert it to parquet
def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days) + 1):
        yield (end_date - timedelta(n)).strftime("%Y%m%d")


def import_day(source_gcs_path, dest_gcs_path, d, schema, version, num_partitions):
    """Convert JSON data stored in an S3 bucket into parquet, indexed by crash_date."""

    log.info("Processing {}, started at {}".format(d, dt.utcnow()))
    cur_source_gcs_path = "{}/{}".format(source_gcs_path, d)
    cur_dest_gcs_path = "{}/v{}/crash_date={}".format(dest_gcs_path, version, d)

    df = spark.read.json(cur_source_gcs_path, schema=schema)
    df.repartition(num_partitions).write.parquet(cur_dest_gcs_path, mode="overwrite")


def backfill(start_date_yyyymmdd, schema, version):
    """ Import data from a start date to yesterday's date.
    Example:
        backfill("20160902", crash_schema, version)
    """
    start_date = dt.strptime(start_date_yyyymmdd, "%Y%m%d")
    end_date = dt.utcnow() - timedelta(1) # yesterday
    for d in daterange(start_date, end_date):
        try:
            import_day(d)
        except Exception as e:
            log.error(e)

if __name__ == "__main__":
    args = parse_args()
    target_date = args.date
    source_gcs_path = args.source_gcs_path
    dest_gcs_path = args.dest_gcs_path
    num_partitions = args.num_partitions

    # fetch and generate the schema
    schema_data = fetch_schema()
    crash_schema = create_struct(schema_data)
    version = schema_data.get('$target_version', 0)  # default to v0

    # process the data
    import_day(source_gcs_path, dest_gcs_path, target_date, crash_schema, version, num_partitions)
