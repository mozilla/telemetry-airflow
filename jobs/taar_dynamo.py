# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
This module replicates the scala script over at

https://github.com/mozilla/telemetry-batch-view/blob/1c544f65ad2852703883fe31a9fba38c39e75698/src/main/scala/com/mozilla/telemetry/views/HBaseAddonRecommenderView.scala

This should be invoked with something like this:

spark-submit \
    --master=spark://ec2-52-32-39-246.us-west-2.compute.amazonaws.com taar_dynamo.py \
    --date=20180218  \
    --region=us-west-2  \
    --table=taar_addon_data_20180206 \
    --prod-iam-role=arn:aws:iam::361527076523:role/taar-write-dynamodb-from-dev

"""

from datetime import date
from datetime import timedelta
from datetime import datetime
import hashlib
import os
from pprint import pprint
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql.functions import desc, row_number
import click
import dateutil.parser
import json
import botocore
import boto3
from botocore.exceptions import ClientError
from boto3.dynamodb.types import Binary as DynamoBinary
import logging
import time
import zlib

PATCH_DAYS = timedelta(days=2)

# We use the os and threading modules to generate a spark worker
# specific identity:w

MAX_RECORDS = 200
EMPTY_TUPLE = (0, 0, [], [])

BOTO_CFG = botocore.config.Config(retries={"max_attempts": 16})


def hash_telemetry_id(telemetry_id):
    """
        This hashing function is a reference implementation based on :
            https://phabricator.services.mozilla.com/D8311

    """
    return hashlib.sha256(telemetry_id.encode("utf8")).hexdigest()


def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""

    try:
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
    except Exception:
        # Some dates are invalid and won't serialize to
        # ISO format if the year is < 1601.  Yes. This actually
        # happens.  Force the date to epoch in this case
        return date(1970, 1, 1).isoformat()
    raise TypeError("Type %s not serializable" % type(obj))


def filterDateAndClientID(row_jstr):
    """
    Filter out any rows where the client_id is None or where the
    subsession_start_date is not a valid date
    """
    (row, jstr) = row_jstr
    try:
        assert row.client_id is not None
        assert row.client_id != ""
        some_date = dateutil.parser.parse(row.subsession_start_date)
        if some_date.year < 1970:
            return False
        return True
    except Exception:
        return False


def list_transformer(row_jsonstr):
    """
    We need to merge two elements of the row data - namely the
    client_id and the start_date into the main JSON blob.

    This is then packaged into a 4-tuple of :

    The first integer represents the number of records that have been
    pushed into DynamoDB.

    The second is the length of the JSON data list. This prevents us
    from having to compute the length of the JSON list unnecessarily.

    The third element of the tuple is the list of JSON data.

    The fourth element is a list of invalid JSON blobs.  We maintain
    this to be no more than 50 elements long.
    """
    (row, json_str) = row_jsonstr
    client_id = row.client_id
    start_date = dateutil.parser.parse(row.subsession_start_date)
    start_date = start_date.date()
    start_date = start_date.strftime("%Y%m%d")
    jdata = json.loads(json_str)
    jdata["client_id"] = client_id
    jdata["start_date"] = start_date

    # Filter out keys with an empty value
    jdata = {key: value for key, value in list(jdata.items()) if value}

    # We need to return a 4-tuple of values
    # (numrec_dynamodb_pushed, json_list_length, json_list, error_json)

    # These 4-tuples can be reduced in a map/reduce
    return (0, 1, [jdata], [])


def _write_batch(table, item_list, pause_time):
    logging.info("Pausing for {} seconds".format(pause_time))
    time.sleep(pause_time)
    with table.batch_writer(overwrite_by_pkeys=["client_id"]) as batch:
        for item in item_list:
            batch.put_item(Item=item)


class DynamoReducer(object):
    def __init__(
        self,
        region_name=None,
        table_name=None,
        aws_access_key_id=None,
        aws_secret_access_key=None,
    ):

        if region_name is None:
            region_name = "us-west-2"

        if table_name is None:
            table_name = "taar_addon_data"

        self._region_name = region_name
        self._table_name = table_name

        self._aws_access_key_id = aws_access_key_id
        self._aws_secret_access_key = aws_secret_access_key

    def hash_client_ids(self, data_tuple):
        """
        # Clobber the client_id by using sha256 hashes encoded as hex
        # Based on the js code in Fx

        """

        for item in data_tuple[2]:
            client_id = item["client_id"]
            item["client_id"] = hash_telemetry_id(client_id)

    def push_to_dynamo(self, data_tuple):  # noqa
        """
        This connects to DynamoDB and pushes records in `item_list` into
        a table.

        We accumulate a list of up to 50 elements long to allow debugging
        of write errors.
        """
        # Transform the data into something that DynamoDB will always
        # accept
        # Set TTL to 60 days from now
        ttl = int(time.time()) + 60 * 60 * 24 * 60

        self.hash_client_ids(data_tuple)

        item_list = [
            {
                "client_id": item["client_id"],
                "TTL": ttl,
                "json_payload": DynamoBinary(
                    zlib.compress(json.dumps(item, default=json_serial).encode("utf8"))
                ),
            }
            for item in data_tuple[2]
        ]

        # Clobber the enviroment variables for boto3 just prior to
        # making the dynamodb connection
        os.environ["AWS_ACCESS_KEY_ID"] = self._aws_access_key_id
        os.environ["AWS_SECRET_ACCESS_KEY"] = self._aws_secret_access_key
        # Obtain credentials from the singleton
        conn = boto3.resource(
            "dynamodb", region_name=self._region_name, config=BOTO_CFG
        )

        table = conn.Table(self._table_name)

        retry_exceptions = (
            "ProvisionedThroughputExceededException",
            "ThrottlingException",
        )

        retries = 0
        pause_time = 0
        while True:
            try:
                _write_batch(table, item_list, pause_time)
                logging.info(
                    "Wrote {:d} items to dynamo batch mode.".format(len(item_list))
                )
            except ClientError as err:
                if err.response["Error"]["Code"] not in retry_exceptions:
                    raise
                pause_time = 2 ** retries
                logging.info("Back-off set to %d seconds", pause_time)
                retries += 1

        return []

    def dynamo_reducer(self, list_a, list_b, force_write=False):
        """
        This function can be used to reduce tuples of the form in
        `list_transformer`. Data is merged and when MAX_RECORDS
        number of JSON blobs are merged, the list of JSON is batch written
        into DynamoDB.
        """
        new_list = [
            list_a[0] + list_b[0],
            list_a[1] + list_b[1],
            list_a[2] + list_b[2],
            list_a[3] + list_b[3],
        ]

        if new_list[1] >= MAX_RECORDS or force_write:
            error_blobs = self.push_to_dynamo(new_list)
            if len(error_blobs) > 0:
                # Gather up to maximum 50 error blobs
                new_list[3].extend(error_blobs[: 50 - new_list[1]])
                # Zero out the number of accumulated records
                new_list[1] = 0
                print("Captured {:d} errors".format(len(error_blobs)))
            else:
                # No errors during write process
                # Update number of records written to dynamo
                new_list[0] += new_list[1]
                # Zero out the number of accumulated records
                new_list[1] = 0
                # Clear out the accumulated JSON records
                new_list[2] = []

        return tuple(new_list)


def etl(
    spark,
    run_date,
    region_name,
    table_name,
    sample_rate,
    aws_access_key_id,
    aws_secret_access_key,
):
    """
    This function is responsible for extract, transform and load.

    Data is extracted from Parquet files in Amazon S3.
    Transforms and filters are applied to the data to create
    3-tuples that are easily merged in a map-reduce fashion.

    The 3-tuples are then loaded into DynamoDB using a map-reduce
    operation in Spark.
    """

    rdd = extract_transform(spark, run_date, sample_rate)

    result = load_rdd(
        region_name, table_name, rdd, aws_access_key_id, aws_secret_access_key
    )
    return result


def extract_transform(spark, run_date, sample_rate=0):
    currentDate = run_date
    currentDateString = currentDate.strftime("%Y%m%d")
    print("Processing %s" % currentDateString)

    # Get the data for the desired date out of parquet
    template = "gs://moz-fx-data-derived-datasets-parquet/main_summary/v4/submission_date_s3=%s"
    datasetForDate = spark.read.parquet(template % currentDateString)

    if sample_rate is not None and sample_rate != 0:
        print("Sample rate set to %0.9f" % sample_rate)
        datasetForDate = datasetForDate.sample(False, sample_rate)
    else:
        print("No sampling on dataset")

    print("Parquet data loaded")

    # Get the most recent (client_id, subsession_start_date) tuple
    # for each client since the main_summary might contain
    # multiple rows per client. We will use it to filter out the
    # full table with all the columns we require.

    clientShortList = datasetForDate.select(
        "client_id",
        "subsession_start_date",
        row_number()
        .over(Window.partitionBy("client_id").orderBy(desc("subsession_start_date")))
        .alias("clientid_rank"),
    )
    print("clientShortList selected")
    clientShortList = clientShortList.where("clientid_rank == 1").drop("clientid_rank")
    print("clientShortList selected")

    select_fields = [
        "client_id",
        "subsession_start_date",
        "subsession_length",
        "city",
        "locale",
        "os",
        "places_bookmarks_count",
        "scalar_parent_browser_engagement_tab_open_event_count",
        "scalar_parent_browser_engagement_total_uri_count",
        "scalar_parent_browser_engagement_unique_domains_count",
        "active_addons",
        "disabled_addons_ids",
    ]
    dataSubset = datasetForDate.select(*select_fields)
    print("datasetForDate select fields completed")

    # Join the two tables: only the elements in both dataframes
    # will make it through.
    clientsData = dataSubset.join(
        clientShortList, ["client_id", "subsession_start_date"]
    )

    print("clientsData join with client_id and subsession_start_date")

    # Convert the DataFrame to JSON and get an RDD out of it.
    subset = clientsData.select("client_id", "subsession_start_date")

    print("clientsData select of client_id and subsession_start_date completed")

    jsonDataRDD = clientsData.select(
        "city",
        "subsession_start_date",
        "subsession_length",
        "locale",
        "os",
        "places_bookmarks_count",
        "scalar_parent_browser_engagement_tab_open_event_count",
        "scalar_parent_browser_engagement_total_uri_count",
        "scalar_parent_browser_engagement_unique_domains_count",
        "active_addons",
        "disabled_addons_ids",
    ).toJSON()

    print("jsonDataRDD selected")
    rdd = subset.rdd.zip(jsonDataRDD)
    print("subset rdd has been zipped")

    # Filter out any records with invalid dates or client_id
    filtered_rdd = rdd.filter(filterDateAndClientID)
    print("rdd filtered by date and client_id")

    # Transform the JSON elements into a 4-tuple as per docstring
    merged_filtered_rdd = filtered_rdd.map(list_transformer)
    print("rdd has been transformed into tuples")

    return merged_filtered_rdd


def load_rdd(region_name, table_name, rdd, aws_access_key_id, aws_secret_access_key):
    # Apply a MapReduce operation to the RDD
    # Note that we're passing around the AWS keys here
    dynReducer = DynamoReducer(
        region_name, table_name, aws_access_key_id, aws_secret_access_key
    )

    reduction_output = rdd.reduce(dynReducer.dynamo_reducer)
    print("1st pass dynamo reduction completed")

    # Apply the reducer one more time to force any lingering
    # data to get pushed into DynamoDB.
    final_reduction_output = dynReducer.dynamo_reducer(
        reduction_output, EMPTY_TUPLE, force_write=True
    )
    return final_reduction_output


def run_etljob(
    spark,
    run_date,
    region_name,
    table_name,
    sample_rate,
    aws_access_key_id,
    aws_secret_access_key,
):

    reduction_output = etl(
        spark,
        run_date,
        region_name,
        table_name,
        sample_rate,
        aws_access_key_id,
        aws_secret_access_key,
    )
    report_data = (reduction_output[0], reduction_output[1])
    print("=" * 40)
    print(
        "%d records inserted to DynamoDB.\n%d records remaining in queue." % report_data
    )
    print("=" * 40)
    return reduction_output


class DynamoProxy:
    """
    This class tests to make sure we can read
    """

    def __init__(self, region, table):
        self._conn = boto3.resource("dynamodb", region_name=region, config=BOTO_CFG)
        self._table = self._conn.Table(table)

    def scan_page(self):
        response = self._table.scan()
        for item in response["Items"]:
            compressed_bytes = item["json_payload"].value
            json_byte_data = zlib.decompress(compressed_bytes)
            json_str_data = json_byte_data.decode("utf8")
            yield json.loads(json_str_data)

    def check_sentinel(self):
        client_id = "write_sentinel"
        response = self._table.get_item(Key={"client_id": client_id})
        compressed_bytes = response["Item"]["json_payload"].value
        json_byte_data = zlib.decompress(compressed_bytes)
        json_str_data = json_byte_data.decode("utf8")
        return json.loads(json_str_data)

    def inject_record(self):
        with self._table.batch_writer(overwrite_by_pkeys=["client_id"]) as batch:
            item = {"update_time": datetime.datetime.now().isoformat()}
            print("Writing: {}".format(item["update_time"]))
            payload = zlib.compress(
                json.dumps(item, default=json_serial).encode("utf8")
            )
            item = {"client_id": "write_sentinel", "json_payload": payload}
            batch.put_item(Item=item)

    def item_count(self):
        print("Approximate row count: {:d}".format(self._table.item_count))


@click.command()
@click.option("--date", required=True)  # YYYYMMDD
@click.option("--aws_access_key_id", required=True)
@click.option("--aws_secret_access_key", required=True)
@click.option("--region", default="us-west-2")
@click.option("--table", default="taar_addon_data_20180206")
@click.option("--sample-rate", default=0)
def main(date, aws_access_key_id, aws_secret_access_key, region, table, sample_rate):

    # Clobber the AWS access credentials
    os.environ["AWS_ACCESS_KEY_ID"] = aws_access_key_id
    os.environ["AWS_SECRET_ACCESS_KEY"] = aws_secret_access_key

    APP_NAME = "TaarDynamo"
    conf = SparkConf().setAppName(APP_NAME)
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    date_obj = datetime.strptime(date, "%Y%m%d") - PATCH_DAYS

    reduction_output = run_etljob(
        spark,
        date_obj,
        region,
        table,
        sample_rate,
        aws_access_key_id,
        aws_secret_access_key,
    )
    pprint(reduction_output)


if __name__ == "__main__":
    main()
