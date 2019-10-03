# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.

"""
This ETL job computes the co-installation occurrence of white-listed
Firefox webextensions for a sample of the longitudinal telemetry dataset.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType
import click
import contextlib
import datetime as dt
import json
import logging
import os
import os.path
import tempfile

import boto3
from botocore.exceptions import ClientError

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

AMO_DUMP_BUCKET = "telemetry-parquet"
AMO_DUMP_KEY = "telemetry-ml/addon_recommender/addons_database.json"

AMO_WHITELIST_KEY = "telemetry-ml/addon_recommender/whitelist_addons_database.json"

OUTPUT_BUCKET = "telemetry-parquet"
OUTPUT_PREFIX = "taar/lite/"
OUTPUT_BASE_FILENAME = "guid_coinstallation"

MAIN_SUMMARY_PATH = "s3://telemetry-parquet/main_summary/v4/"
ONE_WEEK_AGO = (dt.datetime.now() - dt.timedelta(days=7)).strftime("%Y%m%d")


def aws_env_credentials():
    """
    Load the AWS credentials from the enviroment
    """
    result = {
        "aws_access_key_id": os.environ.get("AWS_ACCESS_KEY_ID", None),
        "aws_secret_access_key": os.environ.get("AWS_SECRET_ACCESS_KEY", None),
    }
    logging.info("Loading AWS credentials from enviroment: {}".format(str(result)))
    return result


def is_valid_addon(broadcast_amo_whitelist, guid, addon):
    """ Filter individual addons out to exclude, system addons,
    legacy addons, disabled addons, sideloaded addons.
    """
    return not (
        addon.is_system
        or addon.app_disabled
        or addon.type != "extension"
        or addon.user_disabled
        or addon.foreign_install
        or
        # make sure the amo_whitelist has been broadcast to worker nodes.
        guid not in broadcast_amo_whitelist.value
        or
        # Make sure that the Pioneer addon is explicitly
        # excluded
        guid == "pioneer-opt-in@mozilla.org"
    )


def get_addons_per_client(broadcast_amo_whitelist, users_df):
    """ Extracts a DataFrame that contains one row
    for each client along with the list of active add-on GUIDs.
    """

    # Create an add-ons dataset un-nesting the add-on map from each
    # user to a list of add-on GUIDs. Also filter undesired add-ons.
    rdd_list = users_df.rdd.map(
        lambda p: (
            p["client_id"],
            [
                addon_data.addon_id
                for addon_data in p["active_addons"]
                if is_valid_addon(
                    broadcast_amo_whitelist, addon_data.addon_id, addon_data
                )
            ],
        )
    )
    filtered_rdd = rdd_list.filter(lambda p: len(p[1]) > 1)
    df = filtered_rdd.toDF(["client_id", "addon_ids"])
    return df


def get_initial_sample(spark, thedate):
    """ Takes an initial sample from the longitudinal dataset
    (randomly sampled from main summary). Coarse filtering on:
    - number of installed addons (greater than 1)
    - corrupt and generally wierd telemetry entries
    - isolating release channel
    - column selection
    """
    # Could scale this up to grab more than what is in
    # longitudinal and see how long it takes to run.
    s3_url = "s3a://telemetry-parquet/clients_daily/v6/submission_date_s3={}".format(
        thedate
    )
    logging.info("Loading data from : {}".format(s3_url))
    df = (
        spark.read.parquet(s3_url)
        .where("active_addons IS NOT null")
        .where("size(active_addons) > 1")
        .where("channel = 'release'")
        .where("normalized_channel = 'release'")
        .where("app_name = 'Firefox'")
        .selectExpr("client_id", "active_addons")
    )
    logging.info("Initial dataframe loaded!")
    return df


def extract_telemetry(spark, thedate):
    """ load some training data from telemetry given a sparkContext
    """
    sc = spark.sparkContext

    # Define the set of feature names to be used in the donor computations.

    client_features_frame = get_initial_sample(spark, thedate)

    amo_white_list = load_amo_external_whitelist()
    logging.info("AMO White list loaded")

    broadcast_amo_whitelist = sc.broadcast(amo_white_list)
    logging.info("Broadcast AMO whitelist success")

    addons_info_frame = get_addons_per_client(
        broadcast_amo_whitelist, client_features_frame
    )
    logging.info("Filtered for valid addons only.")

    taar_training = (
        addons_info_frame.join(client_features_frame, "client_id", "inner")
        .drop("active_addons")
        .selectExpr("addon_ids as installed_addons")
    )
    logging.info("JOIN completed on TAAR training data")

    return taar_training


def key_all(a):
    """
    Return (for each Row) a two column set of Rows that contains each individual
    installed addon (the key_addon) as the first column and an array of guids of
    all *other* addons that were seen co-installed with the key_addon. Excluding
    the key_addon from the second column to avoid inflated counts in later aggregation.
    """
    return [(i, [b for b in a if b is not i]) for i in a]


def transform(longitudinal_addons):
    # Only for logging, not used, but may be interesting for later analysis.
    guid_set_unique = (
        longitudinal_addons.withColumn(
            "exploded", F.explode(longitudinal_addons.installed_addons)
        )
        .select("exploded")  # noqa: E501 - long lines
        .rdd.flatMap(lambda x: x)
        .distinct()
        .collect()
    )
    logging.info(
        "Number of unique guids co-installed in sample: " + str(len(guid_set_unique))
    )

    restructured = longitudinal_addons.rdd.flatMap(
        lambda x: key_all(x.installed_addons)
    ).toDF(["key_addon", "coinstalled_addons"])

    # Explode the list of co-installs and count pair occurrences.
    addon_co_installations = (
        restructured.select(
            "key_addon", F.explode("coinstalled_addons").alias("coinstalled_addon")
        )  # noqa: E501 - long lines
        .groupBy("key_addon", "coinstalled_addon")
        .count()
    )

    # Collect the set of coinstalled_addon, count pairs for each key_addon.
    combine_and_map_cols = F.udf(
        lambda x, y: (x, y),
        StructType([StructField("id", StringType()), StructField("n", LongType())]),
    )

    # Spark functions are sometimes long and unwieldy. Tough luck.
    # Ignore E128 and E501 long line errors
    addon_co_installations_collapsed = (
        addon_co_installations.select(  # noqa: E128
            "key_addon",
            combine_and_map_cols("coinstalled_addon", "count").alias(  # noqa: E501
                "id_n"
            ),
        )
        .groupby("key_addon")
        .agg(F.collect_list("id_n").alias("coinstallation_counts"))
    )
    logging.info(addon_co_installations_collapsed.printSchema())
    logging.info("Collecting final result of co-installations.")

    return addon_co_installations_collapsed


def load_s3(result_df, date, prefix, bucket):
    result_list = result_df.collect()
    result_json = {}

    for row in result_list:
        key_addon = row.key_addon
        coinstalls = row.coinstallation_counts
        value_json = {}
        for _id, n in coinstalls:
            value_json[_id] = n
        result_json[key_addon] = value_json

    store_json_to_s3(
        json.dumps(result_json, indent=2), OUTPUT_BASE_FILENAME, date, prefix, bucket
    )


def store_json_to_s3(json_data, base_filename, date, prefix, bucket):
    """Saves the JSON data to a local file and then uploads it to S3.

    Two copies of the file will get uploaded: one with as "<base_filename>.json"
    and the other as "<base_filename><YYYYMMDD>.json" for backup purposes.

    :param json_data: A string with the JSON content to write.
    :param base_filename: A string with the base name of the file to use for saving
        locally and uploading to S3.
    :param date: A date string in the "YYYYMMDD" format.
    :param prefix: The S3 prefix.
    :param bucket: The S3 bucket name.
    """

    tempdir = tempfile.mkdtemp()

    with selfdestructing_path(tempdir):
        JSON_FILENAME = "{}.json".format(base_filename)
        FULL_FILENAME = os.path.join(tempdir, JSON_FILENAME)
        with open(FULL_FILENAME, "w+") as json_file:
            json_file.write(json_data)

        archived_file_copy = "{}{}.json".format(base_filename, date)

        # Store a copy of the current JSON with datestamp.
        write_to_s3(FULL_FILENAME, archived_file_copy, prefix, bucket)
        write_to_s3(FULL_FILENAME, JSON_FILENAME, prefix, bucket)


def write_to_s3(source_file_name, s3_dest_file_name, s3_prefix, bucket):
    """Store the new json file containing current top addons per locale to S3.

    :param source_file_name: The name of the local source file.
    :param s3_dest_file_name: The name of the destination file on S3.
    :param s3_prefix: The S3 prefix in the bucket.
    :param bucket: The S3 bucket.
    """
    client = boto3.client(
        service_name="s3", region_name="us-west-2", **aws_env_credentials()
    )
    transfer = boto3.s3.transfer.S3Transfer(client)

    # Update the state in the analysis bucket.
    key_path = s3_prefix + s3_dest_file_name
    transfer.upload_file(source_file_name, bucket, key_path)


def load_amo_external_whitelist():
    """ Download and parse the AMO add-on whitelist.

    :raises RuntimeError: the AMO whitelist file cannot be downloaded or contains
                          no valid add-ons.
    """
    final_whitelist = []
    amo_dump = {}
    try:
        # Load the most current AMO dump JSON resource.
        s3 = boto3.client(service_name="s3", **aws_env_credentials())
        s3_contents = s3.get_object(Bucket=AMO_DUMP_BUCKET, Key=AMO_WHITELIST_KEY)
        amo_dump = json.loads(s3_contents["Body"].read().decode("utf-8"))
    except ClientError:
        logger.exception(
            "Failed to download from S3",
            extra=dict(
                bucket=AMO_DUMP_BUCKET, key=AMO_DUMP_KEY, **aws_env_credentials()
            ),
        )

    # If the load fails, we will have an empty whitelist, this may be problematic.
    for key, value in list(amo_dump.items()):
        addon_files = value.get("current_version", {}).get("files", {})
        # If any of the addon files are web_extensions compatible, it can be recommended.
        if any([f.get("is_webextension", False) for f in addon_files]):
            final_whitelist.append(value["guid"])

    if len(final_whitelist) == 0:
        raise RuntimeError("Empty AMO whitelist detected")

    return final_whitelist


@contextlib.contextmanager
def selfdestructing_path(dirname):
    import shutil

    yield dirname
    shutil.rmtree(dirname)


@click.command()
@click.option("--date", required=True)
@click.option("--aws_access_key_id", required=True)
@click.option("--aws_secret_access_key", required=True)
@click.option("--date", required=True)
@click.option("--bucket", default=OUTPUT_BUCKET)
@click.option("--prefix", default=OUTPUT_PREFIX)
def main(date, aws_access_key_id, aws_secret_access_key, bucket, prefix):
    thedate = date

    # Clobber the AWS access credentials
    os.environ["AWS_ACCESS_KEY_ID"] = aws_access_key_id
    os.environ["AWS_SECRET_ACCESS_KEY"] = aws_secret_access_key

    logging.info("Starting taarlite-guidguid")
    logging.info("Acquiring spark session")
    spark = SparkSession.builder.appName("taar_lite").getOrCreate()

    logging.info("Loading telemetry sample.")

    longitudinal_addons = extract_telemetry(spark, thedate)
    result_df = transform(longitudinal_addons)
    load_s3(result_df, thedate, prefix, bucket)

    spark.stop()


if __name__ == "__main__":
    main()
