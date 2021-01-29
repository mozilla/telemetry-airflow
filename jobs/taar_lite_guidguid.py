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
import bz2
import io
from google.cloud import storage, bigquery


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

AMO_DUMP_BUCKET = "taar_models"
AMO_WHITELIST_PREFIX = "addon_recommender"
AMO_WHITELIST_FNAME = "addons_database.json"


OUTPUT_BUCKET = "taar_models"
OUTPUT_PREFIX = "taar/lite"
OUTPUT_FILENAME = "guid_coinstallation.json"


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


def get_table(view):
    """Helper for determining what table underlies a user-facing view, since the Storage API can't read views."""
    bq = bigquery.Client()
    view = view.replace(":", ".")
    # partition filter is required, so try a couple options
    for partition_column in ["DATE(submission_timestamp)", "submission_date"]:
        try:
            job = bq.query(
                f"SELECT * FROM `{view}` WHERE {partition_column} = CURRENT_DATE",
                bigquery.QueryJobConfig(dry_run=True),
            )
            break
        except Exception:
            continue
    else:
        raise ValueError("could not determine partition column")
    assert len(job.referenced_tables) == 1, "View combines multiple tables"
    table = job.referenced_tables[0]
    return f"{table.project}:{table.dataset_id}.{table.table_id}"


def get_initial_sample(spark, ds_dash):
    """
    Get a DataFrame with a valid set of sample to base the next
    processing on.

    Sample is limited to submissions received since `date_from` and latest row per each client.

    Reference documentation is found here:

    Firefox Clients Daily telemetry table
    https://docs.telemetry.mozilla.org/datasets/batch_view/clients_daily/reference.html

    BUG 1485152: PR include active_addons to clients_daily table:
    https://github.com/mozilla/telemetry-batch-view/pull/490
    """
    df = (
        spark.read.format("bigquery")
        .option(
            "table",
            get_table("moz-fx-data-shared-prod.telemetry.clients_last_seen"),
        )
        .option("filter", f"submission_date = '{ds_dash}'")
        .load()
    )
    df.createOrReplaceTempView("tiny_clients_daily")

    df = spark.sql(
        f"""
        SELECT
            client_id,
            active_addons
        FROM
            tiny_clients_daily
        WHERE
            active_addons IS NOT null and
            size(active_addons) > 1 and
            channel = 'release' and
            normalized_channel = 'release' and
            app_name = 'Firefox'
         """
    )

    return df


def extract_telemetry(spark, thedate, bucket):
    """ load some training data from telemetry given a sparkContext
    """
    sc = spark.sparkContext

    # Define the set of feature names to be used in the donor computations.

    client_features_frame = get_initial_sample(spark, thedate)

    amo_white_list = load_amo_external_whitelist(bucket)
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
        "Number of unique guids co-installed in sample: "
        + str(len(guid_set_unique))
    )

    restructured = longitudinal_addons.rdd.flatMap(
        lambda x: key_all(x.installed_addons)
    ).toDF(["key_addon", "coinstalled_addons"])

    # Explode the list of co-installs and count pair occurrences.
    addon_co_installations = (
        restructured.select(
            "key_addon",
            F.explode("coinstalled_addons").alias("coinstalled_addon"),
        )  # noqa: E501 - long lines
        .groupBy("key_addon", "coinstalled_addon")
        .count()
    )

    # Collect the set of coinstalled_addon, count pairs for each key_addon.
    combine_and_map_cols = F.udf(
        lambda x, y: (x, y),
        StructType(
            [StructField("id", StringType()), StructField("n", LongType())]
        ),
    )

    # Spark functions are sometimes long and unwieldy. Tough luck.
    # Ignore E128 and E501 long line errors
    addon_co_installations_collapsed = (
        addon_co_installations.select(  # noqa: E128
            "key_addon",
            combine_and_map_cols(
                "coinstalled_addon", "count"
            ).alias(  # noqa: E501
                "id_n"
            ),
        )
        .groupby("key_addon")
        .agg(F.collect_list("id_n").alias("coinstallation_counts"))
    )
    logging.info(addon_co_installations_collapsed.printSchema())
    logging.info("Collecting final result of co-installations.")

    return addon_co_installations_collapsed


def save_to_gcs(result_df, date, prefix, bucket):
    result_list = result_df.collect()
    result_json = {}

    for row in result_list:
        key_addon = row.key_addon
        coinstalls = row.coinstallation_counts
        value_json = {}
        for _id, n in coinstalls:
            value_json[_id] = n
        result_json[key_addon] = value_json

    store_json_to_gcs(
        bucket, prefix, OUTPUT_FILENAME, result_json, date.strftime("%Y%m%d"),
    )


def store_json_to_gcs(
    bucket, prefix, filename, json_obj, iso_date_str, compress=True
):
    """Saves the JSON data to a local file and then uploads it to GCS.

    Two copies of the file will get uploaded: one with as "<base_filename>.json"
    and the other as "<base_filename><YYYYMMDD>.json" for backup purposes.

    :param bucket: The GCS bucket name.
    :param prefix: The GCS prefix.
    :param filename: A string with the base name of the file to use for saving
        locally and uploading to GCS
    :param json_data: A string with the JSON content to write.
    :param date: A date string in the "YYYYMMDD" format.
    """
    byte_data = json.dumps(json_obj).encode("utf8")

    byte_data = bz2.compress(byte_data)
    logger.info(f"Compressed data is {len(byte_data)} bytes")

    client = storage.Client()
    bucket = client.get_bucket(bucket)
    simple_fname = f"{prefix}/{filename}.bz2"
    blob = bucket.blob(simple_fname)
    blob.chunk_size = 5 * 1024 * 1024  # Set 5 MB blob size
    print(f"Wrote out {simple_fname}")
    blob.upload_from_string(byte_data)
    long_fname = f"{prefix}/{filename}.{iso_date_str}.bz2"
    blob = bucket.blob(long_fname)
    blob.chunk_size = 5 * 1024 * 1024  # Set 5 MB blob size
    print(f"Wrote out {long_fname}")
    blob.upload_from_string(byte_data)


def read_from_gcs(fname, prefix, bucket):
    with io.BytesIO() as tmpfile:
        client = storage.Client()
        bucket = client.get_bucket(bucket)
        simple_fname = f"{prefix}/{fname}.bz2"
        blob = bucket.blob(simple_fname)
        blob.download_to_file(tmpfile)
        tmpfile.seek(0)
        payload = tmpfile.read()
        payload = bz2.decompress(payload)
        return json.loads(payload.decode("utf8"))


def load_amo_external_whitelist(bucket):
    """ Download and parse the AMO add-on whitelist.

    :raises RuntimeError: the AMO whitelist file cannot be downloaded or contains
                          no valid add-ons.
    """
    final_whitelist = []
    amo_dump = read_from_gcs(
        AMO_WHITELIST_FNAME, AMO_WHITELIST_PREFIX, bucket
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
@click.option("--bucket", default=OUTPUT_BUCKET)
@click.option("--prefix", default=OUTPUT_PREFIX)
def main(date, bucket, prefix):
    thedate = dt.datetime.strptime(date, "%Y-%m-%d")

    logging.info("Starting taarlite-guidguid")
    logging.info("Acquiring spark session")
    spark = SparkSession.builder.appName("taar_lite").getOrCreate()

    logging.info("Loading telemetry sample.")

    longitudinal_addons = extract_telemetry(spark, date, bucket)
    result_df = transform(longitudinal_addons)
    save_to_gcs(result_df, thedate, prefix, bucket)

    spark.stop()


if __name__ == "__main__":
    main()
