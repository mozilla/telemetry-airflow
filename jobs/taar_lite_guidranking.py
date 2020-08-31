"""
This ETL job computes the installation rate of all addons and then
cross references against the whitelist to compute the total install
rate for all whitelisted addons.
"""


import click
import contextlib
import boto3
import tempfile
import json
import logging
from pyspark.sql import SparkSession
import os


OUTPUT_BUCKET = "telemetry-parquet"
OUTPUT_PREFIX = "taar/lite/"
OUTPUT_BASE_FILENAME = "guid_install_ranking"


def aws_env_credentials():
    """
    Load the AWS credentials from the enviroment
    """
    result = {
        "aws_access_key_id": os.environ.get("AWS_ACCESS_KEY_ID", None),
        "aws_secret_access_key": os.environ.get("AWS_SECRET_ACCESS_KEY", None),
    }
    logging.info(
        "Loading AWS credentials from enviroment: {}".format(str(result))
    )
    return result


@contextlib.contextmanager
def selfdestructing_path(dirname):
    import shutil

    yield dirname
    shutil.rmtree(dirname)


def extract_telemetry(spark):
    """ Load some training data from telemetry given a sparkContext
    """

    gs_url = "gs://moz-fx-data-derived-datasets-parquet/clients_daily/v6"
    parquetFile = spark.read.parquet(gs_url)
    # Use the parquet files to create a temporary view and then used
    # in SQL statements.
    parquetFile.createOrReplaceTempView("clients_daily")

    frame = spark.sql(
        """
    SELECT
        addon_row.addon_id as addon_guid,
        count(*) as install_count
    FROM
        (SELECT
            explode(active_addons) as addon_row
        FROM
            clients_daily
        WHERE
            channel='release' AND
            app_name='Firefox' and
            size(active_addons) > 0
        )
        GROUP BY addon_row.addon_id
    """
    )
    return frame


def transform(frame):
    """ Convert the dataframe to JSON and augment each record to
    include the install count for each addon.
    """

    def lambda_func(x):
        return (x.addon_guid, x.install_count)

    return dict(frame.rdd.map(lambda_func).collect())


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


def store_json_to_s3(json_data, base_filename, date, prefix, bucket):
    """Saves the JSON data to a local file and then uploads it to S3.

    Two copies of the file will get uploaded: one with as
    "<base_filename>.json"
    and the other as "<base_filename><YYYYMMDD>.json" for backup purposes.

    :param json_data: A string with the JSON content to write.
    :param base_filename: A string with the base name of the file to
                            use for saving locally and uploading to S3.
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


def load_s3(result_data, date, prefix, bucket):
    store_json_to_s3(
        json.dumps(result_data, indent=2),
        OUTPUT_BASE_FILENAME,
        date,
        prefix,
        bucket,
    )


@click.command()
@click.option("--date", required=True)
@click.option("--aws_access_key_id", required=True)
@click.option("--aws_secret_access_key", required=True)
@click.option("--bucket", default=OUTPUT_BUCKET)
@click.option("--prefix", default=OUTPUT_PREFIX)
def main(date, aws_access_key_id, aws_secret_access_key, bucket, prefix):

    # Clobber the AWS access credentials
    os.environ["AWS_ACCESS_KEY_ID"] = aws_access_key_id
    os.environ["AWS_SECRET_ACCESS_KEY"] = aws_secret_access_key

    spark = (
        SparkSession.builder.appName("taar_lite_ranking")
        .enableHiveSupport()
        .getOrCreate()
    )

    logging.info("Processing GUID install rankings")

    data_frame = extract_telemetry(spark)
    result_data = transform(data_frame)
    load_s3(result_data, date, prefix, bucket)


if __name__ == "__main__":
    main()
