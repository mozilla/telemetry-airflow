import pytest
from moto import mock_s3
from plugins.mozetl import generate_runner


@mock_s3
def test_generate_runner():
    module_name = "mozetl"
    bucket = "test-bucket"
    prefix = "test-prefix"
    conn = boto3.resource("s3")
    conn.create_bucket(Bucket=bucket)

    generate_runner(module_name, bucket, prefix)

    body = (
        conn.Object(bucket, "{}/{}_runner.py".format())
        .get()["Body"]
        .read()
        .decode("utf-8")
    )
    assert body.split("\n")[2] == "from mozetl import cli"
