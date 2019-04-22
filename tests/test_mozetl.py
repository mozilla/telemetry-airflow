import boto3
import pytest
from moto import mock_s3
from plugins.mozetl import generate_runner


@mock_s3
@pytest.mark.parametrize("module_name", ["mozetl", "custom"])
def test_generate_runner(module_name):
    bucket = "test-bucket"
    prefix = "test-prefix"
    conn = boto3.resource("s3")
    conn.create_bucket(Bucket=bucket)

    generate_runner(module_name, bucket, prefix)

    body = (
        conn.Object(bucket, "{}/{}_runner.py".format(prefix, module_name))
        .get()["Body"]
        .read()
        .decode("utf-8")
    )
    assert body.split("\n")[3] == "from {} import cli".format(module_name)
