# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import pytest

import boto3
from moto import mock_s3
from plugins.s3fs_check_success import S3FSCheckSuccessOperator


@mock_s3
def test_single_partition_contains_success():
    bucket = "test"
    prefix = "dataset/v1/submission_date=20190101"

    client = boto3.client("s3")
    client.create_bucket(Bucket=bucket)
    client.put_object(Bucket=bucket, Body="", Key=prefix + "/_SUCCESS")

    operator = S3FSCheckSuccessOperator(
        task_id="test_success", bucket=bucket, prefix=prefix, num_partitions=1
    )
    operator.execute(None)


@mock_s3
def test_single_partition_not_contains_success():
    bucket = "test"
    prefix = "dataset/v1/submission_date=20190101"

    client = boto3.client("s3")
    client.create_bucket(Bucket=bucket)

    operator = S3FSCheckSuccessOperator(
        task_id="test_failure", bucket=bucket, prefix=prefix, num_partitions=1
    )
    with pytest.raises(ValueError):
        operator.execute(None)
