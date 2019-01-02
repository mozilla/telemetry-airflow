# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from datetime import datetime, timedelta

import pytest

import boto3
from airflow.exceptions import AirflowSensorTimeout
from airflow.models import DAG, TaskInstance
from airflow.settings import Session
from airflow.utils.state import State
from moto import mock_s3
from plugins.s3fs_check_success import S3FSCheckSuccessSensor

DEFAULT_DATE = datetime(2019, 1, 1)


@pytest.fixture
def dag():
    return DAG(
        "test_dag", default_args={"owner": "airflow", "start_date": DEFAULT_DATE}
    )


@mock_s3
def test_single_partition_contains_success():
    bucket = "test"
    prefix = "dataset/v1/submission_date=20190101"

    client = boto3.client("s3")
    client.create_bucket(Bucket=bucket)
    client.put_object(Bucket=bucket, Body="", Key=prefix + "/_SUCCESS")

    sensor = S3FSCheckSuccessSensor(
        task_id="test_success", bucket=bucket, prefix=prefix, num_partitions=1
    )
    assert sensor.poke(None)


@mock_s3
def test_single_partition_not_contains_success():
    bucket = "test"
    prefix = "dataset/v1/submission_date=20190101"

    client = boto3.client("s3")
    client.create_bucket(Bucket=bucket)

    sensor = S3FSCheckSuccessSensor(
        task_id="test_failure", bucket=bucket, prefix=prefix, num_partitions=1
    )
    assert not sensor.poke(None)


@mock_s3
def test_single_partition_with_templates(dag):
    bucket = "test"
    prefix = "dataset/v1/submission_date=20190101"

    client = boto3.client("s3")
    client.create_bucket(Bucket=bucket)
    client.put_object(Bucket=bucket, Body="", Key=prefix + "/part=1/_SUCCESS")
    client.put_object(Bucket=bucket, Body="", Key=prefix + "/part=2/garbage")

    sensor_success = S3FSCheckSuccessSensor(
        task_id="test_success_template",
        bucket=bucket,
        prefix="dataset/v1/submission_date={{ ds_nodash }}/part=1",
        num_partitions=1,
        poke_interval=1,
        timeout=2,
        dag=dag,
    )
    sensor_failure = S3FSCheckSuccessSensor(
        task_id="test_failure_template",
        bucket=bucket,
        prefix="dataset/v1/submission_date={{ ds_nodash }}/part=2",
        num_partitions=1,
        poke_interval=1,
        timeout=2,
        dag=dag,
    )

    # execute everything for templating to work
    sensor_success.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)
    with pytest.raises(AirflowSensorTimeout):
        sensor_failure.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

    session = Session()
    tis = session.query(TaskInstance).filter(
        TaskInstance.dag_id == dag.dag_id, TaskInstance.execution_date == DEFAULT_DATE
    )
    session.close()

    count = 0
    for ti in tis:
        if ti.task_id == "test_success_template":
            assert ti.state == State.SUCCESS
        elif ti.task_id == "test_failure_template":
            assert ti.state == State.FAILED
        else:
            assert False
        count += 1
    assert count == 2


@mock_s3
def test_partitions_contain_success():
    bucket = "test"
    prefix = "dataset/v1/submission_date=20190101"

    client = boto3.client("s3")
    client.create_bucket(Bucket=bucket)
    client.put_object(Bucket=bucket, Body="", Key=prefix + "/part=1/_SUCCESS")
    client.put_object(Bucket=bucket, Body="", Key=prefix + "/part=2/_SUCCESS")
    client.put_object(Bucket=bucket, Body="", Key=prefix + "/part=3/_SUCCESS")

    sensor = S3FSCheckSuccessSensor(
        task_id="test_success", bucket=bucket, prefix=prefix, num_partitions=3
    )
    assert sensor.poke(None)


@mock_s3
def test_partitions_contain_partial_success():
    bucket = "test"
    prefix = "dataset/v1/submission_date=20190101"

    client = boto3.client("s3")
    client.create_bucket(Bucket=bucket)
    client.put_object(Bucket=bucket, Body="", Key=prefix + "/part=1/_SUCCESS")
    client.put_object(Bucket=bucket, Body="", Key=prefix + "/part=2/garbage")
    client.put_object(Bucket=bucket, Body="", Key=prefix + "/part=3/_SUCCESS")

    sensor = S3FSCheckSuccessSensor(
        task_id="test_failure", bucket=bucket, prefix=prefix, num_partitions=3
    )
    assert not sensor.poke(None)
