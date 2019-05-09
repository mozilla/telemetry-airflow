import logging

import boto3
from airflow.exceptions import AirflowException
from airflow.operators import BaseOperator, BaseSensorOperator
from airflow.plugins_manager import AirflowPlugin


def check_s3fs_success(bucket, prefix, num_partitions):
    """Check the s3 filesystem for the existence of `_SUCCESS` files in dataset partitions.
    
    :prefix:            Bucket prefix of the table
    :num_partitions:    Number of expected partitions
    """
    s3 = boto3.resource("s3")
    objects = s3.Bucket(bucket).objects.filter(Prefix=prefix)
    success = set([obj.key for obj in objects if "_SUCCESS" in obj.key])
    logging.info("Found {n_success} files".format(n_success=len(success)))
    return len(success) >= num_partitions


class S3FSCheckSuccessOperator(BaseOperator):
    template_fields = ("prefix",)

    def __init__(self, bucket, prefix, num_partitions, *args, **kwargs):
        self.bucket = bucket
        self.prefix = prefix
        self.num_partitions = num_partitions
        super(S3FSCheckSuccessOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        logging.info(
            "Running check against s3:/{}/{} with {} partitions".format(
                self.bucket, self.prefix, self.num_partitions
            )
        )
        if not check_s3fs_success(self.bucket, self.prefix, self.num_partitions):
            raise ValueError("Wrong number of _SUCCESS files")


class S3FSCheckSuccessSensor(BaseSensorOperator):
    template_fields = ("prefix",)

    def __init__(self, bucket, prefix, num_partitions, *args, **kwargs):
        self.bucket = bucket
        self.prefix = prefix
        self.num_partitions = num_partitions
        super(S3FSCheckSuccessSensor, self).__init__(*args, **kwargs)

    def poke(self, context):
        logging.info(
            "Running check against s3:/{}/{} with {} partitions".format(
                self.bucket, self.prefix, self.num_partitions
            )
        )
        return check_s3fs_success(self.bucket, self.prefix, self.num_partitions)


class S3FSCheckSuccessPlugin(AirflowPlugin):
    name = "s3fs_check_success"
    operators = [S3FSCheckSuccessOperator, S3FSCheckSuccessSensor]
