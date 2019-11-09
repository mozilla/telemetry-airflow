"""Disable XCOM in GoogleCloudStorageToS3Operator."""

from airflow.contrib.operators import gcs_to_s3

class GoogleCloudStorageToS3Operator(gcs_to_s3.GoogleCloudStorageToS3Operator):
    def execute(self, context):
        # don't return files moved because the list may be too long for XCOM.
        super(GoogleCloudStorageToS3Operator, self).execute(context)
