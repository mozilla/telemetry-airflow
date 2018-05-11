import boto3
import logging
import re
from airflow.exceptions import AirflowException
from airflow.operators.email_operator import EmailOperator
from airflow.utils.decorators import apply_defaults
from difflib import unified_diff
from os import environ


class EmailSchemaChangeOperator(EmailOperator):
    """
    Execute a Spark job on EMR and send email on schema changes.

    :param key_prefix: The s3 prefix within bucket containing schemas.
    :type key_prefix: string

    :param bucket: The name of the s3 bucket containing schemas.
    :type bucket: string

    :param latest_schema_key: The s3 key after key_prefix for the latest schema.
    :type latest_schema_key: string

    :param previous_schema_key: The s3 key after key_prefix for the previous schema.
    :type previous_schema_key: string

    And all EmailOperator params. The diff will be appended to html_content.
    """
    template_fields = EmailOperator.template_fields + (
        'bucket',
        'key_prefix',
        'latest_schema_key',
        'previous_schema_key',
    )

    # s3 buckets from env
    private_output_bucket = environ['PRIVATE_OUTPUT_BUCKET']
    public_output_bucket = environ['PUBLIC_OUTPUT_BUCKET']

    @apply_defaults
    def __init__(self, key_prefix, bucket='{{ private_output_bucket }}',
                 latest_schema_key='{{ ds_nodash }}',
                 previous_schema_key='{{ yesterday_ds_nodash }}',
                 html_content='schema diff between {{ yesterday_ds }} and {{ ds }}:<br><br>',
                 subject='Airflow: Schema change between {{ yesterday_ds }} and {{ ds }}: {{ ti }}',
                 *args, **kwargs):
        super(EmailSchemaChangeOperator, self).__init__(
                subject=subject, html_content=html_content, *args, **kwargs)
        self.bucket = bucket
        self.key_prefix = key_prefix
        self.latest_schema_key = latest_schema_key
        self.previous_schema_key = previous_schema_key

    def execute(self, context):
        client = boto3.client('s3')
        try:
            previous = client.get_object(
                Key=self.key_prefix + self.previous_schema_key,
                Bucket=self.bucket)['Body'].read().splitlines(True)
        except client.exceptions.NoSuchKey:
            logging.info('previous_schema not found, skipping change detection')
            return  # if there's no previous schema, don't try to compare
        latest = client.get_object(
            Key=self.key_prefix + self.latest_schema_key,
            Bucket=self.bucket)['Body'].read().splitlines(True)
        diff = list(unified_diff(previous, latest))
        if diff:
            logging.info('schema change detected, sending email')
            self.html_content += ''.join(diff).replace('\n', '<br>')
            super(EmailSchemaChangeOperator, self).execute(context)
        else:
            logging.info('no schema change detected')
