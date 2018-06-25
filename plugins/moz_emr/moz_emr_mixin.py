from os import environ
import requests


class MozEmrMixin:
    DEFAULT_EMR_RELEASE = 'emr-5.13.0'

    template_fields = ('environment',)
    region = environ['AWS_REGION']
    key_name = environ['EMR_KEY_NAME']
    flow_role = environ['EMR_FLOW_ROLE']
    service_role = environ['EMR_SERVICE_ROLE']
    instance_type = environ['EMR_INSTANCE_TYPE']
    spark_bucket = environ['SPARK_BUCKET']
    airflow_bucket = environ['AIRFLOW_BUCKET']
    private_output_bucket = environ['PRIVATE_OUTPUT_BUCKET']
    public_output_bucket = environ['PUBLIC_OUTPUT_BUCKET']

    @staticmethod
    def get_jobflow_args(owner, instance_count, job_name="Default Job Name",
                         release_label=DEFAULT_EMR_RELEASE, keep_alive=False):

        config_url = (
            'https://s3-{}.amazonaws.com/{}/configuration/configuration.json'
            .format(MozEmrMixin.region, MozEmrMixin.spark_bucket)
        )

        return {
            "Name": job_name,
            "ReleaseLabel": release_label,
            "JobFlowRole": MozEmrMixin.flow_role,
            "ServiceRole": MozEmrMixin.service_role,
            "Applications": [{'Name': 'Spark'}, {'Name': 'Hive'}],
            "VisibleToAllUsers": True,
            "Configurations": requests.get(config_url).json(),
            "LogUri": (
                's3://{}/logs/{}/{}/'
                .format(MozEmrMixin.airflow_bucket,
                        owner,
                        job_name)
            ),
            "Instances": {
                'MasterInstanceType': MozEmrMixin.instance_type,
                'SlaveInstanceType': MozEmrMixin.instance_type,
                'InstanceCount': instance_count,
                'Ec2KeyName': MozEmrMixin.key_name,
                'KeepJobFlowAliveWhenNoSteps': keep_alive,
            },
            "BootstrapActions": [{
                'Name': 'telemetry-bootstrap',
                'ScriptBootstrapAction': {
                    'Path': (
                        's3://{}/bootstrap/telemetry.sh'
                        .format(MozEmrMixin.spark_bucket)
                    )
                }
            }],
            "Tags": [
                {'Key': 'Owner', 'Value': owner},
                {'Key': 'Application',
                 'Value': 'telemetry-analysis-worker-instance'},
                {'Key': 'Source',
                 'Value': 'Airflow'},
                {'Key': 'Job',
                 'Value': job_name},
            ]
        }

    @staticmethod
    def get_step_args(job_name,
                      owner,
                      uri,
                      env=None,
                      output_visibility='private',
                      arguments='',
                      action_on_failure='TERMINATE_JOB_FLOW'):
        if output_visibility == 'public':
            data_bucket = MozEmrMixin.public_output_bucket
        elif output_visibility == 'private':
            data_bucket = MozEmrMixin.private_output_bucket

        if env is not None:
            environment = ' '.join(['{}={}'.format(k, v)
                                    for k, v in env.items()])

        jar_url = (
            's3://{}.elasticmapreduce/libs/script-runner/script-runner.jar'
            .format(MozEmrMixin.region)
        )

        args = [
            's3://{}/steps/airflow.sh'.format(
                MozEmrMixin.airflow_bucket
            ),
            '--job-name', job_name,
            '--user', owner,
            '--uri', uri,
            '--data-bucket', data_bucket,
            '--environment', environment
        ]
        # Empty quotes will be parsed as literals in the shell, so avoid
        # passing in arguments unless they are actually needed. See issue #189.
        if arguments:
            args += ['--arguments', '"{}"'.format(arguments)]

        return [{
            'Name': job_name,
            'ActionOnFailure': action_on_failure,
            'HadoopJarStep': {
                'Jar': jar_url,
                'Args': args
            }
        }]
