import logging
import boto3
import requests
import time

from airflow.models import BaseOperator
from airflow.utils import apply_defaults, AirflowException
from os import environ

class EMRSparkOperator(BaseOperator):
    """
    Execute a Spark job on EMR.

    :param job_name: The name of the job.
    :type job_name: string

    :param owner: The e-mail address of the user owning the job.
    :type owner: string

    :param uri: The URI of the job to run, which can be either a Jupyter notebook
                or a JAR file.
    :type uri: string

    :param instance_count: The number of workers the cluster should have.
    :type instance_count: int

    :param env: If env is not None, it must be a mapping that defines the environment
                variables for the new process (templated).
    :type env: string
    """
    template_fields = ('environment', )

    region = environ["REGION"]
    key_name = environ["EMR_KEY_NAME"]
    release_label = environ["EMR_RELEASE_LABEL"]
    flow_role = environ["EMR_FLOW_ROLE"]
    service_role = environ["EMR_SERVICE_ROLE"]
    instance_type = environ["EMR_INSTANCE_TYPE"]
    spark_bucket = environ["SPARK_BUCKET"]
    airflow_bucket = environ["AIRFLOW_BUCKET"]

    def __del__(self):
        self.on_kill()


    def post_execute(self, context):
        self.on_kill()


    def on_kill(self):
        if self.job_flow_id is None:
            return

        client = boto3.client('emr', region_name=EMRSparkOperator.region)
        result = client.describe_cluster(ClusterId = self.job_flow_id)
        status = result["Cluster"]["Status"]["State"]

        if status != "TERMINATED_WITH_ERRORS" and status != "TERMINATED":
            logging.warn("Terminating Spark job {}".format(self.job_name))
            client.terminate_job_flows(JobFlowIds = [self.job_flow_id])


    @apply_defaults
    def __init__(self, job_name, owner, uri, instance_count, env={}, arguments="", *args, **kwargs):
        super(EMRSparkOperator, self).__init__(*args, **kwargs)
        self.job_name = job_name
        self.owner = owner
        self.uri = uri
        self.arguments = arguments
        self.environment = " ".join(["{}={}".format(k, v) for k, v in env.iteritems()])
        self.job_flow_id = None
        self.instance_count = instance_count


    def execute(self, context):
        self.steps = Steps=[{
            'Name': 'RunNotebookStep',
            'ActionOnFailure': 'TERMINATE_JOB_FLOW',
            'HadoopJarStep': {
                'Jar': 's3://{}.elasticmapreduce/libs/script-runner/script-runner.jar'.format(EMRSparkOperator.region),
                'Args': [
                    "s3://{}/steps/airflow.sh".format(EMRSparkOperator.airflow_bucket),
                    "--job-name", self.job_name,
                    "--user", self.owner,
                    "--uri", self.uri,
                    "--arguments", '"{}"'.format(self.arguments),
                    "--data-bucket", EMRSparkOperator.airflow_bucket,
                    "--environment", self.environment
                ]
            }
        }]

        client = boto3.client('emr', region_name=EMRSparkOperator.region)
        response = client.run_job_flow(
            Name = self.job_name,
            ReleaseLabel = EMRSparkOperator.release_label,
            JobFlowRole = EMRSparkOperator.flow_role,
            ServiceRole = EMRSparkOperator.service_role,
            Applications = [{'Name': 'Spark'}, {'Name': 'Hive'}],
            Configurations = requests.get("https://s3-{}.amazonaws.com/{}/configuration/configuration.json".format(EMRSparkOperator.region, EMRSparkOperator.spark_bucket)).json(),
            LogUri = "s3://{}/logs/{}/{}/".format(EMRSparkOperator.airflow_bucket, self.owner, self.job_name),
            Instances = {
                'MasterInstanceType': EMRSparkOperator.instance_type,
                'SlaveInstanceType': EMRSparkOperator.instance_type,
                'InstanceCount': self.instance_count,
                'Ec2KeyName': EMRSparkOperator.key_name
            },
            BootstrapActions=[{
                'Name': 'telemetry-bootstrap',
                'ScriptBootstrapAction': {
                    'Path': "s3://{}/bootstrap/telemetry.sh".format(EMRSparkOperator.spark_bucket)
                }
            }],
            Tags=[
                {'Key': 'Owner', 'Value': self.owner},
                {'Key': 'Application', 'Value': "telemetry-analysis-worker-instance"},
            ],
            Steps=self.steps
        )

        self.job_flow_id = response["JobFlowId"]
        logging.info("Running Spark Job {} with JobFlow ID {}".format(self.job_name, self.job_flow_id))

        while True:
            result = client.describe_cluster(ClusterId = self.job_flow_id)
            status = result["Cluster"]["Status"]["State"]

            if status == "TERMINATED_WITH_ERRORS":
                reason_code = result["Cluster"]["Status"]["StateChangeReason"]["Code"]
                reason_message = result["Cluster"]["Status"]["StateChangeReason"]["Message"]
                raise AirflowException("Spark job {} terminated with errors: {} - {}".format(self.job_name, reason_code, reason_message))
            elif status == "TERMINATED":
                break
            elif status == "WAITING":
                raise AirflowException("Spark job {} is waiting".format(self.job_name))

            logging.info("Spark Job '{}' status' is {}".format(self.job_name, status))
            time.sleep(60)
