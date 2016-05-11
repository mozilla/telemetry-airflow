import logging
import boto3
import requests
import time

from airflow.models import BaseOperator
from airflow.utils import apply_defaults

class EMRSparkOperator(BaseOperator):
    """
    Execute a Spark job on EMR.

    :param job_name: The name of the job.
    :type job_name: string

    :param user: The e-mail address of the user owning the job.
    :type user: string

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

    release_label = "emr-4.3.0"
    flow_role = "telemetry-spark-cloudformation-TelemetrySparkInstanceProfile-1SATUBVEXG7E3"
    region = "us-west-2"
    spark_bucket = "telemetry-spark-emr-2"
    service_role = "EMR_DefaultRole"
    instance_type = "c3.4xlarge"
    key_name = "mozilla_vitillo"
    data_bucket = "telemetry-airflow"

    def __del__(self):
        self.on_kill()


    def post_execute(self):
        self.on_kill()


    def on_kill(self):
        if self.job_flow_id is None:
            return

        client = boto3.client('emr')
        result = client.describe_cluster(ClusterId = self.job_flow_id)
        status = result["Cluster"]["Status"]["State"]

        if status != "TERMINATED_WITH_ERRORS" and status != "TERMINATED":
            logging.warn("Terminating Spark job {}".format(self.job_name))
            client.terminate_job_flows(JobFlowIds = [self.job_flow_id])


    @apply_defaults
    def __init__(self, job_name, email, uri, instance_count, env={}, *args, **kwargs):
        super(EMRSparkOperator, self).__init__(*args, **kwargs)
        self.user = ",".join(email)
        self.instance_count = instance_count
        self.job_name = job_name
        self.job_flow_id = None
        self.environment = " ".join(["{}={}".format(k, v) for k, v in env.iteritems()])
        self.uri = uri


    def execute(self, context):
        if self.uri.endswith(".ipynb"):
            self.steps = Steps=[{
                'Name': 'RunNotebookStep',
                'ActionOnFailure': 'TERMINATE_JOB_FLOW',
                'HadoopJarStep': {
                    'Jar': 's3://{}.elasticmapreduce/libs/script-runner/script-runner.jar'.format(EMRSparkOperator.region),
                    'Args': [
                        "s3://{}/steps/batch.sh".format(EMRSparkOperator.spark_bucket),
                        "--job-name", self.job_name,
                        "--notebook", self.uri,
                        "--data-bucket", EMRSparkOperator.data_bucket,
                        "--environment", self.environment
                    ]
                }
            }]
        elif self.uri.ends_with(".jar"):
            raise AirflowException("Not implemented yet")
        else:
            raise AirflowException("Invalid job URI")

        client = boto3.client('emr')
        response = client.run_job_flow(
            Name = "airflow-test",
            ReleaseLabel = EMRSparkOperator.release_label,
            JobFlowRole = EMRSparkOperator.flow_role,
            ServiceRole = EMRSparkOperator.service_role,
            Applications = [{'Name': 'Spark'}, {'Name': 'Hive'}],
            Configurations = requests.get("https://s3-{}.amazonaws.com/{}/configuration/configuration.json".format(EMRSparkOperator.region, EMRSparkOperator.spark_bucket)).json(),
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
                {'Key': 'Owner', 'Value': self.user},
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
