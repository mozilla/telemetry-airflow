from os import environ
from pprint import pformat

from airflow.plugins_manager import AirflowPlugin
from airflow.contrib.operators.databricks_operator import DatabricksSubmitRunOperator


class MozDatabricksSubmitRunOperator(DatabricksSubmitRunOperator):
    """Execute a Spark job on Databricks."""

    template_fields = ('json',)
    region = environ['AWS_REGION']
    instance_type = environ['EMR_INSTANCE_TYPE']
    spark_bucket = environ['SPARK_BUCKET']
    private_output_bucket = environ['PRIVATE_OUTPUT_BUCKET']
    public_output_bucket = environ['PUBLIC_OUTPUT_BUCKET']
    deploy_environment = environ['DEPLOY_ENVIRONMENT']
    deploy_tag = environ['DEPLOY_TAG']
    artifacts_bucket = environ['ARTIFACTS_BUCKET']

    # constants
    mozilla_slug = 'mozilla'
    telemetry_streaming_slug = 'telemetry-streaming'
    telemetry_batch_view_slug = 'telemetry-batch-view'

    def __init__(self, job_name, env, instance_count,
                 dev_instance_count=1,
                 max_instance_count=None,
                 dev_max_instance_count=3,
                 enable_autoscale=False,
                 disable_on_dev=False,
                 release_label='4.3.x-scala2.11',
                 owner="", uri=None, output_visibility=None, **kwargs):
        """
        Generate parameters for running a job through the Databricks run-submit
        api. This is designed to be backwards compatible with EMRSparkOperator.

        See: https://docs.databricks.com/api/latest/jobs.html#runs-submit

        :param job_name: Name of the job
        :param env: Parameters via mozetl and tbv envvar wrappers
        :param instance_count: The number of instances to use in production
        :param dev_instance_count: The number of instances to use in development
        :param max_instance_count: Max number of instances during autoscaling
        :param dev_max_instance_count: Max number of instances during
            autoscaling in dev
        :param enable_autoscale: Enable autoscaling for the job
        :param disable_on_dev: Turn the job into a no-op if run in development
        :param release_label: Databricks Runtime versions,
            run `databricks clusters spark-versions` for possible values.
        :param owner: The e-mail address of the user owning the job.
        :param kwargs: Keyword arguments to pass to DatabricksSubmitRunOperator
        """

        if enable_autoscale:
            if not max_instance_count:
                raise ValueError("`max_instance_count` should be set when "
                                 "`enable_autoscale` is enabled.")
            if (max_instance_count < instance_count or
                    dev_max_instance_count < dev_instance_count):
                raise ValueError("The max instance count should be greater "
                                 "than the instance count.")

        is_dev = self.deploy_environment == 'dev'
        self.disable_on_dev = disable_on_dev
        self.job_name = job_name

        jar_task = None
        python_task = None
        libraries = []

        # Create the cluster configuration
        new_cluster = {
            "spark_version": release_label,
            "node_type_id": self.instance_type,
            "aws_attributes": {
                "availability": "ON_DEMAND",
                "instance_profile_arn":
                    "arn:aws:iam::144996185633:instance-profile/databricks-ec2"
            },
            "spark_env_vars": env,
            "custom_tags": {
                "Owner": owner,
                "Application": "databricks",
                "Source": "Airflow",
                "Job": job_name,
            }
        }

        min_workers = dev_instance_count if is_dev else instance_count
        max_workers = dev_max_instance_count if is_dev else max_instance_count
        if enable_autoscale:
            new_cluster["autoscale"] = {
                "min_workers": min_workers,
                "max_workers": max_workers,
            }
        else:
            new_cluster["num_workers"] = min_workers

        # Parse the environment variables to bootstrap the tbv/mozetl workflow
        if env.get("TBV_CLASS"):
            jar_task = {
                "main_class_name": env["TBV_CLASS"],
                # Reconstruct TBV parameters from the environment, scallop does
                # not support reading arguments in this form
                "parameters": sum([
                    ["--{}".format(key[4:].replace("_", "-")), value]
                    for key, value in env.items()
                    if key.startswith("TBV_") and key != "TBV_CLASS"
                ], [])
            }

            # Currently the artifacts are fetched via HTTP. Databricks
            # expects either dbfs:// or s3:// for resources.
            artifact_path = env.get("ARTIFACT_URL").split("amazonaws.com/")[-1]
            artifact_path_s3 = "s3://{}".format(artifact_path)
            libraries.append({'jar': artifact_path_s3})

        elif env.get("MOZETL_COMMAND"):
            # options are read directly from the environment via Click
            python_task = {
                "python_file": (
                    "s3://net-mozaws-prod-us-west-2-pipeline-analysis/"
                    "amiyaguchi/databricks-poc/mozetl_runner.py"
                ),
                "parameters": [env["MOZETL_COMMAND"]]
            }

            # Proper pip dependencies in Databriks is only supported via pypi.
            # Dependencies for source/binary distributions need to be added
            # manually.
            libraries.append({
                "pypi": {
                    "package": "git+https://github.com/mozilla/python_mozetl.git"
                }
            })
        else:
            raise ValueError("Missing options for running tbv or mozetl tasks")

        json = {
            "run_name": job_name,
            "new_cluster": new_cluster,
            "spark_jar_task": jar_task,
            "spark_python_task": python_task,
            "libraries": libraries
        }
        json = {k: v for k, v in json.items() if v}
        super(MozDatabricksSubmitRunOperator, self).__init__(json, **kwargs)

    def execute(self, context):
        self.log.info("Running {} with parameters:\n{}"
                      .format(self.job_name, pformat(self.json)))

        if self.disable_on_dev:
            self.log.info("Skipping {} in the development environment"
                          .format(self.job_name))
            return

        super(MozDatabricksSubmitRunOperator, self).execute(context)


class MozDatabricksPlugin(AirflowPlugin):
    name = 'moz_databricks'
    operators = [MozDatabricksSubmitRunOperator]
