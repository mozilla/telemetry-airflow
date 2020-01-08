import boto3
import botocore

from os import environ
from pprint import pformat

from airflow.plugins_manager import AirflowPlugin
from airflow.contrib.operators.databricks_operator import DatabricksSubmitRunOperator
from mozetl import generate_runner


class MozDatabricksSubmitRunOperator(DatabricksSubmitRunOperator):
    """Execute a Spark job on Databricks."""

    template_fields = ('json',)
    region = environ['AWS_REGION']
    spark_bucket = environ['SPARK_BUCKET']
    private_output_bucket = environ['PRIVATE_OUTPUT_BUCKET']
    public_output_bucket = environ['PUBLIC_OUTPUT_BUCKET']
    deploy_environment = environ['DEPLOY_ENVIRONMENT']
    deploy_tag = environ['DEPLOY_TAG']
    artifacts_bucket = environ['ARTIFACTS_BUCKET']

    # constants
    mozilla_slug = 'mozilla'
    telemetry_batch_view_slug = 'telemetry-batch-view'

    def __init__(self, job_name, env, instance_count,
                 dev_instance_count=1,
                 max_instance_count=None,
                 dev_max_instance_count=3,
                 enable_autoscale=False,
                 on_demand_instances=1,
                 spot_bid_price_percent=75,
                 disable_on_dev=False,
                 release_label='4.3.x-scala2.11',
                 iam_role=environ["DATABRICKS_DEFAULT_IAM"],
                 instance_type=environ['EMR_INSTANCE_TYPE'],
                 driver_instance_type=None,
                 owner="",
                 uri=None,
                 output_visibility=None,
                 ebs_volume_count=None,
                 ebs_volume_size=None,
                 python_version=3,
                 pypi_libs=None,
                 *args, **kwargs):
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
        :param on_demand_instances: Minimum number of on-demand instances. All
            other instances will first be requested from the spot market,
            with on-demand being a backup.
        :param spot_bid_price_percent: Percent of max price to bid in spot market
        :param enable_autoscale: Enable autoscaling for the job
        :param disable_on_dev: Turn the job into a no-op if run in development
        :param release_label: Databricks Runtime versions,
            run `databricks clusters spark-versions` for possible values.
        :param iam_role: An Amazon Resource Name (ARN) specifying an iam role
        :param instance_type: An EC2 instance type (worker nodes and driver, if not specified)
        :param driver_instance_type: Driver node instance type.
            If not set, the same type is used across driver and worker nodes.
        :param owner: The e-mail address of the user owning the job.
        :param uri: argument from EMRSparkOperator for compatibility
        :param output_visibility: argument from EMRSparkOperator for compatibility
        :param ebs_volume_count: number of ebs volumes to attach to each node
        :param ebs_volume_size: size of ebs volumes attached to each node
        :param python_version: the default python runtime on the cluster (python 3.5.2)
            See https://docs.databricks.com/release-notes/runtime/4.3.html#system-environment
            for more details.
        :param pypi_libs: PyPI libraries to install. ex: "['pylib1==0.1', 'pylib2==3.1']"
        :param kwargs: Keyword arguments to pass to DatabricksSubmitRunOperator
        """
        if python_version not in (2, 3):
            raise ValueError("Only Python versions 2 or 3 allowed")
        elif python_version == 3:
            env["PYSPARK_PYTHON"] = "/databricks/python3/bin/python3"

        if enable_autoscale:
            if not max_instance_count:
                raise ValueError("`max_instance_count` should be set when "
                                 "`enable_autoscale` is enabled.")
            if (max_instance_count < instance_count or
                    dev_max_instance_count < dev_instance_count):
                raise ValueError("The max instance count should be greater "
                                 "than the instance count.")

        is_dev = self.deploy_environment == 'dev'
        self.is_dev = is_dev
        self.disable_on_dev = disable_on_dev
        self.job_name = job_name
        self.env = env

        jar_task = None
        python_task = None
        libraries = []

        aws_attributes = {
            "availability": "SPOT_WITH_FALLBACK",
            "spot_bid_price_percent": spot_bid_price_percent,
            "first_on_demand": on_demand_instances,
            "instance_profile_arn": iam_role
        }

        if bool(ebs_volume_size) ^ bool(ebs_volume_count):
            raise ValueError("`ebs_volume_count` and `ebs_volume_size` "
                             "must be set together.")

        if ebs_volume_count is not None:
            aws_attributes["ebs_volume_count"] = ebs_volume_count

        if ebs_volume_size is not None:
            aws_attributes["ebs_volume_size"] = ebs_volume_size

        # Create the cluster configuration
        new_cluster = {
            "spark_version": release_label,
            "node_type_id": instance_type,
            "aws_attributes": aws_attributes,
            "spark_env_vars": env,
            "custom_tags": {
                "Owner": owner,
                "Application": "databricks",
                "Source": "Airflow",
                "Job": job_name,
            }
        }

        if driver_instance_type is not None:
            new_cluster["driver_node_type_id"] = driver_instance_type

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
            opts = [
                ["--{}".format(key[4:].replace("_", "-")), value]
                for key, value in env.items()
                if key.startswith("TBV_") and key != "TBV_CLASS"
            ]

            formatted_opts = [v for opt in opts for v in opt if v]

            jar_task = {
                "main_class_name": env["TBV_CLASS"],
                # Reconstruct TBV parameters from the environment, scallop does
                # not support reading arguments in this form
                "parameters": formatted_opts
            }

            # Currently the artifacts are fetched via HTTP. Databricks
            # expects either dbfs:// or s3:// for resources.
            artifact_path = env.get("ARTIFACT_URL").split("amazonaws.com/")[-1]
            artifact_path_s3 = "s3://{}".format(artifact_path)
            libraries.append({'jar': artifact_path_s3})

        elif env.get("MOZETL_COMMAND"):
            # create a runner if it doesn't exist
            bucket = "telemetry-test-bucket" if is_dev else "telemetry-airflow"
            prefix = "steps"

            module_name = env.get("MOZETL_EXTERNAL_MODULE", "mozetl")
            runner_name = "{}_runner.py".format(module_name)

            # options are read directly from the environment via Click
            python_task = {
                "python_file": "s3://{}/{}/{}".format(bucket, prefix, runner_name),
                "parameters": [env["MOZETL_COMMAND"]]
            }

            # Proper pip dependencies in Databricks is only supported via pypi.
            # Dependencies for source/binary distributions need to be added
            # manually.
            path = env.get(
                "MOZETL_GIT_PATH", "https://github.com/mozilla/python_mozetl.git"
            )
            branch = env.get("MOZETL_GIT_BRANCH", "master")

            libraries.append(
                {
                    "pypi": {
                        "package": "git+{path}@{branch}".format(
                            path=path, branch=branch
                        )
                    }
                }
            )

            if pypi_libs is not None and len(pypi_libs) > 0:
                libraries.extend([{"pypi": {"package": lib}} for lib in pypi_libs])

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
        super(MozDatabricksSubmitRunOperator, self).__init__(
            json=json,
            databricks_retry_limit=20,
            databricks_retry_delay=30,
            **kwargs
        )

    def execute(self, context):
        self.log.info("Running {} with parameters:\n{}"
                      .format(self.job_name, pformat(self.json)))

        if self.disable_on_dev:
            self.log.info("Skipping {} in the development environment"
                          .format(self.job_name))
            return

        # Create a runner if it doesn't exist only at execution time of the job.
        if self.env.get("MOZETL_COMMAND"):
            s3 = boto3.resource("s3")
            bucket = "telemetry-test-bucket" if self.is_dev else "telemetry-airflow"
            prefix = "steps"

            module_name = self.env.get("MOZETL_EXTERNAL_MODULE", "mozetl")
            runner_name = "{}_runner.py".format(module_name)

            try:
                s3.Object(bucket, "{}/{}".format(prefix, runner_name)).load()
            except botocore.exceptions.ClientError as e:
                if e.response["Error"]["Code"] == "404":
                    generate_runner(module_name, bucket, prefix)
                else:
                    raise e

        super(MozDatabricksSubmitRunOperator, self).execute(context)


class MozDatabricksPlugin(AirflowPlugin):
    name = 'moz_databricks'
    operators = [MozDatabricksSubmitRunOperator]
