import boto3
import logging
from textwrap import dedent


def generate_runner(module_name, bucket, prefix):
    """Generate a runner for the current module to be run in Databricks.
    
    See https://github.com/mozilla/python_mozetl/blob/master/bin/mozetl-databricks.py for a
    standalone implementation.
    """
    logging.info(
        "Writing new runner to {}/{} for {}".format(bucket, prefix, module_name)
    )

    runner_data = """
    # This runner has been auto-generated from mozilla/telemetry-airflow/plugins/moz_databricks.py.
    # Any changes made to the runner file may be over-written on subsequent runs.
    from {module} import cli
    
    try:
        cli.entry_point(auto_envvar_prefix="MOZETL")
    except SystemExit:
        # avoid calling sys.exit() in databricks
        # http://click.palletsprojects.com/en/7.x/api/?highlight=auto_envvar_prefix#click.BaseCommand.main
        pass
    """.format(
        module=module_name
    )

    s3 = boto3.resource("s3")
    runner_object = s3.Object(bucket, "{}/{}_runner.py".format(prefix, module_name))
    runner_object.put(Body=dedent(runner_data))
