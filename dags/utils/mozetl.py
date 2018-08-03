"""Utility functions for launching mozetl jobs"""
from operators.emr_spark_operator import EMRSparkOperator


def mozetl_envvar(command, options, dev_options={}, other={}):
    """Set up environment variables for mozetl jobs.

    The command line interface can read options from the environment instead of usage
    flags, through a library called Click. All environment variables must be prefixed
    by the command and subcommand names. For example, a job registered with mozetl with
    the name `example` taking a `--date` option can use `MOZETL_EXAMPLE_DATE` instead.

    :command string:    name of the command registered with python_mozel
    :options dict:      environment variables to prefix
    :dev_options dict:  variables to use when in the development environment
    :other dict:        environment variables to pass through

    :returns: a dictionary that contains properly prefixed command and options
    """

    if EMRSparkOperator.deploy_environment == 'dev':
        options.update(dev_options)

    prefixed_options = {
        "MOZETL_{}_{}".format(command.upper(), key.upper().replace("-", "_")): value
        for key, value in options.iteritems()
    }
    prefixed_options["MOZETL_COMMAND"] = command
    prefixed_options.update(other)

    return prefixed_options
