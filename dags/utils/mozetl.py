"""Utility functions for launching mozetl jobs"""


def mozetl_envvar(command, options, other={}):
    """Set up environment variables for mozetl jobs.

    The command line interface can read options from the environment instead of usage
    flags, through a library called Click. All environment variables must be prefixed
    by the command and subcommand names. For example, a job registered with mozetl with
    the name `example` taking a `--date` option can use `MOZETL_EXAMPLE_DATE` instead.

    :command string:    name of the command registered with python_mozel
    :options dict:      environment variables to prefix
    :other dict:        environment variables to pass through

    :returns: a dictionary that contains properly prefixed command and options
    """
    prefixed_options = {
        "MOZETL_{}_{}".format(command.upper(), key.upper()): value
        for key, value in options.iteritems()
    }
    prefixed_options["MOZETL_COMMAND"] = command
    prefixed_options.update(other)

    return prefixed_options
