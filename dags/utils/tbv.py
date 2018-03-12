"""Utility functions for launching telemetry-batch-view jobs"""


def tbv_envvar(klass, options, other={}):
    """Set up environment variables for telemetry-batch-view jobs.

    The command line interface can read options from the environment. All
    environment variables must be prefixed by `TBV_`. For example, a class in
    telemetry-batch-view taking a `--date` option can use `TBV_DATE` instead.
    There is a limitation that spaces cannot be in environment variables, so
    ValueError is thrown if spaces are found outside templating brackets.

    :klass string:      name of the class in telemetry-batch-view
    :options dict:      environment variables to prefix
    :other dict:        environment variables to pass through

    :returns: a dictionary that contains properly prefixed class and options
    """
    prefixed_options = {
        "TBV_{}".format(key.replace("-", "_")): value
        for key, value in options.items()
    }
    prefixed_options["TBV_CLASS"] = klass
    prefixed_options.update(other)

    # raise ValueError if spaces found in non-templated envvar values
    for item in prefixed_options.values():
        if "{{" not in item and " " in item:
            raise ValueError("env cannot contain spaces: '{}'".format(item))

    return prefixed_options
