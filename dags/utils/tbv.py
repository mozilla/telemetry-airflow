"""Utility functions for launching telemetry-batch-view jobs"""

from operators.emr_spark_operator import EMRSparkOperator
from utils.deploy import get_artifact_url


def tbv_envvar(klass, options, dev_options={}, branch=None, tag=None, other={},
               artifact_url=None):
    """Set up environment variables for telemetry-batch-view jobs.

    The command line interface can read options from the environment. All
    environment variables must be prefixed by `TBV_`. For example, a class in
    telemetry-batch-view taking a `--date` option can use `TBV_DATE` instead.
    There is a limitation that spaces cannot be in environment variables, so
    ValueError is thrown if spaces are found outside templating brackets.

    :klass string:      name of the class in telemetry-batch-view
    :options dict:      environment variables to prefix
    :dev_options dict:  variables to use when in the development environment
    :branch string:     the branch to run the job from, incompatible with tag
    :tag string:        the tag to run the job from, incompatible with branch
    :other dict:        environment variables to pass through
    :artifact_url string:       Location of pre-built binaries

    :returns: a dictionary that contains properly prefixed class and options
    """
    if artifact_url is None:
        slug = "{{ task.__class__.telemetry_batch_view_slug }}"
        url = get_artifact_url(slug, branch=branch, tag=tag)
    else:
        url = artifact_url

    if EMRSparkOperator.deploy_environment == 'dev':
        options.update(dev_options)

    prefixed_options = {
        "TBV_{}".format(key.replace("-", "_")): value
        for key, value in options.items()
    }

    if klass is not None:
        prefixed_options["TBV_CLASS"] = klass
    else:
        assert other.get("DO_SUBMIT", "True") == "False", "To submit there must be a class name"

    prefixed_options["ARTIFACT_URL"] = url
    prefixed_options.update(other)

    # raise ValueError if spaces found in non-templated envvar values
    for item in prefixed_options.values():
        if "{{" not in item and " " in item:
            raise ValueError("env cannot contain spaces: '{}'".format(item))

    return prefixed_options
