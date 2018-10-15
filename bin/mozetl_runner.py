"""A mozetl runner script for the MozDatabricksRunSubmit operator.

A copy of this file may be found in `telemetry-airflow/bin`

This file is used as an argument in the SparkPythonTask in the Databricks
api.[0] The library is assumed to be installed on all of the machines in the
cluster.  Arguments are passed to the script through `MOZETL_`-prefixed
environment variables.

This script is deployed to `s3://telemetry-airflow/steps/mozetl_runner.py`.[1]

[0]: https://docs.databricks.com/api/latest/jobs.html#sparkpythontask
[1]: https://bugzilla.mozilla.org/show_bug.cgi?id=1484331
"""

from os import environ
from pprint import pformat
from mozetl import cli

print(
    pformat({
        k: v for k, v in environ.items()
        if k.startswith("MOZETL")
    })
)

try:
    cli.entry_point(auto_envvar_prefix="MOZETL")
except SystemExit:
    # avoid calling sys.exit() in databricks
    # http://click.palletsprojects.com/en/7.x/api/?highlight=auto_envvar_prefix#click.BaseCommand.main
    pass
