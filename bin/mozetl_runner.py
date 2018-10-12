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