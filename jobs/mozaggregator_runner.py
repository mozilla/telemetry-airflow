# This runner has been auto-generated from mozilla/python_mozaggregator/bin/dataproc.sh.
# Any changes made to the runner file will be over-written on subsequent runs.
from mozaggregator import cli

try:
    cli.entry_point(auto_envvar_prefix="MOZETL")
except SystemExit:
    # avoid calling sys.exit() in databricks
    # http://click.palletsprojects.com/en/7.x/api/?highlight=auto_envvar_prefix#click.BaseCommand.main
    pass
