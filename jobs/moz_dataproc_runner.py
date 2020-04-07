import importlib
import sys


"""Generic runner for PySpark jobs

This script runs a `cli.entry_point()` from an arbitrary Python module or CLI application.
Job module name should be provided as a first command line argument. Module argument will be cleared
before executing the `entry_point()`, allowing for the underlying job to be decoupled from this script.

If running on Dataproc, this requires the job to be installed on the cluster
(e.g. via `pip_install` initialization action).
"""
# Retrieve target module name
module_to_run = sys.argv[1]
# Clear retrieved argument in the list of arguments passed to this script
# This allows the target job to properly interpret its command line arguments
del sys.argv[1]

# Import the target module and execute its entry point
cli = importlib.import_module(f"{module_to_run}.cli")
cli.entry_point()
