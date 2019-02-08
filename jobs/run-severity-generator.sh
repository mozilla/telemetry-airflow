#!/bin/bash

# This script does not use mozetl-submit, as we need to do some
# hacky downgrading to get it to work, see:
# https://bugzilla.mozilla.org/show_bug.cgi?id=1525022

# We use jupyter by default, but here we want to use python
unset PYSPARK_DRIVER_PYTHON

# Clone, install, and run
git clone https://github.com/mozilla/python_mozetl.git
cd python_mozetl
pip install .
pip install pandas==0.18.1 # DOWNGRADE pandas to the one installed in EMR
python setup.py bdist_egg

spark-submit scheduling/tab_spinner.py
