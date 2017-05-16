#!/bin/bash

# Create the package for distribution across the cluster
git clone https://github.com/mozilla/python_mozetl.git
cd python_mozetl
python setup.py bdist_egg

# Generate the driver script
echo 'from mozetl.search import daily_search_rollups as dsr; dsr.main()' > run.py

# Avoid errors caused by jupyter
unset PYSPARK_DRIVER_PYTHON

spark-submit --master yarn \
             --deploy-mode client \
             --py-files dist/*.egg \
             run.py
