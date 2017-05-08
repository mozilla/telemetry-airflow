#!/bin/bash

set -x

if [[ -z "$mode" ]]; then
   echo "Missing arguments!" 1>&2
   exit 1
fi

bucket="net-mozaws-prod-metrics-data"
prefix="firefox-dashboard"

git clone https://github.com/mozilla/python_mozetl.git
cd python_mozetl
python setup.py bdist_egg

# Generate the driver script
echo "from mozetl.topline import topline_dashboard; topline_dashboard.main()" > run.py

# Avoid errors caused by jupyter
unset PYSPARK_DRIVER_PYTHON

spark-submit --master yarn \
             --deploy-mode client \
             --py-files dist/*.egg \
             run.py $mode $bucket $prefix
