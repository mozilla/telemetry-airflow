#!/bin/bash

if [[ -z "$mode" || -z "$date" ]]; then
   echo "Missing arguments!" 1>&2
   exit 1
fi

# Create the package for distribution across the cluster
git clone https://github.com/mozilla/python_mozetl.git
cd python_mozetl
python setup.py bdist_egg

bucket="net-mozaws-prod-us-west-2-pipeline-analysis"
prefix="spenrose/search/to_vertica"

# Generate the driver script
echo 'from mozetl.search import search_rollups as sr; sr.main()' > run.py

# Avoid errors caused by jupyter
unset PYSPARK_DRIVER_PYTHON

spark-submit --master yarn \
             --deploy-mode client \
             --py-files dist/*.egg \
             run.py $mode $date $bucket $prefix
