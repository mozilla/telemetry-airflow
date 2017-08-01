#!/bin/bash

if [[ -z "$bucket" || -z "$date" ]]; then
    echo "Missing arguments!" 1>&2
    exit 1
fi

# Create the package for distribution across the cluster
git clone https://github.com/mozilla/python_mozetl.git
cd python_mozetl
pip install .
python setup.py bdist_egg

# Generate the driver script
echo 'from mozetl.hardware_report import hardware_dashboard; hardware_dashboard.main()' > run.py

# Avoid errors caused by jupyter
unset PYSPARK_DRIVER_PYTHON

spark-submit --master yarn \
         --deploy-mode client \
         --py-files dist/*.egg \
         run.py --start_date=$date --bucket=$bucket
