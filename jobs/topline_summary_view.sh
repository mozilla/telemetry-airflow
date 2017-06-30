#!/bin/bash

if [[ -z "$date" || -z "$mode" || -z "$bucket" ]]; then
   echo "Missing arguments!" 1>&2
   exit 1
fi

# Create the package for distribution across the cluster
git clone https://github.com/mozilla/python_mozetl.git
cd python_mozetl
python setup.py bdist_egg

pip install -e .

# Generate the driver script
echo 'from mozetl.topline import topline_summary as ts; ts.main()' > run.py

# Avoid errors caused by jupyter
unset PYSPARK_DRIVER_PYTHON

spark-submit --master yarn \
             --deploy-mode client \
             --py-files dist/*.egg \
             run.py $date $mode $bucket topline_summary/v1
