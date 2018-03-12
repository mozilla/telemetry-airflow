#!/bin/bash

if [[ -z "$date" ]]; then
   echo "Missing arguments!" 1>&2
   exit 1
fi

# Setup a temp directory to work in 
mkdir tmp_ensemble
cd tmp_ensemble

# Fetch the airflow job script
wget https://raw.githubusercontent.com/crankycoder/taar_loader/releases/1.0/scripts/airflow_job.py

# Fetch the python egg
wget https://github.com/crankycoder/taar_loader/releases/download/releases%2F1.0/taar_loader-1.0-py3.5.egg

spark-submit --master=yarn \
             --py-files=taar_loader-1.0-py3.5.egg airflow_job.py --date=$date
