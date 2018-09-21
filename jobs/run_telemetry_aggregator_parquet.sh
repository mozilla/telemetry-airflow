#!/bin/bash

git clone https://github.com/mozilla/python_mozaggregator
cd python_mozaggregator
python setup.py bdist_egg
package_file=$(pwd)/dist/$(ls dist)

wget https://raw.githubusercontent.com/mozilla/telemetry-airflow/master/jobs/telemetry_aggregator_parquet.py
pip install -U py4j
chmod 777 telemetry_aggregator_parquet.py
./telemetry_aggregator_parquet.py $package_file
