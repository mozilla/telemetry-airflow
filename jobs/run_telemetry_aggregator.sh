#!/bin/bash

git clone https://github.com/mozilla/python_mozaggregator
cd python_mozaggregator
python setup.py bdist_egg
package_file=$(pwd)/dist/$(ls dist)

wget https://raw.githubusercontent.com/mozilla/telemetry-airflow/master/jobs/telemetry_aggregator.py
pip install -U py4j
./telemetry_aggregator.py $package_file
