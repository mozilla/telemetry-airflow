#!/bin/bash

git clone https://github.com/mozilla/python_mozaggregator
cd python_mozaggregator
python setup.py bdist_egg
package_file=$(pwd)/dist/$(ls dist)

wget https://raw.githubusercontent.com/mozilla/telemetry-airflow/master/jobs/mobile_aggregator.py
pip install -U py4j
chmod 777 mobile_aggregator.py
./mobile_aggregator.py $package_file
