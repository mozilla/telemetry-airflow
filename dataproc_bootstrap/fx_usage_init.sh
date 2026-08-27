#!/bin/bash

set -exo pipefail

# Logs will be available on the dataproc nodes at /var/log/dataproc-initialization-script-X.log
# or via the GCP Dataproc UI

/opt/conda/default/bin/pip install --upgrade pip

/opt/conda/default/bin/pip install arrow==0.10.0
/opt/conda/default/bin/pip install boto3==1.9.199
/opt/conda/default/bin/pip install click==6.7
/opt/conda/default/bin/pip install click_datetime==0.2
/opt/conda/default/bin/pip install --ignore-installed flake8==3.7.8
/opt/conda/default/bin/pip install pyspark==2.2.2
/opt/conda/default/bin/pip install pytest==4.6.4
/opt/conda/default/bin/pip install scipy==1.0.0rc1

/opt/conda/default/bin/pip install py4j --upgrade
/opt/conda/default/bin/pip install numpy==1.16.4
/opt/conda/default/bin/pip install python-dateutil==2.5.0
/opt/conda/default/bin/pip install pytz==2011k
/opt/conda/default/bin/pip install --no-dependencies pandas==0.24

# This fixes the PythonAccumulatorV2 does not exist error
export PYTHONPATH=/usr/lib/spark/python/lib/pyspark.zip
