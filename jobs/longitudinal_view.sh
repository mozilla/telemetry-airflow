#!/bin/bash

if [[ -z "$bucket" || -z "$date" ]]; then
    echo "Missing arguments!" 1>&2
    exit 1
fi

set -e

git clone https://github.com/mozilla/telemetry-batch-view.git
cd telemetry-batch-view
sbt assembly
spark-submit --executor-cores 8 \
             --master yarn \
             --deploy-mode client \
             --class com.mozilla.telemetry.views.LongitudinalView \
             target/scala-2.11/telemetry-batch-view-1.1.jar \
             --to $date \
             --bucket $bucket

parquet2hive -ulv 1 --sql s3://telemetry-parquet/longitudinal | beeline -u jdbc:hive2://${HIVE_SERVER}:10000
