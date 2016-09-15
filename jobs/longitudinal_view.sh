#!/bin/bash

if [[ -z "$bucket" || -z "$date" ]]; then
    echo "Missing arguments!" 1>&2
    exit 1
fi

git clone -b spark2 https://github.com/mozilla/telemetry-batch-view.git
cd telemetry-batch-view
sbt assembly
spark-submit --executor-cores 8 \
             --conf spark.memory.useLegacyMode=true \
             --conf spark.storage.memoryFraction=0 \
             --master yarn \
             --deploy-mode client \
             --class com.mozilla.telemetry.views.LongitudinalView \
             target/scala-2.11/telemetry-batch-view-1.1.jar \
             --to $date \
             --bucket $bucket
