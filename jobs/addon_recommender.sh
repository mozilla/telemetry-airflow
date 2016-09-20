#!/bin/bash

if [[ -z "$privateBucket" || -z "$publicBucket" || -z "$date" ]]; then
    echo "Missing arguments!" 1>&2
    exit 1
fi

git clone https://github.com/mozilla/telemetry-batch-view.git
cd telemetry-batch-view
sbt assembly
mkdir ml_output
spark-submit --master yarn \
             --deploy-mode client \
             --class com.mozilla.telemetry.ml.AddonRecommender \
             target/scala-2.11/telemetry-batch-view-1.1.jar \
             train \
             --output ml_output \
             --privateBucket $privateBucket \
             --publicBucket $publicBucket \
             --runDate $date
