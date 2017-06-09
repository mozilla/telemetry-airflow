#!/bin/bash

if [[ -z "$bucket" || -z "$date" ]]; then
   echo "Missing arguments!" 1>&2
   exit 1
fi

git clone https://github.com/mozilla/telemetry-batch-view.git
cd telemetry-batch-view
sbt assembly
spark-submit --master yarn \
             --deploy-mode client \
             --class com.mozilla.telemetry.views.ExperimentAnalysisView \
             target/scala-2.11/telemetry-batch-view-1.1.jar \
             --input "s3://$bucket/experiments/v1" \
             --output "s3://$bucket/experiments_aggregates/v1" \
             --date $date
