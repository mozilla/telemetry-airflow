#!/bin/bash

if [[ -z "$bucket" || -z "$date" ]]; then
   echo "Missing arguments!" 1>&2
   exit 1
fi

git clone -b spark1.6 https://github.com/mozilla/telemetry-batch-view.git
cd telemetry-batch-view
sbt assembly
spark-submit --master yarn-client \
             --class com.mozilla.telemetry.views.MainSummaryView \
             target/scala-2.10/telemetry-batch-view-1.1.jar \
             --bucket $bucket \
             --from $date \
             --to $date
