#!/bin/bash

if [[ -z "$bucket" || -z "$date" ]]; then
   echo "Missing arguments!" 1>&2
   exit 1
fi

git clone https://github.com/mreid-moz/telemetry-batch-view.git
cd telemetry-batch-view
git checkout addons_view
sbt assembly
spark-submit --master yarn \
             --deploy-mode client \
             --class com.mozilla.telemetry.views.AddonsView \
             target/scala-2.11/telemetry-batch-view-1.1.jar \
             --bucket $bucket \
             --from $date \
             --to $date
