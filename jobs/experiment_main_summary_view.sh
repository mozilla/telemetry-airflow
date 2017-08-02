#!/bin/bash

if [[ -z "$bucket" || -z "$date" ]]; then
  echo "Missing arguments!" 1>&2
  exit 1
fi

# If no branch specified, default to "master".
branch=${branch:-master}

# If no input bucket specified, default to the output bucket.
inbucket=${inbucket:-$bucket}


git clone https://github.com/mozilla/telemetry-batch-view.git
cd telemetry-batch-view
git checkout $branch
if [ $? -eq 0 ]; then
   sbt assembly
   spark-submit --master yarn \
                --deploy-mode client \
                --class com.mozilla.telemetry.views.ExperimentSummaryView \
                target/scala-2.11/telemetry-batch-view-1.1.jar \
                --inbucket $inbucket \
                --bucket $bucket \
                --from $date \
                --to $date
fi
