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
             --class com.mozilla.telemetry.views.MainSummaryView \
             target/scala-2.11/telemetry-batch-view-1.1.jar \
             --bucket $bucket \
             --from $date \
             --to $date

if [[ -n "$glue_access_key_id" ]]; then
  pip install --upgrade --user git+https://github.com/robotblake/pdsm.git#egg=pdsm
  AWS_ACCESS_KEY_ID=$glue_access_key_id \
    AWS_SECRET_ACCESS_KEY=$glue_secret_access_key \
    AWS_DEFAULT_REGION=$glue_default_region \
    ~/.local/bin/pdsm s3://$bucket/main_summary
fi
