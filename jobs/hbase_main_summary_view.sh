#!/bin/bash

if [[ -z "$date" ]]; then
    echo "Missing arguments!" 1>&2
    exit 1
fi

git clone https://github.com/mozilla/telemetry-batch-view.git
cd telemetry-batch-view
sbt assembly

hbase=$(aws ec2 describe-addresses --filters Name=public-ip,Values=52.24.192.75 | jq -r '.Addresses[0].PrivateIpAddress')
spark-submit --master yarn \
             --deploy-mode client \
             --class com.mozilla.telemetry.views.HBaseMainSummaryView \
             target/scala-2.11/telemetry-batch-view-1.1.jar \
             --hbase-master $hbase \
             --from $date \
             --to $date
