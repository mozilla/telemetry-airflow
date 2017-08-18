#!/bin/bash

if [[ -z "$date" ]]; then
    echo "Missing arguments!" 1>&2
    exit 1
fi

reservations=$(aws ec2 describe-instances --filters "Name=tag:Name,Values=data-taar-hbase-dev-1" "Name=tag:aws:elasticmapreduce:instance-group-role,Values=MASTER" | jq -r '.Reservations')
num_masters=$(echo $reservations | jq -r 'length')

if [[ $num_masters != 1 ]]; then
  echo "Invalid number of master nodes found!"
  exit 1
fi

hbase=$(echo $reservations | jq -r '.[0].Instances[0].NetworkInterfaces[0].PrivateIpAddress')

git clone https://github.com/mozilla/telemetry-batch-view.git
cd telemetry-batch-view
sbt assembly
spark-submit --master yarn \
             --deploy-mode client \
             --class com.mozilla.telemetry.views.HBaseAddonRecommenderView \
             target/scala-2.11/telemetry-batch-view-1.1.jar \
             --hbase-master $hbase \
             --from $date \
             --to $date
