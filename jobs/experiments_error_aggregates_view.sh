#!/bin/bash

if [[ -z "$bucket" || -z "$date" ]]; then
   echo "Missing arguments!" 1>&2
   exit 1
fi

git clone https://github.com/mozilla/telemetry-streaming.git
cd telemetry-streaming
sbt assembly

spark-submit --master yarn \
             --deploy-mode client \
             --class com.mozilla.telemetry.streaming.ExperimentsErrorAggregator \
             target/scala-2.11/telemetry-streaming-assembly-0.1-SNAPSHOT.jar \
             --outputPath "s3://$bucket" \
             --numParquetFiles 6 \
             --from $date \
             --to $date \
             $(if [[ ! -z "$channel" ]]; then echo "--channel $channel"; fi)
