#!/bin/bash

if [[ -z "$date" || -z "$max_requests" || -z "$key_file" ]]; then
    echo "Missing arguments!" 1>&2
    exit 1
fi

git clone https://github.com/mozilla/telemetry-streaming.git
cd telemetry-streaming
sbt assembly

key_filename="amplitude_key_file"
aws s3 cp "$key_file" "$key_filename"
export AMPLITUDE_API_KEY=$(cat "$key_filename")

spark-submit --master yarn \
             --deploy-mode client \
             --class com.mozilla.telemetry.streaming.EventsToAmplitude \
             target/scala-2.11/telemetry-streaming-assembly-0.1-SNAPSHOT.jar \
             --config-file-path ./configs/focus_android_events_schemas.json \
             --url https://api.amplitude.com/httpapi \
             --from $date \
             --to $date \
             --max-parallel-requests $max_requests
