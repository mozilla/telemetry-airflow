#!/bin/bash

if [[ -z "$date" || -z "$max_requests" || -z "$key_file" || -z "$artifact" ]]; then
    echo "Missing arguments!" 1>&2
    exit 1
fi

artifact_filename="artifact.jar"
wget "$artifact" -O "$artifact_filename"

key_filename="amplitude_key_file"
aws s3 cp "$key_file" "$key_filename"
export AMPLITUDE_API_KEY=$(cat "$key_filename")

spark-submit --master yarn \
             --deploy-mode client \
             --class com.mozilla.telemetry.streaming.EventsToAmplitude $artifact_filename \
             --config-file-path ./configs/focus_android_events_schemas.json \
             --url https://api.amplitude.com/httpapi \
             --from $date \
             --to $date \
             --max-parallel-requests $max_requests
