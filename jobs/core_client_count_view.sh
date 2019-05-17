#!/bin/bash

if [[ -z "$bucket" || -z "$date" ]]; then
    echo "Missing arguments!" 1>&2
    exit 1
fi

git clone https://github.com/mozilla/telemetry-batch-view.git
cd telemetry-batch-view
sbt assembly

base="metadata.normalized_channel as channel,"
base+="default_search," # 143 values
base+="locale,"
base+="app_name,"
base+="os,"
base+="osversion,"
base+="distribution_id," # 31 values
base+="arch" # 5 values

year=$(date +"%Y")
prev_year=$(($year - 1))
next_year=$(($year + 1))

select="regexp_extract(created, '(($prev_year|$year|$next_year)-[0-9]{2}-[0-9]{2})', 1) as created_date,"
select+="metadata.geo_country as geo_country," # 247 values
select+=$base

group="created_date,"
group+="geo_country,"
group+=$base

spark-submit --master yarn \
             --deploy-mode client \
             --class com.mozilla.telemetry.views.GenericCountView \
             target/scala-2.11/telemetry-batch-view-1.1.jar \
             --to $date \
             --files "s3://net-mozaws-prod-us-west-2-pipeline-data/telemetry-core-parquet/v3" \
             --count-column "client_id" \
             --select "$select" \
             --grouping-columns "$group" \
             --where "client_id IS NOT NULL" \
             --output "$bucket/core_client_count"
