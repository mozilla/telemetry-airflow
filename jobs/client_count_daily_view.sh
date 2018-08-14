#!/bin/bash

if [[ -z "$bucket" || -z "$date" ]]; then
    echo "Missing arguments!" 1>&2
    exit 1
fi

git clone https://github.com/mozilla/telemetry-batch-view.git
cd telemetry-batch-view
sbt assembly

base="normalized_channel,"
base+="country,"
base+="locale,"
base+="app_name,"
base+="app_version,"
base+="e10s_enabled,"
base+="os"

select+="devtools_toolbox_opened_count > 0 as devtools_toolbox_opened,"
select+="case when distribution_id in ('canonical', 'MozillaOnline', 'yandex')"
select+=" then distribution_id else null end as top_distribution_id,"
select+="normalized_os_version as os_version,"
select+="bucketed(scalar_parent_browser_engagement_total_uri_count,"
select+=" array(int('0'), int('5'), int('20'), int('50'), int('100'), int('250')))"
select+=" as total_uri_count_threshold,"
select+=$base

group+="devtools_toolbox_opened,"
group+="top_distribution_id,"
group+="os_version,"
group+="total_uri_count_threshold,"
group+=$base

spark-submit --master yarn \
             --deploy-mode client \
             --class com.mozilla.telemetry.views.GenericCountView \
             target/scala-2.11/telemetry-batch-view-1.1.jar \
             --version "v2" \
             --output-partition "submission_date=$date" \
             --from $date \
             --to $date \
             --files "s3://$bucket/main_summary/v4/" \
             --count-column "client_id" \
             --select "$select" \
             --grouping-columns "$group" \
             --where "client_id IS NOT NULL" \
             --output "$bucket/client_count_daily" \
             --num-parquet-files 5
