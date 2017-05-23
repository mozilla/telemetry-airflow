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
base+="e10s_cohort,"
base+="os,"
base+="os_version"

select="substr(subsession_start_date, 0, 10) as activity_date,"
select+="devtools_toolbox_opened_count > 0 as devtools_toolbox_opened,"
select+="case when distribution_id in ('canonical', 'MozillaOnline', 'yandex') "
select+="then distribution_id else null end as top_distribution_id,"
select+=$base

group="activity_date,"
group+="devtools_toolbox_opened,"
group+="top_distribution_id,"
group+=$base

spark-submit --master yarn \
             --deploy-mode client \
             --class com.mozilla.telemetry.views.GenericCountView \
             target/scala-2.11/telemetry-batch-view-1.1.jar \
             --to $date \
             --files "s3://telemetry-parquet/main_summary/v4/" \
             --submission-date-col "submission_date" \
             --count-column "client_id" \
             --select "$select" \
             --grouping-columns "$group" \
             --where "client_id IS NOT NULL" \
             --output "$bucket/client_count"
