
if [[ -z "$bucket" || -z "$date" ]]; then
    echo "Missing arguments!" 1>&2
    exit 1
fi

git clone https://github.com/mozilla/telemetry-batch-view.git

cd telemetry-batch-view
sbt assembly

spark-submit \
  --executor-cores 8  \
  --master yarn \
  --deploy-mode client \
  --class com.mozilla.telemetry.views.GenericLongitudinalView target/scala-2.11/telemetry-batch-view-1.1.jar \
  --to "$date" \
  --tablename telemetry_focus_event_parquet \
  --output-path "$bucket/focus_event_longitudinal" \
  --num-parquet-files 30 \
  --ordering-columns "seq,created"
