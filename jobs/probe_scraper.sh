#!/bin/bash

# Print all the executed commands to the terminal.
set -x

OUTPUT_BUCKET="net-mozaws-prod-us-west-2-data-pitmo"
CACHE_BUCKET="telemetry-private-analysis-2/probe-scraper"
CACHE_DIR="probe_cache"
OUTPUT_DIR="probe_data"

aws s3 sync s3://$CACHE_BUCKET/ $CACHE_DIR/ --delete

# Clone and setup the scraper
git clone https://github.com/mozilla/probe-scraper.git
cd probe-scraper
python setup.py bdist_egg
pip install -r requirements.txt

# Create the output and cache directories.
mkdir $CACHE_DIR $OUTPUT_DIR

# Finally run the scraper.
python probe_scraper/runner.py --outdir $OUTPUT_DIR --tempdir $CACHE_DIR

if [ -n "$(find $OUTPUT_DIR -prune -empty 2>/dev/null)" ]
then
    echo "$OUTPUT_DIR is empty"
    exit 1
else
    # The Cloudfront distribution will automatically gzip objects
    # Upload to S3.
    aws s3 sync $OUTPUT_DIR/ s3://$OUTPUT_BUCKET/ \
           --delete \
           --content-type 'application/json' \
           --cache-control 'max-age=28800' \
           --acl public-read
fi

if [ -n "$(find $CACHE_DIR -prune -empty 2>/dev/null)" ]
then
    echo "$CACHE_DIR is empty"
    exit 1
else
    aws s3 sync $CACHE_DIR/ s3://$CACHE_BUCKET/  --delete
fi
