#!/bin/bash

# Print all the executed commands to the terminal.
set -x

BUCKET="net-mozaws-prod-us-west-2-data-pitmo"
CACHE_DIR="probe_cache"
OUTPUT_DIR="probe_data"

# Clone and setup the scraper
git clone https://github.com/mozilla/probe-scraper.git
cd probe-scraper
python setup.py bdist_egg
pip install -r requirements.txt

# Create the output and cache directories.
mkdir $CACHE_DIR $OUTPUT_DIR

# Finally run the scraper.
python probe_scraper/runner.py --out-dir $OUTPUT_DIR --cache-dir $CACHE_DIR

if [ -n "$(find $OUTPUT_DIR -prune -empty 2>/dev/null)" ]
then
    echo "$OUTPUT_DIR is empty"
    exit 1
else
    # The Cloudfront distribution will automatically gzip objects
    # Upload to S3.
    aws s3 sync $OUTPUT_DIR/ s3://$BUCKET/ \
           --delete \
           --content-type 'application/json' \
           --cache-control 'max-age=28800' \
           --acl public-read
fi
