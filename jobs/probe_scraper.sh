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
python probe_scraper/runner.py --outdir $OUTPUT_DIR --tempdir $CACHE_DIR

# Recursively GZIP the files in the output dir. Each file
# will be compressed separately, creating a 'file.gz' file.
gzip -9 -r $OUTPUT_DIR

# Drop the '.gz' extension from the files.
find $OUTPUT_DIR -type f -name '*.gz' | while read f; do mv "$f" "${f%.gz}"; done

# Upload to S3.
aws s3 sync $OUTPUT_DIR/ s3://$BUCKET/ \
       --delete \
       --content-encoding 'gzip' \
       --content-type 'application/json' \
       --cache-control 'max-age=28800' \
       --acl public-read
