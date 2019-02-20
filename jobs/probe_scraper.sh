#!/bin/bash

# Print all the executed commands to the terminal.
set -x

BUCKET="net-mozaws-prod-us-west-2-data-pitmo"
CACHE_DIR="probe_cache"
OUTPUT_DIR="probe_data"

ANACONDA_PATH=/mnt/miniconda3
ANACONDA_SCRIPT=Miniconda3-4.5.12-Linux-x86_64.sh

wget --no-clobber --no-verbose -P /mnt https://repo.anaconda.com/miniconda/$ANACONDA_SCRIPT
rm -rf $ANACONDA_PATH
bash /mnt/$ANACONDA_SCRIPT -b -p $ANACONDA_PATH

export PATH=$ANACONDA_PATH/bin:$PATH
rm /mnt/$ANACONDA_SCRIPT

# Just to ensure, when looking at logs, we are using the correct python binary
echo $(which python3)

# Clone and setup the scraper
git clone https://github.com/mozilla/probe-scraper.git
cd probe-scraper
python3 setup.py bdist_egg
python3 -m pip install -r requirements.txt

# Create the output and cache directories.
mkdir $CACHE_DIR $OUTPUT_DIR

# Finally run the scraper.
# Using -m allows for relative imports in runner.py
python3 -m probe_scraper.runner --out-dir $OUTPUT_DIR --cache-dir $CACHE_DIR

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
