#!/mnt/anaconda2/bin/ipython

import json
import logging
from os import environ
from boto.s3.connection import S3Connection
from pyspark import SparkContext, SparkConf
from mozaggregator.aggregator import aggregate_metrics
from mozaggregator.db import submit_aggregates, _preparedb
import sys

# This job runs on the Spark cluster and doesn't have access to the Airflow worker's environment,
# but mozaggregator expects a series of POSTGRES_* variables in order to connect to a db instance;
# we pull them into the environment now by reading an object from S3.
creds = json.loads(
    S3Connection(host="s3-us-west-2.amazonaws.com")
    .get_bucket("telemetry-spark-emr-2")
    .get_key("aggregator_database_envvars.json")
    .get_contents_as_string()
)

for k, v in creds.items():
    environ[k] = v

# Attempt a database connection now so we can fail fast if credentials are broken.
_preparedb()

# Send jobs to the spark workers.
package_file = sys.argv[1]
print "Adding dependency " + package_file
conf = SparkConf().setAppName('telemetry-aggregates')
sc = SparkContext(conf=conf)
sc.addPyFile(package_file)

date = environ['date']
channels = [c.strip() for c in environ['channels'].split(',')]

print "Running job for {}".format(date)
aggregates = aggregate_metrics(sc, channels, date)
print "Number of build-id aggregates: {}".format(aggregates[0].count())
print "Number of submission date aggregates: {}".format(aggregates[1].count())

# Store the results in Postgres.
submit_aggregates(aggregates)

sc.stop()
