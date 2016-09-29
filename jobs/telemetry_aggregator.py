#!/home/hadoop/anaconda2/bin/ipython

import logging
from os import environ
from pyspark import SparkContext, SparkConf
from mozaggregator.aggregator import aggregate_metrics
from mozaggregator.db import submit_aggregates

conf = SparkConf().setAppName('telemetry-aggregates')
sc = SparkContext(conf=conf)
date = environ['date']
print "Running job for {}".format(date)
aggregates = aggregate_metrics(sc, ("nightly", "aurora", "beta", "release"), date)
print "Number of build-id aggregates: {}".format(aggregates[0].count())
print "Number of submission date aggregates: {}".format(aggregates[1].count())
submit_aggregates(aggregates)
sc.stop()
