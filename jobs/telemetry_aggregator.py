#!/home/hadoop/anaconda2/bin/ipython

import logging
from os import environ
from mozaggregator.aggregator import aggregate_metrics
from mozaggregator.db import submit_aggregates

date = environ['date']
print "Running job for {}".format(date)
aggregates = aggregate_metrics(sc, ("nightly", "aurora", "beta", "release"), date)
print "Number of build-id aggregates: {}".format(aggregates[0].count())
print "Number of submission date aggregates: {}".format(aggregates[1].count())
submit_aggregates(aggregates)
