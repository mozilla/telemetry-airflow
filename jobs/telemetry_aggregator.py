#!/home/hadoop/anaconda2/bin/ipython

import logging
from os import environ
from mozaggregator.aggregator import aggregate_metrics
from mozaggregator.db import submit_aggregates

root = logging.getLogger(__name__)
root.setLevel(logging.INFO)

date = environ['date']
logging.info("Running job for {}".format(date))
aggregates = aggregate_metrics(sc, ("nightly", "aurora", "beta", "release"), date)
logging.info("Number of build-id aggregates: {}".format(aggregates[0].count()))
logging.info("Number of submission date aggregates: {}".format(aggregates[1].count()))
submit_aggregates(aggregates)
