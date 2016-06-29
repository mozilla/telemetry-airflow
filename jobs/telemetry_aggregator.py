#!/home/hadoop/anaconda2/bin/ipython

import logging
from os import environ
from mozaggregator.aggregator import aggregate_metrics
from mozaggregator.db import submit_aggregates

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler())

date = environ['date']

logger.info("Running job for {}".format(date))
aggregates = aggregate_metrics(sc, ("nightly", "aurora", "beta", "release"), date)
logger.info("Number of build-id aggregates: {}".format(aggregates[0].count()))
logger.info("Number of submission date aggregates: {}".format(aggregates[1].count()))
submit_aggregates(aggregates)
