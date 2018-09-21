#!/mnt/anaconda2/bin/ipython
import sys
from os import environ

from pyspark.sql import SparkSession
from mozaggregator.parquet import aggregate_metrics, write_aggregates


# Send jobs to the spark workers.
package_file = sys.argv[1]
print "Adding dependency " + package_file
spark = SparkSession.builder.appName('telemetry-aggregates-parquet').getOrCreate()
sc = spark.sparkContext
sc.addPyFile(package_file)

date = environ['date']
channels = [c.strip() for c in environ['channels'].split(',')]

print "Running job for {}".format(date)
aggregates = aggregate_metrics(sc, channels, date)
print "Number of build-id aggregates: {}".format(aggregates[0].count())
print "Number of submission date aggregates: {}".format(aggregates[1].count())

# Write parquet files.
write_aggregates(spark, aggregates, 'append')

sc.stop()
