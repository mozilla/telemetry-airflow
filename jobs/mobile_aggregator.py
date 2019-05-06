#!/mnt/anaconda2/bin/ipython
import sys
from os import environ

from pyspark.sql import SparkSession
from mozaggregator.mobile import run


# Send jobs to the spark workers.
package_file = sys.argv[1]
print("Adding dependency: {}".format(package_file))
spark = SparkSession.builder.appName('mobile-aggregates').getOrCreate()
sc = spark.sparkContext
sc.addPyFile(package_file)

date = environ['date']

print("Running job for {}".format(date))
run(spark, date)

sc.stop()
