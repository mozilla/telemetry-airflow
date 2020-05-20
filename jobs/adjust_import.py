import click
from pyspark.sql import SparkSession
from pyspark.sql.functions import isnull, col, lit, udf


@click.command()
@click.option("--salt", required=True)
@click.option("--iterations", default=1000)
@click.option("--klen", default=32)
@click.option("--project", required=True)
@click.option("--input_table", required=True)
@click.option("--output_table", required=True)
@click.option("--bucket", required=True)
def main(
    salt,
    iterations,
    klen,
    project,
    input_table,
    output_table,
    bucket,
):
    spark = (SparkSession
        .builder
        .appName("adjust_gps_hash")
        .getOrCreate())

    @udf("string")
    def pbkdf2_sha1hmac(msg, salt, iterations, klen):
        import hashlib
        import base64
        return base64.b64encode(
            hashlib.pbkdf2_hmac('sha1', str.encode(msg), str.encode(salt), iterations, klen)
        ).decode()

    (spark.read
        .format("bigquery").option("table", f"{project}.{input_table}").load()
        .where(~isnull("gps_adid"))
        .withColumn("identifier",
            pbkdf2_sha1hmac(
                col("gps_adid"),
                lit(salt),
                lit(iterations),
                lit(klen)))
        .select("identifier", "installed_at")
        .write.format("bigquery")
        .option("table", f"{project}.{output_table}")
        .option("temporaryGcsBucket", bucket)
        .mode("overwrite")
        .save())

    spark.stop()

if __name__ == "__main__":
    main()
