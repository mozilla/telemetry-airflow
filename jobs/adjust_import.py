import click
from pyspark.sql import SparkSession
from pyspark.sql.functions import isnull, col, lit, udf
import logging
from google.cloud import bigquery


@click.command()
@click.option('--pbkdf2', 'hash_type', flag_value='pbkdf2', default=True)
@click.option('--bcrypt', 'hash_type', flag_value='bcrypt')
@click.option("--salt", required=True)
@click.option("--iterations", default=1000)
@click.option("--klen", default=32)
@click.option("--project", required=True)
@click.option("--input_table", required=True)
@click.option("--output_table", required=True)
@click.option("--bucket", required=True)
def main(
    hash_type,
    salt,
    iterations,
    klen,
    project,
    input_table,
    output_table,
    bucket,
    **kwargs
):
    bq = bigquery.Client()
    job_config = bigquery.QueryJobConfig()
    tmp_table_name = input_table.replace(".", "_")
    job_config.destination = bq.dataset("tmp", project=project).table(input_table.replace(".", "_"))
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE

    query = f"""
	SELECT
	    *
	FROM
	    `{project}.{input_table}`
	"""
    query_job = bq.query(query, job_config=job_config)
    query_job.result()

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

    @udf("string")
    def bcrypt(msg, salt, iterations, klen):
        import bcrypt
        if msg is None:
            return None
        return bcrypt.hashpw(str.encode(msg), str.encode(salt)).decode('utf-8')

    if hash_type == "pbkdf2":
        hash_func = pbkdf2_sha1hmac
    elif hash_type == "bcrypt":
        hash_func = bcrypt

    (spark.read
        .format("bigquery").option("table", f"{project}.tmp.{tmp_table_name}").load()
        .repartition(1000)
        .where(~isnull("gps_adid"))
        .withColumn("identifier",
            hash_func(
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
