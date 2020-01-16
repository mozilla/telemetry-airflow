"""
Bug 1386274 - TAAR similarity-based add-on donor list

This job clusters users into different groups based on their
active add-ons. A representative users sample is selected from
each cluster ("donors") and is saved to a model file along
with a feature vector that will be used, by the TAAR library
module, to perform recommendations.
"""

from datetime import date, timedelta

import json
import logging
import contextlib
import tempfile
import os

import click
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.ml.feature import HashingTF, IDF
from pyspark.ml.clustering import BisectingKMeans
from pyspark.ml import Pipeline
from pyspark.mllib.stat import KernelDensity
from pyspark.statcounter import StatCounter
from scipy.spatial import distance
from mozetl.utils import stop_session_safely
import boto3


def aws_env_credentials():
    """
    Load the AWS credentials from the enviroment
    """
    result = {
        "aws_access_key_id": os.environ.get("AWS_ACCESS_KEY_ID", None),
        "aws_secret_access_key": os.environ.get("AWS_SECRET_ACCESS_KEY", None),
    }
    logging.info("Loading AWS credentials from enviroment: {}".format(str(result)))
    return result


@contextlib.contextmanager
def selfdestructing_path(dirname):
    import shutil

    yield dirname
    shutil.rmtree(dirname)


def write_to_s3(source_file_name, s3_dest_file_name, s3_prefix, bucket):
    """Store the new json file containing current top addons per locale to S3.

    :param source_file_name: The name of the local source file.
    :param s3_dest_file_name: The name of the destination file on S3.
    :param s3_prefix: The S3 prefix in the bucket.
    :param bucket: The S3 bucket.
    """
    client = boto3.client(
        service_name="s3", region_name="us-west-2", **aws_env_credentials()
    )
    transfer = boto3.s3.transfer.S3Transfer(client)

    # Update the state in the analysis bucket.
    key_path = s3_prefix + s3_dest_file_name
    transfer.upload_file(source_file_name, bucket, key_path)


def read_from_s3(s3_dest_file_name, s3_prefix, bucket):
    """
    Read JSON from an S3 bucket and return the decoded JSON blob
    """

    full_s3_name = "{}{}".format(s3_prefix, s3_dest_file_name)
    conn = boto3.resource("s3", region_name="us-west-2")
    stored_data = json.loads(
        conn.Object(bucket, full_s3_name).get()["Body"].read().decode("utf-8")
    )
    return stored_data


def load_amo_curated_whitelist():
    """
    Return the curated whitelist of addon GUIDs
    """
    whitelist = read_from_s3(
        "only_guids_top_200.json",
        "telemetry-ml/addon_recommender/",
        "telemetry-parquet",
    )
    return list(whitelist)


def store_json_to_s3(json_data, base_filename, date, prefix, bucket):
    """Saves the JSON data to a local file and then uploads it to S3.

    Two copies of the file will get uploaded: one with as "<base_filename>.json"
    and the other as "<base_filename><YYYYMMDD>.json" for backup purposes.

    :param json_data: A string with the JSON content to write.
    :param base_filename: A string with the base name of the file to use for saving
        locally and uploading to S3.
    :param date: A date string in the "YYYYMMDD" format.
    :param prefix: The S3 prefix.
    :param bucket: The S3 bucket name.
    """

    tempdir = tempfile.mkdtemp()

    with selfdestructing_path(tempdir):
        JSON_FILENAME = "{}.json".format(base_filename)
        FULL_FILENAME = os.path.join(tempdir, JSON_FILENAME)
        with open(FULL_FILENAME, "w+") as json_file:
            json_file.write(json_data)

        archived_file_copy = "{}{}.json".format(base_filename, date)

        # Store a copy of the current JSON with datestamp.
        write_to_s3(FULL_FILENAME, archived_file_copy, prefix, bucket)
        write_to_s3(FULL_FILENAME, JSON_FILENAME, prefix, bucket)


# Define the set of feature names to be used in the donor computations.
CATEGORICAL_FEATURES = ["city", "locale", "os"]
CONTINUOUS_FEATURES = [
    "subsession_hours_sum",
    "bookmark_count",
    "tab_open_count",
    "total_uri",
    "unique_tlds",
]

logging.basicConfig(level=logging.INFO)
logging.getLogger("py4j").setLevel(logging.ERROR)
logger = logging.getLogger(__name__)


def get_samples(spark, date_from):
    """
    Get a DataFrame with a valid set of sample to base the next
    processing on.

    Sample is limited to submissions received since `date_from` and latest row per each client.

    Reference documentation is found here:

    Firefox Clients Daily telemetry table
    https://docs.telemetry.mozilla.org/datasets/batch_view/clients_daily/reference.html

    BUG 1485152: PR include active_addons to clients_daily table:
    https://github.com/mozilla/telemetry-batch-view/pull/490
    """
    gs_url = "gs://moz-fx-data-derived-datasets-parquet/clients_daily/v6/submission_date_s3={}".format(
        date_from
    )
    parquetFile = spark.read.parquet(gs_url)

    # Use the parquet files to create a temporary view and then used in SQL statements.
    parquetFile.createOrReplaceTempView("clients_daily")

    df = (
        spark.sql("SELECT * FROM clients_daily")
        .where("client_id IS NOT null")
        .where("active_addons IS NOT null")
        .where("size(active_addons) > 2")
        .where("size(active_addons) < 100")
        .where("channel = 'release'")
        .where("app_name = 'Firefox'")
        .where("submission_date_s3 >= {}".format(date_from))
        .selectExpr(
            "client_id as client_id",
            "active_addons as active_addons",
            "city as city",
            "cast(subsession_hours_sum as double)",
            "locale as locale",
            "os as os",
            "places_bookmarks_count_mean AS bookmark_count",
            "scalar_parent_browser_engagement_tab_open_event_count_sum "
            "AS tab_open_count",
            "scalar_parent_browser_engagement_total_uri_count_sum AS total_uri",
            "scalar_parent_browser_engagement_unique_domains_count_mean AS unique_tlds",
            "row_number() OVER (PARTITION BY client_id ORDER BY submission_date_s3 desc) as rn",
        )
        .where("rn = 1")
        .drop("rn")
    )
    return df


def get_addons_per_client(users_df, addon_whitelist, minimum_addons_count):
    """ Extracts a DataFrame that contains one row
    for each client along with the list of active add-on GUIDs.
    """

    def is_valid_addon(guid, addon):
        return not (
            addon.is_system
            or addon.app_disabled
            or addon.type != "extension"
            or addon.user_disabled
            or addon.foreign_install
            or guid not in addon_whitelist
        )

    # Create an add-ons dataset un-nesting the add-on map from each
    # user to a list of add-on GUIDs. Also filter undesired add-ons.

    # Note that this list comprehension was restructured
    # from the original longitudinal query.  In particular, note that
    # each client's 'active_addons' entry is a list containing the
    # a dictionary of {addon_guid: {addon_metadata_dict}}

    def flatten_valid_guid_generator(p):
        for data in p["active_addons"]:
            addon_guid = data["addon_id"]
            if not is_valid_addon(addon_guid, data):
                continue
            yield addon_guid

    return (
        users_df.rdd.map(
            lambda p: (p["client_id"], list(flatten_valid_guid_generator(p)))
        )
        .filter(lambda p: len(p[1]) > minimum_addons_count)
        .toDF(["client_id", "addon_ids"])
    )


def compute_clusters(addons_df, num_clusters, random_seed):
    """ Performs user clustering by using add-on ids as features.
    """

    # Build the stages of the pipeline. We need hashing to make the next
    # steps work.
    hashing_stage = HashingTF(inputCol="addon_ids", outputCol="hashed_features")
    idf_stage = IDF(inputCol="hashed_features", outputCol="features", minDocFreq=1)
    # As a future improvement, we may add a sane value for the minimum cluster size
    # to BisectingKMeans (e.g. minDivisibleClusterSize). For now, just make sure
    # to pass along the random seed if needed for tests.
    kmeans_kwargs = {"seed": random_seed} if random_seed else {}
    bkmeans_stage = BisectingKMeans(k=num_clusters, **kmeans_kwargs)
    pipeline = Pipeline(stages=[hashing_stage, idf_stage, bkmeans_stage])

    # Run the pipeline and compute the results.
    model = pipeline.fit(addons_df)
    return model.transform(addons_df).select(["client_id", "prediction"])


def get_donor_pools(users_df, clusters_df, num_donors, random_seed=None):
    """ Samples users from each cluster.
    """
    cluster_population = clusters_df.groupBy("prediction").count().collect()
    clusters_histogram = [(x["prediction"], x["count"]) for x in cluster_population]

    # Sort in-place from highest to lowest populated cluster.
    clusters_histogram.sort(key=lambda x: x[0], reverse=False)

    # Save the cluster ids and their respective scores separately.
    clusters = [cluster_id for cluster_id, _ in clusters_histogram]
    counts = [donor_count for _, donor_count in clusters_histogram]

    # Compute the proportion of user in each cluster.
    total_donors_in_clusters = sum(counts)
    clust_sample = [float(t) / total_donors_in_clusters for t in counts]
    sampling_proportions = dict(list(zip(clusters, clust_sample)))

    # Sample the users in each cluster according to the proportions
    # and pass along the random seed if needed for tests.
    sampling_kwargs = {"seed": random_seed} if random_seed else {}
    donor_df = clusters_df.sampleBy(
        "prediction", fractions=sampling_proportions, **sampling_kwargs
    )
    # Get the specific number of donors for each cluster and drop the
    # predicted cluster number information.
    current_sample_size = donor_df.count()
    donor_pool_df = donor_df.sample(
        False, float(num_donors) / current_sample_size, **sampling_kwargs
    )
    return clusters, donor_pool_df


def get_donors(
    spark, num_clusters, num_donors, addon_whitelist, date_from, random_seed=None
):
    # Get the data for the potential add-on donors.
    users_sample = get_samples(spark, date_from)
    # Get add-ons from selected users and make sure they are
    # useful for making a recommendation.
    addons_df = get_addons_per_client(users_sample, addon_whitelist, 2)
    addons_df.cache()
    # Perform clustering by using the add-on info.
    clusters = compute_clusters(addons_df, num_clusters, random_seed)
    # Sample representative ("donors") users from each cluster.
    cluster_ids, donors_df = get_donor_pools(
        users_sample, clusters, num_donors, random_seed
    )

    # Finally, get the feature vectors for users that represent
    # each cluster. Since the "active_addons" in "users_sample"
    # are in a |MapType| and contain system add-ons as well, just
    # use the cleaned up list from "addons_df".
    return (
        cluster_ids,
        (
            users_sample.join(donors_df, "client_id")
            .drop("active_addons")
            .join(addons_df, "client_id", "left")
            .drop("client_id")
            .withColumnRenamed("addon_ids", "active_addons")
        ),
    )


def format_donors_dictionary(donors_df):
    cleaned_records = donors_df.drop("prediction").collect()
    # Convert each row to a dictionary.
    return [row.asDict() for row in cleaned_records]


def similarity_function(x, y):
    """ Similarity function for comparing user features.

    This actually really should be implemented in taar.similarity_recommender
    and then imported here for consistency.
    """

    def safe_get(field, row, default_value):
        # Safely get a value from the Row. If the value is None, get the
        # default value.
        return row[field] if row[field] is not None else default_value

    # Extract the values for the categorical and continuous features for both
    # the x and y samples. Use an empty string as the default value for missing
    # categorical fields and 0 for the continuous ones.
    x_categorical_features = [safe_get(k, x, "") for k in CATEGORICAL_FEATURES]
    x_continuous_features = [safe_get(k, x, 0) for k in CONTINUOUS_FEATURES]
    y_categorical_features = [safe_get(k, y, "") for k in CATEGORICAL_FEATURES]
    y_continuous_features = [safe_get(k, y, 0) for k in CONTINUOUS_FEATURES]

    # Here a larger distance indicates a poorer match between categorical variables.
    j_d = distance.hamming(x_categorical_features, y_categorical_features)
    j_c = distance.canberra(x_continuous_features, y_continuous_features)

    # Take the product of similarities to attain a univariate similarity score.
    # Add a minimal constant to prevent zero values from categorical features.
    # Note: since both the distance function return a Numpy type, we need to
    # call the |item| function to get the underlying Python type. If we don't
    # do that this job will fail when performing KDE due to SPARK-20803 on
    # Spark 2.2.0.
    return abs((j_c + 0.001) * j_d).item()


def generate_non_cartesian_pairs(first_rdd, second_rdd):
    # Add an index to all the elements in each RDD.
    rdd1_with_indices = first_rdd.zipWithIndex().map(lambda p: (p[1], p[0]))
    rdd2_with_indices = second_rdd.zipWithIndex().map(lambda p: (p[1], p[0]))
    # Join the RDDs using the indices as keys, then strip
    # them off before returning an RDD like [<v1, v2>, ...]
    return rdd1_with_indices.join(rdd2_with_indices).map(lambda p: p[1])


def get_lr_curves(
    spark, features_df, cluster_ids, kernel_bandwidth, num_pdf_points, random_seed=None
):
    """ Compute the likelihood ratio curves for clustered clients.

    Work-flow followed in this function is as follows:

     * Access the DataFrame including cluster numbers and features.
     * Load same similarity function that will be used in TAAR module.
     * Iterate through each cluster and compute in-cluster similarity.
     * Iterate through each cluster and compute out-cluster similarity.
     * Compute the kernel density estimate (KDE) per similarity score.
     * Linearly down-sample both PDFs to 1000 points.

    :param spark: the SparkSession object.
    :param features_df: the DataFrame containing the user features (e.g. the
                        ones coming from |get_donors|).
    :param cluster_ids: the list of cluster ids (e.g. the one coming from |get_donors|).
    :param kernel_bandwidth: the kernel bandwidth used to estimate the kernel densities.
    :param num_pdf_points: the number of points to sample for the LR-curves.
    :param random_seed: the provided random seed (fixed in tests).
    :return: A list in the following format
        [(idx, (lr-numerator-for-idx, lr-denominator-for-idx)), (...), ...]
    """

    # Instantiate holder lists for inter- and intra-cluster scores.
    same_cluster_scores_rdd = spark.sparkContext.emptyRDD()
    different_clusters_scores_rdd = spark.sparkContext.emptyRDD()

    random_split_kwargs = {"seed": random_seed} if random_seed else {}

    for cluster_number in cluster_ids:
        # Pick the features for users belonging to the current cluster.
        current_cluster_df = features_df.where(col("prediction") == cluster_number)
        # Pick the features for users belonging to all the other clusters.
        other_clusters_df = features_df.where(col("prediction") != cluster_number)

        logger.debug(
            "Computing scores for cluster", extra={"cluster_id": cluster_number}
        )

        # Compares the similarity score between pairs of clients in the same cluster.
        cluster_half_1, cluster_half_2 = current_cluster_df.rdd.randomSplit(
            [0.5, 0.5], **random_split_kwargs
        )
        pair_rdd = generate_non_cartesian_pairs(cluster_half_1, cluster_half_2)
        intra_scores_rdd = pair_rdd.map(lambda r: similarity_function(*r))
        same_cluster_scores_rdd = same_cluster_scores_rdd.union(intra_scores_rdd)

        # Compares the similarity score between pairs of clients in different clusters.
        pair_rdd = generate_non_cartesian_pairs(
            current_cluster_df.rdd, other_clusters_df.rdd
        )
        inter_scores_rdd = pair_rdd.map(lambda r: similarity_function(*r))
        different_clusters_scores_rdd = different_clusters_scores_rdd.union(
            inter_scores_rdd
        )

    # Determine a range of observed similarity values linearly spaced.
    all_scores_rdd = same_cluster_scores_rdd.union(different_clusters_scores_rdd)
    stats = all_scores_rdd.aggregate(
        StatCounter(), StatCounter.merge, StatCounter.mergeStats
    )
    min_similarity = stats.minValue
    max_similarity = stats.maxValue
    lr_index = np.arange(
        min_similarity,
        max_similarity,
        float(abs(min_similarity - max_similarity)) / num_pdf_points,
    )

    # Kernel density estimate for the inter-cluster comparison scores.
    kd_dc = KernelDensity()
    kd_dc.setSample(different_clusters_scores_rdd)
    kd_dc.setBandwidth(kernel_bandwidth)
    denominator_density = kd_dc.estimate(lr_index)

    # Kernel density estimate for the intra-cluster comparison scores.
    kd_sc = KernelDensity()
    kd_sc.setSample(same_cluster_scores_rdd)
    kd_sc.setBandwidth(kernel_bandwidth)
    numerator_density = kd_sc.estimate(lr_index)

    # Structure this in the correct output format.
    return list(zip(lr_index, list(zip(numerator_density, denominator_density))))


def today_minus_90_days():
    return (date.today() + timedelta(days=-90)).strftime("%Y%m%d")


@click.command()
@click.option("--date", required=True)
@click.option("--aws_access_key_id", required=True)
@click.option("--aws_secret_access_key", required=True)
@click.option("--bucket", default="telemetry-parquet")
@click.option("--prefix", default="taar/similarity/")
@click.option("--num_clusters", default=20)
@click.option("--num_donors", default=1000)
@click.option("--kernel_bandwidth", default=0.35)
@click.option("--num_pdf_points", default=1000)
@click.option("--clients_sample_date_from", default=today_minus_90_days())
def main(
    date,
    aws_access_key_id,
    aws_secret_access_key,
    bucket,
    prefix,
    num_clusters,
    num_donors,
    kernel_bandwidth,
    num_pdf_points,
    clients_sample_date_from,
):
    logger.info("Sampling clients since {}".format(clients_sample_date_from))

    # Clobber the AWS access credentials
    os.environ["AWS_ACCESS_KEY_ID"] = aws_access_key_id
    os.environ["AWS_SECRET_ACCESS_KEY"] = aws_secret_access_key

    spark = (
        SparkSession.builder.appName("taar_similarity")
        .enableHiveSupport()
        .getOrCreate()
    )

    if num_donors < 100:
        logger.warn(
            "Less than 100 donors were requested.", extra={"donors": num_donors}
        )
        num_donors = 100

    logger.info("Loading the AMO whitelist...")
    whitelist = load_amo_curated_whitelist()

    logger.info("Computing the list of donors...")

    # Compute the donors clusters and the LR curves.
    cluster_ids, donors_df = get_donors(
        spark, num_clusters, num_donors, whitelist, clients_sample_date_from
    )
    donors_df.cache()
    lr_curves = get_lr_curves(
        spark, donors_df, cluster_ids, kernel_bandwidth, num_pdf_points
    )

    # Store them.
    donors = format_donors_dictionary(donors_df)
    store_json_to_s3(json.dumps(donors, indent=2), "donors", date, prefix, bucket)
    store_json_to_s3(json.dumps(lr_curves, indent=2), "lr_curves", date, prefix, bucket)
    stop_session_safely(spark)
