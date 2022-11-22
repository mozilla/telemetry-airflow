"""
Bug 1386274 - TAAR similarity-based add-on donor list

This job clusters users into different groups based on their
active add-ons. A representative users sample is selected from
each cluster ("donors") and is saved to a model file along
with a feature vector that will be used, by the TAAR library
module, to perform recommendations.

Migrated from https://github.com/mozilla/python_mozetl/blob/3d1ca45f7460c3efa3336abaf18685598219125c/mozetl/taar/taar_similarity.py
"""

from datetime import date, timedelta

import io
import bz2
from collections.abc import Mapping, Iterable
from decimal import Decimal
import json
import logging
import contextlib
import tempfile
import os

import click
import numpy as np
from google.cloud import storage
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.ml.feature import HashingTF, IDF
from pyspark.ml.clustering import BisectingKMeans
from pyspark.ml import Pipeline
from pyspark.mllib.stat import KernelDensity
from pyspark.statcounter import StatCounter
from scipy.spatial import distance
from google.cloud import bigquery


def read_from_gcs(fname, prefix, bucket):
    simple_fname = f"{prefix}/{fname}.bz2"
    try:
        with io.BytesIO() as tmpfile:
            client = storage.Client()
            bucket = client.get_bucket(bucket)
            blob = bucket.blob(simple_fname)
            blob.download_to_file(tmpfile)
            tmpfile.seek(0)
            payload = tmpfile.read()
            payload = bz2.decompress(payload)
            return json.loads(payload.decode("utf8"))
    except Exception:
        logger.exception(f"Error reading from GCS gs://{bucket}/{simple_fname}")


def load_amo_curated_whitelist(bucket):
    """
    Return the curated whitelist of addon GUIDs
    """
    whitelist = read_from_gcs(
        "only_guids_top_200.json", "addon_recommender", bucket,
    )
    return list(whitelist)


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


def get_table(view):
    """Helper for determining what table underlies a user-facing view, since the Storage API can't read views."""
    bq = bigquery.Client()
    view = view.replace(":", ".")
    # partition filter is required, so try a couple options
    for partition_column in ["DATE(submission_timestamp)", "submission_date"]:
        try:
            job = bq.query(
                f"SELECT * FROM `{view}` WHERE {partition_column} = CURRENT_DATE",
                bigquery.QueryJobConfig(dry_run=True),
            )
            break
        except Exception:
            continue
    else:
        raise ValueError("could not determine partition column")
    assert len(job.referenced_tables) == 1, "View combines multiple tables"
    table = job.referenced_tables[0]
    return f"{table.project}:{table.dataset_id}.{table.table_id}"


def get_samples(spark, iso_today):
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

    df = (
        spark.read.format("bigquery")
        .option(
            "table",
            get_table("moz-fx-data-shared-prod.telemetry.clients_last_seen"),
        )
        .option("filter", "submission_date = '{}'".format(iso_today))
        .load()
    )
    df.createOrReplaceTempView("tiny_clients_daily")

    df = spark.sql(
        """
       select
            client_id,
            active_addons as active_addons,
            city as city,
            subsession_hours_sum,
            locale as locale,
            os as os,
            places_bookmarks_count_mean AS bookmark_count,
            scalar_parent_browser_engagement_tab_open_event_count_sum AS tab_open_count,
            scalar_parent_browser_engagement_total_uri_count_sum AS total_uri,
            scalar_parent_browser_engagement_unique_domains_count_mean AS unique_tlds
        FROM
            tiny_clients_daily
        WHERE
            client_id is not null and
            size(active_addons) > 2 and
            size(active_addons) < 100 and
            channel = 'release' and
            app_name = 'Firefox'
     """
    )

    return df


def get_addons_per_client(users_df, addon_whitelist, minimum_addons_count):
    """ Extracts a DataFrame that contains one row
    for each client along with the list of active add-on GUIDs.
    """

    print("min addon count = {}".format(minimum_addons_count))

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
    idf_stage = IDF(
        inputCol="hashed_features", outputCol="features", minDocFreq=1
    )
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
    clusters_histogram = [
        (x["prediction"], x["count"]) for x in cluster_population
    ]

    # Sort in-place from highest to lowest populated cluster.
    clusters_histogram.sort(key=lambda x: x[0], reverse=False)

    # Save the cluster ids and their respective scores separately.
    clusters = [cluster_id for cluster_id, _ in clusters_histogram]
    counts = [donor_count for _, donor_count in clusters_histogram]

    # Compute the proportion of user in each cluster.
    total_donors_in_clusters = sum(counts)
    print("Total donors in cluster: %d" % total_donors_in_clusters)
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
    print("num_donors: %f" % float(num_donors))
    print("current_sample_size: %d" % current_sample_size)
    donor_pool_df = donor_df.sample(
        False, float(num_donors) / current_sample_size, **sampling_kwargs
    )
    return clusters, donor_pool_df


def get_donors(
    spark,
    num_clusters,
    num_donors,
    addon_whitelist,
    iso_today,
    random_seed=None,
):
    # Get the data for the potential add-on donors.
    users_sample = get_samples(spark, iso_today)
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
    y_categorical_features = [safe_get(k, y, "") for k in CATEGORICAL_FEATURES]
    x_continuous_features = [
        float(safe_get(k, x, 0)) for k in CONTINUOUS_FEATURES
    ]
    y_continuous_features = [
        float(safe_get(k, y, 0)) for k in CONTINUOUS_FEATURES
    ]

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
    spark,
    features_df,
    cluster_ids,
    kernel_bandwidth,
    num_pdf_points,
    random_seed=None,
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
        current_cluster_df = features_df.where(
            col("prediction") == cluster_number
        )
        # Pick the features for users belonging to all the other clusters.
        other_clusters_df = features_df.where(
            col("prediction") != cluster_number
        )

        logger.debug(
            "Computing scores for cluster", extra={"cluster_id": cluster_number}
        )

        # Compares the similarity score between pairs of clients in the same cluster.
        cluster_half_1, cluster_half_2 = current_cluster_df.rdd.randomSplit(
            [0.5, 0.5], **random_split_kwargs
        )
        pair_rdd = generate_non_cartesian_pairs(cluster_half_1, cluster_half_2)
        intra_scores_rdd = pair_rdd.map(lambda r: similarity_function(*r))
        same_cluster_scores_rdd = same_cluster_scores_rdd.union(
            intra_scores_rdd
        )

        # Compares the similarity score between pairs of clients in different clusters.
        pair_rdd = generate_non_cartesian_pairs(
            current_cluster_df.rdd, other_clusters_df.rdd
        )
        inter_scores_rdd = pair_rdd.map(lambda r: similarity_function(*r))
        different_clusters_scores_rdd = different_clusters_scores_rdd.union(
            inter_scores_rdd
        )

    # Determine a range of observed similarity values linearly spaced.
    all_scores_rdd = same_cluster_scores_rdd.union(
        different_clusters_scores_rdd
    )
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
    return list(
        zip(lr_index, list(zip(numerator_density, denominator_density)))
    )


class DecimalEncoder(json.JSONEncoder):
    def encode(self, obj):
        if isinstance(obj, Mapping):
            return (
                "{"
                + ", ".join(
                    f"{self.encode(k)}: {self.encode(v)}"
                    for (k, v) in obj.items()
                )
                + "}"
            )
        if isinstance(obj, Iterable) and (not isinstance(obj, str)):
            return "[" + ", ".join(map(self.encode, obj)) + "]"
        if isinstance(obj, Decimal):
            return f"{obj.normalize():f}"  # using normalize() gets rid of trailing 0s, using ':f' prevents scientific notation
        return super().encode(obj)


# TODO: json reading from GCS as well as writing is repeated several times across Spark jobs
# TODO: we should move them to a library, may be taar one, since similar code is used there too
def store_json_to_gcs(
    bucket, prefix, filename, json_obj, iso_date_str,
):
    """Saves the JSON data to a local file and then uploads it to GCS.
    Two copies of the file will get uploaded: one with as "<base_filename>.json"
    and the other as "<base_filename><YYYYMMDD>.json" for backup purposes.

    :param bucket: The GCS bucket name.
    :param prefix: The GCS prefix.
    :param filename: A string with the base name of the file to use for saving
        locally and uploading to GCS
    :param json_data: A string with the JSON content to write.
    :param date: A date string in the "YYYYMMDD" format.
    """

    try:
        byte_data = json.dumps(json_obj, cls=DecimalEncoder).encode("utf8")

        byte_data = bz2.compress(byte_data)
        logger.info(f"Compressed data is {len(byte_data)} bytes")

        client = storage.Client()
        bucket = client.get_bucket(bucket)
        simple_fname = f"{prefix}/{filename}.bz2"
        blob = bucket.blob(simple_fname)
        blob.chunk_size = 5 * 1024 * 1024  # Set 5 MB blob size
        print(f"Wrote out {simple_fname}")
        blob.upload_from_string(byte_data)
        long_fname = f"{prefix}/{filename}.{iso_date_str}.bz2"
        blob = bucket.blob(long_fname)
        blob.chunk_size = 5 * 1024 * 1024  # Set 5 MB blob size
        print(f"Wrote out {long_fname}")
        blob.upload_from_string(byte_data)
    except Exception:
        logger.exception(f"Error saving to GCS, Bucket: {bucket}, base object name: {prefix}/{filename}")


@click.command()
@click.option("--date", required=True)
@click.option("--bucket", default="taar_models")
@click.option("--prefix", default="taar/similarity")
@click.option("--num_clusters", default=20)
@click.option("--num_donors", default=1000)
@click.option("--kernel_bandwidth", default=0.35)
@click.option("--num_pdf_points", default=1000)
def main(
    date,
    bucket,
    prefix,
    num_clusters,
    num_donors,
    kernel_bandwidth,
    num_pdf_points,
):
    logger.info("Sampling clients since {}".format(date))

    spark = (
        SparkSession.builder.appName("taar_similarity")
        .getOrCreate()
    )

    if num_donors < 100:
        logger.warning(
            "Less than 100 donors were requested.", extra={"donors": num_donors}
        )
        num_donors = 100

    logger.info("Loading the AMO whitelist...")
    whitelist = load_amo_curated_whitelist(bucket)

    logger.info("Computing the list of donors...")

    # Compute the donors clusters and the LR curves.
    cluster_ids, donors_df = get_donors(
        spark, num_clusters, num_donors, whitelist, date,
    )
    donors_df.cache()
    lr_curves = get_lr_curves(
        spark, donors_df, cluster_ids, kernel_bandwidth, num_pdf_points
    )

    # Store them.
    donors = format_donors_dictionary(donors_df)

    store_json_to_gcs(bucket, prefix, "donors.json", donors, date)
    store_json_to_gcs(bucket, prefix, "lr_curves.json", lr_curves, date)

    spark.stop()


if __name__ == "__main__":
    main()
