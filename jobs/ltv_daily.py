import argparse
import os
from itertools import chain

import numpy as np
import pandas as pd
import pyspark.sql.functions as F
from google.cloud import bigquery
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType

from lifetimes import BetaGeoFitter


def train_metric(d, metric, plot=True, penalty=0):
    frequency = metric + "_frequency"
    recency = metric + "_recency"
    T = metric + "_T"
    train = d
    train = train[(train[frequency] > 0) & (train[recency] >= 0)]
    train[frequency] = train[frequency] - 1

    bgf = BetaGeoFitter(penalizer_coef=penalty)
    bgf.fit(train[frequency], train[recency], train[T])
    n = bgf.data.shape[0]
    simulated_data = bgf.generate_new_data(size=n)

    model_counts = pd.DataFrame(
        bgf.data["frequency"].value_counts().sort_index().iloc[:28]
    )
    simulated_counts = pd.DataFrame(
        simulated_data["frequency"].value_counts().sort_index().iloc[:28]
    )
    combined_counts = model_counts.merge(
        simulated_counts, how="outer", left_index=True, right_index=True
    ).fillna(0)
    combined_counts.columns = ["Actual", "Model"]
    if plot:
        combined_counts.plot.bar()
        display()
    return combined_counts, bgf


def catch_none(x):
    if x == None:
        return 0
    return x


def ltv_predict(t, frequency, recency, T, model):
    pred = model.conditional_expected_number_of_purchases_up_to_time(
        t, catch_none(frequency), catch_none(recency), catch_none(T)
    )

    if pred > t:
        return float(t)
    return float(pred)


def main(
    submission_date,
    project_id,
    dataset_id,
    source_qualified_table_id,
    intermediate_table_id,
    model_input_table_id,
    model_output_table_id,
    temporary_gcs_bucket,
    training_sample,
    prediction_days,
):
    """Model the lifetime-value (LTV) of clients based on search activity.

    This reads a single partition from a source table into an intermediate table
    in an analysis dataset. The table is transformed for modeling. The model
    inputs and predictions are then stored into separate tables in the same
    dataset.
    """
    print(f"Running ltv_daily job for {submission_date}")

    bq = bigquery.Client()
    table_ref = bq.dataset(dataset_id, project=project_id).table(intermediate_table_id)

    # define the job configuration for the query
    # set params and output destination for the materialized
    # dataset
    job_config = bigquery.QueryJobConfig()
    job_config.query_parameters = [
        bigquery.ScalarQueryParameter("submission_date", "STRING", submission_date)
    ]
    job_config.destination = table_ref
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE

    query = f"""
	SELECT
	    *
	FROM
	    `{source_qualified_table_id}`
	WHERE
	    submission_date = @submission_date
	"""
    query_job = bq.query(query, job_config=job_config)
    query_job.result()

    spark = SparkSession.builder.getOrCreate()
    search_rfm_full = (
        spark.read.format("bigquery")
        .option("table", f"{project_id}.{dataset_id}.{intermediate_table_id}")
        .load()
    )

    columns = [
        [
            F.col(str(metric + ".frequency")).alias(metric + "_frequency"),
            F.col(str(metric + ".recency")).alias(metric + "_recency"),
            F.col(str(metric + ".T")).alias(metric + "_T"),
        ]
        for metric in ["days_searched", "days_tagged_searched", "days_clicked_ads"]
    ]

    # flatten list
    columns = [item for sublist in columns for item in sublist]

    model_perf_data = pd.DataFrame()
    model_pred_data = None
    pred_metrics = ["days_searched", "days_tagged_searched", "days_clicked_ads"]
    search_rfm_ds = search_rfm_full.limit(training_sample).select(columns).toPandas()
    for metric in pred_metrics:
        # train and extract model performace
        model_perf, model = train_metric(search_rfm_ds, metric, plot=False, penalty=0.8)
        model_perf["pct"] = model_perf.Model / (model_perf.Actual + 1) - 1
        model_perf["metric"] = metric
        model_perf["date"] = submission_date
        model_perf_data = pd.concat([model_perf_data, model_perf])

        # make predictions using model
        @F.udf(DoubleType())
        def ltv_predict_metric(metric, model=model):
            import lifetimes

            return ltv_predict(
                prediction_days, metric.frequency, metric.recency, metric.T, model
            )

        # go back to full sample here
        predictions = search_rfm_full.select(
            "*", ltv_predict_metric(metric).alias("prediction_" + metric)
        )

        if not model_pred_data:
            model_pred_data = predictions
        else:
            model_pred_data = model_pred_data.join(
                predictions.select("client_id", "prediction_" + metric), on="client_id"
            )

    predictions = F.create_map(
        list(
            chain(
                *((F.lit(name), F.col("prediction_" + name)) for name in pred_metrics)
            )
        )
    ).alias("predictions")

    model_pred_data = model_pred_data.withColumn("predictions", predictions)
    model_perf_data["active_days"] = model_perf_data.index
    model_perf_data_sdf = spark.createDataFrame(model_perf_data).withColumn(
        "date", F.to_date("date")
    )

    (
        model_perf_data_sdf.write.format("bigquery")
        .option("table", f"{project_id}.{dataset_id}.{model_input_table_id}")
        .option("temporaryGcsBucket", temporary_gcs_bucket)
        .option("partitionField", "date")
        .mode("overwrite")
        .save()
    )

    (
        model_pred_data.write.format("bigquery")
        .option("table", f"{project_id}.{dataset_id}.{model_output_table_id}")
        .option("temporaryGcsBucket", temporary_gcs_bucket)
        .mode("overwrite")
        .save()
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(__doc__)
    parser.add_argument("--submission-date", help="date in YYYY-MM-DD")
    parser.add_argument("--training-sample", type=int, default=500_000)
    parser.add_argument("--prediction-days", type=int, default=28)
    parser.add_argument("--project-id", default="moz-fx-data-bq-data-science")
    parser.add_argument(
        "--source-qualified-table-id",
        default="moz-fx-data-shared-prod.search.search_rfm",
    )
    parser.add_argument("--dataset-id", default="bmiroglio")
    parser.add_argument("--intermediate-table-id", default="search_rfm_day")
    parser.add_argument("--model-input-table-id", default="ltv_daily_model_perf_script")
    parser.add_argument("--model-output-table-id", default="ltv_daily_script")
    parser.add_argument(
        "--temporary-gcs-bucket", default="moz-fx-data-bq-data-science-bmiroglio"
    )
    args = parser.parse_args()

    main(**vars(args))
