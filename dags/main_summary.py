from airflow import DAG
from datetime import datetime, timedelta
from operators.emr_spark_operator import EMRSparkOperator
from airflow.operators.moz_databricks import MozDatabricksSubmitRunOperator
from airflow.operators.subdag_operator import SubDagOperator
from operators.email_schema_change_operator import EmailSchemaChangeOperator
from utils.mozetl import mozetl_envvar
from utils.tbv import tbv_envvar
from utils.status import register_status
from utils.gcp import (
    bigquery_etl_query,
    bigquery_etl_copy_deduplicate,
    export_to_parquet,
    load_to_bigquery
)

default_args = {
    'owner': 'frank@mozilla.com',
    'depends_on_past': False,
    'start_date': datetime(2018, 11, 27),
    'email': ['telemetry-alerts@mozilla.com', 'frank@mozilla.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=30),
}

# Make sure all the data for the given day has arrived before running.
# Running at 1am should suffice.
dag = DAG('main_summary', default_args=default_args, schedule_interval='0 1 * * *')

# We copy yesterday's main pings from telemetry_live to telemetry_stable
# at the root of this DAG because telemetry_stable.main_v4 will become
# the source for main_summary, etc. once we are comfortable retiring parquet
# data imports.
copy_deduplicate_main_ping = bigquery_etl_copy_deduplicate(
    task_id="copy_deduplicate_main_ping",
    target_project_id="moz-fx-data-shared-prod",
    only_tables=["telemetry_live.main_v4"],
    owner="jklukas@mozilla.com",
    email=["telemetry-alerts@mozilla.com", "relud@mozilla.com", "jklukas@mozilla.com"],
    dag=dag)

main_summary_all_histograms = MozDatabricksSubmitRunOperator(
    task_id="main_summary_all_histograms",
    job_name="Main Summary View - All Histograms",
    execution_timeout=timedelta(hours=12),
    instance_count=5,
    max_instance_count=50,
    enable_autoscale=True,
    instance_type="c4.4xlarge",
    spot_bid_price_percent=50,
    ebs_volume_count=1,
    ebs_volume_size=250,
    env=tbv_envvar("com.mozilla.telemetry.views.MainSummaryView",
        options={
            "from": "{{ ds_nodash }}",
            "to": "{{ ds_nodash }}",
            "bucket": "telemetry-backfill",
            "all_histograms": "",
            "read-mode": "aligned",
            "input-partition-multiplier": "400",
        },
        dev_options={
            "channel": "nightly",
        }),
    dag=dag)

main_summary = MozDatabricksSubmitRunOperator(
    task_id="main_summary",
    job_name="Main Summary View",
    execution_timeout=timedelta(hours=4),
    email=["telemetry-alerts@mozilla.com", "frank@mozilla.com", "main_summary_dataset@moz-svc-ops.pagerduty.com"],
    instance_count=5,
    max_instance_count=40,
    enable_autoscale=True,
    instance_type="c4.4xlarge",
    spot_bid_price_percent=50,
    ebs_volume_count=1,
    ebs_volume_size=250,
    env=tbv_envvar("com.mozilla.telemetry.views.MainSummaryView",
        options={
            "from": "{{ ds_nodash }}",
            "to": "{{ ds_nodash }}",
            "schema-report-location": "s3://{{ task.__class__.private_output_bucket }}/schema/main_summary/submission_date_s3={{ ds_nodash }}",
            "bucket": "{{ task.__class__.private_output_bucket }}",
            "read-mode": "aligned",
            "input-partition-multiplier": "400"
        },
        dev_options={
            "channel": "nightly",   # run on smaller nightly data rather than release
        }),
    dag=dag)

register_status(main_summary, "Main Summary", "A summary view of main pings.")

main_summary_schema = EmailSchemaChangeOperator(
    task_id="main_summary_schema",
    email=["telemetry-alerts@mozilla.com", "relud@mozilla.com"],
    to=["bimsland@mozilla.com", "telemetry-alerts@mozilla.com"],
    key_prefix='schema/main_summary/submission_date_s3=',
    dag=dag)

main_summary_bigquery_load = SubDagOperator(
    subdag=load_to_bigquery(
        parent_dag_name=dag.dag_id,
        dag_name="main_summary_bigquery_load",
        default_args=default_args,
        dataset_s3_bucket="telemetry-parquet",
        aws_conn_id="aws_dev_iam_s3",
        dataset="main_summary",
        dataset_version="v4",
        gke_cluster_name="bq-load-gke-1",
        bigquery_dataset="telemetry_derived",
        cluster_by=["sample_id"],
        drop=["submission_date"],
        rename={"submission_date_s3": "submission_date"},
        replace=["SAFE_CAST(sample_id AS INT64) AS sample_id"],
        ),
    task_id="main_summary_bigquery_load",
    dag=dag)

engagement_ratio = EMRSparkOperator(
    task_id="engagement_ratio",
    job_name="Update Engagement Ratio",
    execution_timeout=timedelta(hours=6),
    instance_count=10,
    env=mozetl_envvar("engagement_ratio",
        options={
            "input_bucket": "{{ task.__class__.private_output_bucket }}",
            "output_bucket": "net-mozaws-prod-us-west-2-pipeline-analysis"
        },
        dev_options={
            "output_bucket": "{{ task.__class__.private_output_bucket }}"
        }),
    uri="https://raw.githubusercontent.com/mozilla/python_mozetl/master/bin/mozetl-submit.sh",
    output_visibility="public",
    dag=dag)

addons = EMRSparkOperator(
    task_id="addons",
    job_name="Addons View",
    execution_timeout=timedelta(hours=4),
    instance_count=3,
    env=tbv_envvar("com.mozilla.telemetry.views.AddonsView", {
        "from": "{{ ds_nodash }}",
        "to": "{{ ds_nodash }}",
        "bucket": "{{ task.__class__.private_output_bucket }}"}),
    uri="https://raw.githubusercontent.com/mozilla/telemetry-airflow/master/jobs/telemetry_batch_view.py",
    dag=dag)

addons_bigquery_load = SubDagOperator(
    subdag=load_to_bigquery(
        parent_dag_name=dag.dag_id,
        dag_name="addons_bigquery_load",
        default_args=default_args,
        dataset_s3_bucket="telemetry-parquet",
        aws_conn_id="aws_dev_iam_s3",
        dataset="addons",
        dataset_version="v2",
        gke_cluster_name="bq-load-gke-1",
        bigquery_dataset="telemetry_derived",
        cluster_by=["sample_id"],
        rename={"submission_date_s3": "submission_date"},
        replace=["SAFE_CAST(sample_id AS INT64) AS sample_id"],
        ),
    task_id="addons_bigquery_load",
    dag=dag)

main_events = EMRSparkOperator(
    task_id="main_events",
    job_name="Main Events View",
    execution_timeout=timedelta(hours=4),
    owner="ssuh@mozilla.com",
    email=["telemetry-alerts@mozilla.com", "ssuh@mozilla.com"],
    instance_count=1,
    env=tbv_envvar("com.mozilla.telemetry.views.MainEventsView", {
        "from": "{{ ds_nodash }}",
        "to": "{{ ds_nodash }}",
        "bucket": "{{ task.__class__.private_output_bucket }}"}),
    uri="https://raw.githubusercontent.com/mozilla/telemetry-airflow/master/jobs/telemetry_batch_view.py",
    dag=dag)

main_events_bigquery_load = SubDagOperator(
    subdag=load_to_bigquery(
        parent_dag_name=dag.dag_id,
        dag_name="main_events_bigquery_load",
        default_args=default_args,
        dataset_s3_bucket="telemetry-parquet",
        aws_conn_id="aws_dev_iam_s3",
        dataset="events",
        dataset_version="v1",
        gke_cluster_name="bq-load-gke-1",
        bigquery_dataset="telemetry_derived",
        cluster_by=["sample_id"],
        rename={"submission_date_s3": "submission_date"},
        replace=["SAFE_CAST(sample_id AS INT64) AS sample_id"],
        ),
    task_id="main_events_bigquery_load",
    dag=dag)

addon_aggregates = EMRSparkOperator(
    task_id="addon_aggregates",
    job_name="Addon Aggregates View",
    execution_timeout=timedelta(hours=8),
    owner="bmiroglio@mozilla.com",
    email=["telemetry-alerts@mozilla.com", "bmiroglio@mozilla.com"],
    instance_count=10,
    env=mozetl_envvar("addon_aggregates", {
        "date": "{{ ds_nodash }}",
        "input-bucket": "{{ task.__class__.private_output_bucket }}",
        "output-bucket": "{{ task.__class__.private_output_bucket }}"
    }),
    uri="https://raw.githubusercontent.com/mozilla/python_mozetl/master/bin/mozetl-submit.sh",
    dag=dag)

addon_aggregates_bigquery_load = SubDagOperator(
    subdag=load_to_bigquery(
        parent_dag_name=dag.dag_id,
        dag_name="addon_aggregates_bigquery_load",
        default_args=default_args,
        dataset_s3_bucket="telemetry-parquet",
        aws_conn_id="aws_dev_iam_s3",
        dataset="addons/agg",
        dataset_version="v2",
        gke_cluster_name="bq-load-gke-1",
        p2b_table_alias="addons_aggregates_v2",
        bigquery_dataset="telemetry_derived",
        cluster_by=["sample_id"],
        rename={"submission_date_s3": "submission_date"},
        replace=["SAFE_CAST(sample_id AS INT64) AS sample_id"],
        ),
    task_id="addon_aggregates_bigquery_load",
    dag=dag)

main_summary_experiments = MozDatabricksSubmitRunOperator(
    task_id="main_summary_experiments",
    job_name="Experiments Main Summary View",
    execution_timeout=timedelta(hours=10),
    instance_count=5,
    max_instance_count=40,
    enable_autoscale=True,
    instance_type="i3.2xlarge",
    spot_bid_price_percent=50,
    owner="ssuh@mozilla.com",
    email=["telemetry-alerts@mozilla.com", "frank@mozilla.com", "ssuh@mozilla.com", "robhudson@mozilla.com"],
    env=tbv_envvar("com.mozilla.telemetry.views.ExperimentSummaryView", {
        "from": "{{ ds_nodash }}",
        "to": "{{ ds_nodash }}",
        "bucket": "{{ task.__class__.private_output_bucket }}"}),
    uri="https://raw.githubusercontent.com/mozilla/telemetry-airflow/master/jobs/telemetry_batch_view.py",
    dag=dag)

main_summary_experiments_bigquery_load = SubDagOperator(
    subdag=load_to_bigquery(
        parent_dag_name=dag.dag_id,
        dag_name="main_summary_experiments_bigquery_load",
        default_args=default_args,
        dataset_s3_bucket="telemetry-parquet",
        aws_conn_id="aws_dev_iam_s3",
        dataset="experiments",
        dataset_version="v1",
        objects_prefix='experiments/v1',
        spark_gs_dataset_location='gs://moz-fx-data-derived-datasets-parquet-tmp/experiments/v1/*/submission_date_s3={{ds_nodash}}',
        gke_cluster_name="bq-load-gke-1",
        p2b_resume=True,
        reprocess=True,
        bigquery_dataset="telemetry_derived",
        ),
    task_id="main_summary_experiments_bigquery_load",
    dag=dag)

experiments_aggregates_import = EMRSparkOperator(
    task_id="experiments_aggregates_import",
    job_name="Experiments Aggregates Import",
    execution_timeout=timedelta(hours=10),
    instance_count=1,
    disable_on_dev=True,
    owner="robhudson@mozilla.com",
    email=["telemetry-alerts@mozilla.com", "robhudson@mozilla.com"],
    env={"date": "{{ ds_nodash }}", "bucket": "{{ task.__class__.private_output_bucket }}"},
    uri="https://raw.githubusercontent.com/mozilla/firefox-test-tube/master/notebook/import.py",
    dag=dag)

search_dashboard = EMRSparkOperator(
    task_id="search_dashboard",
    job_name="Search Dashboard",
    execution_timeout=timedelta(hours=3),
    instance_count=3,
    owner="harterrt@mozilla.com",
    email=["telemetry-alerts@mozilla.com", "harterrt@mozilla.com", "wlachance@mozilla.com"],
    env=mozetl_envvar("search_dashboard", {
        "submission_date": "{{ ds_nodash }}",
        "input_bucket": "{{ task.__class__.private_output_bucket }}",
        "bucket": "{{ task.__class__.private_output_bucket }}",
        "prefix": "harter/searchdb",
        "save_mode": "overwrite"
    }),
    uri="https://raw.githubusercontent.com/mozilla/python_mozetl/master/bin/mozetl-submit.sh",
    output_visibility="private",
    dag=dag)

search_dashboard_bigquery_load = SubDagOperator(
    subdag=load_to_bigquery(
        parent_dag_name=dag.dag_id,
        dag_name="search_dashboard_bigquery_load",
        default_args=default_args,
        dataset_s3_bucket="telemetry-parquet",
        aws_conn_id="aws_dev_iam_s3",
        dataset="harter/searchdb",
        dataset_version="v7",
        bigquery_dataset="search",
        gke_cluster_name="bq-load-gke-1",
        p2b_table_alias="search_aggregates_v7",
        ),
    task_id="search_dashboard_bigquery_load",
    dag=dag)

search_clients_daily = EMRSparkOperator(
    task_id="search_clients_daily",
    job_name="Search Clients Daily",
    execution_timeout=timedelta(hours=5),
    instance_count=5,
    owner="harterrt@mozilla.com",
    email=["telemetry-alerts@mozilla.com", "harterrt@mozilla.com", "wlachance@mozilla.com"],
    env=mozetl_envvar("search_clients_daily", {
        "submission_date": "{{ ds_nodash }}",
        "input_bucket": "{{ task.__class__.private_output_bucket }}",
        "bucket": "{{ task.__class__.private_output_bucket }}",
        "prefix": "search_clients_daily",
        "save_mode": "overwrite"
    }),
    uri="https://raw.githubusercontent.com/mozilla/python_mozetl/master/bin/mozetl-submit.sh",
    output_visibility="private",
    dag=dag)

search_clients_daily_bigquery_load = SubDagOperator(
    subdag=load_to_bigquery(
        parent_dag_name=dag.dag_id,
        dag_name="search_clients_daily_bigquery_load",
        default_args=default_args,
        dataset_s3_bucket="telemetry-parquet",
        aws_conn_id="aws_dev_iam_s3",
        dataset="search_clients_daily",
        dataset_version="v7",
        bigquery_dataset="search",
        gke_cluster_name="bq-load-gke-1",
        reprocess=True,
        ),
    task_id="search_clients_daily_bigquery_load",
    dag=dag)

clients_daily = EMRSparkOperator(
    task_id="clients_daily",
    job_name="Clients Daily",
    execution_timeout=timedelta(hours=5),
    instance_count=10,
    env=mozetl_envvar("clients_daily", {
        # Note that the output of this job will be earlier
        # than this date to account for submission latency.
        # See the clients_daily code in the python_mozetl
        # repo for more details.
        "date": "{{ ds }}",
        "input-bucket": "{{ task.__class__.private_output_bucket }}",
        "output-bucket": "{{ task.__class__.private_output_bucket }}"
    }),
    uri="https://raw.githubusercontent.com/mozilla/python_mozetl/master/bin/mozetl-submit.sh",
    dag=dag)

clients_daily_v6 = EMRSparkOperator(
    task_id="clients_daily_v6",
    job_name="Clients Daily v6",
    owner="relud@mozilla.com",
    email=["telemetry-alerts@mozilla.com", "relud@mozilla.com"],
    execution_timeout=timedelta(hours=5),
    instance_count=10,
    env=tbv_envvar("com.mozilla.telemetry.views.ClientsDailyView", {
        "date": "{{ ds_nodash }}",
        "input-bucket": "{{ task.__class__.private_output_bucket }}",
        "output-bucket": "{{ task.__class__.private_output_bucket }}"
    }),
    uri="https://raw.githubusercontent.com/mozilla/telemetry-airflow/master/jobs/telemetry_batch_view.py",
    dag=dag)

register_status(clients_daily_v6, "Clients Daily", "A view of main pings with one row per client per day.")

clients_daily_v6_bigquery_load = SubDagOperator(
    subdag=load_to_bigquery(
        parent_dag_name=dag.dag_id,
        dag_name="clients_daily_v6_bigquery_load",
        default_args=default_args,
        dataset_s3_bucket="telemetry-parquet",
        aws_conn_id="aws_dev_iam_s3",
        dataset="clients_daily",
        dataset_version="v6",
        gke_cluster_name="bq-load-gke-1",
        reprocess=True,
        bigquery_dataset="telemetry_derived",
        cluster_by=["sample_id"],
        rename={"submission_date_s3": "submission_date"},
        replace=["SAFE_CAST(sample_id AS INT64) AS sample_id"],
        ),
    task_id="clients_daily_v6_bigquery_load",
    dag=dag)

clients_last_seen = bigquery_etl_query(
    task_id="clients_last_seen",
    destination_table="clients_last_seen_v1",
    dataset_id="telemetry_derived",
    owner="relud@mozilla.com",
    email=["telemetry-alerts@mozilla.com", "relud@mozilla.com", "jklukas@mozilla.com"],
    depends_on_past=True,
    start_date=datetime(2019, 4, 15),
    dag=dag)

clients_last_seen_export = SubDagOperator(
    subdag=export_to_parquet(
        table="clients_last_seen_v1",
        arguments=[
            "--dataset=telemetry_derived",
            "--submission-date={{ds}}",
            "--destination-table=clients_last_seen_v1",
            "--select",
            "cast(log2(days_seen_bits & -days_seen_bits) as long) as days_since_seen",
            "cast(log2(days_visited_5_uri_bits & -days_visited_5_uri_bits) as long) as days_since_visited_5_uri",
            "cast(log2(days_opened_dev_tools_bits & -days_opened_dev_tools_bits) as long) as days_since_opened_dev_tools",
            "cast(log2(days_created_profile_bits & -days_created_profile_bits) as long) as days_since_created_profile",
            "*"
        ],
        parent_dag_name=dag.dag_id,
        dag_name="clients_last_seen_export",
        default_args=default_args,
        num_preemptible_workers=10),
    task_id="clients_last_seen_export",
    dag=dag)

exact_mau_by_dimensions = bigquery_etl_query(
    task_id="exact_mau_by_dimensions",
    destination_table="firefox_desktop_exact_mau28_by_dimensions_v1",
    dataset_id="telemetry",
    owner="relud@mozilla.com",
    email=["telemetry-alerts@mozilla.com", "relud@mozilla.com"],
    dag=dag)

exact_mau_by_dimensions_export = SubDagOperator(
    subdag=export_to_parquet(
        table="firefox_desktop_exact_mau28_by_dimensions_v1",
        arguments=["--submission-date={{ds}}"],
        parent_dag_name=dag.dag_id,
        dag_name="exact_mau_by_dimensions_export",
        default_args=default_args),
    task_id="exact_mau_by_dimensions_export",
    dag=dag)

smoot_usage_desktop_raw = bigquery_etl_query(
    task_id='smoot_usage_desktop_raw',
    destination_table='smoot_usage_desktop_raw_v1',
    dataset_id='telemetry',
    owner="jklukas@mozilla.com",
    email=["telemetry-alerts@mozilla.com", "jklukas@mozilla.com"],
    dag=dag)

client_count_daily_view = EMRSparkOperator(
    task_id="client_count_daily_view",
    job_name="Client Count Daily View",
    execution_timeout=timedelta(hours=10),
    owner="relud@mozilla.com",
    email=["telemetry-alerts@mozilla.com", "relud@mozilla.com"],
    instance_count=10,
    env={"date": "{{ ds_nodash }}", "bucket": "{{ task.__class__.private_output_bucket }}"},
    uri="https://raw.githubusercontent.com/mozilla/telemetry-airflow/master/jobs/client_count_daily_view.sh",
    dag=dag)

main_summary_glue = EMRSparkOperator(
    task_id="main_summary_glue",
    job_name="Main Summary Update Glue",
    execution_timeout=timedelta(hours=8),
    owner="bimsland@mozilla.com",
    email=["telemetry-alerts@mozilla.com", "bimsland@mozilla.com"],
    instance_count=1,
    disable_on_dev=True,
    env={
        "bucket": "{{ task.__class__.private_output_bucket }}",
        "prefix": "main_summary",
        "pdsm_version": "{{ var.value.pdsm_version }}",
        "glue_access_key_id": "{{ var.value.glue_access_key_id }}",
        "glue_secret_access_key": "{{ var.value.glue_secret_access_key }}",
        "glue_default_region": "{{ var.value.glue_default_region }}",
    },
    uri="https://raw.githubusercontent.com/mozilla/telemetry-airflow/master/jobs/update_glue.sh",
    dag=dag)

taar_dynamo = EMRSparkOperator(
    task_id="taar_dynamo",
    job_name="TAAR DynamoDB loader",
    execution_timeout=timedelta(hours=14),
    instance_count=6,
    disable_on_dev=True,
    owner="vng@mozilla.com",
    email=["mlopatka@mozilla.com", "vng@mozilla.com", "sbird@mozilla.com"],
    env=mozetl_envvar("taar_dynamo", {
        "date": "{{ ds_nodash }}"
    }),
    uri="https://raw.githubusercontent.com/mozilla/python_mozetl/master/bin/mozetl-submit.sh",
    output_visibility="private",
    dag=dag)

desktop_dau = EMRSparkOperator(
    task_id="desktop_dau",
    job_name="Desktop DAU",
    owner="relud@mozilla.com",
    email=["telemetry-alerts@mozilla.com", "relud@mozilla.com"],
    execution_timeout=timedelta(hours=1),
    instance_count=1,
    env=tbv_envvar("com.mozilla.telemetry.views.dau.DesktopDauView", {
        "to": "{{ ds_nodash }}",
        "bucket": "{{ task.__class__.private_output_bucket }}",
    }),
    uri="https://raw.githubusercontent.com/mozilla/telemetry-airflow/master/jobs/telemetry_batch_view.py",
    dag=dag)

desktop_active_dau = EMRSparkOperator(
    task_id="desktop_active_dau",
    job_name="Desktop Active DAU",
    owner="relud@mozilla.com",
    email=["telemetry-alerts@mozilla.com", "relud@mozilla.com"],
    execution_timeout=timedelta(hours=3),
    instance_count=1,
    env=tbv_envvar("com.mozilla.telemetry.views.dau.DesktopActiveDauView", {
        "to": "{{ ds_nodash }}",
        "bucket": "{{ task.__class__.private_output_bucket }}",
    }),
    uri="https://raw.githubusercontent.com/mozilla/telemetry-airflow/master/jobs/telemetry_batch_view.py",
    dag=dag)

taar_locale_job = EMRSparkOperator(
    task_id="taar_locale_job",
    job_name="TAAR Locale Model",
    owner="mlopatka@mozilla.com",
    email=["vng@mozilla.com", "mlopatka@mozilla.com"],
    execution_timeout=timedelta(hours=10),
    instance_count=8,
    env=mozetl_envvar("taar_locale", {
          "date": "{{ ds_nodash }}",
          "bucket": "{{ task.__class__.private_output_bucket }}",
          "prefix": "taar/locale/"
    }),
    uri="https://raw.githubusercontent.com/mozilla/python_mozetl/master/bin/mozetl-submit.sh",
    output_visibility="private",
    dag=dag)

taar_similarity = MozDatabricksSubmitRunOperator(
    task_id="taar_similarity",
    job_name="Taar Similarity model",
    owner="akomar@mozilla.com",
    email=["vng@mozilla.com", "mlopatka@mozilla.com", "akomar@mozilla.com"],
    execution_timeout=timedelta(hours=2),
    instance_count=11,
    instance_type="i3.8xlarge",
    driver_instance_type="i3.xlarge",
    env=mozetl_envvar("taar_similarity",
        options={
            "date": "{{ ds_nodash }}",
            "bucket": "{{ task.__class__.private_output_bucket }}",
            "prefix": "taar/similarity/"
        }),
    uri="https://raw.githubusercontent.com/mozilla/python_mozetl/master/bin/mozetl-submit.sh",
    output_visibility="private",
    dag=dag)

taar_collaborative_recommender = EMRSparkOperator(
    task_id="addon_recommender",
    job_name="Train the Collaborative Addon Recommender",
    execution_timeout=timedelta(hours=10),
    instance_count=20,
    owner="mlopatka@mozilla.com",
    email=["telemetry-alerts@mozilla.com", "mlopatka@mozilla.com", "vng@mozilla.com"],
    env={"date": "{{ ds_nodash }}",
         "privateBucket": "{{ task.__class__.private_output_bucket }}",
         "publicBucket": "{{ task.__class__.public_output_bucket }}"},
    uri="https://raw.githubusercontent.com/mozilla/telemetry-airflow/master/jobs/addon_recommender.sh",
    dag=dag)

bgbb_pred = MozDatabricksSubmitRunOperator(
    task_id="bgbb_pred",
    job_name="Predict retention from a BGBB model",
    execution_timeout=timedelta(hours=2),
    email=[
        "telemetry-alerts@mozilla.com",
        "wbeard@mozilla.com",
        "amiyaguchi@mozilla.com",
    ],
    instance_count=10,
    env=mozetl_envvar(
        "bgbb_pred",
        {
            "submission-date": "{{ ds }}",
            "model-win": "90",
            "sample-ids": "[]",
            "param-bucket": "{{ task.__class__.private_output_bucket }}",
            "param-prefix": "bgbb/params/v1",
            "pred-bucket": "{{ task.__class__.private_output_bucket }}",
            "pred-prefix": "bgbb/active_profiles/v1",
        },
        dev_options={
            "model-win": "30",
            "sample-ids": "[1]",
        },
        other={
            "MOZETL_GIT_PATH": "https://github.com/wcbeard/bgbb_airflow.git",
            "MOZETL_EXTERNAL_MODULE": "bgbb_airflow",
        },
    ),
    dag=dag
)

bgbb_pred_bigquery_load = SubDagOperator(
    subdag=load_to_bigquery(
        parent_dag_name=dag.dag_id,
        dag_name="bgbb_pred_bigquery_load",
        default_args=default_args,
        dataset_s3_bucket="telemetry-parquet",
        aws_conn_id="aws_dev_iam_s3",
        dataset="bgbb/active_profiles",
        dataset_version="v1",
        p2b_table_alias="active_profiles_v1",
        bigquery_dataset="telemetry_derived",
        ds_type="ds",
        gke_cluster_name="bq-load-gke-1",
        cluster_by=["sample_id"],
        rename={"submission_date_s3": "submission_date"},
        replace=["SAFE_CAST(sample_id AS INT64) AS sample_id"],
        ),
    task_id="bgbb_pred_bigquery_load",
    dag=dag)


main_summary_schema.set_upstream(main_summary)
main_summary_bigquery_load.set_upstream(main_summary)

engagement_ratio.set_upstream(main_summary)

addons.set_upstream(main_summary)
addons_bigquery_load.set_upstream(addons)
addon_aggregates.set_upstream(addons)
addon_aggregates_bigquery_load.set_upstream(addon_aggregates)

main_events.set_upstream(main_summary)
main_events_bigquery_load.set_upstream(main_events)

main_summary_experiments.set_upstream(main_summary)
main_summary_experiments_bigquery_load.set_upstream(main_summary_experiments)

experiments_aggregates_import.set_upstream(main_summary_experiments)
search_dashboard.set_upstream(main_summary)
search_dashboard_bigquery_load.set_upstream(search_dashboard)
search_clients_daily.set_upstream(main_summary)
search_clients_daily_bigquery_load.set_upstream(search_clients_daily)

taar_dynamo.set_upstream(main_summary)
taar_similarity.set_upstream(clients_daily_v6)

clients_daily.set_upstream(main_summary)
clients_daily_v6.set_upstream(main_summary)
desktop_active_dau.set_upstream(clients_daily_v6)
clients_daily_v6_bigquery_load.set_upstream(clients_daily_v6)
clients_last_seen.set_upstream(clients_daily_v6_bigquery_load)
clients_last_seen_export.set_upstream(clients_last_seen)
exact_mau_by_dimensions.set_upstream(clients_last_seen)
exact_mau_by_dimensions_export.set_upstream(exact_mau_by_dimensions)
smoot_usage_desktop_raw.set_upstream(clients_last_seen)

client_count_daily_view.set_upstream(main_summary)
desktop_dau.set_upstream(client_count_daily_view)

main_summary_glue.set_upstream(main_summary)

taar_locale_job.set_upstream(clients_daily_v6)
taar_collaborative_recommender.set_upstream(clients_daily_v6)

bgbb_pred.set_upstream(clients_daily_v6)
bgbb_pred_bigquery_load.set_upstream(bgbb_pred)
