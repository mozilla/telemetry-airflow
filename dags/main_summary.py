from airflow import DAG
from datetime import datetime, timedelta
from operators.emr_spark_operator import EMRSparkOperator
from utils.mozetl import mozetl_envvar
from search_rollup import add_search_rollup

default_args = {
    'owner': 'mreid@mozilla.com',
    'depends_on_past': False,
    'start_date': datetime(2015, 11, 3),
    'email': ['telemetry-alerts@mozilla.com', 'mreid@mozilla.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=30),
}

# Make sure all the data for the given day has arrived before running.
# Running at 1am should suffice.
dag = DAG('main_summary', default_args=default_args, schedule_interval='0 1 * * *')

main_summary = EMRSparkOperator(
    task_id="main_summary",
    job_name="Main Summary View",
    execution_timeout=timedelta(hours=10),
    instance_count=25,
    env={"date": "{{ ds_nodash }}", "bucket": "{{ task.__class__.private_output_bucket }}"},
    uri="https://raw.githubusercontent.com/mozilla/telemetry-airflow/master/jobs/main_summary_view.sh",
    dag=dag)

engagement_ratio = EMRSparkOperator(
    task_id="engagement_ratio",
    job_name="Update Engagement Ratio",
    execution_timeout=timedelta(hours=6),
    instance_count=10,
    uri="https://raw.githubusercontent.com/mozilla/telemetry-airflow/master/jobs/engagement_ratio.sh",
    output_visibility="public",
    dag=dag)

addons = EMRSparkOperator(
    task_id="addons",
    job_name="Addons View",
    execution_timeout=timedelta(hours=4),
    instance_count=3,
    env={"date": "{{ ds_nodash }}", "bucket": "{{ task.__class__.private_output_bucket }}"},
    uri="https://raw.githubusercontent.com/mozilla/telemetry-airflow/master/jobs/addons_view.sh",
    dag=dag)

hbase_main_summary = EMRSparkOperator(
    task_id="hbase_main_summary",
    job_name="HBase Main Summary View",
    owner="jason@mozilla.com",
    email=["telemetry-alerts@mozilla.com", "jason@mozilla.com"],
    execution_timeout=timedelta(hours=10),
    instance_count=5,
    env={"date": "{{ ds_nodash }}"},
    uri="https://raw.githubusercontent.com/mozilla/telemetry-airflow/master/jobs/hbase_main_summary_view.sh",
    dag=dag)

main_events = EMRSparkOperator(
    task_id="main_events",
    job_name="Main Events View",
    execution_timeout=timedelta(hours=4),
    owner="ssuh@mozilla.com",
    email=["telemetry-alerts@mozilla.com", "ssuh@mozilla.com"],
    instance_count=1,
    env={"date": "{{ ds_nodash }}", "bucket": "{{ task.__class__.private_output_bucket }}"},
    uri="https://raw.githubusercontent.com/mozilla/telemetry-airflow/master/jobs/main_events_view.sh",
    dag=dag)

addon_aggregates = EMRSparkOperator(
    task_id="addon_aggregates",
    job_name="Addon Aggregates View",
    execution_timeout=timedelta(hours=4),
    owner="bmiroglio@mozilla.com",
    email=["telemetry-alerts@mozilla.com", "bmiroglio@mozilla.com"],
    instance_count=3,
    env={"date": "{{ ds_nodash }}", "bucket": "{{ task.__class__.private_output_bucket }}"},
    uri="https://raw.githubusercontent.com/mozilla/mozilla-reports/master/addons/okr-daily-script.kp/orig_src/addonOKRDataCollection.ipynb",
    dag=dag)

txp_mau_dau = EMRSparkOperator(
    task_id="txp_mau_dau",
    job_name="Test Pilot MAU DAU",
    execution_timeout=timedelta(hours=4),
    owner="ssuh@mozilla.com",
    email=["telemetry-alerts@mozilla.com", "ssuh@mozilla.com"],
    instance_count=5,
    env={"date": "{{ ds_nodash }}",
         "bucket": "{{ task.__class__.private_output_bucket }}",
         "prefix": "txp_mau_dau_simple",
         "inbucket": "{{ task.__class__.private_output_bucket }}",
         "inprefix": "addons/v2"},
    uri="https://raw.githubusercontent.com/mozilla/python_mozetl/master/mozetl/testpilot/txp_mau_dau.py",
    dag=dag)

main_summary_experiments = EMRSparkOperator(
    task_id="main_summary_experiments",
    job_name="Experiments Main Summary View",
    execution_timeout=timedelta(hours=10),
    instance_count=10,
    owner="ssuh@mozilla.com",
    email=["telemetry-alerts@mozilla.com", "frank@mozilla.com", "ssuh@mozilla.com", "robhudson@mozilla.com"],
    env={"date": "{{ ds_nodash }}", "bucket": "{{ task.__class__.private_output_bucket }}"},
    uri="https://raw.githubusercontent.com/mozilla/telemetry-airflow/master/jobs/experiment_main_summary_view.sh",
    dag=dag)

experiments_aggregates = EMRSparkOperator(
    task_id="experiments_aggregates",
    job_name="Experiments Aggregates View",
    execution_timeout=timedelta(hours=15),
    instance_count=20,
    release_label="emr-5.8.0",
    owner="ssuh@mozilla.com",
    email=["telemetry-alerts@mozilla.com", "frank@mozilla.com", "ssuh@mozilla.com", "robhudson@mozilla.com"],
    env={"date": "{{ ds_nodash }}", "bucket": "{{ task.__class__.private_output_bucket }}"},
    uri="https://raw.githubusercontent.com/mozilla/telemetry-airflow/master/jobs/experiment_aggregates_view.sh",
    dag=dag)

experiments_aggregates_import = EMRSparkOperator(
    task_id="experiments_aggregates_import",
    job_name="Experiments Aggregates Import",
    execution_timeout=timedelta(hours=10),
    instance_count=1,
    owner="robhudson@mozilla.com",
    email=["telemetry-alerts@mozilla.com", "robhudson@mozilla.com"],
    env={"date": "{{ ds_nodash }}", "bucket": "{{ task.__class__.private_output_bucket }}"},
    uri="https://raw.githubusercontent.com/mozilla/experiments-viewer/master/notebooks/import.py",
    dag=dag)

hbase_addon_recommender = EMRSparkOperator(
    task_id="hbase_addon_recommender",
    job_name="HBase Addon Recommender View",
    owner="aplacitelli@mozilla.com",
    email=["telemetry-alerts@mozilla.com", "aplacitelli@mozilla.com"],
    execution_timeout=timedelta(hours=10),
    instance_count=5,
    env={"date": "{{ ds_nodash }}"},
    release_label="emr-5.8.0",
    uri="https://raw.githubusercontent.com/mozilla/telemetry-airflow/master/jobs/hbase_addon_recommender_view.sh",
    dag=dag)

search_dashboard = EMRSparkOperator(
    task_id="search_dashboard",
    job_name="Search Dashboard",
    execution_timeout=timedelta(hours=3),
    instance_count=3,
    owner="harterrt@mozilla.com",
    email=["telemetry-alerts@mozilla.com", "harterrt@mozilla.com"],
    env=mozetl_envvar("search_dashboard", {
        "submission_date": "{{ ds_nodash }}",
        "bucket": "{{ task.__class__.private_output_bucket }}",
        "prefix": "harter/searchdb",
        "save_mode": "overwrite"
    }),
    uri="https://raw.githubusercontent.com/mozilla/python_mozetl/master/bin/mozetl-submit.sh",
    output_visibility="private",
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
        "output-bucket": "{{ task.__class__.private_output_bucket }}"
    }),
    uri="https://raw.githubusercontent.com/mozilla/python_mozetl/master/bin/mozetl-submit.sh",
    dag=dag)

experiments_daily = EMRSparkOperator(
    task_id="experiments_daily",
    job_name="Experiments Daily",
    execution_timeout=timedelta(hours=8),
    instance_count=10,
    env=mozetl_envvar("experiments_daily", {
        "output-bucket": "{{ task.__class__.private_output_bucket }}"
    }),
    uri="https://raw.githubusercontent.com/mozilla/python_mozetl/master/bin/mozetl-submit.sh",
    dag=dag)

heavy_users = EMRSparkOperator(task_id="heavy_users_view",
    job_name="Heavy Users View",
    owner="frank@mozilla.com",
    email=["telemetry-alerts@mozilla.com", "frank@mozilla.com", "ssuh@mozilla.com"],
    execution_timeout=timedelta(hours=4),
    instance_count=10,
    env={"date": "{{ ds_nodash }}", "bucket": "{{ task.__class__.private_output_bucket }}"},
    uri="https://raw.githubusercontent.com/mozilla/telemetry-airflow/master/jobs/heavy_users_view.sh",
    dag=dag)

retention = EMRSparkOperator(task_id="retention",
    job_name="1-Day Firefox Retention",
    owner="amiyaguchi@mozilla.com",
    email=["telemetry-alerts@mozilla.com", "amiyaguchi@mozilla.com"],
    execution_timeout=timedelta(hours=4),
    instance_count=6,
    env=mozetl_envvar("retention", {
        "start_date": "{{ ds_nodash }}",
        "bucket": "{{ task.__class__.private_output_bucket }}",
        "slack": 4
    }),
    uri="https://raw.githubusercontent.com/mozilla/telemetry-airflow/master/jobs/retention.sh",
    dag=dag)

engagement_ratio.set_upstream(main_summary)

addons.set_upstream(main_summary)
addon_aggregates.set_upstream(addons)
txp_mau_dau.set_upstream(addons)

hbase_main_summary.set_upstream(main_summary)
main_events.set_upstream(main_summary)

main_summary_experiments.set_upstream(main_summary)
experiments_aggregates.set_upstream(main_summary_experiments)
experiments_daily.set_upstream(main_summary_experiments)

experiments_aggregates_import.set_upstream(experiments_aggregates)
hbase_addon_recommender.set_upstream(main_summary)
search_dashboard.set_upstream(main_summary)

add_search_rollup(dag, "daily", 1, upstream=main_summary)

clients_daily.set_upstream(main_summary)

heavy_users.set_upstream(main_summary)

retention.set_upstream(main_summary)
