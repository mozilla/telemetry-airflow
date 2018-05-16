from airflow import DAG
from datetime import datetime, timedelta
from operators.emr_spark_operator import EMRSparkOperator
from utils.mozetl import mozetl_envvar
from utils.tbv import tbv_envvar
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
    execution_timeout=timedelta(hours=14),
    instance_count=40,
    env=tbv_envvar("com.mozilla.telemetry.views.MainSummaryView", {
        "from": "{{ ds_nodash }}",
        "to": "{{ ds_nodash }}",
        "bucket": "{{ task.__class__.private_output_bucket }}"}),
    uri="https://raw.githubusercontent.com/mozilla/telemetry-airflow/master/jobs/telemetry_batch_view.py",
    dag=dag)

experiments_error_aggregates = EMRSparkOperator(
    task_id="experiments_error_aggregates",
    job_name="Experiments Error Aggregates View",
    execution_timeout=timedelta(hours=5),
    instance_count=20,
    release_label="emr-5.13.0",
    owner="frank@mozilla.com",
    email=["telemetry-alerts@mozilla.com", "frank@mozilla.com"],
    env={"date": "{{ ds_nodash }}", "bucket": "{{ task.__class__.private_output_bucket }}"},
    uri="https://raw.githubusercontent.com/mozilla/telemetry-airflow/master/jobs/experiments_error_aggregates_view.sh",
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
    env=tbv_envvar("com.mozilla.telemetry.views.AddonsView", {
        "from": "{{ ds_nodash }}",
        "to": "{{ ds_nodash }}",
        "bucket": "{{ task.__class__.private_output_bucket }}"}),
    uri="https://raw.githubusercontent.com/mozilla/telemetry-airflow/master/jobs/telemetry_batch_view.py",
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

addon_aggregates = EMRSparkOperator(
    task_id="addon_aggregates",
    job_name="Addon Aggregates View",
    execution_timeout=timedelta(hours=8),
    owner="bmiroglio@mozilla.com",
    email=["telemetry-alerts@mozilla.com", "bmiroglio@mozilla.com"],
    instance_count=10,
    env=mozetl_envvar("addon_aggregates", {
        "date": "{{ ds_nodash }}",
        "output-bucket": "{{ task.__class__.private_output_bucket }}"
    }),
    uri="https://raw.githubusercontent.com/mozilla/python_mozetl/master/bin/mozetl-submit.sh",
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
    env=tbv_envvar("com.mozilla.telemetry.views.ExperimentSummaryView", {
        "from": "{{ ds_nodash }}",
        "to": "{{ ds_nodash }}",
        "bucket": "{{ task.__class__.private_output_bucket }}"}),
    uri="https://raw.githubusercontent.com/mozilla/telemetry-airflow/master/jobs/telemetry_batch_view.py",
    dag=dag)

experiments_aggregates = EMRSparkOperator(
    task_id="experiments_aggregates",
    job_name="Experiments Aggregates View",
    execution_timeout=timedelta(hours=15),
    instance_count=20,
    release_label="emr-5.8.0",
    owner="ssuh@mozilla.com",
    email=["telemetry-alerts@mozilla.com", "frank@mozilla.com", "ssuh@mozilla.com", "robhudson@mozilla.com"],
    env=tbv_envvar("com.mozilla.telemetry.views.ExperimentAnalysisView", {
        "date": "{{ ds_nodash }}",
        "input": "s3://{{ task.__class__.private_output_bucket }}/experiments/v1",
        "output": "s3://{{ task.__class__.private_output_bucket }}/experiments_aggregates/v1"}),
    uri="https://raw.githubusercontent.com/mozilla/telemetry-airflow/master/jobs/telemetry_batch_view.py",
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

search_dashboard = EMRSparkOperator(
    task_id="search_dashboard",
    job_name="Search Dashboard",
    execution_timeout=timedelta(hours=3),
    instance_count=3,
    owner="harterrt@mozilla.com",
    email=["telemetry-alerts@mozilla.com", "harterrt@mozilla.com", "wlachance@mozilla.com"],
    env=mozetl_envvar("search_dashboard", {
        "submission_date": "{{ ds_nodash }}",
        "bucket": "{{ task.__class__.private_output_bucket }}",
        "prefix": "harter/searchdb",
        "save_mode": "overwrite"
    }),
    uri="https://raw.githubusercontent.com/mozilla/python_mozetl/master/bin/mozetl-submit.sh",
    output_visibility="private",
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
        "bucket": "{{ task.__class__.private_output_bucket }}",
        "prefix": "search_clients_daily",
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

clients_daily_v6 = EMRSparkOperator(
    task_id="clients_daily_v6",
    job_name="Clients Daily v6",
    owner="relud@mozilla.com",
    email=["telemetry-alerts@mozilla.com", "relud@mozilla.com"],
    execution_timeout=timedelta(hours=5),
    instance_count=10,
    env=tbv_envvar("com.mozilla.telemetry.views.ClientsDailyView", {
        "date": "{{ ds_nodash }}",
        "output-bucket": "{{ task.__class__.private_output_bucket }}"
    }),
    uri="https://raw.githubusercontent.com/mozilla/telemetry-airflow/master/jobs/telemetry_batch_view.py",
    dag=dag)

heavy_users = EMRSparkOperator(
    task_id="heavy_users_view",
    job_name="Heavy Users View",
    owner="frank@mozilla.com",
    email=["telemetry-alerts@mozilla.com", "frank@mozilla.com", "ssuh@mozilla.com"],
    execution_timeout=timedelta(hours=8),
    instance_count=10,
    env=tbv_envvar("com.mozilla.telemetry.views.HeavyUsersView", {
        "date": "{{ ds_nodash }}",
        "bucket": "{{ task.__class__.private_output_bucket }}"}),
    uri="https://raw.githubusercontent.com/mozilla/telemetry-airflow/master/jobs/telemetry_batch_view.py",
    dag=dag)

retention = EMRSparkOperator(
    task_id="retention",
    job_name="1-Day Firefox Retention",
    owner="amiyaguchi@mozilla.com",
    email=["telemetry-alerts@mozilla.com", "amiyaguchi@mozilla.com"],
    execution_timeout=timedelta(hours=4),
    instance_count=10,
    env=mozetl_envvar("retention", {
        "start_date": "{{ ds_nodash }}",
        "bucket": "{{ task.__class__.private_output_bucket }}",
        "slack": 4
    }),
    uri="https://raw.githubusercontent.com/mozilla/telemetry-airflow/master/jobs/retention.sh",
    dag=dag)

client_count_daily_view = EMRSparkOperator(
    task_id="client_count_daily_view",
    job_name="Client Count Daily View",
    execution_timeout=timedelta(hours=10),
    owner="relud@mozilla.com",
    email=["telemetry-alerts@mozilla.com", "relud@mozilla.com"],
    instance_count=2,
    env={"date": "{{ ds_nodash }}", "bucket": "{{ task.__class__.private_output_bucket }}"},
    uri="https://raw.githubusercontent.com/mozilla/telemetry-airflow/master/jobs/client_count_daily_view.sh",
    dag=dag)

main_summary_glue = EMRSparkOperator(
    task_id="main_summary_glue",
    job_name="Main Summary Update Glue",
    execution_timeout=timedelta(hours=2),
    owner="bimsland@mozilla.com",
    email=["telemetry-alerts@mozilla.com", "bimsland@mozilla.com"],
    instance_count=1,
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


engagement_ratio.set_upstream(main_summary)

addons.set_upstream(main_summary)
addon_aggregates.set_upstream(addons)
txp_mau_dau.set_upstream(addons)

main_events.set_upstream(main_summary)

main_summary_experiments.set_upstream(main_summary)
experiments_aggregates.set_upstream(main_summary_experiments)
experiments_aggregates.set_upstream(experiments_error_aggregates)

experiments_aggregates_import.set_upstream(experiments_aggregates)
search_dashboard.set_upstream(main_summary)
search_clients_daily.set_upstream(main_summary)


add_search_rollup(dag, "daily", 3, upstream=main_summary)

clients_daily.set_upstream(main_summary)
clients_daily_v6.set_upstream(main_summary)

heavy_users.set_upstream(main_summary)

retention.set_upstream(main_summary)

client_count_daily_view.set_upstream(main_summary)

main_summary_glue.set_upstream(main_summary)
