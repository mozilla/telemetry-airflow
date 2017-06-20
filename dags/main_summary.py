from airflow import DAG
from datetime import datetime, timedelta
from operators.emr_spark_operator import EMRSparkOperator

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

t1 = EMRSparkOperator(task_id="main_summary",
                      job_name="Main Summary View",
                      execution_timeout=timedelta(hours=6),
                      instance_count=20,
                      env={"date": "{{ ds_nodash }}", "bucket": "{{ task.__class__.private_output_bucket }}"},
                      uri="https://raw.githubusercontent.com/mozilla/telemetry-airflow/master/jobs/main_summary_view.sh",
                      dag=dag)

t2 = EMRSparkOperator(task_id="engagement_ratio",
                      job_name="Update Engagement Ratio",
                      execution_timeout=timedelta(hours=6),
                      instance_count=10,
                      uri="https://raw.githubusercontent.com/mozilla-services/data-pipeline/master/reports/engagement_ratio/MauDau.ipynb",
                      output_visibility="public",
                      dag=dag)

t3 = EMRSparkOperator(task_id="addons",
                      job_name="Addons View",
                      execution_timeout=timedelta(hours=4),
                      instance_count=3,
                      env={"date": "{{ ds_nodash }}", "bucket": "{{ task.__class__.private_output_bucket }}"},
                      uri="https://raw.githubusercontent.com/mozilla/telemetry-airflow/master/jobs/addons_view.sh",
                      dag=dag)

t4 = EMRSparkOperator(task_id="hbase_main_summary",
                      job_name="HBase Main Summary View",
                      owner="rvitillo@mozilla.com",
                      email=["telemetry-alerts@mozilla.com", "rvitillo@mozilla.com"],
                      execution_timeout=timedelta(hours=10),
                      instance_count=5,
                      env={"date": "{{ ds_nodash }}"},
                      uri="https://raw.githubusercontent.com/mozilla/telemetry-airflow/master/jobs/hbase_main_summary_view.sh",
                      dag=dag)

t5 = EMRSparkOperator(task_id="daily_search_rollup",
                      job_name="Daily Search Rollup",
                      email=["telemetry-alerts@mozilla.com", "spenrose@mozilla.com", "amiyaguchi@mozilla.com", "harterrt@mozilla.com"],
                      execution_timeout=timedelta(hours=6),
                      instance_count=30,
                      uri="https://raw.githubusercontent.com/mozilla/telemetry-airflow/master/jobs/search_rollups.sh",
                      output_visibility="private",
                      dag=dag)

t6 = EMRSparkOperator(task_id="main_events",
                      job_name="Main Events View",
                      execution_timeout=timedelta(hours=4),
                      owner="ssuh@mozilla.com",
                      email=["telemetry-alerts@mozilla.com", "ssuh@mozilla.com"],
                      instance_count=1,
                      env={"date": "{{ ds_nodash }}", "bucket": "{{ task.__class__.private_output_bucket }}"},
                      uri="https://raw.githubusercontent.com/mozilla/telemetry-airflow/master/jobs/main_events_view.sh",
                      dag=dag)

t7 = EMRSparkOperator(task_id="addon_aggregates",
                      job_name="Addon Aggregates View",
                      execution_timeout=timedelta(hours=4),
                      owner="bmiroglio@mozilla.com",
                      email=["telemetry-alerts@mozilla.com", "bmiroglio@mozilla.com"],
                      instance_count=3,
                      env={"date": "{{ ds_nodash }}", "bucket": "{{ task.__class__.private_output_bucket }}"},
                      uri="https://raw.githubusercontent.com/mozilla/mozilla-reports/master/addons/okr-daily-script.kp/orig_src/addonOKRDataCollection.ipynb",
                      dag=dag)

t8 = EMRSparkOperator(task_id="txp_mau_dau",
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

t9 = EMRSparkOperator(task_id="main_summary_experiments",
                      job_name="Experiments Main Summary View",
                      execution_timeout=timedelta(hours=10),
                      instance_count=10,
                      owner="ssuh@mozilla.com",
                      email=["telemetry-alerts@mozilla.com", "frank@mozilla.com", "ssuh@mozilla.com"],
                      env={"date": "{{ ds_nodash }}", "bucket": "{{ task.__class__.private_output_bucket }}"},
                      uri="https://raw.githubusercontent.com/mozilla/telemetry-airflow/master/jobs/experiment_main_summary_view.sh",
                      dag=dag)

t10 = EMRSparkOperator(task_id="experiments_aggregates",
                       job_name="Experiments Aggregates View",
                       execution_timeout=timedelta(hours=10),
                       instance_count=10,
                       owner="ssuh@mozilla.com",
                       email=["telemetry-alerts@mozilla.com", "frank@mozilla.com", "ssuh@mozilla.com"],
                       env={"date": "{{ ds_nodash }}", "bucket": "{{ task.__class__.private_output_bucket }}"},
                       uri="https://raw.githubusercontent.com/mozilla/telemetry-airflow/master/jobs/experiment_aggregates_view.sh",
                       dag=dag)

t11 = EMRSparkOperator(task_id="experiments_aggregates_import",
                       job_name="Experiments Aggregates Import",
                       execution_timeout=timedelta(hours=2),
                       instance_count=1,
                       owner="chudson@mozilla.com",
                       email=["telemetry-alerts@mozilla.com", "chudson@mozilla.com"],
                       env={"date": "{{ ds_nodash }}", "bucket": "{{ task.__class__.private_output_bucket }}"},
                       uri="https://raw.githubusercontent.com/mozilla/experiments-viewer/master/notebooks/import.py",
                       dag=dag)

t2.set_upstream(t1)

t3.set_upstream(t1)
t7.set_upstream(t3)
t8.set_upstream(t3)

t4.set_upstream(t1)
t5.set_upstream(t1)
t6.set_upstream(t1)

t9.set_upstream(t1)
t10.set_upstream(t9)
t11.set_upstream(t10)
