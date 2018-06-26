from airflow import DAG
from datetime import datetime, timedelta
from operators.emr_spark_operator import EMRSparkOperator
from utils.constants import DS_WEEKLY
from utils.mozetl import mozetl_envvar
from utils.tbv import tbv_envvar

default_args = {
    'owner': 'frank@mozilla.com',
    'depends_on_past': False,
    'start_date': datetime(2016, 6, 30),
    'email': ['telemetry-alerts@mozilla.com', 'frank@mozilla.com', 'rharter@mozilla.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=30),
}

dag = DAG('longitudinal', default_args=default_args, schedule_interval='@weekly')

longitudinal = EMRSparkOperator(
    task_id="longitudinal",
    job_name="Longitudinal View",
    execution_timeout=timedelta(hours=12),
    instance_count=50,
    env=tbv_envvar(
        "com.mozilla.telemetry.views.LongitudinalView",
        {
            "bucket": "{{ task.__class__.private_output_bucket }}",
            "to": DS_WEEKLY
        },
        metastore_location="s3://telemetry-parquet/longitudinal"),
    uri="https://raw.githubusercontent.com/mozilla/telemetry-airflow/master/jobs/telemetry_batch_view.py",
    dag=dag)

addon_recommender = EMRSparkOperator(
    task_id="addon_recommender",
    job_name="Train the Addon Recommender",
    execution_timeout=timedelta(hours=10),
    instance_count=20,
    owner="mlopatka@mozilla.com",
    email=["telemetry-alerts@mozilla.com", "mlopatka@mozilla.com", "vng@mozilla.com"],
    env={"date": DS_WEEKLY,
         "privateBucket": "{{ task.__class__.private_output_bucket }}",
         "publicBucket": "{{ task.__class__.public_output_bucket }}"},
    uri="https://raw.githubusercontent.com/mozilla/telemetry-airflow/master/jobs/addon_recommender.sh",
    dag=dag)

game_hw_survey = EMRSparkOperator(
    task_id="game_hw_survey",
    job_name="Firefox Hardware Report",
    execution_timeout=timedelta(hours=5),
    instance_count=15,
    owner="frank@mozilla.com",
    depends_on_past=True,
    email=["telemetry-alerts@mozilla.com", "frank@mozilla.com",
           "firefox-hardware-report-feedback@mozilla.com"],
    env={"date": "{{ ds_nodash }}", "bucket": "{{ task.__class__.public_output_bucket }}"},
    uri="https://raw.githubusercontent.com/mozilla/telemetry-airflow/master/jobs/hardware_report.sh",
    output_visibility="public",
    dag=dag)

taar_locale_job = EMRSparkOperator(
    task_id="taar_locale_job",
    job_name="TAAR Locale Model",
    owner="mlopatka@mozilla.com",
    email=["vng@mozilla.com", "mlopatka@mozilla.com"],
    execution_timeout=timedelta(hours=10),
    instance_count=5,
    env=mozetl_envvar("taar_locale", {
          "date": "{{ ds_nodash }}",
          "bucket": "{{ task.__class__.private_output_bucket }}",
          "prefix": "taar/locale/"
    }),
    uri="https://raw.githubusercontent.com/mozilla/python_mozetl/master/bin/mozetl-submit.sh",
    output_visibility="private",
    dag=dag)

taar_legacy_job = EMRSparkOperator(
    task_id="taar_legacy_job",
    job_name="TAAR Legacy Model",
    owner="mlopatka@mozilla.com",
    email=["vng@mozilla.com", "mlopatka@mozilla.com"],
    execution_timeout=timedelta(hours=1),
    instance_count=1,
    env=mozetl_envvar("taar_legacy", {
          "date": "{{ ds_nodash }}",
          "bucket": "{{ task.__class__.private_output_bucket }}",
          "prefix": "taar/legacy/"
    }),
    uri="https://raw.githubusercontent.com/mozilla/python_mozetl/master/bin/mozetl-submit.sh",
    output_visibility="private",
    dag=dag)

taar_lite_guidranking = EMRSparkOperator(
    task_id="taar_lite_guidranking",
    job_name="TAARlite Addon Ranking",
    owner="mlopatka@mozilla.com",
    email=["vng@mozilla.com", "mlopatka@mozilla.com"],
    execution_timeout=timedelta(hours=2),
    instance_count=4,
    env=mozetl_envvar("taar_lite_guidranking",
                      {"date": "{{ ds_nodash }}"},
                      {'MOZETL_SUBMISSION_METHOD': 'spark'}),
    uri="https://raw.githubusercontent.com/mozilla/python_mozetl/master/bin/mozetl-submit.sh",
    output_visibility="private",
    dag=dag)

addon_recommender.set_upstream(longitudinal)
game_hw_survey.set_upstream(longitudinal)
taar_locale_job.set_upstream(longitudinal)
taar_legacy_job.set_upstream(longitudinal)
taar_lite_guidranking.set_upstream(longitudinal)
