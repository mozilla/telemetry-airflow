from airflow import DAG
from datetime import datetime, timedelta
from operators.emr_spark_operator import EMRSparkOperator
from utils.constants import DS_WEEKLY
from utils.mozetl import mozetl_envvar

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
    instance_count=30,
    release_label="emr-5.8.0",
    env={"date": DS_WEEKLY, "bucket": "{{ task.__class__.private_output_bucket }}"},
    uri="https://raw.githubusercontent.com/mozilla/telemetry-airflow/master/jobs/longitudinal_view.sh",
    dag=dag)

addon_recommender = EMRSparkOperator(
    task_id="addon_recommender",
    job_name="Train the Addon Recommender",
    execution_timeout=timedelta(hours=10),
    instance_count=20,
    owner="aplacitelli@mozilla.com",
    email=["telemetry-alerts@mozilla.com", "aplacitelli@mozilla.com"],
    env={"date": DS_WEEKLY,
         "privateBucket": "{{ task.__class__.private_output_bucket }}",
         "publicBucket": "{{ task.__class__.public_output_bucket }}"},
    uri="https://raw.githubusercontent.com/mozilla/telemetry-airflow/master/jobs/addon_recommender.sh",
    dag=dag)

ensemble_recommender = EMRSparkOperator(
    task_id="ensemble_recommender",
    job_name="Extract the addon data for each Telemetry user to DynamoDB",
    execution_timeout=timedelta(hours=10),
    instance_count=20,
    owner="vng@mozilla.com",
    email=["telemetry-alerts@mozilla.com", "vng@mozilla.com"],
    env={},
    uri="https://raw.githubusercontent.com/mozilla/telemetry-airflow/master/jobs/ensemble_taar.sh",
    dag=dag)

game_hw_survey = EMRSparkOperator(
    task_id="game_hw_survey",
    job_name="Firefox Hardware Report",
    execution_timeout=timedelta(hours=5),
    instance_count=15,
    owner="fbertsch@mozilla.com",
    depends_on_past=True,
    email=["telemetry-alerts@mozilla.com", "fbertsch@mozilla.com", "wfu@mozilla.com",
           "firefox-hardware-report-feedback@mozilla.com"],
    env={"date": "{{ ds_nodash }}", "bucket": "{{ task.__class__.public_output_bucket }}"},
    uri="https://raw.githubusercontent.com/mozilla/telemetry-airflow/master/jobs/hardware_report.sh",
    output_visibility="public",
    dag=dag)

cross_sectional = EMRSparkOperator(
    task_id="cross_sectional",
    job_name="Cross Sectional View",
    execution_timeout=timedelta(hours=10),
    instance_count=30,
    env={"date": DS_WEEKLY, "bucket": "{{ task.__class__.private_output_bucket }}"},
    uri="https://raw.githubusercontent.com/mozilla/telemetry-airflow/master/jobs/cross_sectional_view.sh",
    dag=dag)

distribution_viewer = EMRSparkOperator(
    task_id="distribution_viewer",
    job_name="Distribution Viewer",
    owner="chudson@mozilla.com",
    email=["telemetry-alerts@mozilla.com", "chudson@mozilla.com"],
    execution_timeout=timedelta(hours=10),
    instance_count=5,
    env={"date": DS_WEEKLY},
    uri="https://raw.githubusercontent.com/mozilla/distribution-viewer/master/notebooks/aggregate-and-import.py",
    dag=dag)

taar_locale_job = EMRSparkOperator(
    task_id="taar_locale_job",
    job_name="TAAR Locale Model",
    owner="aplacitelli@mozilla.com",
    email=["aplacitelli@mozilla.com", "mlopatka@mozilla.com"],
    execution_timeout=timedelta(hours=10),
    instance_count=5,
    env=mozetl_envvar("taar_locale", {
          "date": "{{ ds_nodash }}",
          "bucket": "{{ task.__class__.private_output_bucket }}",
          "prefix": "taar/locale/"
    }),
    release_label="emr-5.8.0",
    uri="https://raw.githubusercontent.com/mozilla/python_mozetl/master/bin/mozetl-submit.sh",
    output_visibility="private",
    dag=dag)

taar_legacy_job = EMRSparkOperator(
    task_id="taar_legacy_job",
    job_name="TAAR Legacy Model",
    owner="mlopatka@mozilla.com",
    email=["aplacitelli@mozilla.com", "mlopatka@mozilla.com"],
    execution_timeout=timedelta(hours=1),
    instance_count=1,
    env=mozetl_envvar("taar_legacy", {
          "date": "{{ ds_nodash }}",
          "bucket": "{{ task.__class__.private_output_bucket }}",
          "prefix": "taar/legacy/"
    }),
    release_label="emr-5.8.0",
    uri="https://raw.githubusercontent.com/mozilla/python_mozetl/master/bin/mozetl-submit.sh",
    output_visibility="private",
    dag=dag)

addon_recommender.set_upstream(longitudinal)
game_hw_survey.set_upstream(longitudinal)
cross_sectional.set_upstream(longitudinal)
distribution_viewer.set_upstream(cross_sectional)
taar_locale_job.set_upstream(longitudinal)
taar_legacy_job.set_upstream(longitudinal)
ensemble_recommender.set_upstream(longitudinal)
