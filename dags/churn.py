from airflow import DAG
from datetime import datetime, timedelta
from operators.emr_spark_operator import EMRSparkOperator
from utils.gcp import load_to_bigquery
from utils.mozetl import mozetl_envvar
from airflow.operators.moz_databricks import MozDatabricksSubmitRunOperator
from airflow.operators.subdag_operator import SubDagOperator

default_args = {
    'owner': 'amiyaguchi@mozilla.com',
    'depends_on_past': False,
    'start_date': datetime(2018, 12, 12),
    'email': ['telemetry-alerts@mozilla.com', 'amiyaguchi@mozilla.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=30),
}

dag = DAG('churn', default_args=default_args, schedule_interval='0 0 * * 3')

churn = EMRSparkOperator(
    task_id="churn",
    job_name="churn 7-day v3",
    execution_timeout=timedelta(hours=4),
    instance_count=10,
    env=mozetl_envvar("churn", {
        "start_date": "{{ ds_nodash }}",
        "bucket": "{{ task.__class__.private_output_bucket }}"
    }),
    uri="https://raw.githubusercontent.com/mozilla/python_mozetl/master/bin/mozetl-submit.sh",
    output_visibility="public",
    dag=dag)

churn_bigquery_load = SubDagOperator(
    subdag=load_to_bigquery(
        parent_dag_name=dag.dag_id,
        dag_name="churn_bigquery_load",
        default_args=default_args,
        dataset_s3_bucket="{{ task.__class__.private_output_bucket }}",
        aws_conn_id="aws_dev_iam_s3",
        dataset="churn",
        dataset_version="v3",
        date_submission_col="week_start",
        gke_cluster_name="bq-load-gke-1",
        ),
    task_id="churn_bigquery_load",
    dag=dag)

churn_v2 = MozDatabricksSubmitRunOperator(
    task_id="churn_v2",
    job_name="churn 7-day v2",
    execution_timeout=timedelta(hours=4),
    instance_count=5,
    env=mozetl_envvar("churn", {
        "start_date": "{{ ds_nodash }}",
        "bucket": "{{ task.__class__.private_output_bucket }}"
    }, other={
        "MOZETL_GIT_BRANCH": "churn-v2"
    }),
    # the mozetl branch was forked before python 3 support
    python_version=2,
    uri="https://raw.githubusercontent.com/mozilla/python_mozetl/master/bin/mozetl-submit.sh",
    output_visibility="public",
    dag=dag)

churn_v2_bigquery_load = SubDagOperator(
    subdag=load_to_bigquery(
        parent_dag_name=dag.dag_id,
        dag_name="churn_v2_bigquery_load",
        default_args=default_args,
        dataset_s3_bucket="{{ task.__class__.private_output_bucket }}",
        aws_conn_id="aws_dev_iam_s3",
        dataset="churn",
        dataset_version="v2",
        date_submission_col="week_start",
        gke_cluster_name="bq-load-gke-1",
        ),
    task_id="churn_v2_bigquery_load",
    dag=dag)

churn_to_csv = EMRSparkOperator(
    task_id="churn_to_csv",
    job_name="Convert Churn v2 to csv",
    execution_timeout=timedelta(hours=4),
    instance_count=1,
    env=mozetl_envvar("churn_to_csv", {"start_date": "{{ ds_nodash }}"}),
    uri="https://raw.githubusercontent.com/mozilla/python_mozetl/master/bin/mozetl-submit.sh",
    dag=dag)

churn_bigquery_load.set_upstream(churn)

churn_to_csv.set_upstream(churn_v2)
churn_v2_bigquery_load.set_upstream(churn_v2)
