from airflow import DAG
from datetime import timedelta, datetime
from operators.emr_spark_operator import EMRSparkOperaton
from airflow.contrib.operators import kubernetes_pod_operator


default_args = {
    'owner': 'gfritzsche@mozilla.com',
    'depends_on_past': False,
    'start_date': datetime(2018, 11, 26),
    'email': ['telemetry-client-dev@mozilla.com', 'gfritzsche@mozilla.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=30),
}

with DAG('probe_scraper',
                default_args=default_args,
                schedule_interval='@daily') as dag:

    probe_scraper = EMRSparkOperator(
        task_id="probe_scraper",
        job_name="Probe Scraper",
        execution_timeout=timedelta(hours=4),
        instance_count=1,
        email=['telemetry-client-dev@mozilla.com', 'gfritzsche@mozilla.com', 'aplacitelli@mozilla.com', 'frank@mozilla.com'],
        env={},
        uri="https://raw.githubusercontent.com/mozilla/telemetry-airflow/master/jobs/probe_scraper.sh",
        output_visibility="public",
        dag=dag)

    schema_generator = kubernetes_pod_operator.KubernetesPodOperator(
        # The ID specified for the task.
        task_id='mozilla-schema-generator',
        # Name of task you want to run, used to generate Pod ID.
        name='generate-schemas',
        # The namespace to run within Kubernetes, default namespace is
        # `default`. There is the potential for the resource starvation of
        # Airflow workers and scheduler within the Cloud Composer environment,
        # the recommended solution is to increase the amount of nodes in order
        # to satisfy the computing requirements. Alternatively, launching pods
        # into a custom namespace will stop fighting over resources.
        namespace='default',
        # Docker image specified. Defaults to hub.docker.com, but any fully
        # qualified URLs will point to a custom repository. Supports private
        # gcr.io images if the Composer Environment is under the same
        # project-id as the gcr.io images.
        image='mozilla/mozilla-schema-generator')

    schema_generator.set_upstream(probe_scraper)
