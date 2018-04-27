from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import DagBag
from airflow.utils.trigger_rule import TriggerRule
from airflow.contrib.operators.emr_terminate_job_flow_operator import EmrTerminateJobFlowOperator
from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor
from airflow.operators.subdag_operator import SubDagOperator
from airflow.operators.moz_emr import (EmrAddStepsOperator,
                                       EmrCreateJobFlowSelectiveTemplateOperator,
                                       MozEmrClusterStartSensor,
                                       MozEmrClusterEndSensor)

from utils.tbv import tbv_envvar


SCHEDULE_INTERVAL = '@weekly'
START_DATE = datetime(2017, 3, 17)

default_args = {
    'owner': 'ssuh@mozilla.com',
    'depends_on_past': False,
    'start_date': START_DATE,
    'email': ['telemetry-alerts@mozilla.com', 'ssuh@mozilla.com', 'frank@mozilla.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=15)
}


dag = DAG('main_summary_backfill_mar_2018',
          default_args=default_args,
          schedule_interval=SCHEDULE_INTERVAL,
          catchup=True,
          # Change this back to 10 after we get a successful run on prod
          max_active_runs=4)

job_flow_id_template = "{{ task_instance.xcom_pull('setup_backfill_cluster', key='return_value') }}"


# workaround to retry the entire subdag on step failure instead of just that step
# via https://stackoverflow.com/a/49029261/537248
def clear_subdag_callback(context):
    dag_id = "{}.{}".format(context['dag'].dag_id, context['ti'].task_id)
    execution_date = context['execution_date']
    sdag = DagBag().get_dag(dag_id)
    sdag.clear(
        start_date=execution_date,
        end_date=execution_date,
        only_failed=False,
        only_running=False,
        confirm_prompt=False,
        include_subdags=False)


def main_summary_subdag_factory(parent_dag, task_id, day):
    ds = "{{{{ macros.ds_format(macros.ds_add(ds, {0}), '%Y-%m-%d', '%Y%m%d') }}}}".format(day)
    subdag = DAG("{}.{}".format(parent_dag.dag_id, task_id),
                 schedule_interval=SCHEDULE_INTERVAL,
                 start_date=START_DATE,
                 default_args=default_args)

    parent_job_flow_id = ("{{{{ task_instance.xcom_pull('setup_backfill_cluster', "
                          "key='return_value', dag_id={}) }}}}".format(parent_dag.dag_id))

    add_step_task = EmrAddStepsOperator(
        task_id='submit_main_summary_day',
        job_flow_id=parent_job_flow_id,
        execution_timeout=timedelta(minutes=10),
        aws_conn_id='aws_default',
        steps=EmrAddStepsOperator.get_step_args(
            job_name="main_summary {}".format(ds),
            owner="ssuh@mozilla.com",
            action_on_failure='CONTINUE',
            uri="https://raw.githubusercontent.com/mozilla/telemetry-airflow/master/jobs/telemetry_batch_view.py",
            env=tbv_envvar("com.mozilla.telemetry.views.MainSummaryView", {
                "from": ds,
                "to": ds,
                "bucket": "telemetry-backfill"
            }, {
                "DO_ASSEMBLY": "False"
            }),
        ),
        dag=subdag
    )

    step_sensor_task = EmrStepSensor(
        task_id="main_summary_step_sensor",
        timeout=timedelta(hours=10).total_seconds(),
        job_flow_id=parent_job_flow_id,
        step_id="{{ task_instance.xcom_pull('submit_main_summary_day', key='return_value') }}",
        poke_interval=timedelta(minutes=5).total_seconds(),
        dag=subdag
    )

    step_sensor_task.set_upstream(add_step_task)

    return subdag


create_job_flow_task = EmrCreateJobFlowSelectiveTemplateOperator(
    task_id="setup_backfill_cluster",
    execution_timeout=timedelta(minutes=10),
    emr_conn_id='emr_batch_view',
    aws_conn_id='aws_default',
    job_flow_overrides=EmrCreateJobFlowSelectiveTemplateOperator.get_jobflow_args(
        owner="ssuh@mozilla.com",
        instance_count=20,
        keep_alive=True,
        job_name="Main Summary Backfill"
    ),
    templated_job_flow_overrides={
        "Name": "Main Summary Backfill {{ ds }}",
        "Steps": EmrCreateJobFlowSelectiveTemplateOperator.get_step_args(
            job_name="compile_main_summary",
            owner="ssuh@mozilla.com",
            uri="https://raw.githubusercontent.com/mozilla/telemetry-airflow/master/jobs/telemetry_batch_view.py",
            env=tbv_envvar(None, options={}, branch="backfill", other={"DO_SUBMIT": "False"})
        ),
    },
    dag=dag
)

cluster_start_sensor_task = MozEmrClusterStartSensor(
    task_id="wait_for_cluster",
    timeout=timedelta(hours=1).total_seconds(),
    job_flow_id=job_flow_id_template,
    dag=dag
)

terminate_job_flow_task = EmrTerminateJobFlowOperator(
    task_id="terminate_backfill_cluster",
    aws_conn_id='aws_default',
    execution_timeout=timedelta(minutes=10),
    job_flow_id=job_flow_id_template,
    trigger_rule=TriggerRule.ALL_DONE,
    dag=dag
)

job_flow_termination_sensor_task = MozEmrClusterEndSensor(
    task_id="cluster_termination_sensor",
    timeout=timedelta(hours=1).total_seconds(),
    job_flow_id=job_flow_id_template,
    dag=dag
)


cluster_start_sensor_task.set_upstream(create_job_flow_task)

upstream = cluster_start_sensor_task
for day in range(7):
    task_id = "main_summary_day_{}".format(day)
    subdag_task = SubDagOperator(
        task_id=task_id,
        subdag=main_summary_subdag_factory(dag, task_id, day),
        on_retry_callback=clear_subdag_callback,
        dag=dag
    )
    subdag_task.set_upstream(upstream)
    terminate_job_flow_task.set_upstream(subdag_task)
    upstream = subdag_task

job_flow_termination_sensor_task.set_upstream(terminate_job_flow_task)
