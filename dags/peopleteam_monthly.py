# Adapted from https://github.com/mozilla-services/cloudops-infra/blob/f31bfee056b77b509db7548aa636b9cf58efcaf9/projects/data-composer/tf/modules/dags/dags/peopleteam-monthly.py#L1-L132
import datetime

from airflow import DAG
from airflow.providers.cncf.kubernetes.secret import Secret

from operators.gcp_container_operator import GKEPodOperator
from utils.tags import Tag

PROJECT = "moz-fx-data-bq-people"  # was dp2-prod

INCOMING_BUCKET = PROJECT + "-data-incoming-peopleteam-monthly"
OUTPUT_BUCKET = PROJECT + "-data-peopleteam"
QUERIES_BUCKET = PROJECT + "-data-etl-queries"

# these images were manually copied from moz-fx-data-composer-prod to moz-fx-data-artifacts-prod
# by mducharme
DI_IMAGE = (
    "us-docker.pkg.dev/moz-fx-data-artifacts-prod/legacy-images/data-integrations:prod"
)
QUERIES_IMAGE = (
    "us-docker.pkg.dev/moz-fx-data-artifacts-prod/legacy-images/bigquery-queries:prod"
)

WORKDAY_USERNAME = Secret(
    deploy_type="env",
    deploy_target="HR_DASHBOARD_WORKDAY_USERNAME",
    secret="airflow-gke-secrets",
    key="HR_DASHBOARD_WORKDAY_USERNAME",
)
WORKDAY_PASSWORD = Secret(
    deploy_type="env",
    deploy_target="HR_DASHBOARD_WORKDAY_PASSWORD",
    secret="airflow-gke-secrets",
    key="HR_DASHBOARD_WORKDAY_PASSWORD",
)

# (old) FIXME:
# Having this dynamic is not a good idea because retrying subtasks
# at a later date will fail
# ALSO, running more than one at a time will use the same dir? Does that matter?
# Only if we're deleting that directory with a cleanup op, I guess.
#
pull_date = datetime.datetime.now().strftime("%Y-%m-%d")

default_args = {
    "retries": 5,
    "retry_delay": datetime.timedelta(minutes=5),
    "start_date": datetime.datetime(2022, 4, 30),
    "email": ["telemetry-alerts@mozilla.com"],
    #'catchup': False,
}

tags = [Tag.ImpactTier.tier_3]

dag = DAG(
    dag_id="peopleteam-monthly",
    default_args=default_args,
    max_active_runs=1,
    schedule_interval="0 16 1 * *",
    tags=tags,
    doc_md="""\
    # People Team Monthly Report DAG
    This DAG was Adapted from [cloudops-infra's data-composer project](https://github.com/mozilla-services/cloudops-infra/blob/f31bfee056b77b509db7548aa636b9cf58efcaf9/projects/data-composer/tf/modules/dags/dags/peopleteam-monthly.py#L1-L132)
    ## Caveat
    It is expected to have a failing `workday-merge-promotions` task outside of promotion cycles.
    ## Why is this disabled?
    DataSRE has been trying to decommission data related to this DAG for years. Ownership now lies within the Data Org.
    If you want to use this DAG, please contact the Data Org.
    See this comment for more [context](https://mozilla-hub.atlassian.net/browse/SVCSE-3016?focusedCommentId=1127778).
    """
)

# Because airflow won't run 2019-02-01's monthly job until 2019-03-01, we need to
# pass next month's execution datetime (minus 1 day) to our scripts like:
# {{ macros.ds_add(next_execution_date.strftime("%Y-%m-%d"), -1) }}
# I think the newest airflow will let you do:
# {{ macros.ds_add(next_ds, -1) }}
# but Google's not using that version yet
#
peopleteam_fetch = GKEPodOperator(
    task_id="peopleteam-monthly-fetcher",
    name="peopleteam-monthly-fetcher",
    image_pull_policy="Always",
    image=DI_IMAGE,
    secrets=[WORKDAY_USERNAME, WORKDAY_PASSWORD],
    # TODO: move the copying stuff to the di module itself
    cmds=[
        "sh",
        "-c",
        'bin/get_people_dashboard_data.py --monthly -o /tmp/ --date {{ macros.ds_add(next_execution_date.strftime("%Y-%m-%d"), -1) }} && gsutil cp /tmp/*_{{ macros.ds_add(next_execution_date.strftime("%Y-%m-%d"), -1) }}.csv gs://'
        + INCOMING_BUCKET
        + "/peopleteam_dashboard_monthly/pull_date_"
        + pull_date
        + "/",
    ],
    # cmds=['sh', '-c', 'sleep 7200'],
    dag=dag,
)

stage_op = {}
delete_op = {}
insert_op = {}
merge_op = {}
for report_name in ["hires", "terminations", "promotions", "headcount"]:
    stage_op[report_name] = GKEPodOperator(
        task_id="workday-stage-" + report_name,
        image="google/cloud-sdk:536.0.0",
        cmds=[
            "bash",
            "-c",
            "bq load --location=US --source_format CSV --autodetect --column_name_character_map=V1 --skip_leading_rows=1 --replace moz-fx-data-bq-people:composer_workday_staging."
            + report_name
            + " gs://"
            + INCOMING_BUCKET
            + "/peopleteam_dashboard_monthly/pull_date_"
            + pull_date
            + "/"
            + report_name
            + '_{{ macros.ds_add(next_execution_date.strftime("%Y-%m-%d"), -1) }}.csv',
            # + "_2022-05-31.csv",
        ],
        dag=dag,
    )

    stage_op[report_name].set_upstream(peopleteam_fetch)

    query_location = (
        "gs://" + QUERIES_BUCKET + "/peopleteam/" + report_name + "_merge.sql"
    )

    merge_op[report_name] = GKEPodOperator(
        task_id="workday-merge-" + report_name,
        name="workday-merge-" + report_name,
        image=QUERIES_IMAGE,
        cmds=[
            "bash",
            "-c",
            '/usr/local/bin/run_query.py -a query -p snapshot_date={{ macros.ds_add(next_execution_date.strftime("%Y-%m-%d"), -1) }} -q '
            + query_location,
        ],
        # cmds=['sh', '-c', 'sleep 3600'],
        dag=dag,
    )

    merge_op[report_name].set_upstream(stage_op[report_name])
