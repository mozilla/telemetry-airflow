import datetime

from airflow.decorators import dag
from airflow.models.param import Param
from utils.gcp import gke_command
from utils.tags import Tag

doc_md = """
# Android Client History Simulation DAG

#### Use with caution: This will overwrite the dataset of the seed you choose

#### Some tips/notes:
* Date formats are 2020-03-01 or 2020-03-01T00:00:00
"""


@dag(
    dag_id="android_client_history_sim",
    schedule_interval=None,
    doc_md=doc_md,
    catchup=False,
    start_date=datetime.datetime(2023, 7, 1),
    dagrun_timeout=datetime.timedelta(days=1),
    tags=[Tag.ImpactTier.tier_3, Tag.Triage.record_only],
    render_template_as_native_obj=True,
    params={
        "seed": Param(type="integer"),
        "start_date": Param(
            type="string",
            format="date-time",
        ),
        "end_date": Param(
            datetime.datetime.today().isoformat(), type="string", format="date-time"
        ),
        "lookback": Param(7, type="integer"),
    },
)
def client_history_sim_dag():
    gke_command(
        task_id="android_client_history_sim",
        command=[
            "python",
            "client_regeneration/main.py",
            "--seed={{ dag_run.conf['seed'] }}"
            "--start_date={{ dag_run.conf['start_date'] }}",
            "--end_date={{ dag_run.conf['end_date'] }}",
            "--lookback={{ dag_run.conf['lookback'] }}",
        ],
        docker_image="gcr.io/moz-fx-data-airflow-prod-88e0/client-regeneration_docker_etl:latest",
        gcp_conn_id="google_cloud_airflow_gke",
    )


dag = client_history_sim_dag()
