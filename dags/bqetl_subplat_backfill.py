import datetime

from airflow import DAG

from operators.gcp_container_operator import GKEPodOperator

docs = """
### bqetl_subplat_backfill

Temporary DAG for backfilling Python ETLs which are normally run in [`bqetl_subplat`](https://github.com/mozilla/bigquery-etl/blob/generated-sql/dags/bqetl_subplat.py).

#### Owner

srose@mozilla.com
"""

default_args = {
    "owner": "srose@mozilla.com",
    "email": ["telemetry-alerts@mozilla.com", "srose@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=120),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
}

tags = ["impact/tier_1"]

with DAG(
    "bqetl_subplat_backfill",
    default_args=default_args,
    schedule_interval="@daily",
    start_date=datetime.datetime(2022, 12, 1, 0, 0),
    end_date=datetime.datetime(2023, 4, 30, 0, 0),
    catchup=True,
    doc_md=docs,
    tags=tags,
) as dag:
    stripe_external__itemized_tax_transactions__v1 = GKEPodOperator(
        task_id="stripe_external__itemized_tax_transactions__v1",
        arguments=[
            "python",
            "sql/moz-fx-data-shared-prod/stripe_external/itemized_tax_transactions_v1/query.py",
            "--date={{ ds }}",
            "--api-key={{ var.value.stripe_api_key }}",
            "--report-type=tax.transactions.itemized.1",
            "--table=moz-fx-data-shared-prod.stripe_external.itemized_tax_transactions_v1",
            "--time-partitioning-field=transaction_date_utc",
        ],
        image="gcr.io/moz-fx-data-airflow-prod-88e0/bigquery-etl:latest",
    )
