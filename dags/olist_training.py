"""Weekly Olist training DAG with independently recoverable stages."""
from datetime import datetime, timedelta, timezone

from airflow.sdk import dag, get_current_context, task

from olist_pipeline import extract, prepare, train, validate


@dag(
    dag_id="olist_training",
    schedule="@weekly",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    tags=["olist", "training"],
)
def olist_training():
    @task(task_id="extract", retries=3, retry_delay=timedelta(minutes=1), execution_timeout=timedelta(minutes=10))
    def extract_task() -> str:
        return extract(get_current_context()["run_id"])

    @task(task_id="validate", retries=0, trigger_rule="all_success", execution_timeout=timedelta(minutes=5))
    def validate_task(snapshot: str) -> str:
        return validate(snapshot)

    @task(task_id="prepare", trigger_rule="all_success", execution_timeout=timedelta(minutes=10))
    def prepare_task(snapshot: str) -> dict:
        return prepare(snapshot)

    @task(task_id="train", trigger_rule="all_success", execution_timeout=timedelta(minutes=30))
    def train_task(prepared: dict) -> str:
        return train(prepared, dag_run_id=get_current_context()["run_id"])

    train_task(prepare_task(validate_task(extract_task())))


olist_training()
