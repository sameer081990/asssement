"""Airflow DAG: daily batch run of the podcast event pipeline.

ingest_raw_events  ->  dbt_run (staging + core models)  ->  dbt_test (data-quality gate)

Assumes the upstream app drops one NDJSON file per day at
  $PODCAST_RAW_DIR/events/event_logs_YYYY-MM-DD.json
(falling back to the sample data/raw/event_logs.json). Every task is idempotent
(replace-by-file raw load, incremental upsert by event_id), so retries and
`airflow dags backfill` are safe.

In production, swap the DuckDB profile for dbt-snowflake and run dbt with
BashOperator / Cosmos instead of calling dbtRunner in-process.
"""
import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator

from pipeline import ingest
from pipeline.config import DB_PATH, RAW_DIR
from pipeline.run_pipeline import run_dbt

RAW = Path(os.environ.get("PODCAST_RAW_DIR", RAW_DIR))
USERS, EPISODES = RAW / "users.csv", RAW / "episodes.csv"


def _events_file(ds: str) -> Path:
    daily = RAW / "events" / f"event_logs_{ds}.json"
    return daily if daily.exists() else RAW / "event_logs.json"


def ingest_raw_events(ds, **_):
    ingest.load_events(DB_PATH, _events_file(ds))


def dbt_run(**_):
    run_dbt(DB_PATH, USERS, EPISODES, command="run")


def dbt_test(**_):
    run_dbt(DB_PATH, USERS, EPISODES, command="test")


with DAG(
    dag_id="podcast_events_daily",
    start_date=datetime(2024, 1, 1),
    schedule="0 2 * * *",          # 02:00 UTC, after the previous day's logs land
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["podcast", "events"],
) as dag:
    t_ingest = PythonOperator(task_id="ingest_raw_events", python_callable=ingest_raw_events)
    t_run = PythonOperator(task_id="dbt_run", python_callable=dbt_run)
    t_test = PythonOperator(task_id="dbt_test", python_callable=dbt_test)

    t_ingest >> t_run >> t_test
