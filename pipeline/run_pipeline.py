"""CLI entrypoint:  python -m pipeline.run_pipeline [--events FILE] [--db FILE]

1. ingest  - land the raw NDJSON in raw.event_logs (Python)
2. dbt build - staging -> core models, with dbt tests as the data-quality gate

Idempotent: re-running the same file replaces its raw rows and upserts facts by event_id.
"""
import argparse
import json
import logging
import os
from pathlib import Path

from . import ingest
from .config import DB_PATH, DBT_DIR, RAW_DIR

log = logging.getLogger("pipeline")


class DbtFailure(RuntimeError):
    pass


def run_dbt(db: Path, users: Path, episodes: Path, extra_vars: dict = None, command: str = "build") -> None:
    from dbt.cli.main import dbtRunner

    os.environ["DBT_DUCKDB_PATH"] = str(db)
    variables = json.dumps({"users_csv": users.as_posix(), "episodes_csv": episodes.as_posix(), **(extra_vars or {})})
    res = dbtRunner().invoke([
        command, "--project-dir", str(DBT_DIR), "--profiles-dir", str(DBT_DIR),
        "--vars", variables,
    ])
    if not res.success:
        raise DbtFailure(f"dbt {command} failed: {res.exception or 'see log above'}")


def run(events: Path, users: Path, episodes: Path, db: Path, extra_vars: dict = None) -> None:
    ingest.load_events(db, events)
    run_dbt(db, users, episodes, extra_vars)


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--events", type=Path, default=RAW_DIR / "event_logs.json")
    p.add_argument("--users", type=Path, default=RAW_DIR / "users.csv")
    p.add_argument("--episodes", type=Path, default=RAW_DIR / "episodes.csv")
    p.add_argument("--db", type=Path, default=DB_PATH)
    a = p.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    run(a.events.resolve(), a.users.resolve(), a.episodes.resolve(), a.db.resolve())


if __name__ == "__main__":
    main()
