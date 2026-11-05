"""Ingestion: land the raw event file in DuckDB without interpreting it.

Every column is forced to VARCHAR so bad values (e.g. "malformed-date", "" or a
string duration) never abort the load; validation happens in dbt (stg_events),
where rejects are quarantined with a reason. Lines that are not valid JSON come
through as all-NULL rows (ignore_errors) and are quarantined as
'empty_or_unparseable_record'.

Re-ingesting a file replaces that file's rows (replace-by-file), so the raw
table always mirrors the current files and replays are idempotent.
"""
import logging
from pathlib import Path

import duckdb

log = logging.getLogger(__name__)

RAW_DDL = """
CREATE SCHEMA IF NOT EXISTS raw;
CREATE TABLE IF NOT EXISTS raw.event_logs (
    event_type   VARCHAR,
    user_id      VARCHAR,
    episode_id   VARCHAR,
    "timestamp"  VARCHAR,
    duration     VARCHAR,
    _source_file VARCHAR,
    _loaded_at   TIMESTAMP
);
"""

EVENT_COLUMNS = {
    "event_type": "VARCHAR",
    "user_id": "VARCHAR",
    "episode_id": "VARCHAR",
    "timestamp": "VARCHAR",
    "duration": "VARCHAR",
}


def load_events(db: Path, events: Path) -> int:
    """Replace-by-file load of one NDJSON file into raw.event_logs. Returns rows loaded."""
    if not events.exists():
        raise FileNotFoundError(events)
    db.parent.mkdir(parents=True, exist_ok=True)
    cols = ", ".join(f"'{k}': '{v}'" for k, v in EVENT_COLUMNS.items())
    con = duckdb.connect(str(db))
    try:
        con.execute(RAW_DDL)
        con.execute("BEGIN")
        con.execute("DELETE FROM raw.event_logs WHERE _source_file = ?", [events.name])
        con.execute(
            f"""
            INSERT INTO raw.event_logs
            SELECT event_type, user_id, episode_id, "timestamp", duration,
                   ?, current_timestamp::TIMESTAMP
            FROM read_json(?, format='newline_delimited',
                           columns={{{cols}}}, ignore_errors=true)
            """,
            [events.name, str(events)],
        )
        con.execute("COMMIT")
        n = con.execute(
            "SELECT count(*) FROM raw.event_logs WHERE _source_file = ?", [events.name]
        ).fetchone()[0]
    finally:
        con.close()
    log.info("Loaded %d raw events from %s", n, events.name)
    return n
