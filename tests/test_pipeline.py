"""End-to-end tests: raw ingest + dbt build on small hand-made event files.

Each dbt invocation has a few seconds of startup cost, so there are few, broad tests.
"""
import json

import duckdb
import pytest

from pipeline.config import RAW_DIR
from pipeline.run_pipeline import DbtFailure, run

USERS = RAW_DIR / "users.csv"
EPISODES = RAW_DIR / "episodes.csv"


def _write(path, rows):
    path.write_text("\n".join(json.dumps(r) if isinstance(r, dict) else r for r in rows) + "\n")


def ev(t="play", u="user_1", e="ep_1", ts="2024-03-01T10:00:00", d=100):
    return {"event_type": t, "user_id": u, "episode_id": e, "timestamp": ts, "duration": d}


@pytest.fixture
def db(tmp_path):
    return tmp_path / "t.duckdb"


def test_cleaning_rules_and_quarantine(tmp_path, db):
    f = tmp_path / "e.json"
    _write(f, [
        ev(),                                              # valid
        ev(t="PAUSE ", ts="2024-03-01T10:01:00", d=None),  # normalised -> valid
        ev(u=None, ts="2024-03-01T10:02:00"),              # no user
        ev(e="", ts="2024-03-01T10:03:00"),                # no episode
        ev(ts="malformed-date"),                           # bad ts
        ev(ts=None),                                       # null ts
        ev(ts="1999-01-01T00:00:00"),                      # out of range
        ev(t="rewind", ts="2024-03-01T10:04:00"),          # bad type
        ev(t="complete", ts="2024-03-01T10:05:00", d=None),  # missing duration
        ev(u="ghost", ts="2024-03-01T10:06:00"),           # unknown user
        "{not json",                                       # unparseable line
    ])
    run(f, USERS, EPISODES, db, extra_vars={"max_reject_rate": 1.0})  # disable reject-rate gate
    con = duckdb.connect(str(db))
    assert con.execute("SELECT count(*) FROM core.fact_events").fetchone()[0] == 2
    assert dict(con.execute(
        "SELECT reject_reason, count(*) FROM core.quarantined_events GROUP BY 1").fetchall()) == {
        "missing_user_id": 1, "missing_episode_id": 1, "malformed_timestamp": 2,
        "timestamp_out_of_range": 1, "invalid_event_type": 1,
        "missing_duration": 1, "unknown_user": 1, "empty_or_unparseable_record": 1,
    }


def test_idempotent_replay(tmp_path, db):
    f = tmp_path / "e.json"
    _write(f, [ev(), ev(t="complete", ts="2024-03-01T10:05:00", d=200)])
    run(f, USERS, EPISODES, db)
    run(f, USERS, EPISODES, db)  # replay same file
    con = duckdb.connect(str(db))
    assert con.execute("SELECT count(*) FROM raw.event_logs").fetchone()[0] == 2
    assert con.execute("SELECT count(*) FROM core.fact_events").fetchone()[0] == 2


def test_quality_gate_fails_when_too_many_rejects(tmp_path, db):
    f = tmp_path / "e.json"
    _write(f, [ev(u=None, ts=f"2024-03-01T10:0{i}:00") for i in range(5)] + [ev()])
    with pytest.raises(DbtFailure):
        run(f, USERS, EPISODES, db)
