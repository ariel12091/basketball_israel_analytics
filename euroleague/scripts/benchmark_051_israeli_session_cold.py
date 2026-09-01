#!/usr/bin/env python
"""Fresh-backend, session-local cold probe for the Israeli candidate."""

from __future__ import annotations

import sys
import time
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "scripts"))

from benchmark_051_israeli_standard_clutch_dashboard import (  # noqa: E402
    DDL, ENV, PRESETS, candidate, legacy_view,
)
from euroleague_possessions.postgres_backend import (  # noqa: E402
    _split_sql_statements, connect_from_env_file, inspect_target,
)


def open_probe_connection():
    connection = connect_from_env_file(ENV, direct_port=5432)
    connection.autocommit = False
    cursor = connection.cursor()
    target = inspect_target(connection)
    if target["server_port"] != 5432:
        raise RuntimeError(f"unexpected target: {target}")
    cursor.execute("SET LOCAL lock_timeout='5s'")
    cursor.execute("SET LOCAL statement_timeout='90s'")
    source = DDL.read_text(encoding="utf-8").replace(
        "basketball_test.get_team_dashboard_standard_clutch_probe_20260901",
        "pg_temp.get_team_dashboard_standard_clutch_probe_20260901",
        1,
    )
    for statement in _split_sql_statements(source):
        cursor.execute(statement)
    cursor.execute("SELECT pg_backend_pid()")
    return connection, cursor, cursor.fetchone()[0]


def main() -> int:
    # Hold every connection until all four samples finish so the pooler cannot
    # hand the same backend to another side of the comparison.
    # The session pool currently exposes four already-used backends. Holding
    # eight connections forces four additional PIDs for the measured pairs;
    # the first four are deliberately occupied but not sampled.
    probes = [open_probe_connection() for _ in range(8)]
    try:
        pids = [probe[2] for probe in probes]
        if len(set(pids)) != len(pids):
            raise RuntimeError(f"cold probe requires four distinct backends: {pids}")
        print(f"distinct backend PIDs: {pids}", flush=True)
        preset = PRESETS[0]
        for index, view in enumerate(("summary", "four_factors")):
            candidate_cursor = probes[4 + index * 2][1]
            legacy_cursor = probes[5 + index * 2][1]
            started = time.perf_counter()
            new = candidate(candidate_cursor, preset, schema="pg_temp")
            candidate_seconds = time.perf_counter() - started
            legacy_seconds = legacy_view(legacy_cursor, preset, view)
            if not new[1]:
                raise RuntimeError(f"{view}: candidate returned no rows")
            print(
                f"cold {view}+minutes: candidate-first={candidate_seconds:.3f}s "
                f"legacy-first={legacy_seconds:.3f}s rows={len(new[1])}",
                flush=True,
            )
        print("ROLLBACK: pg_temp probes disappear with their sessions", flush=True)
        return 0
    finally:
        for connection, cursor, _ in probes:
            connection.rollback()
            cursor.close()
            connection.close()


if __name__ == "__main__":
    raise SystemExit(main())
