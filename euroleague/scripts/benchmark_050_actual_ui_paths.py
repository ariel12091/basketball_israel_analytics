#!/usr/bin/env python
"""Measure migration 050 against the two actual standard-clutch UI paths."""

from __future__ import annotations

import argparse
import math
import statistics
import sys
import time
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "scripts"))

from euroleague_possessions.postgres_backend import connect_from_env_file, inspect_target  # noqa: E402
from gate_050_combined_team_dashboard_readers import (  # noqa: E402
    ENV, FACTOR_COLUMNS, RATING_COLUMNS, Route, arguments, fetch, value,
)


ROUTE = Route(
    "dynamic", "standard clutch",
    "p_max_margin=>5,p_margin_status=>'all',p_max_time_remaining=>300",
)


def configure(cursor):
    cursor.execute("SET LOCAL statement_timeout='90s'")


def dashboard(cursor):
    return fetch(cursor, "get_team_dashboard_dynamic", arguments(ROUTE.extra))


def legacy_view(cursor, view: str):
    started = time.perf_counter()
    metric = "ratings" if view == "summary" else "four_factors"
    columns, rows, _ = fetch(cursor, f"get_team_{metric}_dynamic", arguments(ROUTE.extra))
    minute_columns, minutes, _ = fetch(cursor, "get_team_minutes_dynamic", arguments(ROUTE.extra))
    result = {}
    for team_id, row in rows.items():
        if team_id not in minutes:
            raise RuntimeError(f"{view}: missing minutes for team_id={team_id}")
        result[team_id] = (columns, row, value(minute_columns, minutes[team_id], "minutes"))
    return result, time.perf_counter() - started


def assert_view_parity(view: str, old, new):
    columns, rows, _ = new
    if set(old) != set(rows) or not rows:
        raise RuntimeError(f"{view}: row keys differ or are empty")
    expected_columns = RATING_COLUMNS if view == "summary" else FACTOR_COLUMNS
    for team_id, candidate_row in rows.items():
        old_columns, old_row, old_minutes = old[team_id]
        for name in expected_columns:
            if value(columns, candidate_row, name) != value(old_columns, old_row, name):
                raise RuntimeError(f"{view}: team_id={team_id} column={name}")
        if value(columns, candidate_row, "minutes") != old_minutes:
            raise RuntimeError(f"{view}: team_id={team_id} column=minutes")


def percentile(values, fraction):
    ordered = sorted(values)
    return ordered[max(0, math.ceil(len(ordered) * fraction) - 1)]


def warm(runs: int):
    connection = connect_from_env_file(ENV, direct_port=5432)
    connection.autocommit = False
    cursor = connection.cursor()
    try:
        configure(cursor)
        for view in ("summary", "four_factors"):
            old, _ = legacy_view(cursor, view)
            new = dashboard(cursor)
            assert_view_parity(view, old, new)
            old_times, new_times = [], []
            for index in range(runs):
                if index % 2:
                    new = dashboard(cursor)
                    old, old_seconds = legacy_view(cursor, view)
                else:
                    old, old_seconds = legacy_view(cursor, view)
                    new = dashboard(cursor)
                assert_view_parity(view, old, new)
                old_times.append(old_seconds)
                new_times.append(new[2])
            print(
                f"warm {runs} {view}+minutes: "
                f"legacy median/p90={statistics.median(old_times):.3f}/{percentile(old_times,0.9):.3f}s "
                f"combined={statistics.median(new_times):.3f}/{percentile(new_times,0.9):.3f}s",
                flush=True,
            )
    finally:
        connection.rollback()
        cursor.close()
        connection.close()


def open_session():
    connection = connect_from_env_file(ENV, direct_port=5432)
    connection.autocommit = False
    cursor = connection.cursor()
    target = inspect_target(connection)
    if target["server_port"] != 5432 or target["euroleague_schema"] != "euroleague":
        raise RuntimeError(f"unexpected target: {target}")
    configure(cursor)
    cursor.execute("SELECT pg_backend_pid()")
    return connection, cursor, int(cursor.fetchone()[0])


def cold():
    sessions = [open_session() for _ in range(4)]
    try:
        pids = [session[2] for session in sessions]
        if len(set(pids)) != 4:
            raise RuntimeError(f"cold comparison requires distinct backends: {pids}")
        print(f"cold backend PIDs: {pids}", flush=True)
        for index, view in enumerate(("summary", "four_factors")):
            candidate_cursor = sessions[index * 2][1]
            legacy_cursor = sessions[index * 2 + 1][1]
            new = dashboard(candidate_cursor)
            old, old_seconds = legacy_view(legacy_cursor, view)
            assert_view_parity(view, old, new)
            print(
                f"cold {view}+minutes: candidate-first={new[2]:.3f}s "
                f"legacy-first={old_seconds:.3f}s rows={len(new[1])}",
                flush=True,
            )
    finally:
        for connection, cursor, _ in sessions:
            connection.rollback()
            cursor.close()
            connection.close()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--runs", type=int, default=15)
    args = parser.parse_args()
    if args.runs < 3:
        parser.error("--runs must be at least 3")
    warm(args.runs)
    cold()
    print("read-only measurement complete; no database change", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
