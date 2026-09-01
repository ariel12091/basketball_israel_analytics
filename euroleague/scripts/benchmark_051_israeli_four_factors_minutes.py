#!/usr/bin/env python
"""Gate the narrow Israeli standard-clutch Four Factors + Minutes reader."""

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

from benchmark_051_israeli_standard_clutch_dashboard import (  # noqa: E402
    ENV, FF_COLUMNS, PRESETS, args_for, fetch, minute_query, value,
)
from euroleague_possessions.postgres_backend import (  # noqa: E402
    _split_sql_statements, connect_from_env_file, inspect_target,
)


DDL = ROOT / "sql" / "candidates" / "051_israeli_four_factors_minutes.sql"
PROBE = "get_team_four_factors_minutes_probe_20260901"


def configure(cursor):
    cursor.execute("SET LOCAL lock_timeout='5s'")
    cursor.execute("SET LOCAL statement_timeout='90s'")


def create_probe(cursor, temporary=False):
    source = DDL.read_text(encoding="utf-8")
    if "DROP " in source.upper() or "COMMIT" in source.upper():
        raise RuntimeError("candidate must remain additive and rollback-only")
    if temporary:
        source = source.replace(
            f"basketball_test.{PROBE}", f"pg_temp.{PROBE}", 1
        )
    for statement in _split_sql_statements(source):
        cursor.execute(statement)


def legacy(cursor, preset):
    started = time.perf_counter()
    fc, factors, _ = fetch(
        cursor, "basketball_test.get_team_four_factors_dynamic", args_for(preset)
    )
    cursor.execute(minute_query(preset))
    mc = tuple(desc.name for desc in cursor.description)
    minutes = {row[mc.index("team_id")]: row for row in cursor.fetchall()}
    return fc, factors, mc, minutes, time.perf_counter() - started


def candidate(cursor, preset, schema="basketball_test"):
    return fetch(cursor, f"{schema}.{PROBE}", args_for(preset))


def assert_parity(preset, old, new):
    fc, factors, mc, minutes, _ = old
    nc, rows, _ = new
    if not rows or set(rows) != set(factors) or set(rows) != set(minutes):
        raise RuntimeError(f"{preset.label}: row-key mismatch")
    for team_id, row in rows.items():
        for name in FF_COLUMNS:
            expected = value(fc, factors[team_id], name)
            actual = value(nc, row, name)
            if expected != actual:
                raise RuntimeError(
                    f"{preset.label}: team={team_id} column={name} "
                    f"expected={expected!r} actual={actual!r}"
                )
        expected_minutes = value(mc, minutes[team_id], "minutes")
        if value(nc, row, "minutes") != expected_minutes:
            raise RuntimeError(f"{preset.label}: team={team_id} column=minutes")


def percentile(values, fraction):
    ordered = sorted(values)
    return ordered[max(0, math.ceil(len(ordered) * fraction) - 1)]


def warm(runs):
    connection = connect_from_env_file(ENV, direct_port=5432)
    connection.autocommit = False
    cursor = connection.cursor()
    try:
        configure(cursor)
        create_probe(cursor)
        for preset in PRESETS:
            old = legacy(cursor, preset)
            new = candidate(cursor, preset)
            assert_parity(preset, old, new)
            print(f"  parity OK {preset.label:<18} rows={len(new[1])}", flush=True)
        preset = PRESETS[0]
        old_times, new_times = [], []
        for index in range(runs):
            if index % 2:
                new = candidate(cursor, preset)
                old = legacy(cursor, preset)
            else:
                old = legacy(cursor, preset)
                new = candidate(cursor, preset)
            assert_parity(preset, old, new)
            old_times.append(old[4])
            new_times.append(new[2])
        print(
            f"warm {runs}: legacy median/p90={statistics.median(old_times):.3f}/"
            f"{percentile(old_times,0.9):.3f}s narrow={statistics.median(new_times):.3f}/"
            f"{percentile(new_times,0.9):.3f}s",
            flush=True,
        )
    finally:
        connection.rollback()
        cursor.close()
        connection.close()


def open_temp_session():
    connection = connect_from_env_file(ENV, direct_port=5432)
    connection.autocommit = False
    cursor = connection.cursor()
    target = inspect_target(connection)
    if target["server_port"] != 5432:
        raise RuntimeError(f"unexpected target: {target}")
    configure(cursor)
    create_probe(cursor, temporary=True)
    cursor.execute("SELECT pg_backend_pid()")
    return connection, cursor, int(cursor.fetchone()[0])


def cold():
    sessions = [open_temp_session() for _ in range(8)]
    try:
        pids = [session[2] for session in sessions]
        if len(set(pids)) != 8:
            raise RuntimeError(f"cold gate requires eight distinct backends: {pids}")
        print(f"cold backend PIDs: {pids}", flush=True)
        preset = PRESETS[0]
        for pair in range(2):
            candidate_cursor = sessions[4 + pair * 2][1]
            legacy_cursor = sessions[5 + pair * 2][1]
            new = candidate(candidate_cursor, preset, schema="pg_temp")
            old = legacy(legacy_cursor, preset)
            assert_parity(preset, old, new)
            print(
                f"cold pair {pair + 1}: candidate-first={new[2]:.3f}s "
                f"legacy-first={old[4]:.3f}s rows={len(new[1])}",
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
    print("all probes rolled back or disappeared with pg_temp sessions", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
