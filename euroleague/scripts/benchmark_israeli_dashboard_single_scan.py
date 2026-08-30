#!/usr/bin/env python
"""Rollback-only parity and performance probe for an Israeli single-scan dashboard.

The candidate is derived from the committed canonical four_factors_compute SQL,
created under a distinct name inside one transaction, exercised as app_readonly,
and always rolled back. This script cannot apply or persist the candidate.
"""
from __future__ import annotations

import json
import statistics
import sys
import time
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
EURO = ROOT / "euroleague"
sys.path.insert(0, str(EURO / "src"))

from euroleague_possessions.postgres_backend import connect_from_env_file  # noqa: E402


SOURCE = ROOT / "sql" / "functions" / "four_factors_dashboard_compute.sql"
ENV = ROOT / "etl" / ".Renviron"
CANDIDATE = "four_factors_dashboard_single_scan_candidate"
BASE = "p_game_year=>2026,p_start_date=>DATE '2025-10-01',p_end_date=>DATE '2026-07-01'"
PRESETS = (
    ("broad", ""),
    ("last 10", "p_last_n_games=>10"),
    ("game type", "p_game_type_csv=>'5'"),
    ("game type multi", "p_game_type_csv=>'5,16'"),
    ("opponents", "p_opp_ids_csv=>'2,3'"),
    ("opponent rank", "p_opp_rank_side=>'top',p_opp_rank_n=>5,p_opp_rank_metric=>'off'"),
    ("gn range", "p_min_gn=>5,p_max_gn=>15"),
    ("home", "p_home_away=>'home'"),
    ("win", "p_outcome=>'win'"),
    ("own starters", "p_num_starters_off_min=>5"),
    ("opponent starters", "p_num_starters_def_max=>3"),
    ("empty", "p_min_gn=>999"),
)
NONEMPTY = {label for label, _ in PRESETS if label != "empty"}


def replace_once(text: str, old: str, new: str) -> str:
    count = text.count(old)
    if count != 1:
        raise RuntimeError(f"candidate source marker occurs {count} times: {old[:80]!r}")
    return text.replace(old, new, 1)


def candidate_ddl() -> str:
    """Rename the exact prepared production DDL for transactional probing."""
    source = SOURCE.read_text(encoding="utf-8")
    source = source[source.index("CREATE OR REPLACE FUNCTION"):]
    source = source[:source.index("\nREVOKE ALL ON FUNCTION")]
    source = replace_once(
        source,
        "CREATE OR REPLACE FUNCTION basketball_test.four_factors_dashboard_compute(",
        f"CREATE OR REPLACE FUNCTION basketball_test.{CANDIDATE}(",
    )
    return source


def args(extra: str) -> str:
    return BASE + (("," + extra) if extra else "")


def fetch(cursor, function: str, extra: str):
    started = time.perf_counter()
    cursor.execute(f"SELECT * FROM basketball_test.{function}({args(extra)})")
    columns = tuple(column.name for column in cursor.description)
    rows = cursor.fetchall()
    return columns, rows, time.perf_counter() - started


def keyed(columns, rows):
    player = columns.index("player_id")
    team = columns.index("team_id")
    return {(row[player], row[team]): tuple(row) for row in rows}


def explain(cursor, function: str, extra: str) -> dict:
    cursor.execute(
        "EXPLAIN (ANALYZE, BUFFERS, SETTINGS, FORMAT JSON) "
        f"SELECT * FROM basketball_test.{function}({args(extra)})"
    )
    payload = cursor.fetchone()[0]
    if isinstance(payload, str):
        payload = json.loads(payload)
    result = payload[0]
    plan = result["Plan"]
    return {
        "execution_ms": Decimal(str(result["Execution Time"])),
        "shared_buffers": int(plan.get("Shared Hit Blocks", 0)) + int(plan.get("Shared Read Blocks", 0)),
        "temp_blocks": int(plan.get("Temp Read Blocks", 0)) + int(plan.get("Temp Written Blocks", 0)),
        "rows": int(plan.get("Actual Rows", 0)),
    }


def main() -> int:
    connection = connect_from_env_file(ENV, direct_port=5432)
    connection.autocommit = False
    cursor = connection.cursor()
    try:
        cursor.execute("SET LOCAL statement_timeout='120s'")
        cursor.execute(candidate_ddl())
        cursor.execute(
            f"REVOKE ALL ON FUNCTION basketball_test.{CANDIDATE} FROM PUBLIC"
        )
        cursor.execute(
            f"GRANT EXECUTE ON FUNCTION basketball_test.{CANDIDATE} TO app_readonly"
        )
        cursor.execute(
            "SELECT current_user, has_function_privilege('app_readonly', p.oid, 'EXECUTE') "
            "FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace "
            "WHERE n.nspname='basketball_test' AND p.proname=%s",
            (CANDIDATE,),
        )
        database_user, app_can_execute = cursor.fetchone()
        if not app_can_execute:
            raise RuntimeError("candidate is not executable by app_readonly")
        print(
            f"database role={database_user}; app_readonly ACL verified "
            "(configured credentials cannot SET ROLE)"
        )

        print("-- exact 47-column parity: current wrapper vs single scan --")
        for label, extra in PRESETS:
            expected_columns, expected_rows, _ = fetch(cursor, "four_factors_dashboard_compute", extra)
            actual_columns, actual_rows, _ = fetch(cursor, CANDIDATE, extra)
            if expected_columns != actual_columns:
                raise RuntimeError(f"{label}: return-column mismatch")
            expected = keyed(expected_columns, expected_rows)
            actual = keyed(actual_columns, actual_rows)
            if expected != actual:
                keys = sorted(set(expected) | set(actual))
                first = next(key for key in keys if expected.get(key) != actual.get(key))
                raise RuntimeError(
                    f"{label}: parity failed at {first}: expected={expected.get(first)!r} actual={actual.get(first)!r}"
                )
            if label in NONEMPTY and not actual_rows:
                raise RuntimeError(f"{label}: vacuous zero-row preset")
            print(f"OK {label:<18} rows={len(actual_rows)}")

        print("\n-- interleaved warm timing, seven runs each --")
        for label, extra in (("broad", ""), ("last 10", "p_last_n_games=>10")):
            timings = {"wrapper": [], "single_scan": []}
            for run in range(7):
                order = (
                    (("wrapper", "four_factors_dashboard_compute"), ("single_scan", CANDIDATE))
                    if run % 2 == 0
                    else (("single_scan", CANDIDATE), ("wrapper", "four_factors_dashboard_compute"))
                )
                for key, function in order:
                    timings[key].append(fetch(cursor, function, extra)[2])
            wrapper = statistics.median(timings["wrapper"])
            single = statistics.median(timings["single_scan"])
            print(
                f"{label:<10} wrapper={wrapper:.3f}s single_scan={single:.3f}s "
                f"change={(single / wrapper - 1):+.1%}"
            )

        print("\n-- EXPLAIN (ANALYZE, BUFFERS) --")
        for label, extra in (("broad", ""), ("last 10", "p_last_n_games=>10")):
            for key, function in (("wrapper", "four_factors_dashboard_compute"), ("single_scan", CANDIDATE)):
                measured = explain(cursor, function, extra)
                print(
                    f"{label:<10} {key:<11} rows={measured['rows']:<4} "
                    f"buffers={measured['shared_buffers']:<7} temp={measured['temp_blocks']:<5} "
                    f"execution={measured['execution_ms']}ms"
                )

        connection.rollback()
        print("\nROLLBACK complete; candidate was not persisted")
        return 0
    except Exception:
        connection.rollback()
        raise
    finally:
        cursor.close()
        connection.close()


if __name__ == "__main__":
    raise SystemExit(main())
