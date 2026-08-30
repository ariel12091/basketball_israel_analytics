#!/usr/bin/env python
"""Rollback-gate migration 046 against the current two-call app result."""

from __future__ import annotations

import argparse
import json
import statistics
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))
from euroleague_possessions.postgres_backend import (  # noqa: E402
    _split_sql_statements, connect_from_env_file, inspect_target,
)

DDL = ROOT / "sql" / "046_player_dashboard_reader.sql"
ENV = ROOT.parent / "etl" / ".Renviron"
BASE = "p_competition=>'E',p_game_year=>2025,p_start_date=>DATE '2025-09-01',p_end_date=>DATE '2026-07-01'"
PRESETS = (
    ("broad app dates", ""),
    ("last 10", "p_last_n_games=>10"),
    ("one team", "p_team_ids_csv=>'24'"),
    ("phase", "p_phase_csv=>'RS'"),
    ("opponent", "p_opp_ids_csv=>'19'"),
    ("home", "p_home_away=>'home'"),
    ("win", "p_outcome=>'win'"),
    ("opponent rank", "p_opp_rank_side=>'top',p_opp_rank_n=>5,p_opp_rank_metric=>'off'"),
    ("own starters", "p_num_starters_off_min=>5"),
    ("opponent starters", "p_num_starters_def_max=>3"),
    ("empty", "p_min_gn=>999"),
)
RATING_COLUMNS = ("Net RTG Diff", "Off ON Diff", "Def ON Diff", "minutes")


def options():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--runs", type=int, default=5)
    return parser.parse_args()


def statements() -> list[str]:
    parsed = _split_sql_statements(DDL.read_text(encoding="utf-8"))
    return [s for s in parsed if s.strip().upper() not in {"BEGIN", "COMMIT"}]


def args_sql(extra: str) -> str:
    return BASE + (("," + extra) if extra else "")


def fetch(cursor, function: str, extra: str):
    cursor.execute(f"SELECT * FROM euroleague.{function}({args_sql(extra)})")
    columns = [d.name for d in cursor.description]
    return columns, [tuple(row) for row in cursor.fetchall()]


def current_app_result(cursor, extra: str):
    ff_columns, ff_rows = fetch(cursor, "four_factors_compute", extra)
    on_extra = extra + ("," if extra else "") + "p_min_net=>-999,p_min_all=>0,p_min_on=>0"
    on_columns, on_rows = fetch(cursor, "onoff_compute", on_extra)
    on_by_key = {
        (row[on_columns.index("player_id")], row[on_columns.index("team_id")]): row
        for row in on_rows
    }
    merged = []
    for row in ff_rows:
        key = (row[ff_columns.index("player_id")], row[ff_columns.index("team_id")])
        rating = on_by_key.get(key)
        values = ((None,) * len(RATING_COLUMNS) if rating is None else
                  tuple(rating[on_columns.index(name)] for name in RATING_COLUMNS))
        merged.append(row + values)
    return ff_columns + list(RATING_COLUMNS), merged


def canonical(columns, rows):
    return columns, sorted(json.dumps(row, default=str, separators=(",", ":")) for row in rows)


def candidate_result(cursor, extra: str):
    return fetch(cursor, "four_factors_dashboard_compute", extra)


def timed(call, runs: int):
    values = []
    result = None
    for _ in range(runs):
        started = time.perf_counter()
        result = call()
        values.append(time.perf_counter() - started)
    return result, values


def main() -> int:
    args = options()
    if args.runs < 1:
        raise ValueError("--runs must be positive")
    con = connect_from_env_file(ENV, direct_port=5432)
    con.autocommit = False
    cur = con.cursor()
    try:
        target = inspect_target(con)
        if target["euroleague_schema"] != "euroleague" or target["server_port"] != 5432:
            raise RuntimeError(f"unexpected target: {target}")
        cur.execute("SET LOCAL statement_timeout='30s'")
        for statement in statements():
            cur.execute(statement)
        print("migration 046 candidate created transactionally")

        for label, extra in PRESETS:
            before = current_app_result(cur, extra)
            after = candidate_result(cur, extra)
            if canonical(*before) != canonical(*after):
                raise RuntimeError(f"{label}: combined result differs from current app composition")
            print(f"  OK {label:<20} rows={len(after[1])}")

        extra = PRESETS[0][1]
        before_result, before_times = timed(lambda: current_app_result(cur, extra), args.runs)
        after_result, after_times = timed(lambda: candidate_result(cur, extra), args.runs)
        if canonical(*before_result) != canonical(*after_result):
            raise RuntimeError("timed broad result changed")
        before = statistics.median(before_times)
        after = statistics.median(after_times)
        print(f"broad complete call median: two-call={before:.3f}s combined={after:.3f}s")
        allowed = min(before * 0.90, before - 0.100)
        if after > allowed:
            raise RuntimeError(f"candidate misses improvement gate: {after:.3f}s > {allowed:.3f}s")

        if args.apply:
            con.commit()
            print("COMMITTED migration 046")
        else:
            con.rollback()
            print("ROLLBACK gate passed; no persistent database change")
        return 0
    except Exception:
        con.rollback()
        raise
    finally:
        cur.close()
        con.close()


if __name__ == "__main__":
    raise SystemExit(main())
