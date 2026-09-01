#!/usr/bin/env python
"""Rollback-only parity and timing gate for migration 050.

The candidate functions are created inside one transaction, compared with the
existing Ratings, Four Factors, and Minutes composition, and always rolled
back. This script has deliberately no apply mode.
"""
from __future__ import annotations

import argparse
import re
import sys
import time
from dataclasses import dataclass
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from euroleague_possessions.postgres_backend import (  # noqa: E402
    _split_sql_statements,
    connect_from_env_file,
    inspect_target,
)


DDL = ROOT / "sql" / "candidates" / "050_two_call_team_dashboard_readers.sql"
ENV = ROOT.parent / "etl" / ".Renviron"
RATING_COLUMNS = (
    "game_year", "team_id", "team_name", "off_ppp", "def_ppp", "net_rtg",
    "games_played", "wins", "losses", "off_poss", "def_poss",
    "rank_net_rtg", "rank_off_ppp", "rank_def_ppp",
)
FACTOR_COLUMNS = (
    "game_year", "team_id", "team_name", "off_ppp", "def_ppp", "net_rtg",
    "off_efg", "def_efg", "off_ts", "def_ts", "off_oreb", "def_oreb",
    "off_tov", "def_tov", "off_ftr", "def_ftr", "off_poss", "def_poss",
)
COMBINED_COLUMNS = RATING_COLUMNS + FACTOR_COLUMNS[6:16] + ("minutes",)
METRIC_COLUMNS = COMBINED_COLUMNS[:-1]


@dataclass(frozen=True)
class Route:
    kind: str
    label: str
    extra: str


ROUTES = (
    Route("pergame", "broad", ""),
    Route("pergame", "last 10", "p_last_n_games=>10"),
    Route("pergame", "starter context", "p_num_starters_off_min=>5,p_num_starters_def_max=>3"),
    Route("dynamic", "standard clutch", "p_max_margin=>5,p_margin_status=>'all',p_max_time_remaining=>300"),
    Route("dynamic", "standard clutch home", "p_max_margin=>5,p_margin_status=>'all',p_max_time_remaining=>300,p_home_away=>'home'"),
    Route("direct", "custom clutch", "p_max_margin=>3,p_margin_status=>'all',p_max_time_remaining=>240"),
    Route("direct", "custom clutch starters", "p_max_margin=>3,p_margin_status=>'trailing',p_max_time_remaining=>180,p_num_starters_off_min=>4"),
)
SMOKE_ROUTES = (ROUTES[0], ROUTES[3], ROUTES[5])


def validate_ddl(source: str) -> list[str]:
    upper = source.upper()
    if "EUROLEAGUE SHADOW SCHEMA" not in upper:
        raise ValueError("EuroLeague migration safety marker is missing")
    if re.search(r"\bBASKETBALL(?:_TEST)?\s*\.", upper):
        raise ValueError("migration references an Israeli schema")
    if "CASCADE" in upper or re.search(r"\bDROP\b", upper):
        raise ValueError("migration 050 must remain additive")
    return [
        statement for statement in _split_sql_statements(source)
        if statement.strip().upper() not in {"BEGIN", "COMMIT"}
    ]


def arguments(extra: str) -> str:
    base = "p_competition=>'E',p_game_year=>2025,p_start_date=>DATE '2025-09-01',p_end_date=>DATE '2026-07-01'"
    return base + (("," + extra) if extra else "")


def fetch(cursor, function: str, args: str):
    started = time.perf_counter()
    cursor.execute(f"SELECT * FROM euroleague.{function}({args})")
    columns = tuple(column.name for column in cursor.description)
    rows = cursor.fetchall()
    elapsed = time.perf_counter() - started
    keyed = {row[columns.index("team_id")]: row for row in rows}
    if len(keyed) != len(rows):
        raise RuntimeError(f"{function} returned duplicate team_id values")
    return columns, keyed, elapsed


def value(columns, row, name):
    return row[columns.index(name)]


def legacy(cursor, route: Route, args: str):
    started = time.perf_counter()
    rating_columns, ratings, _ = fetch(cursor, f"get_team_ratings_{route.kind}", args)
    factor_columns, factors, _ = fetch(cursor, f"get_team_four_factors_{route.kind}", args)
    minute_columns, minutes, _ = fetch(cursor, f"get_team_minutes_{route.kind}", args)
    elapsed = time.perf_counter() - started
    if not ratings or not factors or not minutes:
        raise RuntimeError(
            f"{route.kind}/{route.label} is vacuous: "
            f"ratings={len(ratings)} factors={len(factors)} minutes={len(minutes)}"
        )
    keys = sorted(set(ratings) | set(factors) | set(minutes))
    result = {}
    for team_id in keys:
        if team_id not in ratings or team_id not in factors or team_id not in minutes:
            raise RuntimeError(f"{route.kind}/{route.label} missing team_id={team_id}")
        rating = ratings[team_id]
        factor = factors[team_id]
        minute = minutes[team_id]
        for name in ("game_year", "team_id", "team_name", "off_ppp", "def_ppp", "net_rtg", "off_poss", "def_poss"):
            if value(rating_columns, rating, name) != value(factor_columns, factor, name):
                raise RuntimeError(f"existing companion drift for team_id={team_id} column={name}")
        result[team_id] = (
            *(value(rating_columns, rating, name) for name in RATING_COLUMNS),
            *(value(factor_columns, factor, name) for name in FACTOR_COLUMNS[6:16]),
            value(minute_columns, minute, "minutes"),
        )
    return result, elapsed


def candidate(cursor, route: Route, args: str):
    if route.kind == "dynamic":
        columns, rows, elapsed = fetch(cursor, "get_team_dashboard_dynamic", args)
        if columns != COMBINED_COLUMNS:
            raise RuntimeError(f"unexpected combined columns: {columns!r}")
        return {team_id: tuple(row) for team_id, row in rows.items()}, elapsed

    started = time.perf_counter()
    columns, rows, _ = fetch(cursor, f"get_team_metrics_{route.kind}", args)
    minute_columns, minutes, _ = fetch(cursor, f"get_team_minutes_{route.kind}", args)
    elapsed = time.perf_counter() - started
    if columns != METRIC_COLUMNS:
        raise RuntimeError(f"unexpected metric columns: {columns!r}")
    result = {}
    for team_id, row in rows.items():
        if team_id not in minutes:
            raise RuntimeError(f"candidate minutes missing team_id={team_id}")
        result[team_id] = tuple(row) + (
            value(minute_columns, minutes[team_id], "minutes"),
        )
    return result, elapsed


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--full", action="store_true",
        help="run all filter presets instead of one representative per route",
    )
    parser.add_argument(
        "--candidate-first", action="store_true",
        help="run the combined call before its three-call companion",
    )
    options = parser.parse_args()
    routes = ROUTES if options.full else SMOKE_ROUTES
    statements = validate_ddl(DDL.read_text(encoding="utf-8"))
    connection = connect_from_env_file(ENV, direct_port=5432)
    connection.autocommit = False
    cursor = connection.cursor()
    try:
        target = inspect_target(connection)
        if target["euroleague_schema"] != "euroleague":
            raise RuntimeError(f"unexpected target: {target}")
        cursor.execute("SET LOCAL lock_timeout='5s'")
        cursor.execute("SET LOCAL statement_timeout='90s'")
        print("creating migration 050 candidates inside rollback-only transaction", flush=True)
        for statement in statements:
            cursor.execute(statement)
        print("candidate DDL parsed and created", flush=True)
        for route in routes:
            print(f"checking {route.kind}/{route.label}", flush=True)
            call_args = arguments(route.extra)
            if options.candidate_first:
                actual, new_seconds = candidate(cursor, route, call_args)
                expected, old_seconds = legacy(cursor, route, call_args)
            else:
                expected, old_seconds = legacy(cursor, route, call_args)
                actual, new_seconds = candidate(cursor, route, call_args)
            if expected != actual:
                keys = sorted(set(expected) | set(actual))
                key = next(key for key in keys if expected.get(key) != actual.get(key))
                left, right = expected.get(key), actual.get(key)
                if left is None or right is None:
                    detail = f"team_id={key} expected={left!r} actual={right!r}"
                else:
                    index = next(i for i, pair in enumerate(zip(left, right)) if pair[0] != pair[1])
                    detail = (
                        f"team_id={key} column={COMBINED_COLUMNS[index]} "
                        f"expected={left[index]!r} actual={right[index]!r}"
                    )
                raise RuntimeError(f"{route.kind}/{route.label}: {detail}")
            print(
                f"OK {route.kind:<8} {route.label:<24} rows={len(actual):>2} "
                f"three_calls={old_seconds:.3f}s combined={new_seconds:.3f}s",
                flush=True,
            )
        print("all combined Team reader contracts pass; transaction will roll back")
        return 0
    finally:
        connection.rollback()
        cursor.close()
        connection.close()


if __name__ == "__main__":
    raise SystemExit(main())
