#!/usr/bin/env python
"""Read-only behavioral contract audit for Team Ratings/Four Factors routes."""
from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from euroleague_possessions.postgres_backend import connect_from_env_file  # noqa: E402


ENV = ROOT.parent / "etl" / ".Renviron"
COMMON_COLUMNS = ("off_ppp", "def_ppp", "net_rtg", "off_poss", "def_poss")


@dataclass(frozen=True)
class Route:
    league: str
    label: str
    ratings_sql: str
    factors_sql: str
    must_return_rows: bool = True


def call(schema: str, ratings: str, factors: str, arguments: str, label: str) -> Route:
    return Route(
        schema,
        label,
        f"SELECT * FROM {schema}.{ratings}({arguments})",
        f"SELECT * FROM {schema}.{factors}({arguments})",
    )


IL_BASE = (
    "p_game_year=>2026,p_start_date=>DATE '2025-10-01',"
    "p_end_date=>DATE '2026-07-01'"
)
EL_BASE = (
    "p_competition=>'E',p_game_year=>2025,p_start_date=>DATE '2025-09-01',"
    "p_end_date=>DATE '2026-07-01'"
)


ROUTES = (
    Route(
        "basketball_test",
        "season materialized views",
        "SELECT * FROM basketball_test.team_ppp_ratings_mv WHERE game_year=2026",
        "SELECT * FROM basketball_test.team_four_factors_mv WHERE game_year=2026",
    ),
    call("basketball_test", "get_team_ratings_dynamic", "get_team_four_factors_dynamic", IL_BASE, "filtered broad"),
    call("basketball_test", "get_team_ratings_dynamic", "get_team_four_factors_dynamic", IL_BASE + ",p_last_n_games=>10", "filtered last 10"),
    call("basketball_test", "get_team_ratings_dynamic", "get_team_four_factors_dynamic", IL_BASE + ",p_home_away=>'home'", "filtered home"),
    call("basketball_test", "get_team_ratings_dynamic", "get_team_four_factors_dynamic", IL_BASE + ",p_num_starters_off_min=>5", "filtered own starters"),
    call("basketball_test", "get_team_ratings_dynamic", "get_team_four_factors_dynamic", IL_BASE + ",p_num_starters_def_max=>3", "filtered opponent starters"),
    Route(
        "euroleague",
        "season materialized views",
        "SELECT * FROM euroleague.team_ppp_ratings_mv WHERE competition='E' AND game_year=2025",
        "SELECT * FROM euroleague.team_four_factors_mv WHERE competition='E' AND game_year=2025",
    ),
    call("euroleague", "get_team_ratings_pergame", "get_team_four_factors_pergame", EL_BASE, "pergame broad"),
    call("euroleague", "get_team_ratings_pergame", "get_team_four_factors_pergame", EL_BASE + ",p_last_n_games=>10", "pergame last 10"),
    call("euroleague", "get_team_ratings_pergame", "get_team_four_factors_pergame", EL_BASE + ",p_num_starters_off_min=>5", "pergame own starters"),
    call("euroleague", "get_team_ratings_dynamic", "get_team_four_factors_dynamic", EL_BASE + ",p_max_margin=>5,p_margin_status=>'all',p_max_time_remaining=>300", "standard clutch"),
    call("euroleague", "get_team_ratings_direct", "get_team_four_factors_direct", EL_BASE + ",p_max_margin=>3,p_margin_status=>'all',p_max_time_remaining=>240", "custom clutch"),
)


def fetch(cursor, query: str):
    cursor.execute(query)
    columns = tuple(column.name for column in cursor.description)
    missing = set(("team_id",) + COMMON_COLUMNS) - set(columns)
    if missing:
        raise RuntimeError(f"reader lacks common columns: {sorted(missing)}")
    rows = cursor.fetchall()
    keyed = {row[columns.index("team_id")]: row for row in rows}
    if len(keyed) != len(rows):
        raise RuntimeError("reader returned duplicate team_id keys")
    return columns, keyed


def common(columns, row):
    return tuple(row[columns.index(name)] for name in COMMON_COLUMNS)


def mismatch(cursor, route: Route) -> str | None:
    rating_columns, ratings = fetch(cursor, route.ratings_sql)
    factor_columns, factors = fetch(cursor, route.factors_sql)
    if route.must_return_rows and (not ratings or not factors):
        return f"vacuous rows ratings={len(ratings)} factors={len(factors)}"
    keys = sorted(set(ratings) | set(factors))
    differences = []
    for team_id in keys:
        if team_id not in ratings or team_id not in factors:
            return f"team_id={team_id} missing ratings={team_id not in ratings} factors={team_id not in factors}"
        left = common(rating_columns, ratings[team_id])
        right = common(factor_columns, factors[team_id])
        for column, values in enumerate(zip(left, right)):
            if values[0] != values[1]:
                differences.append(
                    (team_id, COMMON_COLUMNS[column], values[0], values[1])
                )
    if not differences:
        return None
    counts = Counter(item[1] for item in differences)
    first = differences[0]
    summary = ",".join(f"{name}={counts[name]}" for name in COMMON_COLUMNS if counts[name])
    return (
        f"mismatches={len(differences)} columns={summary}; "
        f"first team_id={first[0]} column={first[1]} "
        f"ratings={first[2]!r} factors={first[3]!r}"
    )


def audit(cursor, routes=ROUTES) -> None:
    failures = []
    for route in routes:
        detail = mismatch(cursor, route)
        if detail:
            failures.append(f"{route.league} {route.label}: {detail}")
            print(f"FAIL {route.league:<16} {route.label}: {detail}")
        else:
            print(f"OK   {route.league:<16} {route.label}")
    if failures:
        raise RuntimeError("team reader behavioral drift:\n" + "\n".join(failures))


def catalog_summary(cursor) -> None:
    """Report definition fingerprints/dependencies; never use them as DDL input."""
    objects = (
        ("basketball_test", "team_ppp_ratings_mv", "m"),
        ("basketball_test", "team_four_factors_mv", "m"),
        ("euroleague", "team_ppp_ratings_mv", "m"),
        ("euroleague", "team_four_factors_mv", "m"),
    )
    for schema, name, kind in objects:
        cursor.execute(
            "SELECT md5(pg_get_viewdef(c.oid, true)), "
            "(SELECT count(*) FROM pg_depend d WHERE d.refobjid=c.oid AND d.deptype='n') "
            "FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace "
            "WHERE n.nspname=%s AND c.relname=%s AND c.relkind=%s",
            (schema, name, kind),
        )
        row = cursor.fetchone()
        print(json.dumps({"object": f"{schema}.{name}", "viewdef_md5": row[0],
                          "normal_dependents": row[1]}))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--catalog-summary", action="store_true")
    args = parser.parse_args()
    connection = connect_from_env_file(ENV, direct_port=5432)
    connection.autocommit = False
    cursor = connection.cursor()
    try:
        cursor.execute("SET TRANSACTION READ ONLY")
        cursor.execute("SET LOCAL statement_timeout='60s'")
        if args.catalog_summary:
            catalog_summary(cursor)
        else:
            audit(cursor)
        connection.rollback()
        print("team reader behavioral contracts pass; read-only transaction rolled back")
        return 0
    except Exception:
        connection.rollback()
        raise
    finally:
        cursor.close()
        connection.close()


if __name__ == "__main__":
    raise SystemExit(main())
