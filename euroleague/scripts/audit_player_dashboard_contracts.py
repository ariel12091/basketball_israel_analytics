#!/usr/bin/env python
"""Read-only behavioral contract audit for both player dashboard readers."""
from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from euroleague_possessions.postgres_backend import connect_from_env_file  # noqa: E402


ENV = ROOT.parent / "etl" / ".Renviron"
RATING_COLUMNS = ("Net RTG Diff", "Off ON Diff", "Def ON Diff", "minutes")


@dataclass(frozen=True)
class LeagueContract:
    schema: str
    factor_base: str
    onoff_base: str
    presets: tuple[tuple[str, str], ...]


EUROLEAGUE = LeagueContract(
    schema="euroleague",
    factor_base="p_competition=>'E',p_game_year=>2025,p_start_date=>DATE '2025-09-01',p_end_date=>DATE '2026-07-01'",
    onoff_base="p_competition=>'E',p_game_year=>2025,p_start_date=>DATE '2025-09-01',p_end_date=>DATE '2026-07-01',p_min_net=>-999,p_min_all=>0,p_min_on=>0",
    presets=(
        ("broad", ""),
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
    ),
)

ISRAELI = LeagueContract(
    schema="basketball_test",
    factor_base="p_game_year=>2026,p_start_date=>DATE '2025-10-01',p_end_date=>DATE '2026-07-01'",
    onoff_base="p_game_year=>'2026',p_start_date=>DATE '2025-10-01',p_end_date=>DATE '2026-07-01',p_team_ids=>NULL,p_min_all=>0,p_min_on=>0,p_min_net=>-1000000000",
    presets=(
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
    ),
)


def with_extra(base: str, extra: str) -> str:
    return base + (("," + extra) if extra else "")


def fetch(cursor, schema: str, function: str, arguments: str):
    cursor.execute(f"SELECT * FROM {schema}.{function}({arguments})")
    columns = tuple(column.name for column in cursor.description)
    return columns, [tuple(row) for row in cursor.fetchall()]


def legacy_result(cursor, contract: LeagueContract, extra: str):
    factor_columns, factor_rows = fetch(
        cursor, contract.schema, "four_factors_compute", with_extra(contract.factor_base, extra)
    )
    onoff_columns, onoff_rows = fetch(
        cursor, contract.schema, "onoff_compute", with_extra(contract.onoff_base, extra)
    )
    onoff_by_key = {
        (row[onoff_columns.index("player_id")], row[onoff_columns.index("team_id")]): row
        for row in onoff_rows
    }
    merged = []
    for row in factor_rows:
        key = (row[factor_columns.index("player_id")], row[factor_columns.index("team_id")])
        rating = onoff_by_key.get(key)
        additions = (
            (None,) * len(RATING_COLUMNS)
            if rating is None
            else tuple(rating[onoff_columns.index(name)] for name in RATING_COLUMNS)
        )
        merged.append(row + additions)
    return factor_columns + RATING_COLUMNS, merged


def canonical(columns, rows):
    player = columns.index("player_id")
    team = columns.index("team_id")
    keyed = {(row[player], row[team]): row for row in rows}
    if len(keyed) != len(rows):
        raise RuntimeError("dashboard contract contains duplicate player/team keys")
    return columns, keyed


def mismatch_detail(expected, actual) -> str:
    expected_columns, expected_rows = expected
    actual_columns, actual_rows = actual
    if expected_columns != actual_columns:
        return f"columns expected={expected_columns!r} actual={actual_columns!r}"
    player = expected_columns.index("player_id")
    team = expected_columns.index("team_id")
    expected_by_key = {(row[player], row[team]): row for row in expected_rows}
    actual_by_key = {(row[player], row[team]): row for row in actual_rows}
    keys = sorted(set(expected_by_key) | set(actual_by_key))
    key = next(key for key in keys if expected_by_key.get(key) != actual_by_key.get(key))
    left = expected_by_key.get(key)
    right = actual_by_key.get(key)
    if left is None or right is None:
        return f"key={key} expected={left!r} actual={right!r}"
    column = next(index for index, values in enumerate(zip(left, right)) if values[0] != values[1])
    return (
        f"key={key} column={expected_columns[column]!r} "
        f"expected={left[column]!r} actual={right[column]!r}"
    )


def audit(cursor, contract: LeagueContract) -> None:
    print(f"-- {contract.schema} --")
    for label, extra in contract.presets:
        expected = legacy_result(cursor, contract, extra)
        actual = fetch(
            cursor,
            contract.schema,
            "four_factors_dashboard_compute",
            with_extra(contract.factor_base, extra),
        )
        if canonical(*expected) != canonical(*actual):
            raise RuntimeError(
                f"{contract.schema} {label}: dashboard contract mismatch; "
                + mismatch_detail(expected, actual)
            )
        if label != "empty" and not actual[1]:
            raise RuntimeError(f"{contract.schema} {label}: vacuous zero-row preset")
        print(f"OK {label:<18} rows={len(actual[1])}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--league", choices=("both", "euroleague", "israeli"), default="both")
    args = parser.parse_args()
    selected = {
        "both": (EUROLEAGUE, ISRAELI),
        "euroleague": (EUROLEAGUE,),
        "israeli": (ISRAELI,),
    }[args.league]

    connection = connect_from_env_file(ENV, direct_port=5432)
    connection.autocommit = False
    cursor = connection.cursor()
    try:
        cursor.execute("SET TRANSACTION READ ONLY")
        cursor.execute("SET LOCAL statement_timeout='60s'")
        for contract in selected:
            audit(cursor, contract)
        connection.rollback()
        print("dashboard behavioral contracts pass; read-only transaction rolled back")
        return 0
    except Exception:
        connection.rollback()
        raise
    finally:
        cursor.close()
        connection.close()


if __name__ == "__main__":
    raise SystemExit(main())
