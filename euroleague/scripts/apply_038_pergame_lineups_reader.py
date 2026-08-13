#!/usr/bin/env python
"""Apply and verify EuroLeague migration 038 (per-game Lineups reader).

Fixed to migration 038. The migration is purely additive -- one new function --
so this entry point refuses ANY DROP statement, any CASCADE, and any reference
to an Israeli schema. It touches only the `euroleague` schema.

The gate is output parity, not timing: for every non-clutch preset below, the
new `fetch_lineups_pergame` must return byte-identical ordered rows to
`fetch_lineups_dynamic`, the reader it will replace on that path. All 33
columns are compared, not a summary. Timing is reported for information only.

Unit size is part of the preset because it selects a different code path in
both readers: size 5 bypasses `sub_lineups` in the new reader, sizes 2-4 use
the mapping.
"""

from __future__ import annotations

import re
import sys
import time
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "src"))

from euroleague_possessions.postgres_backend import (  # noqa: E402
    _split_sql_statements,
    connect_from_env_file,
    inspect_target,
)

DDL_PATH = REPO / "sql" / "038_pergame_lineups_reader.sql"
ENV_PATH = REPO.parent / "etl" / ".Renviron"

NEW_FUNCTIONS = {"fetch_lineups_pergame"}

OLD_FN = "fetch_lineups_dynamic"
NEW_FN = "fetch_lineups_pergame"

# Non-clutch presets the app can actually produce, at the default unit size.
SIZE5_PRESETS = [
    ("full season, no filters", ""),
    ("phase = RS", "p_phase_csv => 'RS'"),
    ("last 10 games", "p_last_n_games => 10"),
    ("rounds 5-15", "p_min_gn => 5, p_max_gn => 15"),
    ("home only", "p_home_away => 'home'"),
    ("wins only", "p_outcome => 'win'"),
    ("one team (Real Madrid)", "p_team_ids_csv => '24'"),
    ("two opponents", "p_opp_ids_csv => '19,21'"),
    ("top-5 opponents by net", "p_opp_rank_side => 'top', p_opp_rank_n => 5, p_opp_rank_metric => 'net'"),
    ("own starters >= 4", "p_num_starters_off_min => 4"),
    ("opp starters <= 3", "p_num_starters_def_max => 3"),
    ("own 5 and opp 5", "p_num_starters_off_min => 5, p_num_starters_def_min => 5"),
    ("date window", "p_start_date => DATE '2025-10-01', p_end_date => DATE '2026-01-01'"),
    ("min_poss = 100", "p_min_poss => 100"),
    ("phase + last N + home", "p_phase_csv => 'RS', p_last_n_games => 8, p_home_away => 'home'"),
]

# The mapping path, plus the player-membership filters that read player_ids
# rather than the hash. Players are resolved from the season at run time so the
# preset stays valid as data changes.
SMALL_UNIT_PRESETS = [
    ("no filters", ""),
    ("one team (Real Madrid)", "p_team_ids_csv => '24'"),
    ("last 10 games", "p_last_n_games => 10"),
    ("own starters >= 4", "p_num_starters_off_min => 4"),
]


def validate_ddl(ddl: str) -> list[str]:
    upper = ddl.upper()
    if "EUROLEAGUE SHADOW SCHEMA" not in upper:
        raise ValueError("EuroLeague migration safety marker is missing")
    if re.search(r"\bBASKETBALL(?:_TEST)?\s*\.", upper):
        raise ValueError("migration references a forbidden Israeli schema")
    if re.search(r"\bCASCADE\b", upper):
        raise ValueError("migration contains CASCADE")

    statements = _split_sql_statements(ddl)
    for statement in statements:
        normalized = " ".join(statement.strip().upper().split())
        if normalized.startswith("DROP "):
            raise ValueError(
                f"migration 038 is additive and must not DROP: {statement[:80]!r}"
            )
    return statements


def euroleague_function_names(connection: object) -> set[str]:
    cursor = connection.cursor()  # type: ignore[attr-defined]
    cursor.execute(
        "SELECT p.proname FROM pg_proc p "
        "JOIN pg_namespace n ON n.oid = p.pronamespace "
        "WHERE n.nspname = 'euroleague'"
    )
    names = {str(row[0]) for row in cursor.fetchall()}
    cursor.close()
    return names


def run(cursor, fn: str, preset_args: str, unit_size: int):
    args = f"p_competition => 'E', p_game_year => 2025, p_unit_size => {unit_size}"
    if preset_args:
        args = f"{args}, {preset_args}"
    sql = (
        f"SELECT * FROM euroleague.{fn}({args}) t "
        "ORDER BY t.team_id, t.unit_key"
    )
    started = time.perf_counter()
    cursor.execute(sql)
    rows = [tuple(row) for row in cursor.fetchall()]
    return rows, time.perf_counter() - started


def compare_preset(cursor, label: str, preset_args: str, unit_size: int) -> bool:
    old_rows, old_s = run(cursor, OLD_FN, preset_args, unit_size)
    new_rows, new_s = run(cursor, NEW_FN, preset_args, unit_size)

    tag = f"size {unit_size}  {label}"
    if old_rows == new_rows:
        speedup = (old_s / new_s) if new_s > 0 else float("inf")
        print(
            f"  OK   {tag:<44} {len(old_rows):>5} rows  "
            f"dynamic {old_s:6.2f}s -> pergame {new_s:5.2f}s  ({speedup:.0f}x)"
        )
        return True

    print(f"  FAIL {tag:<44} {len(old_rows)} rows dynamic vs {len(new_rows)} pergame")
    old_keys = {(r[0], r[1]) for r in old_rows}
    new_keys = {(r[0], r[1]) for r in new_rows}
    if old_keys - new_keys:
        print(f"         {len(old_keys - new_keys)} unit(s) only in dynamic, e.g. "
              f"{sorted(old_keys - new_keys)[:3]}")
    if new_keys - old_keys:
        print(f"         {len(new_keys - old_keys)} unit(s) only in pergame, e.g. "
              f"{sorted(new_keys - old_keys)[:3]}")
    old_by_key = {(r[0], r[1]): r for r in old_rows}
    for key in sorted(old_keys & new_keys):
        new_by_key = {(r[0], r[1]): r for r in new_rows}
        if old_by_key[key] != new_by_key[key]:
            print(f"         first differing unit {key}:")
            print(f"           dynamic: {old_by_key[key]}")
            print(f"           pergame: {new_by_key[key]}")
            break
    return False


def player_presets(cursor) -> list[tuple[str, str]]:
    """Two players who actually share a season lineup, and one who does not."""
    cursor.execute(
        """
        SELECT player_ids[1], player_ids[2], player_ids[3]
        FROM euroleague.lineup_totals_by_game
        WHERE competition = 'E' AND game_year = 2025
        ORDER BY possessions DESC
        LIMIT 1
        """
    )
    a, b, c = cursor.fetchone()
    return [
        (f"players on {a},{b}", f"p_players_on_csv => '{a},{b}'"),
        (f"player on {a} off {c}", f"p_players_on_csv => '{a}', p_players_off_csv => '{c}'"),
    ]


def compare(cursor) -> int:
    failures = 0
    total = 0

    print(f"\n=== {OLD_FN}  vs  {NEW_FN} ===")
    for label, preset_args in SIZE5_PRESETS:
        total += 1
        if not compare_preset(cursor, label, preset_args, 5):
            failures += 1

    for label, preset_args in player_presets(cursor):
        total += 1
        if not compare_preset(cursor, label, preset_args, 5):
            failures += 1

    for unit_size in (2, 3, 4):
        for label, preset_args in SMALL_UNIT_PRESETS:
            total += 1
            if not compare_preset(cursor, label, preset_args, unit_size):
                failures += 1

    print(f"\n{total - failures}/{total} presets identical")
    return failures


def main() -> int:
    ddl = DDL_PATH.read_text(encoding="utf-8")
    statements = validate_ddl(ddl)

    connection = connect_from_env_file(ENV_PATH, direct_port=5432)
    cursor = connection.cursor()
    try:
        target = inspect_target(connection)
        if target["euroleague_schema"] != "euroleague":
            raise RuntimeError(f"EuroLeague schema is unavailable: {target}")
        print(
            "target "
            f"database={target['database']} user={target['user']} "
            f"server={target['server_address']}:{target['server_port']}"
        )

        if NEW_FUNCTIONS <= euroleague_function_names(connection):
            print("migration 038 already applied; re-running parity checks only")
        else:
            print(f"applying {DDL_PATH.name}: {len(statements)} reviewed statements")
            for index, statement in enumerate(statements, start=1):
                cursor.execute(statement)
                last_line = statement.strip().splitlines()[-1][:72]
                print(f"  [{index}/{len(statements)}] {last_line}")

            missing = NEW_FUNCTIONS - euroleague_function_names(connection)
            if missing:
                raise RuntimeError(f"migration committed but functions missing: {missing}")

        print("\ncomparing the per-game lineups reader to the live reader ...")
        failures = compare(cursor)

        if failures:
            raise RuntimeError(
                f"{failures} parity comparison(s) failed -- do NOT change app routing"
            )
        print("migration 038 verified")
        return 0
    except Exception:
        try:
            cursor.execute("ROLLBACK")
        except Exception:
            pass
        raise
    finally:
        cursor.close()
        connection.close()


if __name__ == "__main__":
    raise SystemExit(main())
