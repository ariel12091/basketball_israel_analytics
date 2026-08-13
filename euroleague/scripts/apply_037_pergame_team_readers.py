#!/usr/bin/env python
"""Apply and verify EuroLeague migration 037 (per-game Team readers).

Fixed to migration 037. The migration is purely additive -- two new functions
-- so this entry point refuses ANY DROP statement, any CASCADE, and any
reference to an Israeli schema. It touches only the `euroleague` schema.

The gate is output parity, not timing: for every non-clutch preset below, the
new `_pergame` reader must return byte-identical ordered rows to the existing
`_direct` reader it will replace on that path. Timing is reported for
information only.
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

DDL_PATH = REPO / "sql" / "037_pergame_team_readers.sql"
ENV_PATH = REPO.parent / "etl" / ".Renviron"

NEW_FUNCTIONS = {"get_team_ratings_pergame", "get_team_four_factors_pergame"}

# Non-clutch presets the app can actually produce. Each must agree exactly
# between the `_direct` reader (action scan) and the `_pergame` reader.
# Named arguments let each function fill in its own defaults, so the same
# preset is valid against both the 23-param and the 19-param signature.
PRESETS = [
    ("full season, no filters", ""),
    ("phase = RS", "p_phase_csv => 'RS'"),
    ("phase = PO", "p_phase_csv => 'PO'"),
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
    ("phase + last N + home", "p_phase_csv => 'RS', p_last_n_games => 8, p_home_away => 'home'"),
]

PAIRS = [
    ("get_team_ratings_direct", "get_team_ratings_pergame", "t.team_id"),
    ("get_team_four_factors_direct", "get_team_four_factors_pergame", "t.team_id"),
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
                f"migration 037 is additive and must not DROP: {statement[:80]!r}"
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


def run(cursor, fn: str, preset_args: str, order_by: str):
    args = "p_competition => 'E', p_game_year => 2025"
    if preset_args:
        args = f"{args}, {preset_args}"
    sql = f"SELECT * FROM euroleague.{fn}({args}) t ORDER BY {order_by}"
    started = time.perf_counter()
    cursor.execute(sql)
    rows = [tuple(row) for row in cursor.fetchall()]
    return rows, time.perf_counter() - started


def compare(cursor) -> int:
    failures = 0
    for direct_fn, pergame_fn, order_by in PAIRS:
        print(f"\n=== {direct_fn}  vs  {pergame_fn} ===")
        for label, preset_args in PRESETS:
            direct_rows, direct_s = run(cursor, direct_fn, preset_args, order_by)
            pergame_rows, pergame_s = run(cursor, pergame_fn, preset_args, order_by)

            if direct_rows == pergame_rows:
                speedup = (direct_s / pergame_s) if pergame_s > 0 else float("inf")
                print(
                    f"  OK   {label:<32} {len(direct_rows):>3} rows  "
                    f"direct {direct_s:6.2f}s -> pergame {pergame_s:5.2f}s  ({speedup:.1f}x)"
                )
                continue

            failures += 1
            print(
                f"  FAIL {label:<32} "
                f"{len(direct_rows)} rows direct vs {len(pergame_rows)} pergame"
            )
            for index, (a, b) in enumerate(zip(direct_rows, pergame_rows)):
                if a != b:
                    print(f"         first differing row {index}:")
                    print(f"           direct : {a}")
                    print(f"           pergame: {b}")
                    break
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
            print("migration 037 already applied; re-running parity checks only")
        else:
            print(f"applying {DDL_PATH.name}: {len(statements)} reviewed statements")
            for index, statement in enumerate(statements, start=1):
                cursor.execute(statement)
                last_line = statement.strip().splitlines()[-1][:72]
                print(f"  [{index}/{len(statements)}] {last_line}")

            missing = NEW_FUNCTIONS - euroleague_function_names(connection)
            if missing:
                raise RuntimeError(f"migration committed but functions missing: {missing}")

        print("\ncomparing per-game readers to the action-fact readers ...")
        failures = compare(cursor)

        if failures:
            raise RuntimeError(
                f"{failures} parity comparison(s) failed -- do NOT change app routing"
            )
        print(f"\nall {len(PRESETS) * len(PAIRS)} parity comparisons identical")
        print("migration 037 verified")
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
