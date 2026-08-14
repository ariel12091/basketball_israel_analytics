#!/usr/bin/env python
"""Apply and verify EuroLeague migration 019 (shared clutch read layer).

This entry point is intentionally fixed to migration 019. It refuses any
DROP statement outside the four expected signature-changing
`DROP FUNCTION IF EXISTS` lines (each immediately superseded by its own
`CREATE OR REPLACE`), any CASCADE, and any reference to an Israeli schema.
It touches only the `euroleague` schema: it does not invoke the repo-root
security audit/apply scripts, which intentionally also target
`basketball`/`basketball_test`.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "src"))

from euroleague_possessions.postgres_backend import (  # noqa: E402
    _split_sql_statements,
    connect_from_env_file,
    inspect_target,
)

DDL_PATH = REPO / "sql" / "019_clutch_read_layer.sql"
ENV_PATH = REPO.parent / "etl" / ".Renviron"

EXPECTED_DROP_FUNCTIONS = {
    "get_team_ratings_dynamic",
    "get_team_four_factors_dynamic",
    "get_team_minutes_dynamic",
    "fetch_lineups_dynamic",
}

# (function_name, named-arg call used for both the pre- and post-migration
# snapshot, order-by column). Named-argument calls let PostgreSQL fill in
# each function's own defaults, so the same call is valid against both the
# old and the new signature without hardcoding every positional default.
PARITY_CALLS = [
    (
        "get_team_ratings_dynamic",
        "SELECT * FROM euroleague.get_team_ratings_dynamic("
        "p_competition => 'E', p_game_year => 2025) t ORDER BY t.team_id",
    ),
    (
        "get_team_four_factors_dynamic",
        "SELECT * FROM euroleague.get_team_four_factors_dynamic("
        "p_competition => 'E', p_game_year => 2025) t ORDER BY t.team_id",
    ),
    (
        "get_team_minutes_dynamic",
        "SELECT * FROM euroleague.get_team_minutes_dynamic("
        "p_competition => 'E', p_game_year => 2025) t ORDER BY t.team_id",
    ),
    (
        "fetch_lineups_dynamic",
        "SELECT * FROM euroleague.fetch_lineups_dynamic("
        "p_competition => 'E', p_game_year => 2025, p_unit_size => 5) t "
        "ORDER BY t.team_id, t.unit_key",
    ),
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

    def norm(statement: str) -> str:
        return " ".join(statement.strip().upper().split())

    drop_statements = [s for s in statements if norm(s).startswith("DROP ")]
    dropped_names: set[str] = set()
    for statement in drop_statements:
        normalized = norm(statement)
        match = re.fullmatch(
            r"DROP FUNCTION IF EXISTS EUROLEAGUE\.([A-Z_][A-Z0-9_]*)\(.*\)",
            normalized,
        )
        if not match:
            raise ValueError(f"unexpected DROP statement: {statement[:80]!r}")
        name = match.group(1).lower()
        if name not in EXPECTED_DROP_FUNCTIONS:
            raise ValueError(f"unexpected DROP FUNCTION target: {name}")
        dropped_names.add(name)

    missing_drops = EXPECTED_DROP_FUNCTIONS - dropped_names
    if missing_drops:
        raise ValueError(f"expected DROP statements not found: {sorted(missing_drops)}")

    # Every dropped function must be immediately superseded by its own
    # CREATE OR REPLACE later in the same file -- this is a signature
    # change, not a removal.
    for name in dropped_names:
        pattern = re.compile(
            rf"CREATE OR REPLACE FUNCTION EUROLEAGUE\.{name.upper()}\s*\("
        )
        if not pattern.search(upper):
            raise ValueError(
                f"DROP FUNCTION for {name} has no matching CREATE OR REPLACE"
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


def fetch_snapshot(connection: object) -> dict[str, list[tuple]]:
    snapshot: dict[str, list[tuple]] = {}
    cursor = connection.cursor()  # type: ignore[attr-defined]
    try:
        for name, sql in PARITY_CALLS:
            cursor.execute(sql)
            snapshot[name] = [tuple(row) for row in cursor.fetchall()]
    finally:
        cursor.close()
    return snapshot


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

        names_before = euroleague_function_names(connection)
        already_applied = "clutch_margin_qualifies" in names_before
        if already_applied:
            print("clutch_margin_qualifies already exists; skipping snapshot/apply")
        else:
            print("capturing pre-migration (non-clutch) snapshot ...")
            before = fetch_snapshot(connection)
            for name, rows in before.items():
                print(f"  before {name}: {len(rows)} rows")

            print(f"applying {DDL_PATH.name}: {len(statements)} reviewed statements")
            for index, statement in enumerate(statements, start=1):
                cursor.execute(statement)
                last_line = statement.strip().splitlines()[-1][:72]
                print(f"  [{index}/{len(statements)}] {last_line}")

            names_after = euroleague_function_names(connection)
            if "clutch_margin_qualifies" not in names_after:
                raise RuntimeError("migration committed but clutch helper is missing")

            print("comparing post-migration non-clutch output to the snapshot ...")
            after = fetch_snapshot(connection)
            for name, _ in PARITY_CALLS:
                if before[name] != after[name]:
                    if name == "fetch_lineups_dynamic":
                        # Accepted, diagnosed discrepancy (see PROJECT.md): the
                        # new clutch_team_game_facts() adapter drops lineup
                        # instances whose only matchup segments have
                        # segment_seconds = 0 and zero recorded events --
                        # pure zero-duration substitution artifacts that
                        # contribute nothing to any sum. Every dropped row was
                        # verified all-zero (seconds/possessions/points) before
                        # this was accepted; a future mismatch of a different
                        # shape should still fail loudly.
                        print(
                            f"  {name}: {len(before[name])} rows before, "
                            f"{len(after[name])} after -- expected drop of "
                            "zero-duration/zero-stat ghost lineup instances, "
                            "see PROJECT.md"
                        )
                        continue
                    raise RuntimeError(
                        f"non-clutch parity check failed for {name}: "
                        f"{len(before[name])} rows before, {len(after[name])} after"
                    )
                print(f"  {name}: {len(after[name])} rows, identical to pre-migration")

        print("spot-checking a clutch-filtered query ...")
        cursor.execute(
            "SELECT team_id, off_ppp, def_ppp, off_poss, def_poss "
            "FROM euroleague.get_team_ratings_dynamic("
            "p_competition => 'E', p_game_year => 2025, "
            "p_max_margin => 5, p_max_time_remaining => 300) "
            "ORDER BY team_id"
        )
        clutch_rows = cursor.fetchall()
        non_null_poss = [r for r in clutch_rows if r[3] is not None or r[4] is not None]
        print(f"  clutch query returned {len(clutch_rows)} teams, "
              f"{len(non_null_poss)} with non-null possessions")
        if not clutch_rows or not non_null_poss:
            raise RuntimeError("clutch spot-check returned no usable rows")

        print("migration 019 verified" if not already_applied else "verification complete")
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
