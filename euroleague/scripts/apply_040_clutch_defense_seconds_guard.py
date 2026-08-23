#!/usr/bin/env python
"""Apply EuroLeague migration 040 and repair the games it unblocks.

Fixed to migration 040. The migration is a single CREATE OR REPLACE of
`clutch_team_game_facts` with an unchanged signature and return type, so this
entry point refuses ANY DROP, any CASCADE, and any reference to an Israeli
schema. It touches only the `euroleague` schema.

Three phases:

  1. apply   -- replace the function so defense rows carry NULL seconds again.
  2. verify  -- assert the function now satisfies the CHECK that
                default_clutch_lineup_totals_by_game enforces, and that the
                offense rows are numerically unchanged.
  3. repair  -- re-run refresh_derived_for_games() for every published game
                that is missing derived facts, ONE GAME PER TRANSACTION so a
                game-local defect cannot roll back its neighbours (the same
                contract batch_pipeline uses).

Pass --dry-run to do phase 1 and 2 against a rolled-back transaction and skip
phase 3 entirely.
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "src"))

from euroleague_possessions.postgres_backend import (  # noqa: E402
    _split_sql_statements,
    connect_from_env_file,
    inspect_target,
    refresh_derived_for_games,
)

DDL_PATH = REPO / "sql" / "040_clutch_defense_seconds_guard.sql"
ENV_PATH = REPO.parent / "etl" / ".Renviron"


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
            raise ValueError(f"migration 040 must not DROP: {statement[:80]!r}")
    # The DDL carries its own BEGIN/COMMIT. Executing those verbatim would end
    # the transaction mid-script, which silently defeats --dry-run: the
    # ROLLBACK afterwards has no open transaction to undo. Strip them and let
    # this script own transaction control for both modes.
    def is_txn_control(statement: str) -> bool:
        # Leading `--` comments are attached to the statement that follows, so
        # compare only the executable text.
        body = "\n".join(
            line for line in statement.splitlines()
            if not line.strip().startswith("--")
        )
        return " ".join(body.upper().split()).rstrip(";") in ("BEGIN", "COMMIT")

    return [statement for statement in statements if not is_txn_control(statement)]


def sample_game_ids(cursor, limit: int = 25) -> list[int]:
    """Published games that DO have derived facts -- safe parity probes."""
    cursor.execute(
        "SELECT s.game_id FROM euroleague.schedule s "
        "WHERE EXISTS (SELECT 1 FROM euroleague.matchup_segments_actions m "
        "              WHERE m.game_id = s.game_id) "
        "ORDER BY s.game_id LIMIT %s",
        (limit,),
    )
    return [int(row[0]) for row in cursor.fetchall()]


def probe(cursor, game_ids: list[int]) -> list[tuple]:
    cursor.execute(
        "SELECT type_lineup, count(*), count(seconds), "
        "       coalesce(sum(seconds), 0)::numeric, coalesce(sum(points), 0) "
        "FROM euroleague.clutch_team_game_facts(%s::bigint[], 5, 'all', 300, false) "
        "GROUP BY type_lineup ORDER BY type_lineup",
        (game_ids,),
    )
    return [tuple(row) for row in cursor.fetchall()]


def games_missing_derived(cursor) -> list[int]:
    cursor.execute(
        "SELECT s.game_id FROM euroleague.schedule s "
        "WHERE NOT EXISTS (SELECT 1 FROM euroleague.team_four_factors_by_game t "
        "                  WHERE t.game_id = s.game_id) "
        "ORDER BY s.game_id"
    )
    return [int(row[0]) for row in cursor.fetchall()]


def coverage(cursor) -> dict:
    out = {}
    for label, relation in (
        ("schedule", "euroleague.schedule"),
        ("actions", "euroleague.actions"),
        ("team_four_factors_by_game", "euroleague.team_four_factors_by_game"),
        ("player_four_factors_by_game", "euroleague.player_four_factors_by_game"),
        ("lineup_totals_by_game", "euroleague.lineup_totals_by_game"),
    ):
        cursor.execute("SELECT count(DISTINCT game_id) FROM " + relation)
        out[label] = int(cursor.fetchone()[0])
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true",
                    help="apply + verify in a rolled-back transaction; no repair")
    args = ap.parse_args()

    ddl = DDL_PATH.read_text(encoding="utf-8")
    statements = validate_ddl(ddl)

    connection = connect_from_env_file(ENV_PATH, direct_port=5432)
    cursor = connection.cursor()
    try:
        target = inspect_target(connection)
        if target["euroleague_schema"] != "euroleague":
            raise RuntimeError("EuroLeague schema is unavailable: " + str(target))
        print("target database={} user={} server={}:{}".format(
            target["database"], target["user"],
            target["server_address"], target["server_port"]))
        print("mode  : " + ("DRY RUN (rolled back)" if args.dry_run else "EXECUTE"))

        # Own the transaction explicitly; the DDL's own BEGIN/COMMIT was
        # stripped by validate_ddl() so --dry-run can genuinely roll back.
        cursor.execute("BEGIN")
        cursor.execute("SET LOCAL search_path TO euroleague, public")

        probes = sample_game_ids(cursor)
        print("\nparity probe games: {}".format(len(probes)))

        print("\n--- BEFORE ---")
        before = probe(cursor, probes)
        for row in before:
            print("  {:<8} rows={:<5} with_seconds={:<5} sum_seconds={} sum_points={}".format(*row))

        print("\napplying {}: {} reviewed statements".format(DDL_PATH.name, len(statements)))
        for index, statement in enumerate(statements, start=1):
            cursor.execute(statement)
            print("  [{}/{}] {}".format(index, len(statements),
                                        statement.strip().splitlines()[-1][:72]))

        print("\n--- AFTER ---")
        after = probe(cursor, probes)
        for row in after:
            print("  {:<8} rows={:<5} with_seconds={:<5} sum_seconds={} sum_points={}".format(*row))

        # Gate 1: the CHECK contract now holds for both perspectives.
        by_side = {row[0]: row for row in after}
        before_by_side = {row[0]: row for row in before}
        failures = []
        off = by_side.get("offense")
        dfn = by_side.get("defense")
        if off is None or dfn is None:
            failures.append("probe returned only one perspective")
        else:
            if off[2] != off[1]:
                failures.append("offense rows with NULL seconds: {}".format(off[1] - off[2]))
            if dfn[2] != 0:
                failures.append("defense rows with non-NULL seconds: {}".format(dfn[2]))

        # Gate 2: nothing but the seconds column moved. Row counts and the
        # event aggregates must match the pre-migration output exactly.
        for side in ("offense", "defense"):
            b = before_by_side.get(side)
            a = by_side.get(side)
            if b and a:
                if b[1] != a[1]:
                    failures.append("{} row count changed {} -> {}".format(side, b[1], a[1]))
                if b[4] != a[4]:
                    failures.append("{} sum_points changed {} -> {}".format(side, b[4], a[4]))
        if off and before_by_side.get("offense") and off[3] != before_by_side["offense"][3]:
            failures.append("offense sum_seconds changed -- floor time must be untouched")

        if failures:
            for f in failures:
                print("  FAIL " + f)
            raise RuntimeError("{} verification gate(s) failed".format(len(failures)))
        print("\n  OK  defense seconds NULL, offense seconds unchanged, counts identical")

        if args.dry_run:
            cursor.execute("ROLLBACK")
            print("\nDRY RUN: rolled back. Nothing changed. Re-run without --dry-run.")
            return 0

        # Commit the function replacement before the repair phase.
        # refresh_derived_for_games() opens its OWN transaction per game;
        # leaving this one open would nest them and make a single game-local
        # failure ambiguous about what it rolled back.
        cursor.execute("COMMIT")
        print("\nfunction replacement committed")

        missing = games_missing_derived(cursor)
        print("\n--- REPAIR: {} game(s) missing derived facts ---".format(len(missing)))
        before_cov = coverage(cursor)

        repaired = []
        still_failing = []
        for game_id in missing:
            try:
                refresh_derived_for_games(connection, [game_id])
                repaired.append(game_id)
            except Exception as exc:
                still_failing.append((game_id, "{}: {}".format(type(exc).__name__, exc)))
        print("  repaired      : {}".format(len(repaired)))
        print("  still failing : {}".format(len(still_failing)))
        for game_id, err in still_failing[:10]:
            print("    game {}: {}".format(game_id, err.splitlines()[0][:150]))

        print("\n--- COVERAGE ---")
        after_cov = coverage(cursor)
        for key in before_cov:
            print("  {:<30} {:>4} -> {:>4}".format(key, before_cov[key], after_cov[key]))

        print("\nrefreshing app materialized views ...")
        cursor.execute("BEGIN")
        cursor.execute("SELECT euroleague.refresh_app_materialized_views()")
        cursor.execute("COMMIT")
        print("migration 040 applied and derived facts repaired")
        return 1 if still_failing else 0
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
