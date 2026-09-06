#!/usr/bin/env python
"""Apply and verify EuroLeague migration 053 (stint ribbon read layer).

Fixed to migration 053. Purely additive -- two views, `CREATE OR REPLACE VIEW`
only, no DROP -- so this refuses any DROP statement, any CASCADE, and any
reference to an Israeli schema, same as the 037/038 applicators.

This is a REPRODUCIBILITY migration, not a first deploy: both views already
exist in the live database and are already granted to app_readonly. Running
this against that database is a no-op in effect -- CREATE OR REPLACE VIEW with
an identical definition -- but it is still gated the same way any other
migration is, so a divergence between this file and the live definition is
caught rather than silently reproduced.

ribbon_margin_v's definition here is the CORRECTED one (2026-09-05 final
review): the original read euroleague.actions.points_a/points_b directly,
which the provider leaves NULL until a side has scored, so every early event
in every game carried a NULL score -- an invalid SVG path and a blank curve on
100% of EuroLeague games. It was already hotfixed live using this exact DDL,
ahead of this file being written; see the comment above the view's
CREATE OR REPLACE in the migration.

The gate:
1. Refuse DROP / CASCADE / an Israeli-schema reference in the DDL.
2. Apply (idempotent CREATE OR REPLACE VIEW).
3. Verify app_readonly can SELECT both views.
4. Verify app_readonly is STILL DENIED on euroleague.actions,
   euroleague.matchup_segments_actions, and
   euroleague.action_team_context_actions -- the entire point of this read
   layer is that those three stay closed.
5. Sanity-check the views themselves: zero rows with NULL/empty player_ids in
   ribbon_segments_v, no duplicate (game_id, team_id, segment_id) (the fan-out
   signature the feature's data-quality work guarded against), zero rows with
   a NULL margin in ribbon_margin_v (the regression this migration fixes), and
   a non-trivial row count floor on each so no check can pass vacuously on an
   empty view.
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

DDL_PATH = REPO / "sql" / "053_stint_ribbon_read_layer.sql"
ENV_PATH = REPO.parent / "etl" / ".Renviron"

VIEWS = ("ribbon_segments_v", "ribbon_margin_v")
DENIED_RELATIONS = ("actions", "matchup_segments_actions", "action_team_context_actions")

# Row-count floors so the sanity checks below cannot pass on an empty view.
# ribbon_margin_v's floor reflects the measured 109,184 scoring rows across
# 589 games from euroleague.action_team_context_actions (points > 0).
MIN_ROWS = {
    "ribbon_segments_v": 1000,
    "ribbon_margin_v": 50000,
}


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
                f"migration 053 is additive and must not DROP: {statement[:80]!r}"
            )
    return statements


def euroleague_view_names(connection) -> set[str]:
    cursor = connection.cursor()
    cursor.execute(
        "SELECT c.relname FROM pg_class c "
        "JOIN pg_namespace n ON n.oid = c.relnamespace "
        "WHERE n.nspname = 'euroleague' AND c.relkind = 'v'"
    )
    names = {str(row[0]) for row in cursor.fetchall()}
    cursor.close()
    return names


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

        already = VIEWS[0] in euroleague_view_names(connection) and VIEWS[1] in euroleague_view_names(connection)
        if already:
            print("both views already exist; applying is a no-op re-definition")
        print(f"applying {DDL_PATH.name}: {len(statements)} reviewed statements")
        for index, statement in enumerate(statements, start=1):
            cursor.execute(statement)
            last_line = statement.strip().splitlines()[-1][:72]
            print(f"  [{index}/{len(statements)}] {last_line}")

        missing = {v for v in VIEWS if v not in euroleague_view_names(connection)}
        if missing:
            raise RuntimeError(f"migration committed but views missing: {missing}")

        print("\n-- grants: app_readonly can SELECT both views --")
        for view in VIEWS:
            cursor.execute(
                "SELECT has_table_privilege('app_readonly', %s, 'SELECT')",
                (f"euroleague.{view}",),
            )
            ok = bool(cursor.fetchone()[0])
            print(f"  {view:<24} app_readonly SELECT={ok}")
            if not ok:
                raise RuntimeError(f"{view}: app_readonly is missing SELECT")

        print("\n-- grants: app_readonly still DENIED on the raw evidence tables --")
        for relation in DENIED_RELATIONS:
            cursor.execute(
                "SELECT has_table_privilege('app_readonly', %s, 'SELECT')",
                (f"euroleague.{relation}",),
            )
            denied = not bool(cursor.fetchone()[0])
            print(f"  {relation:<28} app_readonly denied={denied}")
            if not denied:
                raise RuntimeError(
                    f"{relation}: app_readonly can read this directly -- "
                    "the read layer no longer narrows access"
                )

        print("\n-- sanity: ribbon_segments_v --")
        cursor.execute("SELECT count(*) FROM euroleague.ribbon_segments_v")
        total = cursor.fetchone()[0]
        cursor.execute(
            "SELECT count(*) FROM euroleague.ribbon_segments_v "
            "WHERE player_ids IS NULL OR cardinality(player_ids) = 0"
        )
        empty_ids = cursor.fetchone()[0]
        cursor.execute(
            "SELECT count(*) FROM ("
            "  SELECT game_id, team_id, segment_id, count(*) c"
            "  FROM euroleague.ribbon_segments_v"
            "  GROUP BY game_id, team_id, segment_id HAVING count(*) > 1"
            ") dup"
        )
        fanout = cursor.fetchone()[0]
        print(f"  rows={total} empty_ids={empty_ids} fanout_groups={fanout}")
        if total < MIN_ROWS["ribbon_segments_v"]:
            raise RuntimeError(f"ribbon_segments_v row floor not met: {total}")
        if empty_ids:
            raise RuntimeError(f"ribbon_segments_v has {empty_ids} rows with empty player_ids")
        if fanout:
            raise RuntimeError(f"ribbon_segments_v has {fanout} fan-out (game,team,segment) groups")

        print("\n-- sanity: ribbon_margin_v --")
        cursor.execute("SELECT count(*) FROM euroleague.ribbon_margin_v")
        margin_total = cursor.fetchone()[0]
        cursor.execute(
            "SELECT count(*) FROM euroleague.ribbon_margin_v WHERE margin IS NULL"
        )
        null_margin = cursor.fetchone()[0]
        print(f"  rows={margin_total} null_margin={null_margin}")
        if margin_total < MIN_ROWS["ribbon_margin_v"]:
            raise RuntimeError(f"ribbon_margin_v row floor not met: {margin_total}")
        if null_margin:
            raise RuntimeError(
                f"ribbon_margin_v has {null_margin} NULL-margin rows -- "
                "this is the exact regression this migration fixes"
            )

        print("\nmigration 053 verified")
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
