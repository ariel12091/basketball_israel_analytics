#!/usr/bin/env python
"""Switch EuroLeague readers to the free-throw credit columns, one object at a
time (docs/superpowers/plans/2026-11-13-euroleague-ft-foul-credit.md, the
reader switch procedure).

  apply_062_readers.py --object NAME          dry run: edit, refresh, gates, ROLLBACK
  CONFIRM_EL_062_READER=NAME apply_062_readers.py --object NAME   commit

For each object: read its LIVE definition, apply exact edits (each must match
once), write sql/062_<object>.sql and the untouched sql/062_<object>_before.sql
(the rollback), CREATE OR REPLACE it, refresh what it feeds, then run the named
verify_062_ft_credit.py checks inside the same transaction. Any FAIL rolls back.
"""

from __future__ import annotations

import argparse
import importlib.util
import os
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "src"))

from euroleague_possessions.postgres_backend import connect_from_env_file, inspect_target  # noqa: E402

ENV_PATH = REPO.parent / "etl" / ".Renviron"
BATCH = 100

_spec = importlib.util.spec_from_file_location("verify_062", REPO / "scripts" / "verify_062_ft_credit.py")
verify = importlib.util.module_from_spec(_spec)
sys.modules["verify_062"] = verify
_spec.loader.exec_module(verify)


@dataclass(frozen=True)
class ReaderSwitch:
    signature: str                 # regprocedure text of the function to edit
    edits: list[tuple[str, str]]   # exact (old, new) pairs on the live definition
    refresh: str | None = None     # per-batch call, %s = bigint[] of game ids
    pre: list[str] = field(default_factory=list)     # statements before the function is replaced
    after: list[str] = field(default_factory=list)   # statements once the refresh is done
    checks: list[str] = field(default_factory=list)  # verify_062 spec names
    # (relation, key columns, value columns): counted before and after, to show
    # that values moved, which the totals-unchanged gates cannot.
    moved: tuple[str, list[str], list[str]] | None = None
    expect_moved: bool = True      # False: the listed values must NOT change
    asserts: list[tuple[str, str]] = field(default_factory=list)  # (label, SQL returning a count that must be 0)


OBJECTS: dict[str, ReaderSwitch] = {
    # Events (the counts CTE) group on the credited lineup; segment time and
    # event spans stay physical. Output names are kept, so the join to the
    # segment rows is unchanged; a credited row always matches one, because its
    # foul's own row sat in that lineup and starters combination.
    "lineup_totals_by_game": ReaderSwitch(
        signature="euroleague.refresh_lineup_totals_by_game(bigint[])",
        edits=[
            ("    SELECT atc.game_id, atc.team_id, atc.own_lineup, atc.type_lineup,\n"
             "      atc.opp_starters,\n      sum(atc.possession_flag)",
             "    SELECT atc.game_id, atc.team_id, atc.credit_own_lineup AS own_lineup, atc.type_lineup,\n"
             "      atc.credit_opp_starters AS opp_starters,\n      sum(atc.possession_flag)"),
            ("    GROUP BY atc.game_id, atc.team_id, atc.own_lineup,\n      atc.type_lineup, atc.opp_starters",
             "    GROUP BY atc.game_id, atc.team_id, atc.credit_own_lineup,\n"
             "      atc.type_lineup, atc.credit_opp_starters"),
        ],
        refresh="SELECT euroleague.refresh_lineup_totals_by_game(%s::bigint[])",
        after=["REFRESH MATERIALIZED VIEW euroleague.sub_lineups_stats_mv"],
        checks=["lineup_totals_by_game", "sub_lineups_stats_mv", "fetch_lineups_pergame_season"],
        moved=("euroleague.lineup_totals_by_game",
               ["game_id", "team_id", "lineup_key", "type_lineup", "opp_starters"],
               ["points", "possessions", "ts_possessions"]),
    ),
    # The per-action player fact carries the credited lineup and starters next
    # to the physical ones (the Israeli player_stats_actions_by_game gained
    # credit_lineup_hash the same way). Nothing reads them until the clutch and
    # direct lineup readers switch; every value it already had is unchanged.
    "player_stats_actions_by_game": ReaderSwitch(
        signature="euroleague.refresh_player_stats_actions_for_games(bigint[])",
        pre=["ALTER TABLE euroleague.player_stats_actions_by_game\n"
             "  ADD COLUMN credit_own_lineup text[],\n"
             "  ADD COLUMN credit_own_starters smallint,\n"
             "  ADD COLUMN credit_opp_starters smallint"],
        edits=[
            ("  own_starters,opp_starters,fg2_made,fg2_att,orebounds,oreb_opportunities,steals)",
             "  own_starters,opp_starters,fg2_made,fg2_att,orebounds,oreb_opportunities,steals,\n"
             "  credit_own_lineup,credit_own_starters,credit_opp_starters)"),
            ("  atc.orebounds,atc.oreb_opportunities,atc.steals\n FROM euroleague.action_team_context_actions atc",
             "  atc.orebounds,atc.oreb_opportunities,atc.steals,\n"
             "  atc.credit_own_lineup,atc.credit_own_starters,atc.credit_opp_starters\n"
             " FROM euroleague.action_team_context_actions atc"),
        ],
        refresh="SELECT euroleague.refresh_player_stats_actions_for_games(%s::bigint[])",
        after=["ALTER TABLE euroleague.player_stats_actions_by_game\n"
               "  ALTER COLUMN credit_own_lineup SET NOT NULL,\n"
               "  ALTER COLUMN credit_own_starters SET NOT NULL,\n"
               "  ALTER COLUMN credit_opp_starters SET NOT NULL"],
        checks=["player_stats_actions_by_game"],
        moved=("euroleague.player_stats_actions_by_game",
               ["game_id", "team_id", "source_event_order"],
               ["points", "own_lineup", "segment_id", "possession_flag"]),
        expect_moved=False,
        asserts=[("action rows whose credit columns differ from the event fact's", """
            SELECT count(*) FROM euroleague.player_stats_actions_by_game a
            JOIN euroleague.action_team_context_actions f
              USING (game_id, team_id, source_event_order)
            WHERE a.credit_own_lineup IS DISTINCT FROM f.credit_own_lineup
               OR a.credit_own_starters IS DISTINCT FROM f.credit_own_starters
               OR a.credit_opp_starters IS DISTINCT FROM f.credit_opp_starters"""),
                 ("action rows without a fact row", """
            SELECT count(*) FROM euroleague.player_stats_actions_by_game a
            WHERE NOT EXISTS (SELECT 1 FROM euroleague.action_team_context_actions f
              WHERE f.game_id = a.game_id AND f.team_id = a.team_id
                AND f.source_event_order = a.source_event_order)""")],
    ),
}


def apply_edits(definition: str, edits: list[tuple[str, str]]) -> str:
    out = definition.replace("\r\n", "\n")
    for old, new in edits:
        hits = out.count(old)
        if hits != 1:
            raise RuntimeError(f"expected one match, found {hits}: {old[:70]!r}")
        out = out.replace(old, new)
    return out.rstrip().rstrip(";") + ";\n"


def header(name: str, what: str) -> str:
    return (f"-- {what} of {name}, EuroLeague migration 062 (EUROLEAGUE SHADOW SCHEMA),\n"
            "-- written by scripts/apply_062_readers.py from the live definition.\n\n")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--object", required=True, choices=sorted(OBJECTS))
    a = ap.parse_args()
    sw = OBJECTS[a.object]
    apply = os.environ.get("CONFIRM_EL_062_READER") == a.object
    conn = connect_from_env_file(ENV_PATH, direct_port=5432)
    cur = conn.cursor()
    try:
        if inspect_target(conn)["euroleague_schema"] != "euroleague":
            raise RuntimeError("EuroLeague schema unavailable")
        print(f"{a.object}: " + ("EXECUTE" if apply else f"DRY RUN (CONFIRM_EL_062_READER={a.object} to commit)"), flush=True)
        cur.execute("BEGIN")
        cur.execute("SET LOCAL lock_timeout = '10s'")
        cur.execute("SET LOCAL statement_timeout = '600s'")
        cur.execute("SELECT pg_get_functiondef(%s::regprocedure)", (sw.signature,))
        live = cur.fetchone()[0]
        new = apply_edits(live, sw.edits)
        (REPO / "sql" / f"062_{a.object}_before.sql").write_text(header(a.object, "Definition before") + live.rstrip() + ";\n", encoding="utf-8")
        (REPO / "sql" / f"062_{a.object}.sql").write_text(header(a.object, "Credit-attribution definition") + new, encoding="utf-8")
        if sw.moved:
            rel, keys, vals = sw.moved
            cur.execute(f"CREATE TEMP TABLE moved_before ON COMMIT DROP AS SELECT {', '.join(keys + vals)} FROM {rel}")
        for s in sw.pre:
            cur.execute(s)
            print(f"  {s.splitlines()[0][:90]}", flush=True)
        cur.execute(new)
        print(f"  {sw.signature} replaced ({len(sw.edits)} edits)", flush=True)
        if sw.refresh:
            cur.execute("SELECT DISTINCT game_id FROM euroleague.action_team_context_actions ORDER BY 1")
            games = [r[0] for r in cur.fetchall()]
            t0 = time.perf_counter()
            slowest = 0.0
            for i in range(0, len(games), BATCH):
                b0 = time.perf_counter()
                cur.execute(sw.refresh, (games[i:i + BATCH],))
                slowest = max(slowest, time.perf_counter() - b0)
            print(f"  refreshed {len(games)} games in {time.perf_counter() - t0:.1f}s "
                  f"(slowest batch of {BATCH}: {slowest:.1f}s)", flush=True)
        for s in sw.after:
            t0 = time.perf_counter()
            cur.execute(s)
            print(f"  {s} ({time.perf_counter() - t0:.1f}s)", flush=True)
        if sw.moved:
            rel, keys, vals = sw.moved
            on = " AND ".join(f"b.{k} = a.{k}" for k in keys)  # key columns are never NULL
            differs = " OR ".join(f"b.{v} IS DISTINCT FROM a.{v}" for v in vals)
            cur.execute(f"""SELECT count(*) FILTER (WHERE {differs}),
                                   coalesce(sum(abs(coalesce(a.{vals[0]}, 0) - coalesce(b.{vals[0]}, 0))), 0) / 2,
                                   count(*)
                              FROM moved_before b FULL JOIN {rel} a ON {on}""")
            rows_changed, half_abs, total = cur.fetchone()
            print(f"  moved: {rows_changed} of {total} rows changed; {vals[0]} moved between rows: {half_abs}", flush=True)
            if sw.expect_moved and not rows_changed:
                raise RuntimeError("nothing moved: the switch did not take effect")
            if not sw.expect_moved and rows_changed:
                raise RuntimeError("existing values changed, which this step must not do")
        for label, sql in sw.asserts:
            cur.execute(sql)
            n = cur.fetchone()[0]
            print(f"  {'ok  ' if n == 0 else 'FAIL'} {label}: {n}", flush=True)
            if n:
                raise RuntimeError(f"{label}: {n}")
        failed = verify.check(conn, sw.checks)
        if failed:
            raise RuntimeError(f"{failed} gate(s) failed")
        if apply:
            cur.execute("COMMIT")
            print(f"{a.object}: applied")
        else:
            cur.execute("ROLLBACK")
            print(f"{a.object}: DRY RUN passed and rolled back")
        return 0
    except Exception:
        try:
            cur.execute("ROLLBACK")
        except Exception:
            pass
        raise
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
