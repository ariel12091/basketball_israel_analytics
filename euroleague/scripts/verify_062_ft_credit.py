#!/usr/bin/env python
"""Baseline and gates for EuroLeague migration 062: free throws credited to the
lineup that drew the foul (docs/superpowers/plans/2026-11-13-euroleague-ft-foul-credit.md).

  --baseline          store, per object, its team totals (G2) and its floor time
                      (G4) in the scratch schema euroleague_ftc_baseline
                      (CONFIRM_EL_062_BASELINE=1; refuses to overwrite)
  --check NAME ...    compare the live object(s) with the baseline
  --check-fact        G1 and G3 on the event fact, plus the moved-points report
  --list              the objects

Only aggregates are stored, never whole tables: the largest object has about a
million rows. Comparison runs in SQL (EXCEPT both ways), so nothing large leaves
the database. Objects marked expect_change (starters filters) are reported, not
gated: a re-credited free throw takes its foul's starters, by design.
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "src"))

BASELINE_SCHEMA = "euroleague_ftc_baseline"
ENV_PATH = REPO.parent / "etl" / ".Renviron"
SEASON = ("E", 2025)  # the largest season: 402 games

LINEUP_EVENTS = ["possessions", "points", "fg2_made", "fg2_att", "fg3_made", "fg3_att",
                 "ts_possessions", "fgm", "fga", "ft_attempts", "orebounds",
                 "oreb_opportunities", "turnovers", "steals"]
UNIT_EVENTS = [f"{side}_{c}" for side in ("off", "def") for c in
               ("poss", "pts", "fg2_made", "fg2_att", "fg3_made", "fg3_att", "ts_poss",
                "fgm", "fga", "fta", "oreb", "oreb_opp", "tov", "steals")]


@dataclass(frozen=True)
class Spec:
    name: str
    source: str            # a relation, or a SELECT
    team: list[str]        # G2 group keys
    events: list[str]      # G2 summed expressions ("expr AS alias" allowed)
    time_keys: list[str] = field(default_factory=list)
    time: list[str] = field(default_factory=list)
    expect_change: bool = False


def season_games() -> str:
    c, y = SEASON
    return (f"ARRAY(SELECT game_id FROM euroleague.schedule WHERE competition = '{c}' "
            f"AND season = {y} ORDER BY game_id)")


def reader(fn: str, **args) -> str:
    c, y = SEASON
    named = {"p_competition": f"'{c}'", "p_game_year": str(y), **args}
    return f"SELECT * FROM euroleague.{fn}({', '.join(f'{k} => {v}' for k, v in named.items())})"


def game_reader(fn: str, clutch: bool = False) -> str:
    # These take all five arguments, with no defaults.
    margin, remaining = ("5", "300") if clutch else ("NULL::integer", "NULL::integer")
    return (f"SELECT * FROM euroleague.{fn}(p_game_ids => {season_games()}, "
            f"p_max_margin => {margin}, p_margin_status => NULL::text, "
            f"p_max_time_remaining => {remaining}, p_ot_margin_filter => false)")


# Lineup readers on a whole season at no possession minimum run past the
# 10-minute statement limit (fetch_lineups_direct, 2026-11-13), so their presets
# take the two teams with the most games: a full season of their units.
TWO_TEAMS = ("(SELECT string_agg(team_id::text, ',' ORDER BY team_id) FROM ("
             "SELECT team_id FROM euroleague.final_schedule_mv WHERE competition = 'E' "
             "AND game_year = 2025 GROUP BY team_id ORDER BY count(*) DESC, team_id LIMIT 2) t)")
CLUTCH = {"p_max_margin": "5", "p_max_time_remaining": "300"}
STARTERS = {"p_num_starters_off_min": "5", "p_num_starters_off_max": "5"}

SPECS = [
    # Derived per-game tables.
    Spec("lineup_totals_by_game", "euroleague.lineup_totals_by_game",
         ["game_id", "team_id", "type_lineup"], LINEUP_EVENTS,
         ["game_id", "team_id", "type_lineup", "lineup_key"], ["seconds", "action_span_seconds"]),
    Spec("default_clutch_lineup_totals_by_game", "euroleague.default_clutch_lineup_totals_by_game",
         ["game_id", "team_id", "type_lineup"], LINEUP_EVENTS,
         ["game_id", "team_id", "type_lineup", "own_lineup"], ["seconds"]),
    Spec("team_four_factors_by_game", "euroleague.team_four_factors_by_game",
         ["game_id", "team_id"],
         [f"{s}_{c}" for s in ("off", "def") for c in
          ("pts", "poss", "ts_poss", "oreb", "oreb_opp", "tov", "fta", "fga", "fgm", "fg3m")] + ["def_steals"]),
    Spec("player_four_factors_by_game", "euroleague.player_four_factors_by_game",
         ["game_id", "team_id", "player_id", "type_lineup"],
         ["total_points", "total_poss", "ts_poss_count", "oreb_count", "oreb_opportunities",
          "tov_count", "total_ft_attempts", "total_fga", "total_fgm"],
         ["game_id", "team_id", "player_id", "is_on_key", "type_lineup"], ["minutes", "onoff_minutes"]),
    Spec("player_stats_actions_by_game", "euroleague.player_stats_actions_by_game",
         ["game_id", "team_id", "type_lineup"],
         ["points", "possession_flag", "fga", "ft_attempts", "ts_possessions", "oreb_opportunities", "turnovers"]),
    Spec("player_traditional_by_game", "euroleague.player_traditional_by_game",
         ["game_id", "team_id"], ["pts", "fg2a", "fg3a", "fta", "poss_on_floor", "player_ts_poss"],
         ["game_id", "team_id", "player_id"], ["minutes", "gp"]),
    Spec("default_clutch_player_totals_by_game", "euroleague.default_clutch_player_totals_by_game",
         ["game_id", "team_id"], ["pts", "fg2a", "fg3a", "fta", "player_ts_poss"]),
    # Season materialized views.
    Spec("sub_lineups_stats_mv", "euroleague.sub_lineups_stats_mv",
         ["competition", "game_year", "team_id", "unit_size"], UNIT_EVENTS,
         ["competition", "game_year", "team_id", "unit_key"], ["minutes"]),
    Spec("player_onoff_default_mv", "euroleague.player_onoff_default_mv",
         ["competition", "game_year", "team_id", "player_id"],
         ['"ON Poss" + "OFF Poss" AS poss_total'],
         ["competition", "game_year", "team_id", "player_id"], ["minutes"]),
    Spec("player_advanced_stats_mv", "euroleague.player_advanced_stats_mv",
         ["competition", "game_year", "team_id", "player_id"],
         ["off_on_poss + off_off_poss AS off_poss", "def_on_poss + def_off_poss AS def_poss"]),
    Spec("player_traditional_stats_mv", "euroleague.player_traditional_stats_mv",
         ["competition", "game_year", "team_id"], ["pts", "fga", "fta", "poss_on_floor"],
         ["competition", "game_year", "team_id", "player_id"], ["minutes", "gp"]),
    Spec("team_ppp_ratings_mv", "euroleague.team_ppp_ratings_mv",
         ["competition", "game_year", "team_id"], ["off_pts", "def_pts", "off_poss", "def_poss", "games_played"]),
    Spec("team_four_factors_mv", "euroleague.team_four_factors_mv",
         ["competition", "game_year", "team_id"],
         ["off_ppp", "def_ppp", "off_efg", "def_efg", "off_ts", "def_ts", "off_oreb", "def_oreb",
          "off_tov", "def_tov", "off_ftr", "def_ftr", "off_poss", "def_poss"]),
    Spec("team_game_ratings_mv", "euroleague.team_game_ratings_mv",
         ["game_id", "team_id"], ["off_pts", "def_pts", "off_poss", "def_poss"]),
    # Readers, on EuroLeague 2025-26.
    *[Spec(f"{fn}_season", reader(fn, p_team_ids_csv=TWO_TEAMS, p_unit_size="5", p_min_poss="0"),
           ["team_id"], UNIT_EVENTS, ["team_id", "unit_key"], ["minutes"])
      for fn in ("fetch_lineups_direct", "fetch_lineups_dynamic", "fetch_lineups_pergame")],
    Spec("fetch_lineups_dynamic_clutch",
         reader("fetch_lineups_dynamic", p_team_ids_csv=TWO_TEAMS, p_unit_size="5", p_min_poss="0", **CLUTCH),
         ["team_id"], UNIT_EVENTS),
    Spec("fetch_lineups_dynamic_starters",
         reader("fetch_lineups_dynamic", p_team_ids_csv=TWO_TEAMS, p_unit_size="5", p_min_poss="0", **STARTERS),
         ["team_id"], UNIT_EVENTS, expect_change=True),
    Spec("get_player_traditional_standard_clutch_season", reader("get_player_traditional_standard_clutch"),
         ["team_id"], ["pts", "fga", "fta", "poss_on_floor"], ["team_id", "player_id"], ["minutes", "gp"]),
    Spec("get_player_traditional_custom_clutch_clutch", reader("get_player_traditional_custom_clutch", **CLUTCH),
         ["team_id"], ["pts", "fga", "fta", "poss_on_floor"], ["team_id", "player_id"], ["minutes", "gp"]),
    Spec("select_team_game_facts_season", game_reader("select_team_game_facts"),
         ["game_id", "team_id", "type_lineup"], LINEUP_EVENTS,
         ["game_id", "team_id", "type_lineup", "own_lineup"], ["seconds"]),
    Spec("clutch_team_game_facts_clutch", game_reader("clutch_team_game_facts", clutch=True),
         ["game_id", "team_id", "type_lineup"], LINEUP_EVENTS),
    Spec("filtered_team_game_facts_season", reader("filtered_team_game_facts"),
         ["game_id", "team_id", "type_lineup"], LINEUP_EVENTS,
         ["game_id", "team_id", "type_lineup", "own_lineup"], ["seconds"]),
    Spec("filtered_team_game_facts_starters", reader("filtered_team_game_facts", **STARTERS),
         ["game_id", "team_id", "type_lineup"], LINEUP_EVENTS, expect_change=True),
    Spec("get_team_traditional_pergame_season", reader("get_team_traditional_pergame"),
         ["team_id"], ["gp", "poss_on_floor", "pts", "reb", "oreb", "dreb", "tov", "fga", "fta"],
         ["team_id"], ["minutes"]),
    Spec("get_team_minutes_pergame_season", reader("get_team_minutes_pergame"),
         ["team_id"], [], ["team_id"], ["minutes"]),
    Spec("get_team_minutes_pergame_starters", reader("get_team_minutes_pergame", **STARTERS),
         ["team_id"], [], ["team_id"], ["minutes"], expect_change=True),
]
SPEC_BY_NAME = {s.name: s for s in SPECS}


def alias(expr: str) -> str:
    return expr.split(" AS ")[-1].strip() if " AS " in expr else expr.strip('"')


def summed(expr: str) -> str:
    body = expr.split(" AS ")[0]
    return f"sum({body}) AS {alias(expr)}"


def team_sql(spec: Spec) -> str:
    cols = ", ".join([*spec.team, *map(summed, spec.events)])
    return f"SELECT {cols} FROM ({_from(spec)}) s GROUP BY {', '.join(spec.team)}"


def time_sql(spec: Spec) -> str | None:
    if not spec.time:
        return None
    sums = ", ".join(f"sum({t}) AS {t}" for t in spec.time)
    nonzero = " OR ".join(f"sum({t}) <> 0" for t in spec.time)
    return (f"SELECT {', '.join(spec.time_keys)}, {sums} FROM ({_from(spec)}) s "
            f"GROUP BY {', '.join(spec.time_keys)} HAVING {nonzero}")


def _from(spec: Spec) -> str:
    return spec.source if spec.source.lstrip().upper().startswith("SELECT") else f"SELECT * FROM {spec.source}"


def diff_sql(live: str, base_table: str) -> str:
    return (f"WITH live AS ({live}), base AS (SELECT * FROM {BASELINE_SCHEMA}.{base_table}) "
            "SELECT 'live_only' AS side, * FROM (SELECT * FROM live EXCEPT SELECT * FROM base) a "
            "UNION ALL SELECT 'base_only', * FROM (SELECT * FROM base EXCEPT SELECT * FROM live) b")


# The event fact: G1 (credit columns are the parent's, or the row's own) and G3.
FACT_G1 = """
WITH f AS (
  SELECT f.*, a.synthetic_parent_order AS parent_order
  FROM euroleague.action_team_context_actions f
  JOIN euroleague.actions a ON a.game_id = f.game_id AND a.source_event_order = f.source_event_order
), expected AS (
  SELECT f.game_id, f.source_event_order, f.team_id,
         CASE WHEN f.play_type IN ('FTM', 'FTA') AND p.play_type IS NOT NULL AND p.play_type NOT IN ('FTM', 'FTA')
              THEN p.own_lineup ELSE f.own_lineup END AS own_lineup,
         CASE WHEN f.play_type IN ('FTM', 'FTA') AND p.play_type IS NOT NULL AND p.play_type NOT IN ('FTM', 'FTA')
              THEN p.opp_lineup ELSE f.opp_lineup END AS opp_lineup,
         CASE WHEN f.play_type IN ('FTM', 'FTA') AND p.play_type IS NOT NULL AND p.play_type NOT IN ('FTM', 'FTA')
              THEN p.own_starters ELSE f.own_starters END AS own_starters,
         CASE WHEN f.play_type IN ('FTM', 'FTA') AND p.play_type IS NOT NULL AND p.play_type NOT IN ('FTM', 'FTA')
              THEN p.opp_starters ELSE f.opp_starters END AS opp_starters,
         CASE WHEN f.play_type IN ('FTM', 'FTA') AND p.play_type IS NOT NULL AND p.play_type NOT IN ('FTM', 'FTA')
              THEN p.segment_id ELSE f.segment_id END AS segment_id,
         f.credit_own_lineup, f.credit_opp_lineup, f.credit_own_starters, f.credit_opp_starters, f.credit_segment_id
  FROM f
  LEFT JOIN euroleague.action_team_context_actions p
    ON p.game_id = f.game_id AND p.source_event_order = f.parent_order AND p.team_id = f.team_id
   AND f.parent_order <> f.source_event_order
)
SELECT count(*) FILTER (WHERE credit_own_lineup IS NULL OR credit_opp_lineup IS NULL) AS null_lineup,
       count(*) FILTER (WHERE credit_own_lineup IS DISTINCT FROM own_lineup OR credit_opp_lineup IS DISTINCT FROM opp_lineup) AS wrong_lineup,
       count(*) FILTER (WHERE credit_own_starters IS DISTINCT FROM own_starters OR credit_opp_starters IS DISTINCT FROM opp_starters) AS wrong_starters,
       count(*) FILTER (WHERE credit_segment_id IS DISTINCT FROM segment_id) AS wrong_segment
FROM expected
"""

FACT_G3 = """
SELECT count(*) AS split_trips FROM (
  SELECT game_id, team_id, synthetic_ft_trip_id
  FROM euroleague.action_team_context_actions
  WHERE synthetic_ft_trip_id IS NOT NULL AND play_type IN ('FTM', 'FTA')
  GROUP BY 1, 2, 3 HAVING count(DISTINCT credit_own_lineup) > 1
) t
"""

FACT_MOVED = """
SELECT s.competition, s.season,
       count(*) FILTER (WHERE f.credit_own_lineup <> f.own_lineup) AS rows_moved,
       sum(f.points) FILTER (WHERE f.credit_own_lineup <> f.own_lineup) AS points_moved,
       count(*) FILTER (WHERE f.credit_own_lineup <> f.own_lineup AND f.ts_possessions > 0) AS trips_moved
FROM euroleague.action_team_context_actions f
JOIN euroleague.schedule s ON s.game_id = f.game_id
GROUP BY 1, 2 ORDER BY 1, 2
"""


def connect():
    from euroleague_possessions.postgres_backend import connect_from_env_file
    conn = connect_from_env_file(ENV_PATH, direct_port=5432)
    conn.execute("SET statement_timeout = '600s'")
    return conn


def take_baseline(conn, names: list[str]) -> None:
    if os.environ.get("CONFIRM_EL_062_BASELINE") != "1":
        raise SystemExit("set CONFIRM_EL_062_BASELINE=1 to write the baseline schema")
    conn.execute(f"CREATE SCHEMA IF NOT EXISTS {BASELINE_SCHEMA}")
    conn.execute(f"REVOKE ALL ON SCHEMA {BASELINE_SCHEMA} FROM PUBLIC")
    for name in names:
        spec = SPEC_BY_NAME[name]
        for suffix, sql in (("team", team_sql(spec)), ("time", time_sql(spec))):
            if sql is None:
                continue
            table = f"{BASELINE_SCHEMA}.{name}__{suffix}"
            if conn.execute("SELECT to_regclass(%s) IS NOT NULL", (table,)).fetchone()[0]:
                print(f"baseline {name}__{suffix}: kept (already taken)")
                continue
            t0 = time.perf_counter()
            conn.execute(f"CREATE TABLE {table} AS {sql}")
            n = conn.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
            print(f"baseline {name}__{suffix}: {n} rows, {time.perf_counter() - t0:.1f}s")


def check(conn, names: list[str]) -> int:
    failed = 0
    for name in names:
        spec = SPEC_BY_NAME[name]
        for suffix, sql in (("team", team_sql(spec)), ("time", time_sql(spec))):
            if sql is None:
                continue
            rows = conn.execute(diff_sql(sql, f"{name}__{suffix}")).fetchall()
            gate = "G2" if suffix == "team" else "G4"
            status = "report" if spec.expect_change else ("ok  " if not rows else "FAIL")
            print(f"{status} {gate} {name}: {len(rows)} differing rows")
            for r in rows[:6]:
                print("       ", r)
            failed += int(bool(rows) and not spec.expect_change)
    return failed


def probe(conn, names: list[str]) -> int:
    """Read-only: run every snapshot query, print rows and seconds."""
    import time
    failed = 0
    for name in names:
        spec = SPEC_BY_NAME[name]
        for suffix, sql in (("team", team_sql(spec)), ("time", time_sql(spec))):
            if sql is None:
                continue
            t0 = time.perf_counter()
            try:
                n = conn.execute(f"SELECT count(*) FROM ({sql}) q").fetchone()[0]
                print(f"probe {name}__{suffix}: {n} rows, {time.perf_counter() - t0:.1f}s")
            except Exception as exc:  # report every broken spec, not just the first
                conn.rollback() if not conn.autocommit else None
                print(f"FAIL  {name}__{suffix}: {str(exc).splitlines()[0]}")
                failed += 1
    return failed


def check_fact(conn) -> int:
    g1 = conn.execute(FACT_G1).fetchone()
    g3 = conn.execute(FACT_G3).fetchone()[0]
    print(f"G1 null={g1[0]} wrong_lineup={g1[1]} wrong_starters={g1[2]} wrong_segment={g1[3]}")
    print(f"G3 split trips={g3}")
    for r in conn.execute(FACT_MOVED).fetchall():
        print("   moved", r)
    return int(any(g1)) + int(bool(g3))


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--baseline", action="store_true")
    ap.add_argument("--check", nargs="*")
    ap.add_argument("--check-fact", action="store_true")
    ap.add_argument("--list", action="store_true")
    ap.add_argument("--probe", action="store_true")
    ap.add_argument("--retake", nargs="+", help="drop and retake these objects' baseline")
    a = ap.parse_args()
    if a.list:
        for s in SPECS:
            print(s.name, "(reported)" if s.expect_change else "")
        return 0
    conn = connect()
    if a.probe:
        return 1 if probe(conn, [s.name for s in SPECS]) else 0
    if a.retake:
        unknown = [n for n in a.retake if n not in SPEC_BY_NAME]
        if unknown:
            raise SystemExit(f"unknown objects: {unknown}")
        if os.environ.get("CONFIRM_EL_062_BASELINE") != "1":
            raise SystemExit("set CONFIRM_EL_062_BASELINE=1 to retake baseline tables")
        for name in a.retake:
            for suffix in ("team", "time"):
                conn.execute(f"DROP TABLE IF EXISTS {BASELINE_SCHEMA}.{name}__{suffix}")
        take_baseline(conn, a.retake)
    if a.baseline:
        take_baseline(conn, [s.name for s in SPECS])
    failed = 0
    if a.check is not None:
        failed += check(conn, a.check or [s.name for s in SPECS])
    if a.check_fact:
        failed += check_fact(conn)
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
