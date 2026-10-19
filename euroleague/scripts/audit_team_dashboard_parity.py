#!/usr/bin/env python
"""Read-only structural and timing audit for the two Team dashboards."""

from __future__ import annotations

import json
import statistics
import sys
import time
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))

from euroleague_possessions.postgres_backend import connect_from_env_file  # noqa: E402


ENV = ROOT.parent / "etl" / ".Renviron"
FUNCTIONS = {
    "israel_ratings": ("basketball_test", "get_team_ratings_dynamic"),
    "israel_ff": ("basketball_test", "get_team_four_factors_dynamic"),
    "euro_ratings_pergame": ("euroleague", "get_team_ratings_pergame"),
    "euro_ff_pergame": ("euroleague", "get_team_four_factors_pergame"),
    "euro_ratings_dynamic": ("euroleague", "get_team_ratings_dynamic"),
    "euro_ff_dynamic": ("euroleague", "get_team_four_factors_dynamic"),
    "euro_ratings_direct": ("euroleague", "get_team_ratings_direct"),
    "euro_ff_direct": ("euroleague", "get_team_four_factors_direct"),
}
RELATIONS = [
    ("basketball_test", "final_schedule_mv"),
    ("basketball_test", "team_ppp_ratings_mv"),
    ("basketball_test", "df_pts_poss_lineups_longer_mv"),
    ("basketball_test", "lineup_four_factors_by_game"),
    ("basketball_test", "team_metrics_by_game_mv"),
    ("euroleague", "final_schedule_mv"),
    ("euroleague", "team_ppp_ratings_mv"),
    ("euroleague", "team_four_factors_by_game"),
    ("euroleague", "player_stats_actions_by_game"),
    ("euroleague", "default_clutch_lineup_totals_by_game"),
]


ISRAEL_GAME_FACT_QUERY = """
WITH schedule_ranked AS (
  SELECT tm.*,
         row_number() OVER (
           PARTITION BY tm.team_id, tm.game_year
           ORDER BY tm.game_date DESC NULLS LAST, tm.game_id DESC
         ) AS rn_recent
  FROM basketball_test.team_metrics_by_game_mv tm
  WHERE tm.game_year = %s
), selected AS (
  SELECT *
  FROM schedule_ranked
  WHERE (%s::date IS NULL OR game_date >= %s::date)
    AND (%s::date IS NULL OR game_date <= %s::date)
    AND (%s::int IS NULL OR rn_recent <= %s::int)
), agg AS (
  SELECT team_id, max(team_name) AS team_name,
         count(DISTINCT game_id)::int AS games_played,
         count(DISTINCT game_id) FILTER (WHERE has_won)::int AS wins,
         count(DISTINCT game_id) FILTER (WHERE NOT has_won)::int AS losses,
         sum(off_points_raw) AS off_pts, sum(def_points_raw) AS def_pts,
         sum(off_poss_raw) AS off_poss, sum(def_poss_raw) AS def_poss,
         sum(off_ts_poss_raw) AS off_ts_poss,
         sum(def_ts_poss_raw) AS def_ts_poss,
         sum(off_oreb_count_raw) AS off_oreb_cnt,
         sum(def_oreb_count_raw) AS def_oreb_cnt,
         sum(off_oreb_opp_raw) AS off_oreb_opps,
         sum(def_oreb_opp_raw) AS def_oreb_opps,
         sum(off_tov_raw) AS off_tov_cnt, sum(def_tov_raw) AS def_tov_cnt,
         sum(off_fta_raw) AS off_fta, sum(def_fta_raw) AS def_fta,
         sum(off_fga_raw) AS off_fga_cnt, sum(def_fga_raw) AS def_fga_cnt,
         sum(off_fgm_raw) AS off_fgm_cnt, sum(def_fgm_raw) AS def_fgm_cnt,
         sum(off_fg3m_raw) AS off_fg3m_cnt, sum(def_fg3m_raw) AS def_fg3m_cnt
  FROM selected GROUP BY team_id
)
SELECT %s::int AS game_year, a.*,
       round(a.off_pts / nullif(a.off_poss, 0), 3) * 100 AS ratings_off_ppp,
       round(a.def_pts / nullif(a.def_poss, 0), 3) * 100 AS ratings_def_ppp,
       round(a.off_pts / nullif(a.off_poss, 0) -
             a.def_pts / nullif(a.def_poss, 0), 3) * 100 AS ratings_net_rtg,
       round(a.off_pts / nullif(2.0 * a.off_ts_poss, 0) * 100, 1) AS off_ts,
       round((a.off_fgm_cnt + 0.5 * a.off_fg3m_cnt) /
             nullif(a.off_fga_cnt, 0) * 100, 1) AS off_efg,
       round(a.off_oreb_cnt / nullif(a.off_oreb_opps, 0) * 100, 1) AS off_oreb,
       round(a.off_tov_cnt / nullif(a.off_poss, 0) * 100, 1) AS off_tov,
       round(a.off_fta / nullif(a.off_fga_cnt, 0) * 100, 1) AS off_ftr,
       round(a.off_pts / nullif(a.off_poss, 0) * 100, 1) AS ff_off_ppp,
       round(a.def_pts / nullif(2.0 * a.def_ts_poss, 0) * 100, 1) AS def_ts,
       round((a.def_fgm_cnt + 0.5 * a.def_fg3m_cnt) /
             nullif(a.def_fga_cnt, 0) * 100, 1) AS def_efg,
       round(a.def_oreb_cnt / nullif(a.def_oreb_opps, 0) * 100, 1) AS def_oreb,
       round(a.def_tov_cnt / nullif(a.def_poss, 0) * 100, 1) AS def_tov,
       round(a.def_fta / nullif(a.def_fga_cnt, 0) * 100, 1) AS def_ftr,
       round(a.def_pts / nullif(a.def_poss, 0) * 100, 1) AS ff_def_ppp,
       round(round(a.off_pts / nullif(a.off_poss, 0) * 100, 1) -
             round(a.def_pts / nullif(a.def_poss, 0) * 100, 1), 1)
         AS ff_net_rtg,
       round(a.off_pts / nullif(a.off_poss, 0) * 100 -
             a.def_pts / nullif(a.def_poss, 0) * 100, 1)
         AS canonical_net_rtg
FROM agg a ORDER BY a.team_id
"""


def function_contracts(cur) -> None:
    print("FUNCTION CONTRACTS")
    for label, (schema, name) in FUNCTIONS.items():
        cur.execute(
            "SELECT p.oid::regprocedure::text, pg_get_function_arguments(p.oid), "
            "       pg_get_function_result(p.oid), l.lanname, p.prosecdef, "
            "       coalesce(p.proconfig, ARRAY[]::text[]) "
            "FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace "
            "JOIN pg_language l ON l.oid=p.prolang "
            "WHERE n.nspname=%s AND p.proname=%s",
            (schema, name),
        )
        rows = cur.fetchall()
        print(json.dumps({"label": label, "overloads": rows}, default=str))


def relation_inventory(cur) -> None:
    print("RELATIONS")
    for schema, name in RELATIONS:
        qualified = schema + "." + name
        cur.execute(
            "SELECT c.relkind::text, c.reltuples::bigint, "
            "       pg_total_relation_size(c.oid), pg_relation_size(c.oid), "
            "       pg_indexes_size(c.oid) "
            "FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace "
            "WHERE n.nspname=%s AND c.relname=%s",
            (schema, name),
        )
        size = cur.fetchone()
        cur.execute(
            "SELECT column_name, data_type, udt_name, is_nullable "
            "FROM information_schema.columns "
            "WHERE table_schema=%s AND table_name=%s ORDER BY ordinal_position",
            (schema, name),
        )
        columns = cur.fetchall()
        cur.execute(
            "SELECT indexname, indexdef FROM pg_indexes "
            "WHERE schemaname=%s AND tablename=%s ORDER BY indexname",
            (schema, name),
        )
        indexes = cur.fetchall()
        print(json.dumps({"relation": qualified, "size": size,
                          "columns": columns, "indexes": indexes}, default=str))


def bounds(cur, schema: str) -> tuple[int, object, object]:
    year_col = "game_year"
    cur.execute(
        "SELECT max(" + year_col + ")::int FROM " + schema + ".final_schedule_mv"
    )
    year = cur.fetchone()[0]
    cur.execute(
        "SELECT min(game_date), max(game_date) FROM " + schema + ".final_schedule_mv "
        "WHERE " + year_col + "=%s",
        (year,),
    )
    start, end = cur.fetchone()
    return year, start, end


def explain(cur, query: str, params: tuple) -> dict[str, object]:
    cur.execute("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + query, params)
    payload = cur.fetchone()[0][0]
    plan = payload["Plan"]
    return {
        "execution_ms": payload.get("Execution Time"),
        "node_ms": plan.get("Actual Total Time"),
        "rows": plan.get("Actual Rows"),
        "shared_hit": plan.get("Shared Hit Blocks", 0),
        "shared_read": plan.get("Shared Read Blocks", 0),
        "temp_read": plan.get("Temp Read Blocks", 0),
        "temp_written": plan.get("Temp Written Blocks", 0),
    }


def benchmark(cur, label: str, query: str, params: tuple) -> None:
    cold_started = time.perf_counter()
    cur.execute(query, params)
    cold_rows = cur.fetchall()
    cold = time.perf_counter() - cold_started
    timings = []
    rows = cold_rows
    for _ in range(3):
        started = time.perf_counter()
        cur.execute(query, params)
        rows = cur.fetchall()
        timings.append(time.perf_counter() - started)
    plan = explain(cur, query, params)
    print(json.dumps({
        "benchmark": label,
        "rows": len(rows),
        "cold_fetch_seconds": cold,
        "warm_seconds": timings,
        "median_seconds": statistics.median(timings),
        "plan": plan,
    }))


def benchmarks(cur) -> None:
    print("BENCHMARKS")
    iy, istart, iend = bounds(cur, "basketball_test")
    ey, estart, eend = bounds(cur, "euroleague")
    print(json.dumps({"bounds": {"israel": [iy, istart, iend],
                                  "euroleague": [ey, estart, eend]}}, default=str))
    calls = [
        ("israel ratings broad",
         "SELECT * FROM basketball_test.get_team_ratings_dynamic("
         "p_game_year=>%s,p_start_date=>%s,p_end_date=>%s)",
         (iy, istart, iend)),
        ("israel ff broad",
         "SELECT * FROM basketball_test.get_team_four_factors_dynamic("
         "p_game_year=>%s,p_start_date=>%s,p_end_date=>%s)",
         (iy, istart, iend)),
        ("euro ratings broad",
         "SELECT * FROM euroleague.get_team_ratings_pergame("
         "p_competition=>'E',p_game_year=>%s,p_start_date=>%s,p_end_date=>%s)",
         (ey, estart, eend)),
        ("euro ff broad",
         "SELECT * FROM euroleague.get_team_four_factors_pergame("
         "p_competition=>'E',p_game_year=>%s,p_start_date=>%s,p_end_date=>%s)",
         (ey, estart, eend)),
        ("israel ratings last10",
         "SELECT * FROM basketball_test.get_team_ratings_dynamic("
         "p_game_year=>%s,p_start_date=>%s,p_end_date=>%s,p_last_n_games=>10)",
         (iy, istart, iend)),
        ("israel ff last10",
         "SELECT * FROM basketball_test.get_team_four_factors_dynamic("
         "p_game_year=>%s,p_start_date=>%s,p_end_date=>%s,p_last_n_games=>10)",
         (iy, istart, iend)),
        ("euro ratings last10",
         "SELECT * FROM euroleague.get_team_ratings_pergame("
         "p_competition=>'E',p_game_year=>%s,p_start_date=>%s,p_end_date=>%s,"
         "p_last_n_games=>10)",
         (ey, estart, eend)),
        ("euro ff last10",
         "SELECT * FROM euroleague.get_team_four_factors_pergame("
         "p_competition=>'E',p_game_year=>%s,p_start_date=>%s,p_end_date=>%s,"
         "p_last_n_games=>10)",
         (ey, estart, eend)),
    ]
    for call in calls:
        benchmark(cur, *call)


def rows_by_team(cur, query: str, params: tuple) -> dict[int, dict[str, object]]:
    cur.execute(query, params)
    names = [column.name for column in cur.description]
    return {int(row[names.index("team_id")]): dict(zip(names, row))
            for row in cur.fetchall()}


def israel_game_fact_parity(cur) -> None:
    """Compare the existing game-team fact with both current public readers."""
    print("ISRAEL GAME-FACT PARITY")
    year, start, end = bounds(cur, "basketball_test")
    for label, last_n in (("broad", None), ("last10", 10)):
        args = (year, start, start, end, end, last_n, last_n, year)
        started = time.perf_counter()
        fact = rows_by_team(cur, ISRAEL_GAME_FACT_QUERY, args)
        fact_seconds = time.perf_counter() - started
        ratings = rows_by_team(
            cur,
            "SELECT * FROM basketball_test.get_team_ratings_dynamic("
            "p_game_year=>%s,p_start_date=>%s,p_end_date=>%s,"
            "p_last_n_games=>%s)",
            (year, start, end, last_n),
        )
        ff = rows_by_team(
            cur,
            "SELECT * FROM basketball_test.get_team_four_factors_dynamic("
            "p_game_year=>%s,p_start_date=>%s,p_end_date=>%s,"
            "p_last_n_games=>%s)",
            (year, start, end, last_n),
        )
        rating_map = {
            "off_ppp": "ratings_off_ppp", "def_ppp": "ratings_def_ppp",
            "net_rtg": "ratings_net_rtg", "games_played": "games_played",
            "wins": "wins", "losses": "losses", "off_poss": "off_poss",
            "def_poss": "def_poss",
        }
        ff_map = {
            name: name for name in (
                "off_ts", "off_efg", "off_oreb", "off_tov", "off_ftr",
                "off_poss", "off_pts", "off_ts_poss", "off_oreb_cnt",
                "off_oreb_opps", "off_tov_cnt", "off_fta", "off_fga_cnt",
                "off_fgm_cnt", "off_fg3m_cnt", "def_ts", "def_efg",
                "def_oreb", "def_tov", "def_ftr", "def_poss", "def_pts",
                "def_ts_poss", "def_oreb_cnt", "def_oreb_opps", "def_tov_cnt",
                "def_fta", "def_fga_cnt", "def_fgm_cnt", "def_fg3m_cnt",
            )
        }
        ff_map.update({"off_ppp": "ff_off_ppp", "def_ppp": "ff_def_ppp",
                       "net_rtg": "ff_net_rtg"})

        def mismatches(actual, mapping):
            out = []
            for team_id in sorted(set(actual) | set(fact)):
                if team_id not in actual or team_id not in fact:
                    out.append({"team_id": team_id, "missing_row": True})
                    continue
                bad = {left: [actual[team_id][left], fact[team_id][right]]
                       for left, right in mapping.items()
                       if actual[team_id][left] != fact[team_id][right]}
                if bad:
                    out.append({"team_id": team_id, "values": bad})
            return out

        print(json.dumps({
            "preset": label, "fact_seconds": fact_seconds,
            "ratings_mismatches": mismatches(ratings, rating_map),
            "four_factor_mismatches": mismatches(ff, ff_map),
            "legacy_vs_single_round_net": [
                {"team_id": team_id, "legacy": row["ff_net_rtg"],
                 "single_round": row["canonical_net_rtg"]}
                for team_id, row in sorted(fact.items())
                if row["ff_net_rtg"] != row["canonical_net_rtg"]
            ],
        }, default=str))


def reader_pair_consistency(cur) -> None:
    """Find visible disagreements between Summary and Four Factors readers."""
    print("READER-PAIR CONSISTENCY")
    iy, istart, iend = bounds(cur, "basketball_test")
    ey, estart, eend = bounds(cur, "euroleague")
    cases = [
        ("israel", "broad", iy, istart, iend, None,
         "SELECT * FROM basketball_test.get_team_ratings_dynamic("
         "p_game_year=>%s,p_start_date=>%s,p_end_date=>%s,p_last_n_games=>%s)",
         "SELECT * FROM basketball_test.get_team_four_factors_dynamic("
         "p_game_year=>%s,p_start_date=>%s,p_end_date=>%s,p_last_n_games=>%s)"),
        ("israel", "last10", iy, istart, iend, 10,
         "SELECT * FROM basketball_test.get_team_ratings_dynamic("
         "p_game_year=>%s,p_start_date=>%s,p_end_date=>%s,p_last_n_games=>%s)",
         "SELECT * FROM basketball_test.get_team_four_factors_dynamic("
         "p_game_year=>%s,p_start_date=>%s,p_end_date=>%s,p_last_n_games=>%s)"),
        ("euroleague", "broad", ey, estart, eend, None,
         "SELECT * FROM euroleague.get_team_ratings_pergame("
         "p_competition=>'E',p_game_year=>%s,p_start_date=>%s,p_end_date=>%s,"
         "p_last_n_games=>%s)",
         "SELECT * FROM euroleague.get_team_four_factors_pergame("
         "p_competition=>'E',p_game_year=>%s,p_start_date=>%s,p_end_date=>%s,"
         "p_last_n_games=>%s)"),
        ("euroleague", "last10", ey, estart, eend, 10,
         "SELECT * FROM euroleague.get_team_ratings_pergame("
         "p_competition=>'E',p_game_year=>%s,p_start_date=>%s,p_end_date=>%s,"
         "p_last_n_games=>%s)",
         "SELECT * FROM euroleague.get_team_four_factors_pergame("
         "p_competition=>'E',p_game_year=>%s,p_start_date=>%s,p_end_date=>%s,"
         "p_last_n_games=>%s)"),
    ]
    for league, preset, year, start, end, last_n, ratings_sql, ff_sql in cases:
        params = (year, start, end, last_n)
        ratings = rows_by_team(cur, ratings_sql, params)
        ff = rows_by_team(cur, ff_sql, params)
        differences = []
        for team_id in sorted(set(ratings) & set(ff)):
            values = {name: [ratings[team_id][name], ff[team_id][name]]
                      for name in ("off_ppp", "def_ppp", "net_rtg",
                                   "off_poss", "def_poss")
                      if ratings[team_id][name] != ff[team_id][name]}
            if values:
                differences.append({"team_id": team_id, "values": values})
        print(json.dumps({"league": league, "preset": preset,
                          "differences": differences}, default=str))


def main() -> int:
    con = connect_from_env_file(ENV, direct_port=5432)
    cur = con.cursor()
    try:
        cur.execute("SET statement_timeout='120s'")
        function_contracts(cur)
        relation_inventory(cur)
        benchmarks(cur)
        israel_game_fact_parity(cur)
        reader_pair_consistency(cur)
        return 0
    finally:
        cur.close()
        con.close()


if __name__ == "__main__":
    raise SystemExit(main())
