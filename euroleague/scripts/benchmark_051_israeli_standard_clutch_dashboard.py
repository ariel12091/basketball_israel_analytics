#!/usr/bin/env python
"""Rollback-only Israeli standard-clutch Team dashboard parity/latency probe."""

from __future__ import annotations

import argparse
import statistics
import sys
import time
from dataclasses import dataclass
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
from euroleague_possessions.postgres_backend import (  # noqa: E402
    _split_sql_statements,
    connect_from_env_file,
    inspect_target,
)


DDL = ROOT / "sql" / "candidates" / "051_israeli_standard_clutch_dashboard_probe.sql"
ENV = ROOT.parent / "etl" / ".Renviron"
PROBE = "get_team_dashboard_standard_clutch_probe_20260901"

RATING_COLUMNS = (
    "game_year", "team_id", "team_name", "off_ppp", "def_ppp", "net_rtg",
    "games_played", "wins", "losses", "off_poss", "def_poss",
    "rank_net_rtg", "rank_off_ppp", "rank_def_ppp", "off_fga",
    "off_layup_att", "off_dunk_att", "off_fg3_att", "off_c3_att",
    "off_c3_known_att", "def_fga", "def_layup_att", "def_dunk_att",
    "def_fg3_att", "def_c3_att", "def_c3_known_att",
)
FF_COLUMNS = (
    "team_id", "game_year", "team_name", "off_ts", "off_efg", "off_oreb",
    "off_tov", "off_ftr", "off_ppp", "off_poss", "off_pts", "off_ts_poss",
    "off_oreb_cnt", "off_oreb_opps", "off_tov_cnt", "off_fta", "off_fga_cnt",
    "off_fgm_cnt", "off_fg3m_cnt", "def_ts", "def_efg", "def_oreb", "def_tov",
    "def_ftr", "def_ppp", "def_poss", "def_pts", "def_ts_poss", "def_oreb_cnt",
    "def_oreb_opps", "def_tov_cnt", "def_fta", "def_fga_cnt", "def_fgm_cnt",
    "def_fg3m_cnt", "net_rtg",
)


@dataclass(frozen=True)
class Preset:
    label: str
    extra: str


PRESETS = (
    Preset("standard clutch", ""),
    Preset("home", "p_home_away=>'home'"),
    Preset("last 10", "p_last_n_games=>10"),
    Preset("starter context", "p_num_starters_off_min=>3,p_num_starters_def_max=>3"),
)
BASE = (
    "p_game_year=>2026,p_start_date=>DATE '2025-09-01',"
    "p_end_date=>DATE '2026-07-01',p_max_margin=>5,"
    "p_margin_status=>'all',p_max_time_remaining=>300,p_ot_margin_filter=>false"
)

# Exact clutch-active branch used by fetch_team_game_minutes(), expressed with
# named SQL-function arguments so the same preset string drives every reader.
MINUTES_SQL = """
WITH selected AS (
  SELECT fs.game_id,fs.team_id
  FROM basketball_test.final_schedule_mv fs
  WHERE fs.game_year=2026 AND fs.game_date>=DATE '2025-09-01'
    AND fs.game_date<=DATE '2026-07-01'
    AND ({home}) AND ({last_n})
), filtered_rows AS (
  SELECT d.team_id,d.game_id,d.lineup_hash,d.segment_id,d.id,d.event_elapsed_seconds
  FROM basketball_test.df_pts_poss_lineups_longer_mv d
  JOIN selected s USING(game_id,team_id)
  WHERE (abs(CASE WHEN d.type_lineup='offense'
      THEN (d.own_team_score-coalesce(d.team_score,0))-d.opp_team_score
      ELSE d.own_team_score-(d.opp_team_score-coalesce(d.team_score,0)) END)<=5
      OR d.quarter>4)
    AND (d.end_game_seconds_remaining<=300 OR d.quarter>4)
    AND ({st_off}) AND ({st_def})
    AND d.lineup_hash IS NOT NULL AND d.segment_id IS NOT NULL
    AND d.event_elapsed_seconds IS NOT NULL
), filtered_segments AS (
  SELECT team_id,game_id,lineup_hash,segment_id,
    greatest((array_agg(event_elapsed_seconds order by id desc))[1]-
             (array_agg(event_elapsed_seconds order by id))[1],0)::numeric seg_seconds
  FROM filtered_rows GROUP BY team_id,game_id,lineup_hash,segment_id
)
SELECT team_id,round(sum(seg_seconds)/60.0,3)::numeric minutes
FROM filtered_segments GROUP BY team_id
"""


def options():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--runs", type=int, default=15)
    return parser.parse_args()


def args_for(preset: Preset) -> str:
    return BASE + (("," + preset.extra) if preset.extra else "")


def fetch(cursor, function: str, args: str):
    started = time.perf_counter()
    cursor.execute(f"SELECT * FROM {function}({args})")
    columns = tuple(desc.name for desc in cursor.description)
    rows = cursor.fetchall()
    return columns, {row[columns.index("team_id")]: row for row in rows}, time.perf_counter() - started


def value(columns, row, name):
    return row[columns.index(name)]


def minute_query(preset: Preset) -> str:
    home = "fs.is_home" if "p_home_away=>'home'" in preset.extra else "true"
    # row_number must be assigned over the complete season before date/other
    # filters. These measured dates cover the loaded season, so this compact
    # expression is exact for the selected matrix.
    last_n = "true"
    if "p_last_n_games=>10" in preset.extra:
        last_n = "fs.game_id IN (SELECT game_id FROM (SELECT f2.game_id,row_number() over(partition by f2.team_id order by f2.game_date desc nulls last,f2.game_id desc) rn FROM basketball_test.final_schedule_mv f2 WHERE f2.game_year=2026 AND f2.team_id=fs.team_id) z WHERE z.rn<=10)"
    st_off = "d.own_starters>=3" if "p_num_starters_off_min=>3" in preset.extra else "true"
    st_def = "d.opp_starters<=3" if "p_num_starters_def_max=>3" in preset.extra else "true"
    return MINUTES_SQL.format(home=home, last_n=last_n, st_off=st_off, st_def=st_def)


def legacy(cursor, preset: Preset):
    args = args_for(preset)
    started = time.perf_counter()
    rc, ratings, _ = fetch(cursor, "basketball_test.get_team_ratings_dynamic", args)
    fc, factors, _ = fetch(cursor, "basketball_test.get_team_four_factors_dynamic", args)
    cursor.execute(minute_query(preset))
    mc = tuple(desc.name for desc in cursor.description)
    minutes = {row[mc.index("team_id")]: row for row in cursor.fetchall()}
    elapsed = time.perf_counter() - started
    return (rc, ratings), (fc, factors), (mc, minutes), elapsed


def legacy_view(cursor, preset: Preset, view: str):
    args = args_for(preset)
    started = time.perf_counter()
    function = (
        "basketball_test.get_team_ratings_dynamic"
        if view == "summary"
        else "basketball_test.get_team_four_factors_dynamic"
    )
    fetch(cursor, function, args)
    cursor.execute(minute_query(preset))
    cursor.fetchall()
    return time.perf_counter() - started


def candidate(cursor, preset: Preset, schema="basketball_test"):
    return fetch(cursor, f"{schema}.{PROBE}", args_for(preset))


def assert_parity(preset, old, new):
    (rc, ratings), (fc, factors), (mc, minutes), _ = old
    nc, dashboard, _ = new
    if not dashboard or set(dashboard) != set(ratings) or set(dashboard) != set(factors) or set(dashboard) != set(minutes):
        raise RuntimeError(
            f"{preset.label}: row-key mismatch candidate={len(dashboard)} "
            f"ratings={len(ratings)} factors={len(factors)} minutes={len(minutes)}"
        )
    for team_id, row in dashboard.items():
        for name in RATING_COLUMNS:
            if value(nc, row, name) != value(rc, ratings[team_id], name):
                raise RuntimeError(
                    f"{preset.label}: ratings team={team_id} column={name} "
                    f"expected={value(rc, ratings[team_id], name)!r} "
                    f"actual={value(nc, row, name)!r}"
                )
        for name in FF_COLUMNS:
            if value(nc, row, name) != value(fc, factors[team_id], name):
                raise RuntimeError(
                    f"{preset.label}: factors team={team_id} column={name} "
                    f"expected={value(fc, factors[team_id], name)!r} "
                    f"actual={value(nc, row, name)!r}"
                )
        if value(nc, row, "minutes") != value(mc, minutes[team_id], "minutes"):
            raise RuntimeError(f"{preset.label}: minutes team={team_id}")


def main() -> int:
    args = options()
    if args.runs < 3:
        raise ValueError("--runs must be at least 3")
    source = DDL.read_text(encoding="utf-8")
    if "DROP " in source.upper() or "COMMIT" in source.upper():
        raise RuntimeError("probe DDL must be rollback-only and additive")
    statements = _split_sql_statements(source)
    con = connect_from_env_file(ENV, direct_port=5432)
    con.autocommit = False
    cur = con.cursor()
    try:
        target = inspect_target(con)
        cur.execute("SELECT to_regnamespace('basketball_test')::text")
        basketball_test_schema = cur.fetchone()[0]
        if target["server_port"] != 5432 or basketball_test_schema != "basketball_test":
            raise RuntimeError(f"unexpected target: {target}")
        cur.execute("SET LOCAL lock_timeout='5s'")
        cur.execute("SET LOCAL statement_timeout='90s'")
        for statement in statements:
            cur.execute(statement)
        print("Israeli candidate created inside rollback-only transaction", flush=True)
        for preset in PRESETS:
            old = legacy(cur, preset)
            new = candidate(cur, preset)
            assert_parity(preset, old, new)
            print(f"  parity OK {preset.label:<18} rows={len(new[1])}", flush=True)

        q90 = lambda xs: sorted(xs)[max(0, int(0.9 * len(xs) + 0.999999) - 1)]
        preset = PRESETS[0]
        for view in ("summary", "four_factors"):
            legacy_times, candidate_times = [], []
            for index in range(args.runs):
                if index % 2:
                    new = candidate(cur, preset)
                    old_seconds = legacy_view(cur, preset, view)
                else:
                    old_seconds = legacy_view(cur, preset, view)
                    new = candidate(cur, preset)
                legacy_times.append(old_seconds)
                candidate_times.append(new[2])
            print(
                f"warm {args.runs} alternating {view}+minutes calls: "
                f"legacy median/p90={statistics.median(legacy_times):.3f}/{q90(legacy_times):.3f}s "
                f"candidate={statistics.median(candidate_times):.3f}/{q90(candidate_times):.3f}s",
                flush=True,
            )
        print("ROLLBACK: no Israeli database change", flush=True)
        return 0
    finally:
        con.rollback()
        cur.close()
        con.close()


if __name__ == "__main__":
    raise SystemExit(main())
