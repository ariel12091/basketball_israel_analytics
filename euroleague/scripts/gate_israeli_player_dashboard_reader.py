#!/usr/bin/env python
"""Rollback-only parity/performance gate for the Israeli combined FF reader."""
from __future__ import annotations
import argparse, json, statistics, sys, time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "euroleague" / "src"))
from euroleague_possessions.postgres_backend import _split_sql_statements, connect_from_env_file  # noqa:E402

DDL = ROOT / "sql" / "functions" / "four_factors_dashboard_compute.sql"
ENV = ROOT / "etl" / ".Renviron"
FF_BASE = "p_game_year=>2026,p_start_date=>DATE '2025-10-01',p_end_date=>DATE '2026-07-01'"
ON_BASE = "p_game_year=>'2026',p_start_date=>DATE '2025-10-01',p_end_date=>DATE '2026-07-01',p_team_ids=>NULL,p_min_all=>0,p_min_on=>0,p_min_net=>-1000000000"
_EXTRAS = (
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
)
PRESETS = tuple((label, extra, extra) for label, extra in _EXTRAS)
# Presets that must return rows: an empty result compares equal on both sides
# and would pass without exercising anything.
MUST_RETURN_ROWS = tuple(label for label, _ in _EXTRAS if label != "empty")
RATING = ("Net RTG Diff", "Off ON Diff", "Def ON Diff", "minutes")

def call(cur, fn, base, extra):
    args = base + (("," + extra) if extra else "")
    started=time.perf_counter(); cur.execute(f"SELECT * FROM basketball_test.{fn}({args})")
    cols=[d.name for d in cur.description]; rows=[tuple(x) for x in cur.fetchall()]
    return cols,rows,time.perf_counter()-started

def old(cur, ff_extra, on_extra):
    fc,fr,ft=call(cur,"four_factors_compute",FF_BASE,ff_extra)
    oc,orr,ot=call(cur,"onoff_compute",ON_BASE,on_extra)
    om={(x[oc.index('player_id')],x[oc.index('team_id')]):x for x in orr}
    rows=[]
    for x in fr:
        y=om.get((x[fc.index('player_id')],x[fc.index('team_id')]))
        rows.append(x+((None,)*4 if y is None else tuple(y[oc.index(k)] for k in RATING)))
    return fc+list(RATING),rows,ft+ot

def canon(value):
    c,r,_=value; return c,{(x[0],x[1]):x for x in r}

def main():
    parser=argparse.ArgumentParser(description=__doc__); parser.add_argument('--apply',action='store_true'); args=parser.parse_args()
    con=connect_from_env_file(ENV,direct_port=5432); con.autocommit=False; cur=con.cursor()
    try:
        cur.execute("SET LOCAL statement_timeout='60s'")
        for s in _split_sql_statements(DDL.read_text(encoding='utf-8')): cur.execute(s)
        for label,fe,oe in PRESETS:
            before=old(cur,fe,oe); cc,cr,ct=call(cur,"four_factors_dashboard_compute",FF_BASE,fe)
            expected=canon(before); actual=(cc,{(x[0],x[1]):x for x in cr})
            if expected!=actual:
                print(f"columns_equal={expected[0]==actual[0]} rows={len(expected[1])}/{len(actual[1])}")
                keys=sorted(set(expected[1])|set(actual[1])); key=next(k for k in keys if expected[1].get(k)!=actual[1].get(k))
                print("first_key=",key,"expected=",expected[1].get(key),"candidate=",actual[1].get(key))
                raise RuntimeError(label+": parity failed")
            if label in MUST_RETURN_ROWS and not cr:
                raise RuntimeError(label+": preset returned 0 rows; parity is vacuous")
            print(f"OK {label:<18} rows={len(cr)}")
        old_t=[]; new_t=[]
        for _ in range(5): old_t.append(old(cur,'','')[2]); new_t.append(call(cur,"four_factors_dashboard_compute",FF_BASE,'')[2])
        a=statistics.median(old_t); b=statistics.median(new_t)
        print(f"broad median two-call={a:.3f}s combined={b:.3f}s")
        if b>min(a*.90,a-.100): raise RuntimeError("latency gate failed")
        if args.apply:
            con.commit(); print("COMMITTED Israeli combined reader")
        else:
            con.rollback(); print("ROLLBACK gate passed; no persistent database change")
        return 0
    except Exception: con.rollback(); raise
    finally: cur.close(); con.close()
if __name__=='__main__': raise SystemExit(main())
