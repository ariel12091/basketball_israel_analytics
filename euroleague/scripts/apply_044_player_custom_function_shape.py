#!/usr/bin/env python
"""Gate migration 044 with parity, 15 broad samples, and narrow regressions."""

from __future__ import annotations

import argparse
import re
import statistics
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))
from euroleague_possessions.postgres_backend import (  # noqa: E402
    _split_sql_statements, connect_from_env_file, inspect_target,
)

DDL = ROOT / "sql" / "044_player_custom_function_shape.sql"
ENV = ROOT.parent / "etl" / ".Renviron"
FN = "euroleague.get_player_traditional_custom_clutch"
PRESETS = (
    ("team1 margin3 final4", "p_team_ids_csv=>'1',p_max_margin=>3,p_margin_status=>'all',p_max_time_remaining=>240"),
    ("team1 trailing final2", "p_team_ids_csv=>'1',p_max_margin=>7,p_margin_status=>'trailing',p_max_time_remaining=>120"),
    ("team1 last5 margin3", "p_team_ids_csv=>'1',p_last_n_games=>5,p_max_margin=>3,p_margin_status=>'all',p_max_time_remaining=>240"),
)
BROAD = "p_max_margin=>3,p_margin_status=>'all',p_max_time_remaining=>240"


def executable(sql: str) -> str:
    return "\n".join(x for x in sql.splitlines() if not x.strip().startswith("--"))


def statements() -> list[str]:
    ddl = DDL.read_text(encoding="utf-8")
    body = executable(ddl).upper()
    if "EUROLEAGUE SHADOW SCHEMA" not in ddl.upper():
        raise ValueError("missing EuroLeague marker")
    if re.search(r"\bBASKETBALL(?:_TEST)?\s*\.", body):
        raise ValueError("Israeli schema reference")
    if re.search(r"\b(?:DROP|TRUNCATE|CASCADE)\b", body):
        raise ValueError("migration must be additive")
    return [s for s in _split_sql_statements(ddl)
            if " ".join(executable(s).upper().split()).rstrip(";") not in {"BEGIN", "COMMIT"}]


def run(cur, extra: str) -> tuple[list[str], float]:
    args = "p_competition=>'E',p_game_year=>2025," + extra
    started = time.perf_counter()
    cur.execute(f"SELECT row_to_json(x)::text FROM {FN}({args}) x ORDER BY 1")
    return [r[0] for r in cur.fetchall()], time.perf_counter() - started


def sample(cur, extra: str, count: int) -> tuple[list[str], list[float]]:
    rows: list[str] = []
    timings: list[float] = []
    for _ in range(count):
        current, elapsed = run(cur, extra)
        if not rows:
            rows = current
        elif current != rows:
            raise RuntimeError("repeated query returned unstable rows")
        timings.append(elapsed)
    return rows, timings


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    con = connect_from_env_file(ENV, direct_port=5432)
    cur = con.cursor()
    try:
        target = inspect_target(con)
        if target["euroleague_schema"] != "euroleague":
            raise RuntimeError("wrong target: " + str(target))
        cur.execute("SELECT pg_get_functiondef(%s::regprocedure)", (
            "euroleague.get_player_traditional_custom_clutch(text,integer,date,date,text,text,text,text,text,text,integer,text,integer,text,integer,boolean,integer,integer,integer)",
        ))
        if "lineup_identities AS" in cur.fetchone()[0]:
            raise RuntimeError("migration 044 already applied")
        cur.execute("BEGIN")
        cur.execute("SET LOCAL lock_timeout='5s'")
        cur.execute("SET LOCAL statement_timeout='30s'")
        print("mode=" + ("EXECUTE" if args.apply else "DRY RUN"))
        legacy = []
        print("legacy bounded samples:")
        for label, preset in PRESETS:
            rows, timings = sample(cur, preset, 5)
            legacy.append((label, preset, rows, statistics.median(timings)))
            print(f"  {label:<25} rows={len(rows):<3} median={statistics.median(timings):.3f}s")
        for statement in statements():
            cur.execute(statement)
        print("candidate bounded parity/regression:")
        for label, preset, old_rows, old_median in legacy:
            rows, timings = sample(cur, preset, 5)
            median = statistics.median(timings)
            if rows != old_rows:
                raise RuntimeError(label + " parity failed")
            allowed = max(old_median * 1.10, old_median + 0.100)
            print(f"  OK {label:<22} old={old_median:.3f}s new={median:.3f}s limit={allowed:.3f}s")
            if median > allowed:
                raise RuntimeError(label + " narrow regression gate failed")
        print("candidate broad 15-sample gate:")
        _, cold = run(cur, BROAD)
        _, warm = sample(cur, BROAD, 15)
        median = statistics.median(warm)
        p90 = sorted(warm)[13]
        print(f"  cold={cold:.3f}s")
        print("  warm=" + ",".join(f"{x:.3f}" for x in warm))
        print(f"  median={median:.3f}s p90={p90:.3f}s")
        if median > 0.550:
            raise RuntimeError("broad median exceeds revised 550 ms gate")
        cur.execute("SELECT has_function_privilege('app_readonly', %s, 'EXECUTE')", (
            "euroleague.get_player_traditional_custom_clutch(text,integer,date,date,text,text,text,text,text,text,integer,text,integer,text,integer,boolean,integer,integer,integer)",
        ))
        if not cur.fetchone()[0]:
            raise RuntimeError("app_readonly lacks EXECUTE")
        cur.execute("COMMIT" if args.apply else "ROLLBACK")
        print("migration 044 applied" if args.apply else "DRY RUN passed and rolled back")
        return 0
    except Exception:
        try:
            cur.execute("ROLLBACK")
        except Exception:
            pass
        raise
    finally:
        cur.close()
        con.close()


if __name__ == "__main__":
    raise SystemExit(main())
