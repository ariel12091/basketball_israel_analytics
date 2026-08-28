#!/usr/bin/env python
"""Baseline, gate, and (only on request) apply migration 045 for Tab 8.

Scope: euroleague.onoff_compute() and euroleague.four_factors_compute().

Modes
  --baseline            read-only report: target, definition hashes, sizes,
                        settings, privileges, full preset matrix, plans.
  --candidate {A,B,C}   capture baselines outside a transaction, apply the
                        candidate DDL inside one, run every gate, ROLL BACK.
  --candidate X --apply the only commit path; runs the identical gates first.

Baselines are always captured before any candidate DDL and held in memory, so
no comparison is ever made between two post-change executions.

Plan evidence: the analytical query lives inside a plpgsql body, so EXPLAIN
cannot expose its inner nodes. PostgreSQL does attribute the nested buffer and
temp counters to the outer Function Scan node, which is what this script gates
on: shared-buffer traffic (a full fact scan is ~25k blocks and a narrow probe a
few hundred) and temp blocks (any new on-disk sort).
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
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

ENV = ROOT.parent / "etl" / ".Renviron"
CANDIDATE_DIR = ROOT / "sql" / "candidates"
MIGRATION = ROOT / "sql" / "045_tab8_query_shape.sql"

FACT = "euroleague.player_four_factors_by_game"
INDEX_NAME = "euroleague_pff_game_team_idx"
INDEX_DDL = (
    "CREATE INDEX IF NOT EXISTS " + INDEX_NAME + " ON " + FACT + " (game_id, team_id)"
)

ONOFF_SIG = (
    "euroleague.onoff_compute(text,integer,date,date,text,text,text,text,text,"
    "text,integer,text,integer,integer,integer,integer,integer,integer,integer,"
    "numeric,integer,integer)"
)
FF_SIG = (
    "euroleague.four_factors_compute(text,integer,date,date,text,text,text,"
    "text,text,text,integer,text,integer,integer,integer,integer,integer,"
    "integer,integer)"
)
FUNCTIONS = {"onoff": ONOFF_SIG, "ff": FF_SIG}

# onoff_compute has a deterministic ORDER BY, so its row ORDER is part of the
# contract and is compared as a sequence. four_factors_compute has no ORDER BY,
# so its row order is a plan artefact rather than a contract: it is compared as
# a multiset. Every value, NULL, type rendering and row count is compared for
# both functions either way.
ORDERED = {"onoff": True, "ff": False}

PARAM_TYPES = {
    "p_competition": "text", "p_game_year": "int",
    "p_start_date": "date", "p_end_date": "date",
    "p_team_ids_csv": "text", "p_phase_csv": "text", "p_opp_ids_csv": "text",
    "p_home_away": "text", "p_outcome": "text",
    "p_opp_rank_side": "text", "p_opp_rank_n": "int", "p_opp_rank_metric": "text",
    "p_min_gn": "int", "p_max_gn": "int", "p_last_n_games": "int",
    "p_num_starters_off_min": "int", "p_num_starters_off_max": "int",
    "p_num_starters_def_min": "int", "p_num_starters_def_max": "int",
    "p_min_net": "numeric", "p_min_all": "int", "p_min_on": "int",
}
ONOFF_ONLY = {"p_min_net", "p_min_all", "p_min_on"}

BASE = {"p_competition": "E", "p_game_year": 2025}

# (label, extra parameters, warm sample count). Sample count 15 marks the
# presets the spec gates on latency; the rest only guard against regression.
PRESETS = [
    ("broad season",        {}, 15),
    # What Tab 8 actually sends: the Shiny date inputs are always populated from
    # euro_season_date_bounds(), so the app's broad call carries an explicit
    # full-season window rather than NULL dates.
    ("broad app dates",     {"p_start_date": "2025-09-01", "p_end_date": "2026-07-01"}, 15),
    ("last 10",             {"p_last_n_games": 10}, 15),
    ("one team",            {"p_team_ids_csv": "1"}, 15),
    ("one phase RS",        {"p_phase_csv": "RS"}, 5),
    ("bounded dates",       {"p_start_date": "2025-10-01", "p_end_date": "2025-12-31"}, 5),
    ("one opponent",        {"p_opp_ids_csv": "2"}, 5),
    ("home",                {"p_home_away": "home"}, 5),
    ("away",                {"p_home_away": "away"}, 5),
    ("win",                 {"p_outcome": "win"}, 5),
    ("loss",                {"p_outcome": "loss"}, 5),
    ("opp rank top5 off",   {"p_opp_rank_side": "top", "p_opp_rank_n": 5,
                             "p_opp_rank_metric": "off"}, 5),
    ("opp rank top5 def",   {"p_opp_rank_side": "top", "p_opp_rank_n": 5,
                             "p_opp_rank_metric": "def"}, 5),
    ("opp rank bottom5 net", {"p_opp_rank_side": "bottom", "p_opp_rank_n": 5,
                              "p_opp_rank_metric": "net"}, 5),
    ("round range 5-15",    {"p_min_gn": 5, "p_max_gn": 15}, 5),
    ("starters off min 5",  {"p_num_starters_off_min": 5}, 5),
    ("starters off max 3",  {"p_num_starters_off_max": 3}, 5),
    ("starters def min 5",  {"p_num_starters_def_min": 5}, 5),
    ("starters def max 3",  {"p_num_starters_def_max": 3}, 5),
    ("sparse both starters", {"p_num_starters_off_min": 5,
                              "p_num_starters_def_min": 5}, 5),
    ("team+lastN+starters", {"p_team_ids_csv": "1", "p_last_n_games": 5,
                             "p_num_starters_off_min": 4,
                             "p_num_starters_def_max": 4}, 5),
    ("min poss thresholds", {"p_min_all": 100, "p_min_on": 200}, 5),
    ("min net floor",       {"p_min_net": 0}, 5),
    ("eurocup broad",       {"p_competition": "U"}, 5),
    ("empty result",        {"p_min_gn": 999}, 5),
]

BROAD = "broad season"
BROAD_LABELS = ("broad season", "broad app dates")
NARROW_PROBE = ("last 10", "one team", "team+lastN+starters", "one opponent")
# Performance gates, per Addendum A of the design document: no worse than the
# Israeli companion, using the spec's own max(10%, 100 ms) tolerance.
#
# Companion measured 2026-08-28 on direct port 5432, same method, comparable
# fact table (basketball_test.player_four_factors_by_game, 213 MB / 736,403 rows
# vs euroleague 272 MB / 766,146). Broad app-date window, warm median of 15:
#
#   basketball_test.onoff_compute          1.745s median  1.797s p90  47,835 buf
#   basketball_test.four_factors_compute   1.583s median  1.598s p90  41,084 buf
#   basketball_test.onoff_compute last 10  0.549s median  0.608s p90  15,499 buf
#
# The companion already reads its base fact joined to the filtered game set on
# (game_id, team_id) with no context view -- the shape this migration adopts.
COMPANION = {
    "onoff": {"broad_median": 1.745, "broad_p90": 1.797},
    "ff": {"broad_median": 1.583, "broad_p90": 1.598},
}
COMPANION_LAST10_MEDIAN = 0.549


# Same-run companion measurement. Run-to-run variance on this shared instance
# is ~15%, so gating a EuroLeague median against a companion median captured in
# a different session is not sound in either direction. The companion is
# therefore re-measured in the same connection, immediately after the EuroLeague
# baseline, and the constants above are only the recorded reference and the
# fallback if the companion cannot be read.
#
# This is a read of the Israeli schema by the BENCHMARK, not by the shipped
# functions: migration 045 adds no Israeli-schema dependency, and the candidate
# SQL parser still rejects any basketball_test reference in DDL.
COMPANION_CALLS = {
    "onoff": (
        "SELECT * FROM basketball_test.onoff_compute("
        "p_start_date => %s::date, p_end_date => %s::date, p_team_ids => NULL,"
        "p_min_all => 0, p_min_on => 0, p_min_net => %s::numeric,"
        "p_game_year => %s::text)",
        ["2025-10-01", "2026-07-01", -999, "2026"],
    ),
    "ff": (
        "SELECT * FROM basketball_test.four_factors_compute("
        "p_game_year => %s::int, p_start_date => %s::date, p_end_date => %s::date)",
        [2026, "2025-10-01", "2026-07-01"],
    ),
}
COMPANION_LAST10_CALL = (
    "SELECT * FROM basketball_test.onoff_compute("
    "p_start_date => %s::date, p_end_date => %s::date, p_team_ids => NULL,"
    "p_min_all => 0, p_min_on => 0, p_min_net => %s::numeric,"
    "p_game_year => %s::text, p_last_n_games => 10)",
    ["2025-10-01", "2026-07-01", -999, "2026"],
)


def measure_companion(cur, samples=15):
    """Time the Israeli companion's broad call in this same session.

    Returns {key: {"broad_median", "broad_p90"}, "last10": median} or None if
    the companion cannot be read, in which case the pinned constants stand.
    """
    def timed(sql, params):
        cur.execute(sql, params)
        cur.fetchall()
        timings = []
        for _ in range(samples):
            started = time.perf_counter()
            cur.execute(sql, params)
            rows = cur.fetchall()
            timings.append(time.perf_counter() - started)
        return len(rows), statistics.median(timings), p90_of(timings)

    try:
        measured = {}
        for key, (sql, params) in COMPANION_CALLS.items():
            count, median, p90 = timed(sql, params)
            if not count:
                raise RuntimeError("companion " + key + " returned no rows")
            measured[key] = {"broad_median": median, "broad_p90": p90}
            print("  companion %-6s broad app dates rows=%-4d median=%6.3fs p90=%6.3fs"
                  % (key, count, median, p90))
        count, median, _ = timed(*COMPANION_LAST10_CALL)
        measured["last10"] = median
        print("  companion %-6s last 10         rows=%-4d median=%6.3fs"
              % ("onoff", count, median))
        return measured
    except Exception as error:
        print("  companion unavailable (%s); falling back to the pinned "
              "Addendum A constants" % str(error)[:90])
        return None


def with_tolerance(value):
    """The spec's own no-regression tolerance: max(10%, 100 ms)."""
    return max(value * 1.10, value + 0.100)


GATE_BROAD_MEDIAN = {k: with_tolerance(v["broad_median"]) for k, v in COMPANION.items()}
GATE_BROAD_P90 = {k: with_tolerance(v["broad_p90"]) for k, v in COMPANION.items()}
GATE_LAST10_MEDIAN = with_tolerance(COMPANION_LAST10_MEDIAN)
FULL_SCAN_BLOCKS = 20000


# --------------------------------------------------------------------- calls


def build_call(signature, preset):
    """Return 'fn(named => %s::type, ...)' plus its parameter values."""
    merged = dict(BASE)
    merged.update(preset)
    if signature == FF_SIG:
        merged = {k: v for k, v in merged.items() if k not in ONOFF_ONLY}
    name = signature.split("(")[0]
    parts, values = [], []
    for key, value in merged.items():
        parts.append(key + " => %s::" + PARAM_TYPES[key])
        values.append(value)
    return name + "(" + ", ".join(parts) + ")", values


def run(cur, signature, preset):
    call, values = build_call(signature, preset)
    started = time.perf_counter()
    cur.execute("SELECT row_to_json(x)::text FROM " + call + " x", values)
    rows = [r[0] for r in cur.fetchall()]
    return rows, time.perf_counter() - started


def sample(cur, signature, preset, count):
    rows, cold = run(cur, signature, preset)
    timings = []
    for _ in range(count):
        current, elapsed = run(cur, signature, preset)
        if current != rows:
            raise RuntimeError("repeated call returned unstable rows")
        timings.append(elapsed)
    return rows, cold, timings


def explain(cur, signature, preset):
    call, values = build_call(signature, preset)
    cur.execute(
        "EXPLAIN (ANALYZE, BUFFERS, WAL, SETTINGS, FORMAT JSON) SELECT * FROM " + call,
        values)
    root = cur.fetchone()[0][0]
    node = root["Plan"]
    return {
        "shared_hit": node.get("Shared Hit Blocks", 0),
        "shared_read": node.get("Shared Read Blocks", 0),
        "temp_read": node.get("Temp Read Blocks", 0),
        "temp_written": node.get("Temp Written Blocks", 0),
        "node_ms": node.get("Actual Total Time", 0.0),
        "planning_ms": root.get("Planning Time", 0.0),
        "execution_ms": root.get("Execution Time", 0.0),
        "settings": root.get("Settings", {}),
    }


# --------------------------------------------------------------------- gates
# Pure and importable, so the test suite can prove each one rejects a bad
# candidate without touching the database.


def compare_rows(label, ordered, before, after):
    if len(before) != len(after):
        raise RuntimeError(
            label + ": row count changed " + str(len(before)) + " -> " + str(len(after)))
    lhs, rhs = (before, after) if ordered else (sorted(before), sorted(after))
    for index, (old, new) in enumerate(zip(lhs, rhs)):
        if old != new:
            raise RuntimeError(
                label + ": row " + str(index) + " differs\n  before " + old +
                "\n  after  " + new)


def check_regression(label, before, after):
    allowed = max(before * 1.10, before + 0.100)
    if after > allowed:
        raise RuntimeError(
            "%s: warm median %.3fs exceeds allowed %.3fs (baseline %.3fs)"
            % (label, after, allowed, before))
    return allowed


def check_absolute(label, median, p90, median_gate, p90_gate=None):
    if median > median_gate:
        raise RuntimeError(
            "%s: warm median %.3fs exceeds gate %.3fs" % (label, median, median_gate))
    if p90_gate is not None and p90 is not None and p90 > p90_gate:
        raise RuntimeError(
            "%s: warm p90 %.3fs exceeds gate %.3fs" % (label, p90, p90_gate))


def check_plan(label, before, after, narrow):
    buffers_before = before["shared_hit"] + before["shared_read"]
    buffers_after = after["shared_hit"] + after["shared_read"]
    if buffers_after > buffers_before:
        raise RuntimeError(
            label + ": shared buffers rose " + str(buffers_before) + " -> " +
            str(buffers_after))
    if after["temp_written"] > before["temp_written"]:
        raise RuntimeError(
            label + ": new on-disk sort, temp written " +
            str(before["temp_written"]) + " -> " + str(after["temp_written"]))
    if narrow and buffers_after >= FULL_SCAN_BLOCKS:
        raise RuntimeError(
            label + ": narrow preset touched " + str(buffers_after) +
            " blocks, which is a full fact scan rather than a "
            "(game_id, team_id) probe")


def p90_of(timings):
    """Nearest-rank p90: the smallest sample at or above the 90th percentile."""
    ordered = sorted(timings)
    index = max(0, min(len(ordered) - 1, math.ceil(0.9 * len(ordered)) - 1))
    return ordered[index]


# ------------------------------------------------------------------ candidate


def executable(sql):
    return "\n".join(x for x in sql.splitlines() if not x.strip().startswith("--"))


def candidate_statements(path):
    text = path.read_text(encoding="utf-8")
    body = executable(text).upper()
    if "EUROLEAGUE SHADOW SCHEMA" not in text.upper():
        raise ValueError(path.name + ": missing EuroLeague safety marker")
    if re.search(r"\bBASKETBALL(?:_TEST)?\s*\.", body):
        raise ValueError(path.name + ": Israeli schema reference")
    if re.search(r"\b(?:DROP|TRUNCATE|CASCADE|ALTER\s+TABLE|CREATE\s+SCHEMA|"
                 r"ALTER\s+SCHEMA|OWNER\s+TO|GRANT|REVOKE)\b", body):
        raise ValueError(path.name + ": migration must be additive and non-destructive")
    kept = []
    for statement in _split_sql_statements(text):
        normalised = " ".join(executable(statement).upper().split()).rstrip(";")
        if normalised in {"BEGIN", "COMMIT", ""}:
            continue
        kept.append(statement)
    return kept


WORK_MEM = "16MB"


def work_mem_statements():
    return ["ALTER FUNCTION " + signature + " SET work_mem = '" + WORK_MEM + "'"
            for signature in (ONOFF_SIG, FF_SIG)]


def candidate_plan(name):
    """Return (DDL statements, human description) for a candidate.

    A  = direct fact source + the (game_id, team_id) index.
    B  = A + one aggregation at (player_id, team_id).
    C  = B + function-local work_mem.
    AC = A + function-local work_mem. This is what the plan means by "from the
         best of A/B, add work_mem": A won, and A still spills ~9k/17k temp
         blocks on the broad app-dates call.
    """
    if name == "A":
        return ([INDEX_DDL] +
                candidate_statements(CANDIDATE_DIR / "045a_direct_fact.sql"),
                ["index (game_id, team_id)", "direct fact source"])
    if name == "AC":
        return ([INDEX_DDL] +
                candidate_statements(CANDIDATE_DIR / "045a_direct_fact.sql") +
                work_mem_statements(),
                ["index (game_id, team_id)", "direct fact source",
                 "function-local work_mem " + WORK_MEM])
    if name in {"B", "C"}:
        ddl = ([INDEX_DDL] +
               candidate_statements(CANDIDATE_DIR / "045b_single_aggregate.sql"))
        notes = ["index (game_id, team_id)", "direct fact source",
                 "single aggregation"]
        if name == "C":
            ddl.extend(work_mem_statements())
            notes.append("function-local work_mem " + WORK_MEM)
        return ddl, notes
    if name == "MIGRATION":
        return candidate_statements(MIGRATION), ["sql/045_tab8_query_shape.sql"]
    raise ValueError("unknown candidate " + name)


# ---------------------------------------------------------------- reporting


def function_state(cur):
    state = {}
    for key, signature in FUNCTIONS.items():
        cur.execute("SELECT pg_get_functiondef(%s::regprocedure)", (signature,))
        definition = cur.fetchone()[0]
        cur.execute("SELECT has_function_privilege('app_readonly', %s, 'EXECUTE')",
                    (signature,))
        state[key] = {
            "sha256": hashlib.sha256(definition.encode()).hexdigest(),
            "reads_view": "player_game_context" in definition,
            "reads_fact": "player_four_factors_by_game c" in definition,
            "security_definer": "SECURITY DEFINER" in definition,
            "app_readonly_execute": cur.fetchone()[0],
        }
    return state


def index_state(cur):
    cur.execute(
        "SELECT i.relname, pg_get_indexdef(i.oid), pg_relation_size(i.oid), "
        "       x.indisvalid, x.indisready "
        "FROM pg_index x JOIN pg_class i ON i.oid = x.indexrelid "
        "JOIN pg_class t ON t.oid = x.indrelid "
        "JOIN pg_namespace n ON n.oid = t.relnamespace "
        "WHERE n.nspname = 'euroleague' AND t.relname = %s ORDER BY 1",
        (FACT.split(".")[1],))
    return [{"name": r[0], "definition": r[1], "bytes": r[2],
             "valid": r[3], "ready": r[4]} for r in cur.fetchall()]


def report_environment(cur):
    target = inspect_target(cur.connection)
    if target["euroleague_schema"] != "euroleague":
        raise RuntimeError("wrong target: " + str(target))
    if target["server_port"] != 5432:
        raise RuntimeError("candidate gating requires direct port 5432: " + str(target))
    cur.execute("SELECT version(), current_setting('work_mem'), current_setting('jit')")
    version, work_mem, jit = cur.fetchone()
    cur.execute("SELECT pg_total_relation_size(%s::regclass), "
                "pg_relation_size(%s::regclass), "
                "(SELECT reltuples::bigint FROM pg_class WHERE oid = %s::regclass)",
                (FACT, FACT, FACT))
    total, heap, tuples = cur.fetchone()
    print("target:", target)
    print("server:", version.split(" on ")[0], "| work_mem", work_mem, "| jit", jit)
    print("fact: %s total=%.0fMB heap=%.0fMB rows=%d"
          % (FACT, total / 1e6, heap / 1e6, tuples))
    indexes = index_state(cur)
    for row in indexes:
        print("  index %-42s %6.1fMB valid=%s ready=%s"
              % (row["name"], row["bytes"] / 1e6, row["valid"], row["ready"]))
        if not row["valid"] or not row["ready"]:
            raise RuntimeError("invalid/unready index shell present: " + row["name"])
    state = function_state(cur)
    for key, info in state.items():
        print("  %-6s sha256=%s view=%s fact=%s definer=%s execute=%s"
              % (key, info["sha256"][:16], info["reads_view"], info["reads_fact"],
                 info["security_definer"], info["app_readonly_execute"]))
        if not info["app_readonly_execute"]:
            raise RuntimeError(key + ": app_readonly lost EXECUTE")
    return {"target": target, "work_mem": work_mem, "jit": jit,
            "functions": state, "indexes": indexes}


def assert_no_active_publication(cur):
    """Refuse to take the fact table's SHARE lock while a load run is writing."""
    cur.execute(
        "SELECT pid, state, wait_event_type, "
        "       EXTRACT(epoch FROM now() - xact_start)::int AS xact_seconds, "
        "       left(regexp_replace(query, '\\s+', ' ', 'g'), 120) "
        "FROM pg_stat_activity "
        "WHERE pid <> pg_backend_pid() AND datname = current_database() "
        "  AND state <> 'idle' AND xact_start IS NOT NULL "
        "ORDER BY xact_start")
    busy = cur.fetchall()
    for row in busy:
        print("  other session pid=%s state=%s xact=%ss %s" % (row[0], row[1], row[3], row[4]))
    cur.execute(
        "SELECT count(*) FROM pg_locks l JOIN pg_class c ON c.oid = l.relation "
        "JOIN pg_namespace n ON n.oid = c.relnamespace "
        "WHERE n.nspname = 'euroleague' AND l.pid <> pg_backend_pid() "
        "  AND l.mode IN ('RowExclusiveLock', 'ShareRowExclusiveLock', "
        "                 'ExclusiveLock', 'AccessExclusiveLock')")
    writers = cur.fetchone()[0]
    if writers:
        raise RuntimeError(
            "%d write lock(s) held on euroleague relations; a publication or load "
            "run is active. Wait for it to finish before taking the index lock."
            % writers)
    print("  no writer holds a lock on any euroleague relation")


def capture_baseline(cur, expect_hashes):
    state = function_state(cur)
    for key, wanted in (expect_hashes or {}).items():
        if state[key]["sha256"] != wanted:
            raise RuntimeError(
                key + ": live definition " + state[key]["sha256"] +
                " does not match the expected pre-apply hash " + wanted +
                "; target is stale")
    baseline = {}
    for key, signature in FUNCTIONS.items():
        for label, preset, count in PRESETS:
            rows, cold, timings = sample(cur, signature, preset, count)
            baseline[(key, label)] = {
                "rows": rows, "cold": cold, "timings": timings,
                "median": statistics.median(timings), "p90": p90_of(timings),
            }
            print("  %-6s %-22s rows=%-4d cold=%6.3fs median=%6.3fs p90=%6.3fs"
                  % (key, label, len(rows), cold,
                     baseline[(key, label)]["median"], baseline[(key, label)]["p90"]))
        for label in BROAD_LABELS + NARROW_PROBE:
            preset = next(p for lbl, p, _ in PRESETS if lbl == label)
            baseline[(key, label)]["plan"] = explain(cur, signature, preset)
    missing = [lbl for lbl, _, _ in PRESETS if not any(k[1] == lbl for k in baseline)]
    if missing:
        raise RuntimeError("preset matrix incomplete: " + str(missing))
    if not any(len(v["rows"]) == 0 for v in baseline.values()):
        raise RuntimeError("preset matrix never produced an empty result")
    return baseline


# --------------------------------------------------------------------- main


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--baseline", action="store_true",
                        help="read-only report; performs no DDL at all")
    parser.add_argument("--candidate",
                        choices=["A", "B", "C", "AC", "MIGRATION"])
    parser.add_argument("--apply", action="store_true",
                        help="commit the candidate; the only non-rollback path")
    parser.add_argument("--no-companion", action="store_true",
                        help="skip the same-session Israeli companion "
                             "measurement and gate on the pinned constants")
    parser.add_argument("--presets",
                        help="comma-separated preset labels to restrict the "
                             "matrix to; 'empty result' is always included")
    parser.add_argument("--expect-onoff-sha256")
    parser.add_argument("--expect-ff-sha256")
    args = parser.parse_args()
    if args.apply and not args.candidate:
        parser.error("--apply requires --candidate")
    if not args.baseline and not args.candidate:
        parser.error("choose --baseline or --candidate")

    if args.presets:
        wanted = {x.strip() for x in args.presets.split(",") if x.strip()}
        wanted.add("empty result")
        known = {label for label, _, _ in PRESETS}
        unknown = wanted - known
        if unknown:
            parser.error("unknown preset(s): " + ", ".join(sorted(unknown)))
        PRESETS[:] = [row for row in PRESETS if row[0] in wanted]
        globals()["BROAD_LABELS"] = tuple(x for x in BROAD_LABELS if x in wanted)
        globals()["NARROW_PROBE"] = tuple(x for x in NARROW_PROBE if x in wanted)
        print("preset matrix restricted to: " + ", ".join(sorted(wanted)))

    expect = {}
    if args.expect_onoff_sha256:
        expect["onoff"] = args.expect_onoff_sha256
    if args.expect_ff_sha256:
        expect["ff"] = args.expect_ff_sha256

    con = connect_from_env_file(ENV, direct_port=5432)
    cur = con.cursor()
    opened = False
    try:
        cur.execute("SET statement_timeout = '30s'")
        print("=" * 78)
        environment = report_environment(cur)
        print("=" * 78)

        if args.baseline:
            print("MODE: BASELINE (read-only, no DDL)")
            capture_baseline(cur, expect)
            print("baseline complete; nothing was changed")
            return 0

        ddl, notes = candidate_plan(args.candidate)
        mode = "APPLY (commits)" if args.apply else "DRY RUN (rolls back)"
        print("MODE: candidate " + args.candidate + " -- " + mode)
        print("candidate: " + " + ".join(notes))
        print("publication pre-flight:")
        assert_no_active_publication(cur)
        print("baseline (captured before any DDL, outside the transaction):")
        baseline = capture_baseline(cur, expect)
        companion = None
        if not args.no_companion:
            print("Israeli companion, same session, before any DDL:")
            companion = measure_companion(cur)

        cur.execute("BEGIN")
        opened = True
        cur.execute("SET LOCAL lock_timeout = '5s'")
        cur.execute("SET LOCAL statement_timeout = '120s'")
        build_started = time.perf_counter()
        for statement in ddl:
            cur.execute(statement)
        print("candidate DDL applied in %.1fs" % (time.perf_counter() - build_started))
        cur.execute("SET LOCAL statement_timeout = '30s'")

        cur.execute("SELECT pg_relation_size(%s::regclass)",
                    ("euroleague." + INDEX_NAME,))
        print("candidate index %s: %.1fMB" % (INDEX_NAME, cur.fetchone()[0] / 1e6))

        # Every gate is recorded rather than raised on the spot, so one failing
        # preset cannot abort the run and cost the evidence for the rest. The
        # collected failures are raised together below, before any COMMIT.
        failures = []

        def gate(check, *args, **kwargs):
            try:
                return check(*args, **kwargs)
            except RuntimeError as error:
                failures.append(str(error))
                return None

        print("parity / regression:")
        for key, signature in FUNCTIONS.items():
            for label, preset, count in PRESETS:
                before = baseline[(key, label)]
                rows, cold, timings = sample(cur, signature, preset, count)
                median = statistics.median(timings)
                parity = gate(compare_rows, key + " " + label, ORDERED[key],
                              before["rows"], rows) is None and failures
                allowed = gate(check_regression, key + " " + label,
                               before["median"], median)
                print("  %-4s %-6s %-22s rows=%-4d cold=%6.3fs median=%6.3fs "
                      "(was %6.3fs, limit %s)"
                      % ("FAIL" if allowed is None or parity else "ok",
                         key, label, len(rows), cold, median, before["median"],
                         "%6.3fs" % allowed if allowed is not None else "exceeded"))
                before["after"] = {"cold": cold, "median": median,
                                   "p90": p90_of(timings), "timings": timings}
            for label in BROAD_LABELS + NARROW_PROBE:
                preset = next(p for lbl, p, _ in PRESETS if lbl == label)
                after_plan = explain(cur, signature, preset)
                before_plan = baseline[(key, label)]["plan"]
                gate(check_plan, key + " " + label, before_plan, after_plan,
                     narrow=(label not in BROAD_LABELS))
                print("  PLAN %-6s %-22s buffers %d -> %d  temp %d -> %d  node %.0f -> %.0f ms"
                      % (key, label,
                         before_plan["shared_hit"] + before_plan["shared_read"],
                         after_plan["shared_hit"] + after_plan["shared_read"],
                         before_plan["temp_written"], after_plan["temp_written"],
                         before_plan["node_ms"], after_plan["node_ms"]))
                baseline[(key, label)]["after_plan"] = after_plan

        broad_median_gate = dict(GATE_BROAD_MEDIAN)
        broad_p90_gate = dict(GATE_BROAD_P90)
        last10_gate = GATE_LAST10_MEDIAN
        if companion:
            broad_median_gate = {k: with_tolerance(companion[k]["broad_median"])
                                 for k in FUNCTIONS}
            broad_p90_gate = {k: with_tolerance(companion[k]["broad_p90"])
                              for k in FUNCTIONS}
            last10_gate = with_tolerance(companion["last10"])
            print("absolute gates (vs the companion measured in this session):")
        else:
            print("absolute gates (vs the pinned Addendum A constants):")
        for key in FUNCTIONS:
            for label in BROAD_LABELS:
                broad = baseline[(key, label)]["after"]
                before = len(failures)
                gate(check_absolute, key + " " + label, broad["median"],
                     broad["p90"], broad_median_gate[key], broad_p90_gate[key])
                print("  %-4s %-6s %-22s median=%.3fs (gate %.3fs) "
                      "p90=%.3fs (gate %.3fs)"
                      % ("FAIL" if len(failures) > before else "ok", key, label,
                         broad["median"], broad_median_gate[key],
                         broad["p90"], broad_p90_gate[key]))
            if (key, "last 10") not in baseline:
                continue
            last10 = baseline[(key, "last 10")]["after"]
            before = len(failures)
            gate(check_absolute, key + " last 10", last10["median"], None,
                 last10_gate)
            print("  %-4s %-6s %-22s median=%.3fs (gate %.3fs)"
                  % ("FAIL" if len(failures) > before else "ok",
                     key, "last 10", last10["median"], last10_gate))

        after_state = function_state(cur)
        for key, info in after_state.items():
            if not info["app_readonly_execute"]:
                raise RuntimeError(key + ": candidate dropped app_readonly EXECUTE")
            if info["security_definer"] != environment["functions"][key]["security_definer"]:
                raise RuntimeError(key + ": SECURITY mode changed")
            if info["reads_view"]:
                raise RuntimeError(key + ": still reads player_game_context")
            if not info["reads_fact"]:
                raise RuntimeError(key + ": does not read the base fact")
        print("privileges and contracts unchanged")

        if failures:
            raise RuntimeError(
                "%d gate failure(s):\n  - %s" % (len(failures), "\n  - ".join(failures)))

        cur.execute("COMMIT" if args.apply else "ROLLBACK")
        opened = False
        print("=" * 78)
        if args.apply:
            print("COMMITTED. Post-commit state:")
            report_environment(cur)
            print("Next: run the pooled-port gate before declaring success.")
        else:
            print("DRY RUN passed; transaction rolled back. Restored state:")
            report_environment(cur)
        return 0
    except Exception as error:
        if opened:
            try:
                cur.execute("ROLLBACK")
            except Exception:
                pass
        print("\nFAILED: " + str(error), file=sys.stderr)
        try:
            print("post-failure state:", file=sys.stderr)
            for row in index_state(cur):
                if not row["valid"] or not row["ready"]:
                    print("  INVALID INDEX SHELL: " + row["name"] +
                          " -- drop it explicitly", file=sys.stderr)
            print("  " + json.dumps(function_state(cur)), file=sys.stderr)
        except Exception:
            pass
        raise
    finally:
        cur.close()
        con.close()


if __name__ == "__main__":
    raise SystemExit(main())
