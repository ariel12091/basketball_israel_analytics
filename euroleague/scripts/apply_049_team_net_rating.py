#!/usr/bin/env python
"""Baseline, gate, and (only on request) apply migration 046.

Scope: euroleague.get_team_ratings_{pergame,dynamic,direct} plus the defective
euroleague.team_ppp_ratings_mv. The already-correct Four Factors MV is a
read-only companion and is never dropped.

Modes
  --baseline    read-only report: target, dependents, grants, indexes,
                definition hashes, and the bounded validation matrix
                and MV snapshots. Performs no DDL.
  --candidate   capture baselines outside a transaction, apply the migration
                DDL inside one, run every gate, then ROLL BACK.
  --candidate --apply   the only commit path; runs the identical gates first.

Baselines are always captured before any candidate DDL and held in memory, so
no comparison is ever made between two post-change executions.

This script deliberately carries none of apply_045's performance-companion
machinery: migration 046 changes one arithmetic expression and the DROP/CREATE
plumbing it forces, not a query shape, so there is nothing to time.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
import time
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "src"))
from euroleague_possessions.postgres_backend import (  # noqa: E402
    _split_sql_statements, connect_from_env_file, inspect_target,
)
from scripts.audit_team_reader_contracts import (  # noqa: E402
    ROUTES as TEAM_ROUTES,
    audit as audit_team_contracts,
)

# etl/.Renviron is gitignored and therefore absent from this worktree; it
# exists only in the parent checkout. Point at it by absolute path -- never
# copy the credentials file into the worktree.
ENV = Path(r"C:\Users\ariel\documents\on_off_israel_pbp\etl\.Renviron")
MIGRATION = ROOT / "sql" / "049_team_net_rating_single_round.sql"

TARGET_MVS = ("team_ppp_ratings_mv",)
REFERENCE_MVS = ("team_four_factors_mv",)
MVS = TARGET_MVS + REFERENCE_MVS

# Full live hashes captured read-only on 2026-08-29. Candidate mode refuses to
# execute any DDL if another migration or manual edit changed one of these
# definitions. Update only after reviewing the newer live definition.
EXPECTED_FUNCTION_HASHES = {
    "ratings_pergame": "b037e97cfed2a7a00fce57d51d5f76749dab220b99fbb82ba9608d60810f0339",
    "ratings_dynamic": "c43fd6031508b757537cd4429b26d77fa19416d49262707485dc7270a7455d7a",
    "ratings_direct": "99c85dd78e092e99edc77f6a5daeace398f15abf0c3f23a8a30a006ab5dc54b0",
}
EXPECTED_MV_HASHES = {
    "team_ppp_ratings_mv": "3064590b89581da65823758a580f259af694b29c44c011b2a781237aa1540290",
    "team_four_factors_mv": "2d33bec0d2dc8c389290ad3e97298ff5d651823d2d2b35269eef52d7174ffb3d",
}

# --------------------------------------------------------------- signatures

# The three readers this migration's functions touch (Task 3's fix, already
# applied to this file; unchanged by this task). Same param list/types as
# their Four Factors counterparts below -- verified by reading both bodies
# side by side (037_pergame_team_readers.sql, 019_clutch_read_layer.sql,
# 031_direct_team_custom_readers.sql, 046_team_net_rating_single_round.sql).
PARAM_ORDER_PERGAME = [
    "p_competition", "p_game_year", "p_start_date", "p_end_date",
    "p_team_ids_csv", "p_phase_csv", "p_opp_ids_csv", "p_home_away",
    "p_outcome", "p_opp_rank_side", "p_opp_rank_n", "p_opp_rank_metric",
    "p_min_gn", "p_max_gn", "p_last_n_games",
    "p_num_starters_off_min", "p_num_starters_off_max",
    "p_num_starters_def_min", "p_num_starters_def_max",
]
PARAM_ORDER_CLUTCH = [
    "p_competition", "p_game_year", "p_start_date", "p_end_date",
    "p_team_ids_csv", "p_phase_csv", "p_opp_ids_csv", "p_home_away",
    "p_outcome", "p_opp_rank_side", "p_opp_rank_n", "p_opp_rank_metric",
    "p_max_margin", "p_margin_status", "p_max_time_remaining",
    "p_ot_margin_filter",
    "p_min_gn", "p_max_gn", "p_last_n_games",
    "p_num_starters_off_min", "p_num_starters_off_max",
    "p_num_starters_def_min", "p_num_starters_def_max",
]
PARAM_TYPES = {
    "p_competition": "text", "p_game_year": "int",
    "p_start_date": "date", "p_end_date": "date",
    "p_team_ids_csv": "text", "p_phase_csv": "text", "p_opp_ids_csv": "text",
    "p_home_away": "text", "p_outcome": "text",
    "p_opp_rank_side": "text", "p_opp_rank_n": "int", "p_opp_rank_metric": "text",
    "p_max_margin": "int", "p_margin_status": "text",
    "p_max_time_remaining": "int", "p_ot_margin_filter": "boolean",
    "p_min_gn": "int", "p_max_gn": "int", "p_last_n_games": "int",
    "p_num_starters_off_min": "int", "p_num_starters_off_max": "int",
    "p_num_starters_def_min": "int", "p_num_starters_def_max": "int",
}
# pg_get_functiondef's regprocedure form needs real Postgres type names, not
# the ::cast spellings above.
SIG_TYPE = {"text": "text", "int": "integer", "date": "date", "boolean": "boolean"}


def _signature(name, param_order):
    types = ",".join(SIG_TYPE[PARAM_TYPES[p]] for p in param_order)
    return "euroleague.%s(%s)" % (name, types)


READERS = {
    "ratings_pergame": (_signature("get_team_ratings_pergame", PARAM_ORDER_PERGAME),
                         PARAM_ORDER_PERGAME),
    "ratings_dynamic": (_signature("get_team_ratings_dynamic", PARAM_ORDER_CLUTCH),
                         PARAM_ORDER_CLUTCH),
    "ratings_direct": (_signature("get_team_ratings_direct", PARAM_ORDER_CLUTCH),
                        PARAM_ORDER_CLUTCH),
    "ff_pergame": (_signature("get_team_four_factors_pergame", PARAM_ORDER_PERGAME),
                   PARAM_ORDER_PERGAME),
    "ff_dynamic": (_signature("get_team_four_factors_dynamic", PARAM_ORDER_CLUTCH),
                   PARAM_ORDER_CLUTCH),
    "ff_direct": (_signature("get_team_four_factors_direct", PARAM_ORDER_CLUTCH),
                  PARAM_ORDER_CLUTCH),
}
# Only these three are touched by migration 046's function half (Task 3).
# Gate 1 hashes exactly these against --baseline's recorded values.
MODIFIED_READERS = ("ratings_pergame", "ratings_dynamic", "ratings_direct")
BASE = {"p_competition": "E", "p_game_year": 2025}

# Brief: "Run the preset matrix over at least: broad season, last-10, a phase
# filter, an opponent filter, home/away, and an opponent-rank filter (gate 5
# matters most on the last one)."
PRESETS = [
    ("broad season",       {}),
    ("last 10",            {"p_last_n_games": 10}),
    ("phase RS",           {"p_phase_csv": "RS"}),
    ("one opponent",       {"p_opp_ids_csv": "2"}),
    ("home",               {"p_home_away": "home"}),
    ("away",               {"p_home_away": "away"}),
    ("opp rank top5 net",  {"p_opp_rank_side": "top", "p_opp_rank_n": 5,
                             "p_opp_rank_metric": "net"}),
]

# The complete game-filter matrix is exercised on the per-game route. The two
# action-capable routes need one representative clutch case each to validate
# the expression without repeating broad action scans dozens of times.
VALIDATION_CASES = [
    (label, "ratings_pergame", "ff_pergame", preset)
    for label, preset in PRESETS
] + [
    (
        "standard clutch one game",
        "ratings_dynamic",
        "ff_dynamic",
        {"p_team_ids_csv": "1", "p_max_margin": 5,
         "p_margin_status": "all", "p_max_time_remaining": 300,
         "p_ot_margin_filter": False, "p_min_gn": 1, "p_max_gn": 1},
    ),
    (
        "custom clutch one team",
        "ratings_direct",
        "ff_direct",
        {"p_team_ids_csv": "1", "p_max_margin": 100,
         "p_margin_status": "all", "p_max_time_remaining": 3000,
         "p_ot_margin_filter": False, "p_min_gn": 1, "p_max_gn": 1},
    ),
]

MV_COLUMNS = {
    "team_ppp_ratings_mv": [
        "team_id", "off_pts", "off_poss", "def_pts", "def_poss",
        "games_played", "wins", "losses", "off_ppp", "def_ppp", "net_rtg",
        "rank_net_rtg", "rank_off_ppp", "rank_def_ppp",
        "off_rank", "def_rank", "net_rank",
    ],
    "team_four_factors_mv": [
        "team_id", "off_poss", "def_poss", "off_ppp", "def_ppp", "net_rtg",
    ],
}


# ---------------------------------------------------------------------- SQL


def build_call(name, preset):
    signature, param_order = READERS[name]
    fn_name = signature.split("(")[0]
    merged = dict(BASE)
    merged.update(preset)
    parts, values = [], []
    for key in param_order:
        if key not in merged:
            continue
        parts.append(key + " => %s::" + PARAM_TYPES[key])
        values.append(merged[key])
    return fn_name + "(" + ", ".join(parts) + ")", values


def run_reader(cur, name, preset):
    call, values = build_call(name, preset)
    cur.execute("SELECT row_to_json(x)::text FROM " + call + " x ORDER BY 1", values)
    return [row[0] for row in cur.fetchall()]


def mv_rows(cur, mv):
    cols = ", ".join(MV_COLUMNS[mv])
    cur.execute(
        "SELECT " + cols + " FROM euroleague." + mv +
        " WHERE competition = %s AND game_year = %s ORDER BY team_id",
        (BASE["p_competition"], BASE["p_game_year"]))
    names = MV_COLUMNS[mv]
    return {row[0]: dict(zip(names, row)) for row in cur.fetchall()}


# --------------------------------------------------------------------- gates
# Pure and importable, so the test suite can exercise each one without a live
# database.


def gate_definition_hashes(label, before, after):
    for key, expected in before.items():
        actual = after.get(key)
        if actual != expected:
            raise RuntimeError(
                "%s: %s definition hash changed unexpectedly %s -> %s"
                % (label, key, expected, actual))


def gate_additive_parity(before, after):
    fields = ("off_pts", "off_poss", "def_pts", "def_poss",
              "games_played", "wins", "losses")
    if set(before) != set(after):
        raise RuntimeError(
            "team_ppp_ratings_mv: team set changed %s -> %s"
            % (sorted(before), sorted(after)))
    for team_id, row in before.items():
        new = after[team_id]
        for field in fields:
            if row[field] != new[field]:
                raise RuntimeError(
                    "team %s: %s changed %r -> %r (additive parity broken)"
                    % (team_id, field, row[field], new[field]))


def gate_ppp_unchanged(before, after):
    for team_id, row in before.items():
        new = after[team_id]
        for field in ("off_ppp", "def_ppp"):
            if row[field] != new[field]:
                raise RuntimeError(
                    "team %s: %s moved %r -> %r; this change must not touch "
                    "off_ppp/def_ppp" % (team_id, field, row[field], new[field]))


def gate_net_rtg_delta(before, after):
    for team_id, row in before.items():
        new = after[team_id]
        old_net, new_net = row["net_rtg"], new["net_rtg"]
        if old_net is None or new_net is None:
            if old_net != new_net:
                raise RuntimeError("team %s: net_rtg NULL-ness changed" % team_id)
            continue
        expected = (
            Decimal(100) * Decimal(row["off_pts"]) / Decimal(row["off_poss"])
            - Decimal(100) * Decimal(row["def_pts"]) / Decimal(row["def_poss"])
        ).quantize(Decimal("0.1"), rounding=ROUND_HALF_UP)
        if Decimal(new_net) != expected:
            raise RuntimeError(
                "team %s: net_rtg %s does not equal canonical additive %s"
                % (team_id, new_net, expected)
            )
        delta = abs(float(new_net) - float(old_net))
        if delta > 0.1 + 1e-9:
            raise RuntimeError(
                "team %s: net_rtg moved %.1f (%.1f -> %.1f), exceeds the 0.1 "
                "budget for removing one rounding step"
                % (team_id, delta, old_net, new_net))


def gate_ranks_unchanged(before, after):
    fields = ("rank_net_rtg", "rank_off_ppp", "rank_def_ppp",
              "off_rank", "def_rank", "net_rank")
    for team_id, row in before.items():
        new = after[team_id]
        for field in fields:
            if row[field] != new[field]:
                raise RuntimeError(
                    "team %s: %s moved %r -> %r; ranks are deliberately "
                    "deferred and must not change (opponent-strength filter "
                    "in Tabs 8/9/10)" % (team_id, field, row[field], new[field]))


def _json_rows_by_team(rows):
    parsed = [json.loads(row) for row in rows]
    return {row["team_id"]: row for row in parsed}


def gate_mv_matches_pergame(mv_rows, reader_rows):
    """Season Ratings must equal its publication-eligible broad reader."""
    reader = _json_rows_by_team(reader_rows)
    if set(mv_rows) != set(reader):
        raise RuntimeError(
            "season/pergame team set differs %s -> %s"
            % (sorted(mv_rows), sorted(reader))
        )
    fields = (
        "off_ppp", "def_ppp", "net_rtg", "games_played", "wins", "losses",
        "off_poss", "def_poss", "rank_net_rtg", "rank_off_ppp",
        "rank_def_ppp",
    )
    for team_id, mv_row in mv_rows.items():
        app_row = reader[team_id]
        for field in fields:
            left, right = mv_row[field], app_row[field]
            same = (
                left is None and right is None
                if left is None or right is None
                else Decimal(str(left)) == Decimal(str(right))
            )
            if not same:
                raise RuntimeError(
                    "season/pergame team %s %s differs %r != %r"
                    % (team_id, field, left, right)
                )
    print("  EuroLeague season MV matches broad per-game Ratings (%d teams)" % len(reader))


def gate_mv_net_is_canonical(mv_rows):
    for team_id, row in mv_rows.items():
        expected = (
            Decimal(100) * Decimal(row["off_pts"]) / Decimal(row["off_poss"])
            - Decimal(100) * Decimal(row["def_pts"]) / Decimal(row["def_poss"])
        ).quantize(Decimal("0.1"), rounding=ROUND_HALF_UP)
        if Decimal(row["net_rtg"]) != expected:
            raise RuntimeError(
                "team %s: net_rtg %s does not equal canonical additive %s"
                % (team_id, row["net_rtg"], expected)
            )


def gate_reader_change(before_rows, after_rows, label):
    """Only Ratings net_rtg may change; every other field is byte-stable."""
    before = _json_rows_by_team(before_rows)
    after = _json_rows_by_team(after_rows)
    if set(before) != set(after):
        raise RuntimeError(
            "%s: Ratings team set changed %s -> %s"
            % (label, sorted(before), sorted(after))
        )
    for team_id, old in before.items():
        new = after[team_id]
        if set(old) != set(new):
            raise RuntimeError(
                "%s team %s: output columns changed" % (label, team_id)
            )
        for field, old_value in old.items():
            if field == "net_rtg":
                if old_value is None or new[field] is None:
                    if old_value != new[field]:
                        raise RuntimeError(
                            "%s team %s: net_rtg NULL-ness changed"
                            % (label, team_id)
                        )
                elif abs(float(old_value) - float(new[field])) > 0.1 + 1e-9:
                    raise RuntimeError(
                        "%s team %s: net_rtg moved by more than 0.1"
                        % (label, team_id)
                    )
            elif old_value != new[field]:
                raise RuntimeError(
                    "%s team %s: %s changed %r -> %r"
                    % (label, team_id, field, old_value, new[field])
                )


def gate_summary_ff_agreement(ratings_rows, ff_rows, label):
    """Summary and Four Factors share all common displayed rating fields."""
    ratings = _json_rows_by_team(ratings_rows)
    ff = _json_rows_by_team(ff_rows)
    if set(ratings) != set(ff):
        raise RuntimeError(
            "%s: Summary/Four Factors team sets differ" % label
        )
    fields = ("off_ppp", "def_ppp", "net_rtg", "off_poss", "def_poss")
    for team_id, rating in ratings.items():
        differences = {
            field: (rating[field], ff[team_id][field])
            for field in fields
            if rating[field] != ff[team_id][field]
        }
        if differences:
            raise RuntimeError(
                "%s team %s: Summary/Four Factors disagree: %s"
                % (label, team_id, differences)
            )


def gate_mv_summary_ff_agreement(ratings, ff):
    """Season MV common metrics must agree after the candidate."""
    if set(ratings) != set(ff):
        raise RuntimeError("season MVs: team sets differ")
    fields = ("off_ppp", "def_ppp", "net_rtg", "off_poss", "def_poss")
    for team_id, rating in ratings.items():
        differences = {
            field: (rating[field], ff[team_id][field])
            for field in fields
            if rating[field] != ff[team_id][field]
        }
        if differences:
            raise RuntimeError(
                "season MVs team %s disagree: %s" % (team_id, differences)
            )


# ---------------------------------------------------------------- reporting


def function_hashes(cur, names=READERS.keys()):
    hashes = {}
    for key in names:
        signature, _ = READERS[key]
        cur.execute("SELECT pg_get_functiondef(%s::regprocedure)", (signature,))
        row = cur.fetchone()
        if row is None:
            raise RuntimeError(key + ": function not found (" + signature + ")")
        hashes[key] = hashlib.sha256(row[0].encode()).hexdigest()
        cur.execute("SELECT has_function_privilege('app_readonly', %s, 'EXECUTE')",
                    (signature,))
        if not cur.fetchone()[0]:
            raise RuntimeError(key + ": app_readonly lost EXECUTE")
    return hashes


def mv_definition_hashes(cur):
    hashes = {}
    for mv in MVS:
        cur.execute("SELECT pg_get_viewdef(%s::regclass, true)", ("euroleague." + mv,))
        row = cur.fetchone()
        if row is None:
            raise RuntimeError(mv + ": materialized view not found")
        hashes[mv] = hashlib.sha256(row[0].encode()).hexdigest()
    return hashes


def relation_contract(cur, mv, schema="euroleague"):
    """Capture exact non-data properties that DROP/CREATE must restore."""
    cur.execute(
        "SELECT pg_get_userbyid(c.relowner), c.relacl::text, c.reloptions, "
        "c.reltablespace, obj_description(c.oid, 'pg_class') "
        "FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace "
        "WHERE n.nspname=%s AND c.relname=%s AND c.relkind='m'",
        (schema, mv),
    )
    relation = cur.fetchone()
    if relation is None:
        raise RuntimeError(mv + ": materialized view not found")
    cur.execute(
        "SELECT indexname, indexdef FROM pg_indexes "
        "WHERE schemaname=%s AND tablename=%s ORDER BY indexname",
        (schema, mv),
    )
    return {"relation": relation, "indexes": cur.fetchall()}


def gate_relation_contract(mv, before, after):
    if before != after:
        raise RuntimeError(
            "%s: owner/ACL/options/tablespace/comment/index contract changed: "
            "%r -> %r" % (mv, before, after)
        )


def capture_israeli_factor_routes(cur):
    """Capture every Israeli Four Factors row, including non-common columns."""
    snapshots = {}
    for route in TEAM_ROUTES:
        if route.league != "basketball_test":
            continue
        cur.execute(route.factors_sql)
        columns = tuple(column.name for column in cur.description)
        team_index = columns.index("team_id")
        rows = {row[team_index]: row for row in cur.fetchall()}
        if len(rows) == 0:
            raise RuntimeError(route.label + ": Israeli baseline is vacuous")
        snapshots[route.label] = (columns, rows)
    return snapshots


def gate_israeli_factor_change(before, after):
    """Allow only the intended <=0.1 Net Rating correction in Israeli output."""
    if before.keys() != after.keys():
        raise RuntimeError("Israeli route set changed")
    for label in before:
        before_columns, before_rows = before[label]
        after_columns, after_rows = after[label]
        if before_columns != after_columns or before_rows.keys() != after_rows.keys():
            raise RuntimeError(label + ": Israeli columns/team keys changed")
        net_index = before_columns.index("net_rtg")
        for team_id, old_row in before_rows.items():
            new_row = after_rows[team_id]
            for index, name in enumerate(before_columns):
                if name != "net_rtg" and old_row[index] != new_row[index]:
                    raise RuntimeError(
                        "%s team_id=%s: %s changed" % (label, team_id, name)
                    )
            if old_row[net_index] != new_row[net_index]:
                delta = abs(Decimal(old_row[net_index]) - Decimal(new_row[net_index]))
                if delta > Decimal("0.1"):
                    raise RuntimeError(
                        "%s team_id=%s: net_rtg moved by %s" %
                        (label, team_id, delta)
                    )
        print("  Israeli %-27s rows=%d" % (label, len(after_rows)))


def report_dependents_grants_indexes(cur):
    """Step 1's read-only introspection, re-run every time for a live report."""
    report = {}
    for mv in MVS:
        cur.execute("""
          SELECT DISTINCT dependent.relname, dependent.relkind
          FROM pg_depend d
          JOIN pg_rewrite rw ON rw.oid = d.objid
          JOIN pg_class dependent ON dependent.oid = rw.ev_class
          JOIN pg_class source ON source.oid = d.refobjid
          JOIN pg_namespace n ON n.oid = source.relnamespace
          WHERE n.nspname = 'euroleague' AND source.relname = %s
            AND dependent.relname <> source.relname
        """, (mv,))
        dependents = cur.fetchall()
        cur.execute("SELECT relacl FROM pg_class c JOIN pg_namespace n "
                    "ON n.oid = c.relnamespace "
                    "WHERE n.nspname='euroleague' AND c.relname=%s", (mv,))
        acl = cur.fetchone()
        cur.execute(
            "SELECT indexname, indexdef FROM pg_indexes "
            "WHERE schemaname='euroleague' AND tablename=%s", (mv,))
        indexes = cur.fetchall()
        print("  %-22s dependents=%s acl=%s" % (mv, dependents, acl[0] if acl else None))
        for name, definition in indexes:
            print("    index %-42s %s" % (name, definition))
        if mv in TARGET_MVS and dependents:
            raise RuntimeError(
                mv + " has a dependent view -- the plan's blast-radius "
                "assumption is wrong; STOP for a human decision: " + str(dependents))
        report[mv] = {"dependents": dependents, "acl": acl, "indexes": indexes}
    return report


def report_environment(cur):
    target = inspect_target(cur.connection)
    if target["euroleague_schema"] != "euroleague":
        raise RuntimeError("wrong target: " + str(target))
    if target["server_port"] != 5432:
        raise RuntimeError("candidate gating requires direct port 5432: " + str(target))
    cur.execute("SELECT version()")
    print("target:", target)
    print("server:", cur.fetchone()[0].split(" on ")[0])
    print("dependents / grants / indexes:")
    report_dependents_grants_indexes(cur)
    hashes = function_hashes(cur, MODIFIED_READERS)
    for key, digest in hashes.items():
        print("  function %-18s sha256=%s" % (key, digest[:16]))
    mv_hashes = mv_definition_hashes(cur)
    for mv, digest in mv_hashes.items():
        print("  mv       %-18s sha256=%s" % (mv, digest[:16]))
    return {"target": target, "function_hashes": hashes, "mv_hashes": mv_hashes}


# ------------------------------------------------------------------ baseline


def capture_baseline(cur):
    function_hash = function_hashes(cur, MODIFIED_READERS)
    mv_hash = mv_definition_hashes(cur)
    gate_definition_hashes("functions", EXPECTED_FUNCTION_HASHES, function_hash)
    gate_definition_hashes("materialized views", EXPECTED_MV_HASHES, mv_hash)

    mv_snapshots = {mv: mv_rows(cur, mv) for mv in MVS}
    for mv, rows in mv_snapshots.items():
        print("  %-22s %d team rows (competition=%s game_year=%s)"
              % (mv, len(rows), BASE["p_competition"], BASE["p_game_year"]))
        if not rows:
            raise RuntimeError(mv + ": baseline snapshot is empty -- wrong "
                                "target season or the MV has no data yet")

    reader_rows = {}
    started = time.monotonic()
    for label, ratings_name, _ff_name, preset in VALIDATION_CASES:
        rows = run_reader(cur, ratings_name, preset)
        if not rows:
            raise RuntimeError(
                "%s: baseline reader returned no rows; validation would be "
                "vacuous" % label
            )
        reader_rows[(ratings_name, label)] = rows
        print("  case %-24s %-17s rows=%d elapsed=%.1fs"
              % (label, ratings_name, len(rows), time.monotonic() - started),
              flush=True)

    return {
        "function_hashes": function_hash,
        "mv_hashes": mv_hash,
        "mv_snapshots": mv_snapshots,
        "reader_rows": reader_rows,
        "target_contracts": {
            mv: relation_contract(cur, mv) for mv in TARGET_MVS
        },
    }


# ------------------------------------------------------------------ candidate


def candidate_statements(path):
    """Parse migration 046 and enforce its own, narrower safety envelope.

    Unlike migration 045 (function-only, additive-only), 046 legitimately
    needs DROP MATERIALIZED VIEW / CREATE MATERIALIZED VIEW / GRANT to
    restore what DROP wipes. Allow exactly that; keep every other guard from
    the 045 applier (no Israeli-schema reference, no destructive statement
    outside the two named MVs, no DROP FUNCTION).
    """
    text = path.read_text(encoding="utf-8")
    body = "\n".join(
        x for x in text.splitlines() if not x.strip().startswith("--")).upper()
    if re.search(r"\b(?:TRUNCATE|DELETE\s+FROM|INSERT\s+INTO|UPDATE\s+|"
                 r"ALTER\s+(?:TABLE|SCHEMA|MATERIALIZED\s+VIEW)|"
                 r"CREATE\s+SCHEMA|OWNER\s+TO|"
                 r"DROP\s+(?:FUNCTION|TABLE|VIEW|SCHEMA|INDEX|TYPE|SEQUENCE))\b",
                 body):
        raise ValueError(path.name + ": forbidden statement kind present")
    allowed_drop_targets = {
        "EUROLEAGUE.TEAM_PPP_RATINGS_MV",
        "BASKETBALL_TEST.TEAM_FOUR_FACTORS_MV",
    }
    for match in re.finditer(r"DROP\s+MATERIALIZED\s+VIEW\s+(?:IF\s+EXISTS\s+)?"
                              r"([A-Z0-9_.]+)", body):
        if match.group(1) not in allowed_drop_targets:
            raise ValueError(path.name + ": unexpected DROP target " + match.group(1))
    for action in ("CREATE", "REFRESH"):
        pattern = action + r"\s+MATERIALIZED\s+VIEW\s+([A-Z0-9_.]+)"
        for match in re.finditer(pattern, body):
            if match.group(1) not in allowed_drop_targets:
                raise ValueError(
                    path.name + ": unexpected " + action + " target "
                    + match.group(1)
                )
    kept = []
    for statement in _split_sql_statements(text):
        normalised = " ".join(
            x for x in statement.splitlines() if not x.strip().startswith("--"))
        normalised = " ".join(normalised.split()).rstrip(";").upper()
        if normalised in {"BEGIN", "COMMIT", ""}:
            continue
        kept.append(statement)
    return kept


def assert_no_active_publication(cur):
    """Refuse to take the MVs' ACCESS EXCLUSIVE lock while a load run writes."""
    cur.execute(
        "SELECT count(*) FROM pg_locks l JOIN pg_class c ON c.oid = l.relation "
        "JOIN pg_namespace n ON n.oid = c.relnamespace "
        "WHERE n.nspname IN ('euroleague', 'basketball_test') "
        "  AND l.pid <> pg_backend_pid() "
        "  AND l.mode IN ('RowExclusiveLock', 'ShareRowExclusiveLock', "
        "                 'ExclusiveLock', 'AccessExclusiveLock')")
    writers = cur.fetchone()[0]
    if writers:
        raise RuntimeError(
            "%d write lock(s) held on target-schema relations; a publication or "
            "load run is active. Wait for it to finish." % writers)
    print("  no writer holds a lock on either target schema")


def assert_no_israeli_mv_dependents(cur):
    cur.execute("""
      SELECT DISTINCT dependent.relname, dependent.relkind
      FROM pg_depend d
      JOIN pg_rewrite rw ON rw.oid = d.objid
      JOIN pg_class dependent ON dependent.oid = rw.ev_class
      JOIN pg_class source ON source.oid = d.refobjid
      JOIN pg_namespace n ON n.oid = source.relnamespace
      WHERE n.nspname = 'basketball_test'
        AND source.relname = 'team_four_factors_mv'
        AND dependent.relname <> source.relname
    """)
    dependents = cur.fetchall()
    if dependents:
        raise RuntimeError(
            "basketball_test.team_four_factors_mv has dependents: "
            + str(dependents)
        )
    print("  Israeli target MV has no dependent view")


def run_candidate(cur, apply_):
    print("publication pre-flight:")
    assert_no_active_publication(cur)
    assert_no_israeli_mv_dependents(cur)

    print("baseline (captured before any DDL, outside the transaction):")
    baseline = capture_baseline(cur)
    israeli_before = capture_israeli_factor_routes(cur)
    israeli_contract = relation_contract(
        cur, "team_four_factors_mv", "basketball_test"
    )

    ddl = candidate_statements(MIGRATION)

    cur.execute("BEGIN")
    cur.execute("SET LOCAL lock_timeout = '5s'")
    # The refresh duration is data-dependent. Do not abort it mid-rebuild;
    # lock_timeout still prevents waiting indefinitely to begin the operation.
    cur.execute("SET LOCAL statement_timeout = 0")
    try:
        for statement in ddl:
            cur.execute(statement)
        print("candidate DDL applied (%d statements)" % len(ddl))

        failures = []

        def gate(fn, *args, **kwargs):
            try:
                fn(*args, **kwargs)
                return True
            except RuntimeError as error:
                failures.append(str(error))
                return False

        after_function_hashes = function_hashes(cur, MODIFIED_READERS)
        after_mv_hashes = mv_definition_hashes(cur)
        after_mv_snapshots = {mv: mv_rows(cur, mv) for mv in MVS}

        print("gate 1 (fail-closed definitions and untouched companion):")
        gate(gate_definition_hashes, "reference materialized views",
             {mv: EXPECTED_MV_HASHES[mv] for mv in REFERENCE_MVS},
             {mv: after_mv_hashes[mv] for mv in REFERENCE_MVS})
        for name, digest in after_function_hashes.items():
            if digest == baseline["function_hashes"][name]:
                failures.append(name + ": migration did not change definition")
        for mv in TARGET_MVS:
            if after_mv_hashes[mv] == baseline["mv_hashes"][mv]:
                failures.append(mv + ": migration did not change definition")
        print("gate 2 (season Ratings equals publication-eligible per-game reader):")
        gate(
            gate_mv_matches_pergame,
            after_mv_snapshots["team_ppp_ratings_mv"],
            run_reader(cur, "ratings_pergame", {}),
        )
        print("gate 3 (season net_rtg rounds additive counts once):")
        gate(gate_mv_net_is_canonical, after_mv_snapshots["team_ppp_ratings_mv"])
        print("gate 6 (Summary/Four Factors agreement, post-DDL):")
        gate(gate_mv_summary_ff_agreement,
             after_mv_snapshots["team_ppp_ratings_mv"],
             after_mv_snapshots["team_four_factors_mv"])
        for label, ratings_name, ff_name, preset in VALIDATION_CASES:
            ratings = run_reader(cur, ratings_name, preset)
            ff = run_reader(cur, ff_name, preset)
            gate(gate_reader_change,
                 baseline["reader_rows"][(ratings_name, label)], ratings, label)
            gate(gate_summary_ff_agreement, ratings, ff, label)
            print("  case %-24s ratings=%d ff=%d" %
                  (label, len(ratings), len(ff)), flush=True)
        print("gate 7 (exact target relation contract restored):")
        for mv in TARGET_MVS:
            gate(gate_relation_contract, mv, baseline["target_contracts"][mv],
                 relation_contract(cur, mv))
        gate(
            gate_relation_contract,
            "basketball_test.team_four_factors_mv",
            israeli_contract,
            relation_contract(cur, "team_four_factors_mv", "basketball_test"),
        )
        print("gate 8 (Israeli output changes only in net_rtg):")
        gate(
            gate_israeli_factor_change,
            israeli_before,
            capture_israeli_factor_routes(cur),
        )
        print("gate 9 (all 12 Ratings/Four Factors companion routes agree):")
        gate(audit_team_contracts, cur)

        if failures:
            raise RuntimeError(
                "%d gate failure(s):\n  - %s" % (len(failures), "\n  - ".join(failures)))

        if apply_:
            cur.execute("COMMIT")
            print("COMMITTED.")
        else:
            cur.execute("ROLLBACK")
            print("DRY RUN passed; transaction rolled back.")
        return 0
    except Exception:
        try:
            cur.execute("ROLLBACK")
        except Exception:
            pass
        raise


# --------------------------------------------------------------------- main


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--baseline", action="store_true",
                         help="read-only report; performs no DDL at all")
    parser.add_argument("--candidate", action="store_true",
                         help="apply migration 046 in a transaction and, "
                              "without --apply, roll it back")
    parser.add_argument("--apply", action="store_true",
                         help="commit the candidate; the only non-rollback path")
    args = parser.parse_args()
    if args.apply and not args.candidate:
        parser.error("--apply requires --candidate")
    if args.baseline and (args.candidate or args.apply):
        parser.error("--baseline cannot be combined with --candidate/--apply")
    if not args.baseline and not args.candidate:
        parser.error("choose --baseline or --candidate")

    con = connect_from_env_file(ENV, direct_port=5432)
    cur = con.cursor()
    try:
        cur.execute("SET statement_timeout = '30s'")
        print("=" * 78)
        report_environment(cur)
        print("=" * 78)

        if args.baseline:
            print("MODE: BASELINE (read-only, no DDL)")
            capture_baseline(cur)
            print("baseline complete; nothing was changed")
            return 0

        mode = "APPLY (commits)" if args.apply else "DRY RUN (rolls back)"
        print("MODE: candidate -- " + mode)
        return run_candidate(cur, args.apply)
    finally:
        cur.close()
        con.close()


if __name__ == "__main__":
    raise SystemExit(main())
