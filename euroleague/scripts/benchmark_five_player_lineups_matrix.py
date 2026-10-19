#!/usr/bin/env python
"""Read-only normal/nested-loop-off matrix for custom five-player Lineups.

The public SECURITY DEFINER function is timed and compared as the contract.
The live SQL body is also expanded with the same literal parameters and
explained outside the function boundary so PostgreSQL exposes inner indexes.
The connection is held in a READ ONLY transaction and always rolled back.
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
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "src"))

from euroleague_possessions.postgres_backend import (  # noqa: E402
    connect_from_env_file,
    inspect_target,
)

ENV_PATH = REPO.parent / "etl" / ".Renviron"
EXPECTED_FUNCTION_MD5 = "cc54fe28b824090cf79cac6f1c2f11b7"
FUNCTION_NAME = "euroleague.fetch_lineups_direct"
TRIM_FRACTION = 0.20

PARAM_TYPES = {
    "p_competition": "text",
    "p_game_year": "integer",
    "p_start_date": "date",
    "p_end_date": "date",
    "p_team_ids_csv": "text",
    "p_phase_csv": "text",
    "p_opp_ids_csv": "text",
    "p_home_away": "text",
    "p_outcome": "text",
    "p_opp_rank_side": "text",
    "p_opp_rank_n": "integer",
    "p_opp_rank_metric": "text",
    "p_max_margin": "integer",
    "p_margin_status": "text",
    "p_max_time_remaining": "integer",
    "p_ot_margin_filter": "boolean",
    "p_min_gn": "integer",
    "p_max_gn": "integer",
    "p_last_n_games": "integer",
    "p_num_starters_off_min": "integer",
    "p_num_starters_off_max": "integer",
    "p_num_starters_def_min": "integer",
    "p_num_starters_def_max": "integer",
    "p_unit_size": "integer",
    "p_players_on_csv": "text",
    "p_players_off_csv": "text",
    "p_min_poss": "integer",
}

BASE = {
    "p_competition": "E",
    "p_game_year": 2025,
    "p_start_date": None,
    "p_end_date": None,
    "p_team_ids_csv": None,
    "p_phase_csv": None,
    "p_opp_ids_csv": None,
    "p_home_away": "all",
    "p_outcome": "all",
    "p_opp_rank_side": None,
    "p_opp_rank_n": None,
    "p_opp_rank_metric": None,
    "p_max_margin": 3,
    "p_margin_status": "all",
    "p_max_time_remaining": 240,
    "p_ot_margin_filter": False,
    "p_min_gn": None,
    "p_max_gn": None,
    "p_last_n_games": None,
    "p_num_starters_off_min": None,
    "p_num_starters_off_max": None,
    "p_num_starters_def_min": None,
    "p_num_starters_def_max": None,
    "p_unit_size": 5,
    "p_players_on_csv": None,
    "p_players_off_csv": None,
    "p_min_poss": 0,
}


@dataclass(frozen=True)
class Preset:
    name: str
    overrides: dict[str, Any]


STATIC_PRESETS = (
    Preset("broad no dates", {}),
    Preset(
        "broad app dates",
        {"p_start_date": "2025-09-01", "p_end_date": "2026-07-01"},
    ),
    Preset("last 10", {"p_last_n_games": 10}),
    Preset("one team", {"p_team_ids_csv": "24"}),
    Preset("phase RS", {"p_phase_csv": "RS"}),
    Preset("one opponent", {"p_opp_ids_csv": "19"}),
    Preset("home", {"p_home_away": "home"}),
    Preset("away", {"p_home_away": "away"}),
    Preset("win", {"p_outcome": "win"}),
    Preset("loss", {"p_outcome": "loss"}),
    Preset(
        "opponent rank top 5 offense",
        {
            "p_opp_rank_side": "top",
            "p_opp_rank_n": 5,
            "p_opp_rank_metric": "off",
        },
    ),
    Preset("own starters min 5", {"p_num_starters_off_min": 5}),
    Preset("own starters max 3", {"p_num_starters_off_max": 3}),
    Preset("opponent starters min 5", {"p_num_starters_def_min": 5}),
    Preset("opponent starters max 3", {"p_num_starters_def_max": 3}),
    Preset("minimum 100 possessions", {"p_min_poss": 100}),
    Preset("empty result", {"p_min_gn": 999}),
)


def arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--samples", type=int, default=15)
    parser.add_argument(
        "--preset",
        action="append",
        help="Run only an exact preset name; may be passed more than once.",
    )
    parser.add_argument("--statement-timeout-ms", type=int, default=30_000)
    parser.add_argument(
        "--allow-function-md5-change",
        action="store_true",
        help="Run against a body other than the diagnosed handoff body.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="JSON output path (default: C:/tmp with a UTC timestamp).",
    )
    return parser.parse_args()


def p90(values: list[float]) -> float:
    ordered = sorted(values)
    return ordered[max(0, min(len(ordered) - 1, math.ceil(0.9 * len(ordered)) - 1))]


def summarize(values: list[float]) -> dict[str, Any]:
    ordered = sorted(values)
    drop = int(len(ordered) * TRIM_FRACTION)
    kept = ordered[drop : len(ordered) - drop] if drop else ordered
    if len(kept) < 3:
        kept = ordered
    return {
        "raw_seconds": values,
        "median_seconds": statistics.median(kept),
        "upper_central_seconds": p90(kept),
        "raw_p90_seconds": p90(values),
        "central_sample_count": len(kept),
    }


def sql_literal(value: Any, pg_type: str) -> str:
    if value is None:
        return f"NULL::{pg_type}"
    if pg_type == "boolean":
        return ("true" if value else "false") + "::boolean"
    if pg_type == "integer":
        return f"{int(value)}::integer"
    escaped = str(value).replace("'", "''")
    return f"'{escaped}'::{pg_type}"


def expand_function_body(body: str, params: dict[str, Any]) -> str:
    expanded = body
    for name in sorted(PARAM_TYPES, key=len, reverse=True):
        expanded = re.sub(
            rf"\b{re.escape(name)}\b",
            lambda _match, n=name: sql_literal(params[n], PARAM_TYPES[n]),
            expanded,
        )
    unresolved = sorted(set(re.findall(r"\bp_[a-z0-9_]+\b", expanded)))
    if unresolved:
        raise RuntimeError(f"unresolved function parameters: {unresolved}")
    return expanded.strip().rstrip(";")


def function_body(cursor: Any) -> tuple[str, str]:
    cursor.execute(
        """
        SELECT p.prosrc, md5(p.prosrc)
        FROM pg_catalog.pg_proc p
        JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
        WHERE n.nspname = 'euroleague' AND p.proname = 'fetch_lineups_direct'
        """
    )
    rows = cursor.fetchall()
    if len(rows) != 1:
        raise RuntimeError(f"expected one live fetch_lineups_direct, found {len(rows)}")
    return str(rows[0][0]), str(rows[0][1])


def player_presets(cursor: Any) -> tuple[Preset, Preset]:
    cursor.execute(
        """
        SELECT player_ids[1], player_ids[2], player_ids[3]
        FROM euroleague.lineup_totals_by_game
        WHERE competition = 'E' AND game_year = 2025
          AND cardinality(player_ids) = 5
        ORDER BY possessions DESC NULLS LAST
        LIMIT 1
        """
    )
    row = cursor.fetchone()
    if row is None or any(value is None for value in row):
        raise RuntimeError("could not derive player-membership presets")
    a, b, c = (str(value) for value in row)
    return (
        Preset("players on", {"p_players_on_csv": f"{a},{b}"}),
        Preset("players on and off", {"p_players_on_csv": a, "p_players_off_csv": c}),
    )


def build_call(params: dict[str, Any]) -> tuple[str, list[Any]]:
    parts = [f"{name} => %s::{pg_type}" for name, pg_type in PARAM_TYPES.items()]
    return (
        FUNCTION_NAME + "(" + ", ".join(parts) + ")",
        [params[name] for name in PARAM_TYPES],
    )


def row_token(row: tuple[Any, ...]) -> str:
    return json.dumps(row, default=str, ensure_ascii=False, separators=(",", ":"))


def run_public(cursor: Any, params: dict[str, Any], nestloop: bool) -> dict[str, Any]:
    cursor.execute("SET LOCAL enable_nestloop = " + ("on" if nestloop else "off"))
    call, values = build_call(params)
    started = time.perf_counter()
    cursor.execute(
        "SELECT * FROM " + call + " t ORDER BY t.team_id, t.unit_key",
        values,
    )
    rows = [tuple(row) for row in cursor.fetchall()]
    elapsed = time.perf_counter() - started
    columns = [(item.name, int(item.type_code)) for item in cursor.description]
    tokens = sorted(row_token(row) for row in rows)
    digest = hashlib.sha256("\n".join(tokens).encode("utf-8")).hexdigest()
    return {
        "elapsed": elapsed,
        "rows": rows,
        "columns": columns,
        "tokens": tokens,
        "digest": digest,
    }


def plan_metrics(cursor: Any, body: str, params: dict[str, Any], nestloop: bool) -> dict[str, Any]:
    cursor.execute("SET LOCAL enable_nestloop = " + ("on" if nestloop else "off"))
    sql = expand_function_body(body, params)
    cursor.execute("EXPLAIN (ANALYZE, BUFFERS, SETTINGS, FORMAT JSON) " + sql)
    explained = cursor.fetchone()[0][0]
    root = explained["Plan"]
    indexes: set[str] = set()
    nested_loops = 0
    node_summary: list[dict[str, Any]] = []

    def visit(node: dict[str, Any]) -> None:
        nonlocal nested_loops
        if node.get("Index Name"):
            indexes.add(str(node["Index Name"]))
        if node.get("Node Type") == "Nested Loop":
            nested_loops += 1
        if (
            node.get("Relation Name")
            or node.get("Index Name")
            or node.get("Node Type")
            in {"Aggregate", "Hash", "Hash Join", "Merge Join", "Nested Loop", "Sort"}
        ):
            loops = int(node.get("Actual Loops", 0))
            total_ms = float(node.get("Actual Total Time", 0.0))
            node_summary.append(
                {
                    "node_type": node.get("Node Type"),
                    "relation": node.get("Relation Name"),
                    "index": node.get("Index Name"),
                    "join_type": node.get("Join Type"),
                    "planned_rows": int(node.get("Plan Rows", 0)),
                    "actual_rows": int(node.get("Actual Rows", 0)),
                    "actual_loops": loops,
                    "total_ms_per_loop": total_ms,
                    "loop_weighted_ms": total_ms * loops,
                    "shared_hit_blocks": int(node.get("Shared Hit Blocks", 0)),
                    "shared_read_blocks": int(node.get("Shared Read Blocks", 0)),
                    "temp_written_blocks": int(node.get("Temp Written Blocks", 0)),
                }
            )
        for child in node.get("Plans", []):
            visit(child)

    visit(root)
    return {
        "execution_ms": float(explained.get("Execution Time", 0.0)),
        "planning_ms": float(explained.get("Planning Time", 0.0)),
        "shared_hit_blocks": int(root.get("Shared Hit Blocks", 0)),
        "shared_read_blocks": int(root.get("Shared Read Blocks", 0)),
        "temp_read_blocks": int(root.get("Temp Read Blocks", 0)),
        "temp_written_blocks": int(root.get("Temp Written Blocks", 0)),
        "nested_loop_nodes": nested_loops,
        "indexes": sorted(indexes),
        "settings": explained.get("Settings", {}),
        "node_summary": node_summary,
    }


def compare_contract(label: str, normal: dict[str, Any], off: dict[str, Any]) -> None:
    if normal["columns"] != off["columns"]:
        raise RuntimeError(f"{label}: declared columns changed")
    if normal["tokens"] != off["tokens"]:
        raise RuntimeError(f"{label}: complete value multiset changed")


def measure_preset(
    cursor: Any,
    body: str,
    preset: Preset,
    samples: int,
) -> dict[str, Any]:
    params = dict(BASE)
    params.update(preset.overrides)
    normal_reference = run_public(cursor, params, True)
    off_reference = run_public(cursor, params, False)
    compare_contract(preset.name, normal_reference, off_reference)

    timings = {"normal": [], "nestloop_off": []}
    # Keep every timed transition symmetric. Reversing each pair would produce
    # N-O-O-N-N-O across pair boundaries, giving the first mode in each pair an
    # immediate same-plan repeat. Both references above end in O, so the timed
    # N-O-N-O sequence starts and continues with cross-mode transitions only.
    for _index in range(samples):
        for enabled, key in ((True, "normal"), (False, "nestloop_off")):
            current = run_public(cursor, params, enabled)
            compare_contract(preset.name, normal_reference, current)
            timings[key].append(current["elapsed"])

    normal_summary = summarize(timings["normal"])
    off_summary = summarize(timings["nestloop_off"])
    normal_plan = plan_metrics(cursor, body, params, True)
    off_plan = plan_metrics(cursor, body, params, False)
    improvement = (
        (normal_summary["median_seconds"] - off_summary["median_seconds"])
        / normal_summary["median_seconds"]
        if normal_summary["median_seconds"]
        else 0.0
    )
    return {
        "name": preset.name,
        "parameters": preset.overrides,
        "rows": len(normal_reference["rows"]),
        "columns": [name for name, _type_code in normal_reference["columns"]],
        "column_type_oids": [oid for _name, oid in normal_reference["columns"]],
        "digest": normal_reference["digest"],
        "normal": {"timing": normal_summary, "plan": normal_plan},
        "nestloop_off": {"timing": off_summary, "plan": off_plan},
        "median_improvement_fraction": improvement,
        "execution_sequence": "normal,nestloop_off (strict alternation)",
    }


def choose_presets(all_presets: tuple[Preset, ...], requested: list[str] | None) -> tuple[Preset, ...]:
    if not requested:
        return all_presets
    by_name = {preset.name: preset for preset in all_presets}
    missing = sorted(set(requested) - set(by_name))
    if missing:
        raise ValueError(f"unknown preset(s): {missing}; available: {sorted(by_name)}")
    return tuple(by_name[name] for name in requested)


def default_output() -> Path:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return Path("C:/tmp") / f"euroleague_five_player_lineups_matrix_{stamp}.json"


def main() -> int:
    options = arguments()
    if options.samples < 1:
        raise ValueError("--samples must be positive")
    output = options.output or default_output()
    connection = connect_from_env_file(ENV_PATH, direct_port=5432)
    connection.autocommit = False
    cursor = connection.cursor()
    report: dict[str, Any] = {}
    try:
        cursor.execute("BEGIN READ ONLY")
        cursor.execute(f"SET LOCAL statement_timeout = '{options.statement_timeout_ms}ms'")
        cursor.execute("SET LOCAL search_path = pg_catalog, euroleague, public")
        target = inspect_target(connection)
        if target["euroleague_schema"] != "euroleague" or target["server_port"] != 5432:
            raise RuntimeError(f"unexpected database target: {target}")
        body, body_md5 = function_body(cursor)
        if body_md5 != EXPECTED_FUNCTION_MD5 and not options.allow_function_md5_change:
            raise RuntimeError(
                f"live function MD5 is {body_md5}, expected {EXPECTED_FUNCTION_MD5}; "
                "review the new body or pass --allow-function-md5-change"
            )
        presets = choose_presets(STATIC_PRESETS + player_presets(cursor), options.preset)
        print(
            f"target={target['database']} port={target['server_port']} "
            f"function_md5={body_md5} samples={options.samples} presets={len(presets)}"
        )
        results = []
        for index, preset in enumerate(presets, start=1):
            print(f"\n[{index}/{len(presets)}] {preset.name}", flush=True)
            result = measure_preset(cursor, body, preset, options.samples)
            results.append(result)
            before = result["normal"]["timing"]["median_seconds"]
            after = result["nestloop_off"]["timing"]["median_seconds"]
            print(
                f"  rows={result['rows']} normal={before:.3f}s off={after:.3f}s "
                f"change={result['median_improvement_fraction']:+.1%} "
                f"buffers={result['normal']['plan']['shared_hit_blocks'] + result['normal']['plan']['shared_read_blocks']}"
                f"->{result['nestloop_off']['plan']['shared_hit_blocks'] + result['nestloop_off']['plan']['shared_read_blocks']}",
                flush=True,
            )
        report = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "read_only": True,
            "target": target,
            "function_md5": body_md5,
            "samples_per_mode": options.samples,
            "results": results,
        }
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")
        print(f"\nreport={output}")
        return 0
    finally:
        connection.rollback()
        cursor.close()
        connection.close()


if __name__ == "__main__":
    raise SystemExit(main())
