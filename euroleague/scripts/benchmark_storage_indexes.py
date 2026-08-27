#!/usr/bin/env python
"""Transactionally test storage-saving EuroLeague index removals.

The default mode never commits DDL.  It benchmarks the live index set, drops
the candidate inside the current transaction, repeats the same app-facing
queries, and rolls the transaction back.  ``--apply`` commits the drop only
when every result is byte-identical and the measured warm latency stays within
the configured gate.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import statistics
import sys
import time
from dataclasses import dataclass
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "src"))

from euroleague_possessions.postgres_backend import (  # noqa: E402
    connect_from_env_file,
    inspect_target,
)

ENV_PATH = REPO.parent / "etl" / ".Renviron"
CANDIDATE = "euroleague_player_stats_actions_team_idx"
CANDIDATE_DDL = """
CREATE INDEX euroleague_player_stats_actions_team_idx
ON euroleague.player_stats_actions_by_game USING btree (
  game_id, team_id, regulation_seconds_remaining, pre_abs_margin,
  pre_status, is_overtime, own_starters, opp_starters
)
INCLUDE (
  type_lineup, possession_flag, points, ts_possessions, orebounds,
  oreb_opportunities, turnovers, ft_attempts, fga, fgm, fg3_made
)
""".strip()


@dataclass(frozen=True)
class Probe:
    name: str
    sql: str


COMMON = (
    "p_competition => 'E', p_game_year => 2025, "
    "p_max_margin => 3, p_margin_status => 'all', "
    "p_max_time_remaining => 240, p_ot_margin_filter => false"
)
PROBES = (
    Probe(
        "team_ratings_custom_broad",
        f"SELECT * FROM euroleague.get_team_ratings_direct({COMMON}) t "
        "ORDER BY t.team_id",
    ),
    Probe(
        "team_four_factors_custom_broad",
        f"SELECT * FROM euroleague.get_team_four_factors_direct({COMMON}) t "
        "ORDER BY t.team_id",
    ),
    Probe(
        "team_ratings_custom_one_team",
        f"SELECT * FROM euroleague.get_team_ratings_direct({COMMON}, "
        "p_team_ids_csv => '24') t ORDER BY t.team_id",
    ),
    Probe(
        "team_minutes_custom_broad",
        f"SELECT * FROM euroleague.get_team_minutes_direct({COMMON}) t "
        "ORDER BY t.team_id",
    ),
    Probe(
        "lineups_custom_size5",
        f"SELECT * FROM euroleague.fetch_lineups_direct({COMMON}, "
        "p_unit_size => 5) t ORDER BY t.team_id, t.unit_key",
    ),
)


def args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--runs", type=int, default=3)
    parser.add_argument("--max-relative-regression", type=float, default=0.10)
    parser.add_argument("--max-absolute-regression", type=float, default=0.10)
    return parser.parse_args()


def digest_rows(rows: list[tuple]) -> str:
    payload = json.dumps(rows, default=str, ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def index_definition(cursor) -> tuple[int, str]:
    cursor.execute(
        """
        SELECT pg_relation_size(i.oid), pg_get_indexdef(i.oid)
        FROM pg_class i
        JOIN pg_namespace n ON n.oid = i.relnamespace
        JOIN pg_index x ON x.indexrelid = i.oid
        WHERE n.nspname = 'euroleague' AND i.relname = %s
          AND x.indisvalid AND x.indisready
        """,
        (CANDIDATE,),
    )
    row = cursor.fetchone()
    if row is None:
        raise RuntimeError(f"required candidate index is missing or invalid: {CANDIDATE}")
    return int(row[0]), str(row[1])


def measure(cursor, probe: Probe, runs: int) -> tuple[str, int, list[float], set[str]]:
    cursor.execute(probe.sql)
    rows = [tuple(row) for row in cursor.fetchall()]
    digest = digest_rows(rows)

    elapsed: list[float] = []
    indexes: set[str] = set()
    for _ in range(runs):
        started = time.perf_counter()
        cursor.execute("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + probe.sql)
        plan = cursor.fetchone()[0][0]
        elapsed.append(time.perf_counter() - started)

        def visit(node: dict) -> None:
            if node.get("Index Name"):
                indexes.add(str(node["Index Name"]))
            for child in node.get("Plans", []):
                visit(child)

        visit(plan["Plan"])
    return digest, len(rows), elapsed, indexes


def phase(cursor, label: str, runs: int) -> dict[str, tuple[str, int, float, set[str]]]:
    print(f"\n=== {label} ===")
    result: dict[str, tuple[str, int, float, set[str]]] = {}
    for probe in PROBES:
        digest, rows, elapsed, indexes = measure(cursor, probe, runs)
        median = statistics.median(elapsed)
        result[probe.name] = (digest, rows, median, indexes)
        print(
            f"{probe.name:<36} rows={rows:<5} median={median:6.3f}s "
            f"indexes={','.join(sorted(indexes)) or '-'}"
        )
    return result


def compare(
    baseline: dict[str, tuple[str, int, float, set[str]]],
    candidate: dict[str, tuple[str, int, float, set[str]]],
    relative_limit: float,
    absolute_limit: float,
) -> list[str]:
    failures: list[str] = []
    print("\n=== gates ===")
    for name, before in baseline.items():
        after = candidate[name]
        if before[:2] != after[:2]:
            failures.append(f"{name}: result digest/row count changed")
            print(f"FAIL {name}: result changed")
            continue
        allowed = max(before[2] * (1 + relative_limit), before[2] + absolute_limit)
        status = "OK" if after[2] <= allowed else "FAIL"
        print(
            f"{status:<4} {name:<36} {before[2]:6.3f}s -> {after[2]:6.3f}s "
            f"(limit {allowed:6.3f}s)"
        )
        if status == "FAIL":
            failures.append(
                f"{name}: {after[2]:.3f}s exceeds {allowed:.3f}s latency gate"
            )
    return failures


def main() -> int:
    options = args()
    if options.runs < 1:
        raise ValueError("--runs must be positive")

    connection = connect_from_env_file(ENV_PATH, direct_port=5432)
    # The shared loader helper intentionally returns an autocommit connection.
    # This benchmark relies on transactional DDL, so opt out before issuing
    # even the first catalog query.  Without this line DROP INDEX is permanent.
    connection.autocommit = False
    cursor = connection.cursor()
    try:
        target = inspect_target(connection)
        if target["euroleague_schema"] != "euroleague":
            raise RuntimeError(f"EuroLeague schema is unavailable: {target}")
        size, definition = index_definition(cursor)
        def canonical_ddl(value: str) -> str:
            return " ".join(value.split()).replace("( ", "(").replace(" )", ")")

        normalized = canonical_ddl(definition)
        expected = canonical_ddl(CANDIDATE_DDL)
        if normalized != expected:
            raise RuntimeError(
                "live candidate definition differs from the reviewed DDL:\n"
                f"live:     {normalized}\nexpected: {expected}"
            )
        print(f"candidate={CANDIDATE} size={size / 1048576:.1f} MiB")

        baseline = phase(cursor, "baseline index present", options.runs)

        cursor.execute("SET LOCAL lock_timeout = '3s'")
        cursor.execute(f"DROP INDEX euroleague.{CANDIDATE}")
        candidate = phase(cursor, "candidate index absent (transactional)", options.runs)
        connection.rollback()
        restored = phase(cursor, "index restored after rollback", options.runs)

        # The first baseline warms relation and index pages.  Compare the
        # candidate with the restored A2 phase so both sides benefit from the
        # same warmed shared buffers.  Results must agree in all three phases.
        for name in baseline:
            if baseline[name][:2] != restored[name][:2]:
                raise RuntimeError(f"{name}: A/B/A result changed after rollback")
        failures = compare(
            restored,
            candidate,
            options.max_relative_regression,
            options.max_absolute_regression,
        )
        if failures:
            print("\nROLLED BACK; candidate index remains live")
            for failure in failures:
                print("  " + failure)
            return 1

        if options.apply:
            cursor.execute("SET LOCAL lock_timeout = '3s'")
            cursor.execute(f"DROP INDEX euroleague.{CANDIDATE}")
            connection.commit()
            print(f"\nCOMMITTED; reclaimed {size / 1048576:.1f} MiB")
        else:
            print("\nDRY RUN PASSED; rolled back and left the live index unchanged")
        return 0
    except Exception:
        connection.rollback()
        raise
    finally:
        cursor.close()
        connection.close()


if __name__ == "__main__":
    raise SystemExit(main())
