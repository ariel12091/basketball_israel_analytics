#!/usr/bin/env python
"""Rollback-only benchmark for migration 050 combined Team readers.

Measures complete fetched calls, alternates call order for warm samples, records
shared buffers, and repeats first-use measurements across new client sessions.
Candidate DDL is never committed.
"""
from __future__ import annotations

import argparse
import json
import math
import statistics
import sys
import time
from dataclasses import dataclass
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(ROOT / "src"))

from gate_050_combined_team_dashboard_readers import (  # noqa: E402
    DDL,
    ENV,
    Route,
    arguments,
    candidate,
    legacy,
    validate_ddl,
)
from euroleague_possessions.postgres_backend import connect_from_env_file  # noqa: E402


BENCHMARK_ROUTES = (
    Route("pergame", "broad", ""),
    Route("dynamic", "standard clutch", "p_max_margin=>5,p_margin_status=>'all',p_max_time_remaining=>300"),
    Route("direct", "custom clutch", "p_max_margin=>3,p_margin_status=>'all',p_max_time_remaining=>240"),
)


@dataclass(frozen=True)
class BufferResult:
    hits: int
    reads: int
    dirtied: int
    execution_ms: float

    def __add__(self, other: "BufferResult") -> "BufferResult":
        return BufferResult(
            self.hits + other.hits,
            self.reads + other.reads,
            self.dirtied + other.dirtied,
            self.execution_ms + other.execution_ms,
        )


ZERO_BUFFERS = BufferResult(0, 0, 0, 0.0)


def percentile(values: list[float], fraction: float) -> float:
    ordered = sorted(values)
    return ordered[max(0, math.ceil(len(ordered) * fraction) - 1)]


def summary(values: list[float]) -> dict[str, float | list[float]]:
    return {
        "median": statistics.median(values),
        "p90": percentile(values, 0.9),
        "min": min(values),
        "max": max(values),
        "samples": values,
    }


def execute_candidate_ddl(cursor) -> float:
    started = time.perf_counter()
    for statement in validate_ddl(DDL.read_text(encoding="utf-8")):
        cursor.execute(statement)
    return time.perf_counter() - started


def configure(cursor) -> None:
    cursor.execute("SET LOCAL lock_timeout='5s'")
    cursor.execute("SET LOCAL statement_timeout='90s'")


def query_for(function: str, call_args: str) -> str:
    return f"SELECT * FROM euroleague.{function}({call_args})"


def explain(cursor, query: str) -> BufferResult:
    cursor.execute(f"EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {query}")
    payload = cursor.fetchone()[0]
    if isinstance(payload, str):
        payload = json.loads(payload)
    report = payload[0]
    plan = report["Plan"]
    return BufferResult(
        int(plan.get("Shared Hit Blocks", 0)),
        int(plan.get("Shared Read Blocks", 0)),
        int(plan.get("Shared Dirtied Blocks", 0)),
        float(report.get("Execution Time", 0.0)),
    )


def legacy_buffers(cursor, route: Route, call_args: str) -> BufferResult:
    total = ZERO_BUFFERS
    for metric in ("ratings", "four_factors", "minutes"):
        total += explain(cursor, query_for(f"get_team_{metric}_{route.kind}", call_args))
    return total


def candidate_buffers(cursor, route: Route, call_args: str) -> BufferResult:
    if route.kind == "dynamic":
        return explain(cursor, query_for("get_team_dashboard_dynamic", call_args))
    return (
        explain(cursor, query_for(f"get_team_metrics_{route.kind}", call_args))
        + explain(cursor, query_for(f"get_team_minutes_{route.kind}", call_args))
    )


def warm_benchmark(route: Route, runs: int) -> dict:
    connection = connect_from_env_file(ENV, direct_port=5432)
    connection.autocommit = False
    cursor = connection.cursor()
    try:
        configure(cursor)
        ddl_seconds = execute_candidate_ddl(cursor)
        call_args = arguments(route.extra)
        expected, _ = legacy(cursor, route, call_args)
        actual, _ = candidate(cursor, route, call_args)
        if expected != actual:
            raise RuntimeError(f"warm-up parity failed for {route.kind}/{route.label}")
        # A second untimed pass keeps one side from being the only fully warmed path.
        candidate(cursor, route, call_args)
        legacy(cursor, route, call_args)

        old_samples: list[float] = []
        new_samples: list[float] = []
        for index in range(runs):
            if index % 2 == 0:
                expected, old_seconds = legacy(cursor, route, call_args)
                actual, new_seconds = candidate(cursor, route, call_args)
            else:
                actual, new_seconds = candidate(cursor, route, call_args)
                expected, old_seconds = legacy(cursor, route, call_args)
            if expected != actual:
                raise RuntimeError(f"timed parity failed for {route.kind}/{route.label}")
            old_samples.append(old_seconds)
            new_samples.append(new_seconds)
            print(
                f"  warm {route.kind}/{route.label} {index + 1:02d}/{runs}: "
                f"three_calls={old_seconds:.3f}s combined={new_seconds:.3f}s",
                flush=True,
            )

        old_buffers = legacy_buffers(cursor, route, call_args)
        new_buffers = candidate_buffers(cursor, route, call_args)
        return {
            "route": route.kind,
            "label": route.label,
            "backend_pid": backend_pid(cursor),
            "ddl_seconds": ddl_seconds,
            "three_calls": summary(old_samples),
            "combined": summary(new_samples),
            "three_call_buffers": old_buffers.__dict__,
            "combined_buffers": new_buffers.__dict__,
        }
    finally:
        try:
            connection.rollback()
        except Exception:
            pass
        cursor.close()
        connection.close()


def backend_pid(cursor) -> int:
    cursor.execute("SELECT pg_backend_pid()")
    return int(cursor.fetchone()[0])


def first_use_sample(connection, route: Route, candidate_first: bool,
                     connection_seconds: float, pid: int) -> dict:
    connection.autocommit = False
    cursor = connection.cursor()
    try:
        configure(cursor)
        ddl_seconds = execute_candidate_ddl(cursor)
        call_args = arguments(route.extra)
        if candidate_first:
            actual, new_seconds = candidate(cursor, route, call_args)
            expected, old_seconds = legacy(cursor, route, call_args)
        else:
            expected, old_seconds = legacy(cursor, route, call_args)
            actual, new_seconds = candidate(cursor, route, call_args)
        if expected != actual:
            raise RuntimeError(f"first-use parity failed for {route.kind}/{route.label}")
        return {
            "route": route.kind,
            "label": route.label,
            "order": "candidate_first" if candidate_first else "legacy_first",
            "backend_pid": pid,
            "connection_seconds": connection_seconds,
            "ddl_seconds": ddl_seconds,
            "three_calls_seconds": old_seconds,
            "combined_seconds": new_seconds,
        }
    finally:
        connection.rollback()
        cursor.close()


def first_use_batch(route: Route, runs: int) -> list[dict]:
    """Hold sessions concurrently so session mode assigns distinct backends."""
    sessions = []
    orders = [order for _ in range(runs) for order in (True, False)]
    try:
        for candidate_first in orders:
            connected = time.perf_counter()
            connection = connect_from_env_file(ENV, direct_port=5432)
            connection_seconds = time.perf_counter() - connected
            cursor = connection.cursor()
            try:
                pid = backend_pid(cursor)
                connection.rollback()
            finally:
                cursor.close()
            sessions.append((connection, connection_seconds, pid, candidate_first))

        pids = [session[2] for session in sessions]
        if len(set(pids)) != len(pids):
            raise RuntimeError(f"session-mode batch did not receive distinct backends: {pids}")
        print(f"  reserved distinct backend pids={pids}", flush=True)

        results = []
        for index, (connection, connection_seconds, pid, candidate_first) in enumerate(sessions):
            label = "candidate-first" if candidate_first else "legacy-first"
            print(
                f"first-use {route.kind}/{route.label} {index + 1}/{len(sessions)} {label}",
                flush=True,
            )
            sample = first_use_sample(
                connection, route, candidate_first, connection_seconds, pid
            )
            results.append(sample)
            print(
                f"  pid={sample['backend_pid']} connect={sample['connection_seconds']:.3f}s "
                f"ddl={sample['ddl_seconds']:.3f}s "
                f"three_calls={sample['three_calls_seconds']:.3f}s "
                f"combined={sample['combined_seconds']:.3f}s",
                flush=True,
            )
        return results
    finally:
        for connection, _, _, _ in sessions:
            connection.close()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--warm-runs", type=int, default=15)
    # Session mode on the current pool reliably supports the two simultaneous
    # connections needed for one candidate-first/legacy-first pair. Larger
    # batches may exhaust the pool and must be requested deliberately.
    parser.add_argument("--first-use-runs", type=int, default=1)
    parser.add_argument("--skip-warm", action="store_true")
    parser.add_argument("--skip-first-use", action="store_true")
    parser.add_argument(
        "--route", choices=("all", "pergame", "dynamic", "direct"), default="all"
    )
    parser.add_argument("--output", type=Path, default=None)
    args = parser.parse_args()
    if args.warm_runs < 3 or args.first_use_runs < 1:
        parser.error("--warm-runs must be >=3 and --first-use-runs must be >=1")

    report = {"warm": [], "first_use": []}
    routes = (
        BENCHMARK_ROUTES
        if args.route == "all"
        else tuple(route for route in BENCHMARK_ROUTES if route.kind == args.route)
    )
    if not args.skip_warm:
        for route in routes:
            print(f"benchmarking warm {route.kind}/{route.label}", flush=True)
            report["warm"].append(warm_benchmark(route, args.warm_runs))

    if not args.skip_first_use:
        for route in routes:
            report["first_use"].extend(first_use_batch(route, args.first_use_runs))

    rendered = json.dumps(report, indent=2)
    if args.output:
        args.output.write_text(rendered + "\n", encoding="utf-8")
        print(f"wrote {args.output}", flush=True)
    else:
        print(rendered)
    print("benchmark complete; every candidate transaction rolled back", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
