#!/usr/bin/env python
"""Commit disposable migration-050 probes, measure fresh backends, then drop.

This is the production-like cold-call experiment that rollback-only DDL cannot
provide. It never changes the production reader names. Cleanup targets only
three exact, pre-validated probe signatures and is verified after commit.
"""
from __future__ import annotations

import argparse
import json
import re
import sys
import time
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(ROOT / "src"))

from gate_050_combined_team_dashboard_readers import (  # noqa: E402
    COMBINED_COLUMNS,
    DDL,
    ENV,
    METRIC_COLUMNS,
    Route,
    arguments,
    fetch,
    legacy,
    validate_ddl,
    value,
)
from euroleague_possessions.postgres_backend import connect_from_env_file  # noqa: E402


ROUTES = (
    Route("pergame", "broad", ""),
    Route("dynamic", "standard clutch", "p_max_margin=>5,p_margin_status=>'all',p_max_time_remaining=>300"),
    Route("direct", "custom clutch", "p_max_margin=>3,p_margin_status=>'all',p_max_time_remaining=>240"),
)
PRODUCTION = {
    "pergame": "get_team_metrics_pergame",
    "dynamic": "get_team_dashboard_dynamic",
    "direct": "get_team_metrics_direct",
}
PROBES = {kind: f"{name}_cold_probe" for kind, name in PRODUCTION.items()}
SIGNATURES = {
    "pergame": "text,integer,date,date,text,text,text,text,text,text,integer,text,integer,integer,integer,integer,integer,integer,integer",
    "dynamic": "text,integer,date,date,text,text,text,text,text,text,integer,text,integer,text,integer,boolean,integer,integer,integer,integer,integer,integer,integer",
    "direct": "text,integer,date,date,text,text,text,text,text,text,integer,text,integer,text,integer,boolean,integer,integer,integer,integer,integer,integer,integer",
}


def configure(cursor) -> None:
    cursor.execute("SET LOCAL lock_timeout='10s'")
    cursor.execute("SET LOCAL statement_timeout='90s'")


def probe_source() -> str:
    source = DDL.read_text(encoding="utf-8")
    for kind, probe in PROBES.items():
        production = PRODUCTION[kind]
        if source.count(production) != 3:
            raise RuntimeError(
                f"expected three exact references to {production}, found {source.count(production)}"
            )
        source = source.replace(production, probe)
    source = source.replace("CREATE OR REPLACE FUNCTION", "CREATE FUNCTION")
    if "CREATE OR REPLACE FUNCTION" in source:
        raise RuntimeError("probe transformation left CREATE OR REPLACE behind")
    return source


def probe_statements(atomic: bool) -> list[str]:
    statements = validate_ddl(probe_source())
    if not atomic:
        return statements
    converted = []
    count = 0
    for statement in statements:
        if "CREATE FUNCTION euroleague.get_team_" not in statement:
            converted.append(statement)
            continue
        statement, replacements = re.subn(
            r"AS \$function\$\s*(.*?)\s*\$function\$\s*$",
            lambda match: "BEGIN ATOMIC\n" + match.group(1).rstrip(";\n ") + ";\nEND",
            statement,
            count=1,
            flags=re.S,
        )
        if replacements != 1:
            raise RuntimeError("failed to convert probe body to BEGIN ATOMIC")
        converted.append(statement)
        count += 1
    if count != 3:
        raise RuntimeError(f"expected three atomic probe conversions, found {count}")
    return converted


def catalog_probe_names(cursor) -> set[str]:
    cursor.execute(
        "SELECT p.proname FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace "
        "WHERE n.nspname='euroleague' AND p.proname=ANY(%s)",
        (list(PROBES.values()),),
    )
    return {str(row[0]) for row in cursor.fetchall()}


def deploy_probes(connection, atomic: bool) -> float:
    cursor = connection.cursor()
    try:
        configure(cursor)
        existing = catalog_probe_names(cursor)
        if existing:
            raise RuntimeError(f"refusing to replace existing probe functions: {sorted(existing)}")
        started = time.perf_counter()
        for statement in probe_statements(atomic):
            cursor.execute(statement)
        created = catalog_probe_names(cursor)
        if created != set(PROBES.values()):
            raise RuntimeError(f"probe creation incomplete: {sorted(created)}")
        connection.commit()
        return time.perf_counter() - started
    except Exception:
        connection.rollback()
        raise
    finally:
        cursor.close()


def drop_probes(connection) -> None:
    cursor = connection.cursor()
    try:
        configure(cursor)
        existing = catalog_probe_names(cursor)
        unexpected = existing - set(PROBES.values())
        if unexpected:
            raise RuntimeError(f"unexpected probe catalog state: {sorted(unexpected)}")
        for kind, name in PROBES.items():
            cursor.execute(
                f"DROP FUNCTION IF EXISTS euroleague.{name}({SIGNATURES[kind]})"
            )
        remaining = catalog_probe_names(cursor)
        if remaining:
            raise RuntimeError(f"probe cleanup incomplete before commit: {sorted(remaining)}")
        connection.commit()
        cursor.execute("SET TRANSACTION READ ONLY")
        if catalog_probe_names(cursor):
            raise RuntimeError("probe cleanup verification failed after commit")
        connection.rollback()
    except Exception:
        connection.rollback()
        raise
    finally:
        cursor.close()


def backend_pid(cursor) -> int:
    cursor.execute("SELECT pg_backend_pid()")
    return int(cursor.fetchone()[0])


def open_measurement_session():
    started = time.perf_counter()
    connection = connect_from_env_file(ENV, direct_port=5432)
    elapsed = time.perf_counter() - started
    cursor = connection.cursor()
    try:
        pid = backend_pid(cursor)
        connection.rollback()
    finally:
        cursor.close()
    return connection, pid, elapsed


def fetch_probe(cursor, route: Route, call_args: str):
    started = time.perf_counter()
    cursor.execute(f"SELECT * FROM euroleague.{PROBES[route.kind]}({call_args})")
    columns = tuple(column.name for column in cursor.description)
    rows = cursor.fetchall()
    expected_columns = COMBINED_COLUMNS if route.kind == "dynamic" else METRIC_COLUMNS
    if columns != expected_columns:
        raise RuntimeError(f"unexpected probe columns: {columns!r}")
    team_index = columns.index("team_id")
    keyed = {row[team_index]: tuple(row) for row in rows}
    if len(keyed) != len(rows):
        raise RuntimeError("probe returned duplicate team_id values")
    if route.kind != "dynamic":
        minute_columns, minutes, _ = fetch(
            cursor, f"get_team_minutes_{route.kind}", call_args
        )
        keyed = {
            team_id: row + (value(minute_columns, minutes[team_id], "minutes"),)
            for team_id, row in keyed.items()
        }
    elapsed = time.perf_counter() - started
    return keyed, elapsed


def measure(connection, pid: int, connect_seconds: float, route: Route,
            candidate_first: bool) -> dict:
    cursor = connection.cursor()
    try:
        configure(cursor)
        call_args = arguments(route.extra)
        if candidate_first:
            actual, combined_seconds = fetch_probe(cursor, route, call_args)
            expected, three_call_seconds = legacy(cursor, route, call_args)
        else:
            expected, three_call_seconds = legacy(cursor, route, call_args)
            actual, combined_seconds = fetch_probe(cursor, route, call_args)
        if expected != actual:
            raise RuntimeError(f"cold probe parity failed for {route.kind}/{route.label}")
        connection.rollback()
        return {
            "route": route.kind,
            "label": route.label,
            "order": "candidate_first" if candidate_first else "legacy_first",
            "backend_pid": pid,
            "connection_seconds": connect_seconds,
            "three_calls_seconds": three_call_seconds,
            "combined_seconds": combined_seconds,
        }
    except Exception:
        connection.rollback()
        raise
    finally:
        cursor.close()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--atomic", action="store_true")
    args = parser.parse_args()
    suffix = "atomic" if args.atomic else "baseline"
    output = Path(f"C:/tmp/combined_team_dashboard_committed_cold_{suffix}_2026-09-01.json")
    deploy_connection = connect_from_env_file(ENV, direct_port=5432)
    deploy_connection.autocommit = False
    sessions = []
    report = {
        "variant": suffix,
        "probe_ddl_seconds": None,
        "deploy_backend_pid": None,
        "samples": [],
    }
    probes_committed = False
    try:
        deploy_cursor = deploy_connection.cursor()
        try:
            report["deploy_backend_pid"] = backend_pid(deploy_cursor)
            deploy_connection.rollback()
        finally:
            deploy_cursor.close()
        report["probe_ddl_seconds"] = deploy_probes(deploy_connection, args.atomic)
        probes_committed = True
        print(
            f"committed disposable probes on pid={report['deploy_backend_pid']} "
            f"ddl={report['probe_ddl_seconds']:.3f}s",
            flush=True,
        )

        for route in ROUTES:
            first = open_measurement_session()
            second = open_measurement_session()
            sessions.extend((first[0], second[0]))
            pids = (first[1], second[1])
            if len(set(pids + (report["deploy_backend_pid"],))) != 3:
                raise RuntimeError(
                    f"measurement did not receive untouched distinct backends: "
                    f"deploy={report['deploy_backend_pid']} measurement={pids}"
                )
            for session, candidate_first in ((first, True), (second, False)):
                sample = measure(
                    session[0], session[1], session[2], route, candidate_first
                )
                report["samples"].append(sample)
                print(
                    f"{route.kind:<8} {sample['order']:<15} pid={sample['backend_pid']} "
                    f"three_calls={sample['three_calls_seconds']:.3f}s "
                    f"combined={sample['combined_seconds']:.3f}s",
                    flush=True,
                )
            for connection in (first[0], second[0]):
                connection.close()
                sessions.remove(connection)
    finally:
        for connection in sessions:
            connection.close()
        if probes_committed:
            try:
                drop_probes(deploy_connection)
                print("dropped and verified all disposable probe functions", flush=True)
            except Exception:
                # One fresh cleanup attempt handles a broken deployment session.
                deploy_connection.close()
                deploy_connection = connect_from_env_file(ENV, direct_port=5432)
                deploy_connection.autocommit = False
                drop_probes(deploy_connection)
                print("dropped and verified probes through recovery connection", flush=True)
        deploy_connection.close()

    output.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(f"wrote {output}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
