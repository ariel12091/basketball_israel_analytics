#!/usr/bin/env python
"""Republish staged EuroLeague games and prove exact rollback-safe parity.

The probe exercises the real per-game write path inside a transaction, compares
stable projections before and after validation, and rolls back. Nothing is
committed. Stage-format-4 checkpoints are accepted by discarding their retired
middle-table rows; canonical ``actions`` was already present in that format.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path
from typing import Any


REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "src"))

from euroleague_possessions.postgres_backend import (  # noqa: E402
    PostgresTransactionBackend,
    assert_shadow_schema_compatible,
    connect_from_env_file,
    inspect_target,
)
from euroleague_possessions.transaction_writer import (  # noqa: E402
    DELETE_ORDER,
    INSERT_ORDER,
    GameSnapshot,
    NaturalGameKey,
)


PROJECTIONS: dict[str, str] = {
    "full_rosters": """
        SELECT player_id::text, to_jsonb(r)
          FROM euroleague.full_rosters r WHERE game_id = %(game_id)s
    """,
    "team_boxscores": """
        SELECT team_id::text, to_jsonb(b)
          FROM euroleague.team_boxscores b WHERE game_id = %(game_id)s
    """,
    "actions_raw": """
        SELECT source_event_order::text, to_jsonb(a)
          FROM euroleague.actions_raw a WHERE game_id = %(game_id)s
    """,
    "actions": """
        SELECT source_event_order::text, to_jsonb(a) - 'derived_at'
          FROM euroleague.actions a WHERE game_id = %(game_id)s
    """,
    "matchup_segments_actions": """
        SELECT (team_id, segment_id)::text, to_jsonb(s) - 'derived_at'
          FROM euroleague.matchup_segments_actions s WHERE game_id = %(game_id)s
    """,
    "action_team_context_actions": """
        SELECT (source_event_order, team_id)::text, to_jsonb(a) - 'derived_at'
          FROM euroleague.action_team_context_actions a WHERE game_id = %(game_id)s
    """,
    "player_four_factors_by_game": """
        SELECT (
                 player_id, team_id, is_on_key, type_lineup,
                 own_starters, opp_starters
               )::text,
               to_jsonb(f) - 'derived_at'
          FROM euroleague.player_four_factors_by_game f
         WHERE game_id = %(game_id)s
    """,
    "team_four_factors_by_game": """
        SELECT (team_id, own_starters, opp_starters)::text, to_jsonb(f)
          FROM euroleague.team_four_factors_by_game f
         WHERE game_id = %(game_id)s
    """,
    "reconciliation_metrics": """
        SELECT (team_id, metric)::text, to_jsonb(r)
          FROM euroleague.reconciliation_metrics r
         WHERE game_id = %(game_id)s AND load_run_id = %(load_run_id)s
    """,
    "game_qa": """
        SELECT load_run_id::text, to_jsonb(q)
          FROM euroleague.game_qa q
         WHERE game_id = %(game_id)s AND load_run_id = %(load_run_id)s
    """,
}


def project(
    connection: Any,
    game_id: int,
    load_run_id: int,
) -> dict[str, dict[str, tuple]]:
    out: dict[str, dict[str, tuple]] = {}
    cursor = connection.cursor()
    try:
        parameters = {"game_id": game_id, "load_run_id": load_run_id}
        for name, sql in PROJECTIONS.items():
            cursor.execute(sql, parameters)
            rows = cursor.fetchall()
            out[name] = {str(row[0]): tuple(row[1:]) for row in rows}
            if len(out[name]) != len(rows):
                raise RuntimeError(f"{name}: projection key is not unique")
    finally:
        cursor.close()
    return out


def differences(
    before: dict[str, dict[str, tuple]],
    after: dict[str, dict[str, tuple]],
) -> list[str]:
    problems: list[str] = []
    for name in PROJECTIONS:
        old, new = before[name], after[name]
        comparisons = (
            ("lost", sorted(set(old) - set(new))),
            ("appeared", sorted(set(new) - set(old))),
            ("changed", sorted(k for k in set(old) & set(new) if old[k] != new[k])),
        )
        for label, keys in comparisons:
            if keys:
                detail = f"e.g. {keys[:3]}"
                if label == "changed":
                    key = keys[0]
                    old_value, new_value = old[key], new[key]
                    if (
                        len(old_value) == len(new_value) == 1
                        and isinstance(old_value[0], dict)
                        and isinstance(new_value[0], dict)
                    ):
                        old_row, new_row = old_value[0], new_value[0]
                        fields = sorted(
                            field
                            for field in set(old_row) | set(new_row)
                            if old_row.get(field) != new_row.get(field)
                        )
                        detail = ", ".join(
                            f"{field}: {old_row.get(field)!r} -> {new_row.get(field)!r}"
                            for field in fields[:5]
                        )
                problems.append(
                    f"{name}: {len(keys)} key(s) {label}; {detail}"
                )
    return problems


def probe_game(connection: Any, snapshot: GameSnapshot) -> dict[str, Any]:
    cursor = connection.cursor()
    cursor.execute(
        "SELECT game_id, last_seen_load_run_id FROM euroleague.schedule "
        "WHERE competition = %s AND season = %s AND gamecode = %s",
        (snapshot.key.competition, snapshot.key.season, snapshot.key.gamecode),
    )
    row = cursor.fetchone()
    cursor.close()
    if row is None:
        raise RuntimeError(f"{snapshot.key} is not loaded")
    game_id, load_run_id = int(row[0]), int(row[1])

    before = project(connection, game_id, load_run_id)
    backend = PostgresTransactionBackend(connection, load_run_id=load_run_id)
    timings: dict[str, float] = {}
    opened = False
    try:
        started = time.perf_counter()
        if backend.begin(snapshot.key) != game_id:
            raise RuntimeError("schedule resolved to a different game_id")
        opened = True
        timings["begin"] = time.perf_counter() - started

        started = time.perf_counter()
        for table in DELETE_ORDER:
            backend.delete_game_rows(table, game_id)
        timings["delete"] = time.perf_counter() - started

        started = time.perf_counter()
        for table in INSERT_ORDER:
            rows = snapshot.rows.get(table, ())
            if rows:
                backend.insert_rows(table, game_id, rows)
        timings["insert"] = time.perf_counter() - started

        started = time.perf_counter()
        backend.validate_game(game_id)
        timings["validate"] = time.perf_counter() - started
        problems = differences(before, project(connection, game_id, load_run_id))
    finally:
        if opened:
            backend.rollback()

    return {
        "gamecode": snapshot.key.gamecode,
        "actions": len(before["actions"]),
        "segments": len(before["matchup_segments_actions"]),
        "problems": problems,
        "rollback_problems": differences(
            before, project(connection, game_id, load_run_id)
        ),
        "timings": timings,
    }


def load_snapshot(path: Path) -> GameSnapshot:
    payload = json.loads(path.read_text(encoding="utf-8"))["snapshot"]
    rows = {
        str(table): tuple(table_rows)
        for table, table_rows in payload["rows"].items()
        if table in INSERT_ORDER
    }
    snapshot = GameSnapshot(
        key=NaturalGameKey(
            competition=str(payload["key"]["competition"]),
            season=int(payload["key"]["season"]),
            gamecode=int(payload["key"]["gamecode"]),
        ),
        rows=rows,
    )
    snapshot.validate()
    return snapshot


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--games", default="1-3")
    parser.add_argument("--season", type=int, default=2025)
    parser.add_argument("--competition", default="E")
    parser.add_argument(
        "--checkpoint-dir",
        type=Path,
        default=REPO / "data" / "staging" / "batch_84_v2",
    )
    parser.add_argument(
        "--env-file", type=Path, default=REPO.parent / "etl" / ".Renviron"
    )
    args = parser.parse_args()

    sys.path.insert(0, str(REPO / "scripts"))
    from load_games import parse_games  # noqa: E402

    snapshots = [
        load_snapshot(
            args.checkpoint_dir
            / f"{args.competition}{args.season}_{code}.stage.json"
        )
        for code in parse_games(args.games)
    ]
    connection = connect_from_env_file(args.env_file, direct_port=5432)
    failures = 0
    try:
        target = inspect_target(connection)
        if int(target["server_port"]) != 5432:
            raise SystemExit("probe requires direct PostgreSQL port 5432")
        assert_shadow_schema_compatible(connection)
        for snapshot in snapshots:
            result = probe_game(connection, snapshot)
            timings = result["timings"]
            print(
                f"game {result['gamecode']}: {result['actions']} actions, "
                f"{result['segments']} segments; "
                f"begin={timings['begin']:.2f}s delete={timings['delete']:.2f}s "
                f"insert={timings['insert']:.2f}s validate={timings['validate']:.2f}s"
            )
            for label, problems in (
                ("republish parity", result["problems"]),
                ("rollback restoration", result["rollback_problems"]),
            ):
                if problems:
                    failures += 1
                    print(f"  FAIL {label}: {'; '.join(problems)}")
                else:
                    print(f"  PASS {label}")
    finally:
        connection.close()

    print("ALL PROBES PASSED" if not failures else f"{failures} PROBE(S) FAILED")
    raise SystemExit(1 if failures else 0)


if __name__ == "__main__":
    main()
