#!/usr/bin/env python
"""Prove a publication-path change is equivalent, without changing any data.

Republishes already-loaded games through the real backend against the real
database -- real constraints, real driver, real generated ids -- and then
ROLLS BACK instead of committing. Nothing is written.

Why this exists. The lineups/stints inserts were changed from one row at a
time to a single multi-row ``INSERT ... RETURNING``, with generated ids mapped
back by NATURAL KEY because PostgreSQL does not promise RETURNING follows
VALUES order. Getting that wrong does not raise: it silently mis-wires every
lineup and stint reference in the game. Unit tests cover the mapping with a
fake cursor; only a real database proves the whole path.

The check is a natural-key projection. Surrogate ids are re-generated on every
insert, so they cannot be compared directly. Instead every reference is
rewritten in terms of the key a human would use -- a lineup is
``(team, lineup_hash)``, a stint is ``(team, stint_number)`` -- and that
projection must be identical to the one already in the database, which the
previous code wrote from these same checkpoints.

    .venv/Scripts/python.exe scripts/probe_batched_publish.py --games 1-3

Read-only in effect. It opens a write transaction and rolls it back, so the
only lasting trace is that some id sequences advance.
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

# Each query returns one row per object: a stable natural key, then the values
# compared. Every lineup and stint reference is resolved back to its natural
# key, so the result does not depend on which surrogate ids were generated.
# The last entry is downstream of all the others -- if a lineup reference were
# mis-wired, ON/OFF exposure moves even when the wiring tables look plausible.
PROJECTIONS: dict[str, str] = {
    "lineups": """
        SELECT (l.team_id, l.lineup_hash)::text,
               l.player_count, l.starter_count, l.structure_valid
          FROM euroleague.lineups l
         WHERE l.game_id = %(game_id)s
    """,
    "lineup_players": """
        SELECT (l.team_id, l.lineup_hash)::text,
               array_agg((lp.player_id, lp.package_slot, lp.is_starter)::text
                         ORDER BY lp.package_slot, lp.player_id)
          FROM euroleague.lineup_players lp
          JOIN euroleague.lineups l ON l.lineup_id = lp.lineup_id
         WHERE l.game_id = %(game_id)s
         GROUP BY l.team_id, l.lineup_hash
    """,
    "action_lineups": """
        SELECT al.source_event_order::text,
               (h.team_id, h.lineup_hash)::text,
               (a.team_id, a.lineup_hash)::text,
               al.validate_on_court_player, al.lineup_structure_valid
          FROM euroleague.action_lineups al
          JOIN euroleague.lineups h ON h.lineup_id = al.home_lineup_id
          JOIN euroleague.lineups a ON a.lineup_id = al.away_lineup_id
         WHERE al.game_id = %(game_id)s
    """,
    "stints": """
        SELECT (s.team_id, s.stint_number)::text,
               (l.team_id, l.lineup_hash)::text,
               s.start_event_order, s.end_event_order_exclusive,
               s.start_elapsed_seconds, s.end_elapsed_seconds,
               s.duration_seconds, s.qa_status, s.publishable
          FROM euroleague.stints s
          JOIN euroleague.lineups l ON l.lineup_id = s.lineup_id
         WHERE s.game_id = %(game_id)s
    """,
    "pws": """
        SELECT p.game_possession_number::text,
               (ol.team_id, ol.lineup_hash)::text,
               (dl.team_id, dl.lineup_hash)::text,
               (os.team_id, os.stint_number)::text,
               (ds.team_id, ds.stint_number)::text,
               p.num_starters_offense, p.num_starters_defense,
               p.lineup_validation_clear
          FROM euroleague.pws p
          JOIN euroleague.lineups ol ON ol.lineup_id = p.offense_lineup_id
          JOIN euroleague.lineups dl ON dl.lineup_id = p.defense_lineup_id
          JOIN euroleague.stints os ON os.stint_id = p.offense_stint_id
          JOIN euroleague.stints ds ON ds.stint_id = p.defense_stint_id
         WHERE p.game_id = %(game_id)s
    """,
    "player_game_context": """
        SELECT (team_id, player_id, type_lineup, is_on_key)::text,
               sum(total_points)::text, sum(total_poss)::text,
               sum(total_fga)::text, sum(total_fgm)::text,
               sum(ts_poss_count)::text, sum(oreb_count)::text,
               sum(tov_count)::text, sum(onoff_minutes)::text
          FROM euroleague.player_game_context
         WHERE game_id = %(game_id)s
         GROUP BY 1
    """,
    "action_team_context": """
        SELECT (a.source_event_order, t.provider_team_code)::text,
               a.type_lineup, a.points, a.possession_flag,
               a.ts_possessions, a.orebounds, a.turnovers, a.steals,
               a.own_team_score, a.opp_team_score, a.segment_id,
               (ol.team_id, ol.lineup_hash)::text
          FROM euroleague.action_team_context a
          JOIN euroleague.teams t ON t.team_id = a.team_id
          JOIN euroleague.lineups ol ON ol.lineup_id = a.own_lineup_id
         WHERE a.game_id = %(game_id)s
    """,
    "matchup_segments": """
        SELECT (t.provider_team_code, m.segment_id)::text,
               m.own_starters, m.opp_starters, m.start_event_order,
               m.start_elapsed_seconds, m.end_elapsed_seconds,
               m.segment_seconds,
               (ol.team_id, ol.lineup_hash)::text,
               (pl.team_id, pl.lineup_hash)::text
          FROM euroleague.matchup_segments m
          JOIN euroleague.teams t ON t.team_id = m.team_id
          JOIN euroleague.lineups ol ON ol.lineup_id = m.own_lineup_id
          JOIN euroleague.lineups pl ON pl.lineup_id = m.opp_lineup_id
         WHERE m.game_id = %(game_id)s
    """,
}


def project(connection: Any, game_id: int) -> dict[str, dict[str, tuple]]:
    """Read every projection for one game as {relation: {key: value}}."""
    out: dict[str, dict[str, tuple]] = {}
    cursor = connection.cursor()
    try:
        for name, sql in PROJECTIONS.items():
            cursor.execute(sql, {"game_id": game_id})
            rows = cursor.fetchall()
            out[name] = {str(row[0]): tuple(row[1:]) for row in rows}
            if len(out[name]) != len(rows):
                raise RuntimeError(f"{name}: projection key is not unique")
    finally:
        cursor.close()
    return out


def differences(before: dict[str, dict[str, tuple]], after: dict[str, dict[str, tuple]]) -> list[str]:
    """Every way two projections disagree, one line each."""
    problems = []
    for name in PROJECTIONS:
        old, new = before[name], after[name]
        for label, keys in (
            ("lost", sorted(set(old) - set(new))),
            ("appeared", sorted(set(new) - set(old))),
            ("changed", sorted(k for k in set(old) & set(new) if old[k] != new[k])),
        ):
            if not keys:
                continue
            detail = f", e.g. {keys[0]}: {old[keys[0]]!r} -> {new[keys[0]]!r}" if label == "changed" else f", e.g. {keys[:3]}"
            problems.append(f"{name}: {len(keys)} key(s) {label}{detail}")
    return problems


def probe_game(connection: Any, snapshot: GameSnapshot) -> dict[str, Any]:
    """Republish one game inside a transaction, compare, then roll back."""
    cursor = connection.cursor()
    # begin() resolves source artifacts by (load_run_id, game_id), so the probe
    # reuses the run that last published the game -- which also means it
    # creates no load_runs row of its own. Artifacts are immutable and are
    # excluded from per-game replacement, so a re-published game carries rows
    # from every run that ever loaded it; only the latest owns its facts.
    cursor.execute(
        "SELECT s.game_id, s.last_seen_load_run_id, "
        "  (SELECT count(*) FROM euroleague.source_artifacts sa "
        "    WHERE sa.game_id = s.game_id "
        "      AND sa.load_run_id = s.last_seen_load_run_id) "
        "FROM euroleague.schedule s "
        "WHERE s.competition = %s AND s.season = %s AND s.gamecode = %s",
        (snapshot.key.competition, snapshot.key.season, snapshot.key.gamecode),
    )
    row = cursor.fetchone()
    cursor.close()
    if row is None:
        raise RuntimeError(f"{snapshot.key} is not loaded; nothing to compare against")
    game_id, load_run_id, artifacts = int(row[0]), int(row[1]), int(row[2])
    if artifacts == 0:
        raise RuntimeError(
            f"game {game_id} has no source artifacts under its owning run {load_run_id}"
        )

    before = project(connection, game_id)
    backend = PostgresTransactionBackend(connection, load_run_id=int(load_run_id))
    timings: dict[str, float] = {}
    opened = False
    try:
        clock = time.perf_counter()
        if backend.begin(snapshot.key) != game_id:
            raise RuntimeError("schedule resolved to a different game_id")
        opened = True
        timings["begin"] = time.perf_counter() - clock

        clock = time.perf_counter()
        for table in DELETE_ORDER:
            backend.delete_game_rows(table, game_id)
        timings["delete"] = time.perf_counter() - clock

        clock = time.perf_counter()
        for table in INSERT_ORDER:
            if snapshot.rows.get(table):
                backend.insert_rows(table, game_id, snapshot.rows[table])
        timings["insert"] = time.perf_counter() - clock

        clock = time.perf_counter()
        backend.validate_game(game_id)
        timings["validate"] = time.perf_counter() - clock

        problems = differences(before, project(connection, game_id))
    finally:
        if opened:
            backend.rollback()

    return {
        "gamecode": snapshot.key.gamecode,
        "lineups": len(before["lineups"]),
        "stints": len(before["stints"]),
        "problems": problems,
        "rollback_problems": differences(before, project(connection, game_id)),
        "timings": timings,
    }


def load_snapshot(path: Path) -> GameSnapshot:
    """Read a stage checkpoint's snapshot.

    Deliberately skips staging's input-fingerprint check: this probe replays
    what was published, and the source CSV it came from need not still exist.
    """
    payload = json.loads(path.read_text(encoding="utf-8"))["snapshot"]
    snapshot = GameSnapshot(
        key=NaturalGameKey(
            competition=str(payload["key"]["competition"]),
            season=int(payload["key"]["season"]),
            gamecode=int(payload["key"]["gamecode"]),
        ),
        rows={str(t): tuple(rows) for t, rows in payload["rows"].items()},
    )
    snapshot.validate()
    return snapshot


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--games", default="1-3", help="gamecodes, e.g. '1-3' or '1,5,9'")
    ap.add_argument("--season", type=int, default=2025)
    ap.add_argument("--competition", default="E")
    ap.add_argument("--checkpoint-dir", type=Path,
                    default=REPO / "data" / "staging" / "batch_84_v2")
    ap.add_argument("--env-file", type=Path, default=REPO.parent / "etl" / ".Renviron")
    args = ap.parse_args()

    sys.path.insert(0, str(REPO / "scripts"))
    from load_games import parse_games  # noqa: E402  -- same spec, one definition

    codes = parse_games(args.games)
    print(f"probe: {args.competition}{args.season} games {codes} (rollback only)")

    snapshots = []
    for code in codes:
        path = args.checkpoint_dir / f"{args.competition}{args.season}_{code}.stage.json"
        if not path.exists():
            raise SystemExit(f"missing checkpoint: {path}")
        snapshots.append(load_snapshot(path))

    connection = connect_from_env_file(args.env_file, direct_port=5432)
    failures = 0
    try:
        target = inspect_target(connection)
        if int(target["server_port"]) != 5432:
            raise SystemExit("probe requires the direct PostgreSQL port 5432")
        assert_shadow_schema_compatible(connection)
        print(f"target: {target['database']} as {target['user']} port {target['server_port']}")

        for snapshot in snapshots:
            result = probe_game(connection, snapshot)
            t = result["timings"]
            print(
                f"\n  game {result['gamecode']}: "
                f"{result['lineups']} lineups, {result['stints']} stints  |  "
                f"begin {t['begin']:.2f}s  delete {t['delete']:.2f}s  "
                f"insert {t['insert']:.2f}s  validate {t['validate']:.2f}s"
            )
            for label, problems in (
                ("every lineup/stint reference identical", result["problems"]),
                ("rolled back clean", result["rollback_problems"]),
            ):
                if problems:
                    failures += 1
                    print(f"      FAIL  {label}:")
                    for problem in problems:
                        print(f"            {problem}")
                else:
                    print(f"      PASS  {label}")
    finally:
        connection.close()

    print(f"\n{'ALL PROBES PASSED' if not failures else f'{failures} PROBE(S) FAILED'}")
    raise SystemExit(1 if failures else 0)


if __name__ == "__main__":
    main()
