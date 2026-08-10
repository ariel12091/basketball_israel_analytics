"""Backfill complete package PBP rows into ``actions_raw.raw_event`` only."""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping

import pandas as pd

from .package_lineups import audit_package_lineups, cached_boxscore_frame
from .postgres_backend import connect_from_env_file, inspect_target
from .schema_coverage import _team_sides
from .staging import _package_events_by_order, _restore_package_home_team_marker


@dataclass(frozen=True)
class BackfillResult:
    gamecode: int
    game_id: int
    event_count: int
    matched_before: int
    updated_rows: int


def parse_gamecodes(spec: str) -> list[int]:
    """Parse a deterministic gamecode list such as ``1-84`` or ``1,3-5``."""

    gamecodes: set[int] = set()
    for raw_part in spec.split(","):
        part = raw_part.strip()
        if not part:
            continue
        if "-" in part:
            lower, upper = part.split("-", 1)
            start, end = int(lower), int(upper)
            if start > end:
                raise ValueError(f"invalid descending game range: {part}")
            gamecodes.update(range(start, end + 1))
        else:
            gamecodes.add(int(part))
    if not gamecodes or min(gamecodes) < 1:
        raise ValueError("at least one positive gamecode is required")
    return sorted(gamecodes)


def package_events_for_game(
    pbp: pd.DataFrame,
    boxscore_dir: Path,
    *,
    competition: str,
    season: int,
    gamecode: int,
) -> dict[int, dict[str, Any]]:
    """Return the complete package PBP event keyed by source event order."""

    game = pbp.loc[
        pbp["Season"].eq(season) & pbp["Gamecode"].eq(gamecode)
    ].copy()
    if game.empty:
        raise ValueError(f"cached PBP has no {competition}/{season}/{gamecode}")
    result = audit_package_lineups(
        game,
        boxscore_dir=boxscore_dir,
        competition=competition,
    )
    summary = result.game_summary.iloc[0]
    if summary["status"] != "ok":
        raise ValueError(
            f"package lineup failure for game {gamecode}: {summary['error']}"
        )
    boxscore = cached_boxscore_frame(
        boxscore_dir / f"{competition}{season}_{gamecode}.json",
        season,
        gamecode,
        competition=competition,
    )
    home_team, away_team = _team_sides(boxscore)
    events = _restore_package_home_team_marker(
        result.events, home_team, away_team
    )
    return _package_events_by_order(events)


def _json_payload(events: Mapping[int, Mapping[str, Any]]) -> str:
    return json.dumps(
        [
            {"source_event_order": order, "raw_event": events[order]}
            for order in sorted(events)
        ],
        ensure_ascii=False,
        separators=(",", ":"),
    )


def _game_id(cursor: Any, competition: str, season: int, gamecode: int) -> int:
    cursor.execute(
        "SELECT game_id FROM euroleague.schedule "
        "WHERE competition = %s AND season = %s AND gamecode = %s",
        (competition, season, gamecode),
    )
    rows = cursor.fetchall()
    if len(rows) != 1:
        raise ValueError(
            f"expected one live schedule row for {competition}/{season}/{gamecode}; "
            f"found {len(rows)}"
        )
    return int(rows[0][0])


def _database_event_orders(cursor: Any, game_id: int, *, lock: bool) -> list[int]:
    suffix = " FOR UPDATE" if lock else ""
    cursor.execute(
        "SELECT source_event_order FROM euroleague.actions_raw "
        "WHERE game_id = %s ORDER BY source_event_order" + suffix,
        (game_id,),
    )
    return [int(row[0]) for row in cursor.fetchall()]


def _assert_same_orders(
    gamecode: int,
    package_orders: Iterable[int],
    database_orders: Iterable[int],
) -> None:
    package = list(package_orders)
    database = list(database_orders)
    if package == database:
        return
    package_set = set(package)
    database_set = set(database)
    raise ValueError(
        f"game {gamecode} event keys differ; "
        f"package_only={sorted(package_set - database_set)[:10]}, "
        f"database_only={sorted(database_set - package_set)[:10]}, "
        f"package_count={len(package)}, database_count={len(database)}"
    )


def _exact_match_count(cursor: Any, game_id: int, payload: str) -> int:
    cursor.execute(
        "WITH supplied AS ("
        "  SELECT * FROM jsonb_to_recordset(%s::jsonb) "
        "    AS x(source_event_order integer, raw_event jsonb)"
        ") "
        "SELECT count(*) FROM supplied s "
        "JOIN euroleague.actions_raw a "
        "  ON a.game_id = %s AND a.source_event_order = s.source_event_order "
        "WHERE a.raw_event = s.raw_event",
        (payload, game_id),
    )
    return int(cursor.fetchone()[0])


def _complete_row_count(cursor: Any, game_id: int) -> int:
    cursor.execute(
        "SELECT count(*) FROM euroleague.actions_raw "
        "WHERE game_id = %s "
        "  AND jsonb_typeof(raw_event -> 'Lineup_A') = 'array' "
        "  AND jsonb_array_length(raw_event -> 'Lineup_A') = 5 "
        "  AND jsonb_typeof(raw_event -> 'Lineup_B') = 'array' "
        "  AND jsonb_array_length(raw_event -> 'Lineup_B') = 5 "
        "  AND raw_event ? 'IsHomeTeam' "
        "  AND raw_event ? 'validate_on_court_player' "
        "  AND (raw_event ->> 'TRUE_NUMBEROFPLAY')::integer = source_event_order",
        (game_id,),
    )
    return int(cursor.fetchone()[0])


def backfill_game(
    connection: Any,
    *,
    competition: str,
    season: int,
    gamecode: int,
    events: Mapping[int, Mapping[str, Any]],
    execute: bool,
) -> BackfillResult:
    """Preflight or update one game without touching any other relation."""

    payload = _json_payload(events)
    expected_orders = sorted(events)
    cursor = connection.cursor()
    try:
        if execute:
            cursor.execute("BEGIN")
            cursor.execute("SET LOCAL search_path TO euroleague, public")
            cursor.execute("SET LOCAL statement_timeout = '60s'")
        game_id = _game_id(cursor, competition, season, gamecode)
        database_orders = _database_event_orders(cursor, game_id, lock=execute)
        _assert_same_orders(gamecode, expected_orders, database_orders)
        matched_before = _exact_match_count(cursor, game_id, payload)
        updated_rows = 0

        if execute and matched_before != len(expected_orders):
            cursor.execute(
                "WITH supplied AS ("
                "  SELECT * FROM jsonb_to_recordset(%s::jsonb) "
                "    AS x(source_event_order integer, raw_event jsonb)"
                ") "
                "UPDATE euroleague.actions_raw a "
                "SET raw_event = s.raw_event "
                "FROM supplied s "
                "WHERE a.game_id = %s "
                "  AND a.source_event_order = s.source_event_order",
                (payload, game_id),
            )
            updated_rows = int(cursor.rowcount)
            if updated_rows != len(expected_orders):
                raise RuntimeError(
                    f"game {gamecode} updated {updated_rows}/{len(expected_orders)} rows"
                )

        if execute:
            matched_after = _exact_match_count(cursor, game_id, payload)
            complete_rows = _complete_row_count(cursor, game_id)
            if matched_after != len(expected_orders):
                raise RuntimeError(
                    f"game {gamecode} exact verification matched "
                    f"{matched_after}/{len(expected_orders)} rows"
                )
            if complete_rows != len(expected_orders):
                raise RuntimeError(
                    f"game {gamecode} field verification matched "
                    f"{complete_rows}/{len(expected_orders)} rows"
                )
            cursor.execute("COMMIT")
    except Exception:
        if execute:
            cursor.execute("ROLLBACK")
        raise
    finally:
        cursor.close()

    return BackfillResult(
        gamecode=gamecode,
        game_id=game_id,
        event_count=len(expected_orders),
        matched_before=matched_before,
        updated_rows=updated_rows,
    )


def run_backfill(
    connection: Any,
    pbp: pd.DataFrame,
    boxscore_dir: Path,
    *,
    competition: str,
    season: int,
    gamecodes: Iterable[int],
    execute: bool,
) -> list[BackfillResult]:
    results: list[BackfillResult] = []
    for index, gamecode in enumerate(gamecodes, start=1):
        events = package_events_for_game(
            pbp,
            boxscore_dir,
            competition=competition,
            season=season,
            gamecode=gamecode,
        )
        result = backfill_game(
            connection,
            competition=competition,
            season=season,
            gamecode=gamecode,
            events=events,
            execute=execute,
        )
        results.append(result)
        action = (
            f"updated={result.updated_rows}"
            if execute
            else f"would_update={result.event_count - result.matched_before}"
        )
        print(
            f"[{index}] game={gamecode} game_id={result.game_id} "
            f"events={result.event_count} exact_before={result.matched_before} "
            f"{action}",
            flush=True,
        )
    return results


def main() -> None:
    repository = Path(__file__).resolve().parents[2]
    parser = argparse.ArgumentParser(
        description="Backfill only actions_raw.raw_event with complete package PBP rows."
    )
    parser.add_argument("--games", required=True)
    parser.add_argument("--competition", default="E")
    parser.add_argument("--season", type=int, default=2025)
    parser.add_argument(
        "--pbp",
        type=Path,
        default=Path("C:/tmp/euroleague_pbp_2025_100games.csv"),
    )
    parser.add_argument(
        "--boxscore-dir",
        type=Path,
        default=repository / "data" / "raw" / "boxscores",
    )
    parser.add_argument(
        "--env-file",
        type=Path,
        default=repository.parent / "etl" / ".Renviron",
    )
    parser.add_argument("--execute", action="store_true")
    parser.add_argument(
        "--confirm-actions-raw-only",
        action="store_true",
        help="required with --execute",
    )
    args = parser.parse_args()
    if args.execute and not args.confirm_actions_raw_only:
        raise SystemExit("--execute requires --confirm-actions-raw-only")

    gamecodes = parse_gamecodes(args.games)
    pbp = pd.read_csv(args.pbp)
    connection = connect_from_env_file(args.env_file)
    try:
        target = inspect_target(connection)
        print(
            "target "
            f"database={target['database']} user={target['user']} "
            f"port={target['server_port']} schema={target['euroleague_schema']}"
        )
        if int(target["server_port"]) != 5432:
            raise RuntimeError("backfill requires the direct PostgreSQL port 5432")
        if target["euroleague_schema"] != "euroleague":
            raise RuntimeError("euroleague schema is not present")
        results = run_backfill(
            connection,
            pbp,
            args.boxscore_dir,
            competition=args.competition,
            season=args.season,
            gamecodes=gamecodes,
            execute=args.execute,
        )
    finally:
        connection.close()

    total_events = sum(result.event_count for result in results)
    total_updated = sum(result.updated_rows for result in results)
    total_exact_before = sum(result.matched_before for result in results)
    mode = "executed" if args.execute else "dry-run"
    print(
        f"{mode} complete games={len(results)} events={total_events} "
        f"exact_before={total_exact_before} updated={total_updated}"
    )


if __name__ == "__main__":
    main()
