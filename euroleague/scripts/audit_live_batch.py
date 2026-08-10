"""Read-only audit of published EuroLeague game snapshots and batch lineage."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[2]
SRC = REPO_ROOT / "euroleague" / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from euroleague_possessions.postgres_backend import (  # noqa: E402
    assert_shadow_schema_compatible,
    connect_from_env_file,
    inspect_target,
)


GAME_TABLES = (
    "full_rosters",
    "team_boxscores",
    "actions_raw",
    "actions",
    "matchup_segments_actions",
    "action_team_context_actions",
)
RUN_SCOPED_TABLES = (
    "reconciliation_metrics",
    "game_qa",
    "qa_incidents",
)


def _arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env-file", type=Path, default=Path("etl/.Renviron"))
    parser.add_argument("--competition", default="E")
    parser.add_argument("--season", type=int, required=True)
    parser.add_argument("--gamecodes", required=True)
    parser.add_argument("--checkpoint-dir", type=Path)
    return parser.parse_args()


def _checkpoint_counts(
    checkpoint_dir: Path | None,
    competition: str,
    season: int,
    gamecode: int,
) -> dict[str, int] | None:
    if checkpoint_dir is None:
        return None
    path = checkpoint_dir / f"{competition}{season}_{gamecode}.stage.json"
    payload = json.loads(path.read_text(encoding="utf-8"))
    counts = {
        table: len(rows)
        for table, rows in payload["snapshot"]["rows"].items()
    }
    counts["source_artifacts"] = len(payload["bootstrap"]["source_artifacts"])
    return counts


def _count(cursor: Any, sql: str, parameters: tuple[Any, ...]) -> int:
    cursor.execute(sql, parameters)
    return int(cursor.fetchone()[0])


def main() -> None:
    args = _arguments()
    gamecodes = sorted(
        {int(value.strip()) for value in args.gamecodes.split(",") if value.strip()}
    )
    connection = connect_from_env_file(args.env_file, direct_port=5432)
    output: dict[str, Any] = {"target": inspect_target(connection), "games": []}
    all_match = True
    try:
        assert_shadow_schema_compatible(connection)
        cursor = connection.cursor()
        for gamecode in gamecodes:
            cursor.execute(
                "SELECT game_id FROM euroleague.schedule "
                "WHERE competition = %s AND season = %s AND gamecode = %s",
                (args.competition, args.season, gamecode),
            )
            schedule_row = cursor.fetchone()
            if schedule_row is None:
                output["games"].append(
                    {"gamecode": gamecode, "published": False, "mismatches": []}
                )
                continue

            game_id = int(schedule_row[0])
            cursor.execute(
                "SELECT load_run_id, publication_status, "
                "possession_structural_status, boxscore_metrics_exact, "
                "score_progression_exact, score_progression_reconciled, "
                "lineup_structure_valid, total_possessions, "
                "provisional_ft_rows, unresolved_ft_rows, "
                "lineup_invalid_actor_rows, possession_review_status "
                "FROM euroleague.game_qa WHERE game_id = %s "
                "ORDER BY load_run_id DESC LIMIT 1",
                (game_id,),
            )
            qa_row = cursor.fetchone()
            if qa_row is None:
                raise RuntimeError(f"game {gamecode} has no game_qa row")
            load_run_id = int(qa_row[0])

            actual: dict[str, int] = {"schedule": 1}
            actual["source_artifacts"] = _count(
                cursor,
                "SELECT count(*) FROM euroleague.source_artifacts "
                "WHERE game_id = %s AND load_run_id = %s",
                (game_id, load_run_id),
            )
            for table in GAME_TABLES:
                actual[table] = _count(
                    cursor,
                    f"SELECT count(*) FROM euroleague.{table} WHERE game_id = %s",
                    (game_id,),
                )
            for table in RUN_SCOPED_TABLES:
                actual[table] = _count(
                    cursor,
                    f"SELECT count(*) FROM euroleague.{table} "
                    "WHERE game_id = %s AND load_run_id = %s",
                    (game_id, load_run_id),
                )
            actual["player_four_factors_by_game"] = _count(
                cursor,
                "SELECT count(*) FROM euroleague.player_four_factors_by_game "
                "WHERE game_id = %s",
                (game_id,),
            )

            expected = _checkpoint_counts(
                args.checkpoint_dir,
                args.competition,
                args.season,
                gamecode,
            )
            mismatches = []
            if expected is not None:
                mismatches = [
                    {
                        "table": table,
                        "expected": count,
                        "actual": actual.get(table),
                    }
                    for table, count in expected.items()
                    if actual.get(table) != count
                ]
            all_match = all_match and not mismatches

            cursor.execute(
                "SELECT status, requested_games, successful_games, failed_games "
                "FROM euroleague.load_runs WHERE load_run_id = %s",
                (load_run_id,),
            )
            run_row = cursor.fetchone()
            output["games"].append(
                {
                    "gamecode": gamecode,
                    "published": True,
                    "game_id": game_id,
                    "load_run_id": load_run_id,
                    "load_run": {
                        "status": run_row[0],
                        "requested_games": int(run_row[1]),
                        "successful_games": int(run_row[2]),
                        "failed_games": int(run_row[3]),
                    },
                    "qa": {
                        "publication_status": qa_row[1],
                        "possession_structural_status": qa_row[2],
                        "boxscore_metrics_exact": bool(qa_row[3]),
                        "score_progression_exact": bool(qa_row[4]),
                        "score_progression_reconciled": bool(qa_row[5]),
                        "lineup_structure_valid": bool(qa_row[6]),
                        "total_possessions": int(qa_row[7]),
                        "provisional_ft_rows": int(qa_row[8]),
                        "unresolved_ft_rows": int(qa_row[9]),
                        "lineup_invalid_actor_rows": int(qa_row[10]),
                        "possession_review_status": qa_row[11],
                    },
                    "counts": actual,
                    "mismatches": mismatches,
                }
            )
        cursor.close()
    finally:
        connection.close()

    output["checkpoint_counts_match"] = all_match
    print(json.dumps(output, indent=2, sort_keys=True))
    if not all_match:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
