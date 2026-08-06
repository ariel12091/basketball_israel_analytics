"""CLI for an explicitly approved one-game EuroLeague shadow-schema trial."""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any

import pandas as pd

from .postgres_backend import (
    PostgresTransactionBackend,
    apply_shadow_schema,
    assert_shadow_schema_compatible,
    bootstrap_game,
    connect_from_env_file,
    finish_load_run,
    inspect_target,
)
from .staging import build_staged_game, staged_counts
from .transaction_writer import INSERT_ORDER, NaturalGameKey, write_game_snapshot


def verify_trial(
    connection: Any,
    load_run_id: int,
    game_id: int,
) -> dict[str, Any]:
    backend = PostgresTransactionBackend(connection, load_run_id)
    cursor = connection.cursor()
    counts = {
        table: backend._count_rows(cursor, table, game_id)
        for table in INSERT_ORDER
    }
    cursor.execute(
        "SELECT lr.status, gq.publication_status, "
        "gq.boxscore_metrics_exact, gq.score_progression_reconciled, "
        "gq.lineup_structure_valid "
        "FROM euroleague.load_runs AS lr "
        "JOIN euroleague.game_qa AS gq ON gq.load_run_id = lr.load_run_id "
        "WHERE lr.load_run_id = %s AND gq.game_id = %s",
        (load_run_id, game_id),
    )
    row = cursor.fetchone()
    cursor.close()
    if row is None:
        raise RuntimeError("trial verification could not find load-run QA")
    return {
        "counts": counts,
        "load_run_status": row[0],
        "publication_status": row[1],
        "boxscore_metrics_exact": bool(row[2]),
        "score_progression_reconciled": bool(row[3]),
        "lineup_structure_valid": bool(row[4]),
    }


def probe_rollback(
    connection: Any,
    load_run_id: int,
    key: NaturalGameKey,
    expected_pws: int,
) -> None:
    """Delete PWS inside a transaction, roll back, and verify restoration."""

    backend = PostgresTransactionBackend(connection, load_run_id)
    game_id = backend.begin(key)
    try:
        backend.delete_game_rows("pws", game_id)
        cursor = connection.cursor()
        inside_count = backend._count_rows(cursor, "pws", game_id)
        cursor.close()
        if inside_count != 0:
            raise RuntimeError(
                f"rollback probe expected zero in-transaction PWS rows, got {inside_count}"
            )
    finally:
        backend.rollback()

    cursor = connection.cursor()
    restored_count = backend._count_rows(cursor, "pws", game_id)
    cursor.close()
    if restored_count != expected_pws:
        raise RuntimeError(
            f"rollback probe expected {expected_pws} restored PWS rows, "
            f"got {restored_count}"
        )


def _arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Stage one EuroLeague game and optionally write it to the isolated "
            "PostgreSQL shadow schema."
        )
    )
    parser.add_argument("pbp_csv", type=Path)
    parser.add_argument("boxscore_dir", type=Path)
    parser.add_argument("--season", type=int, required=True)
    parser.add_argument("--gamecode", type=int, required=True)
    parser.add_argument("--competition", default="E")
    parser.add_argument("--env-file", type=Path, default=Path("etl/.Renviron"))
    parser.add_argument(
        "--ddl",
        type=Path,
        default=Path("euroleague/sql/001_core_shadow_schema.sql"),
    )
    parser.add_argument(
        "--analytics-ddl",
        type=Path,
        default=Path(
            "euroleague/sql/002_existing_analytics_compatibility.sql"
        ),
    )
    parser.add_argument(
        "--app-mv-ddl",
        type=Path,
        default=Path("euroleague/sql/003_app_materialized_views.sql"),
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Write the staged game; without this flag the command is offline.",
    )
    parser.add_argument(
        "--apply-schema",
        action="store_true",
        help="Apply the reviewed non-destructive DDL before the trial write.",
    )
    parser.add_argument(
        "--probe-rollback",
        action="store_true",
        help="After loading, prove that an in-transaction delete rolls back.",
    )
    return parser.parse_args()


def main() -> None:
    args = _arguments()
    if args.apply_schema and not args.execute:
        raise ValueError("--apply-schema requires --execute")

    pbp = pd.read_csv(args.pbp_csv)
    staged = build_staged_game(
        pbp,
        args.boxscore_dir,
        season=args.season,
        gamecode=args.gamecode,
        competition=args.competition,
    )
    counts = staged_counts(staged)
    qa = staged.snapshot.rows["game_qa"][0]
    print("offline_stage=valid")
    print(f"season={args.season}")
    print(f"gamecode={args.gamecode}")
    print(f"publication_status={qa['publication_status']}")
    for table, count in counts.items():
        print(f"staged_{table}={count}")

    if not args.execute:
        print("database_write=skipped")
        return

    connection = connect_from_env_file(args.env_file, direct_port=5432)
    bootstrap_result = None
    try:
        target = inspect_target(connection)
        print(f"target_database={target['database']}")
        print(f"target_user={target['user']}")
        print(f"target_server_port={target['server_port']}")
        print(
            "target_euroleague_schema="
            f"{target['euroleague_schema'] or 'absent'}"
        )
        if int(target["server_port"]) != 5432:
            raise RuntimeError("trial must use the direct PostgreSQL port 5432")
        assert_shadow_schema_compatible(connection)
        if args.apply_schema:
            apply_shadow_schema(connection, args.ddl)
            apply_shadow_schema(connection, args.analytics_ddl)
            apply_shadow_schema(connection, args.app_mv_ddl)
            assert_shadow_schema_compatible(connection)
            print("schema_apply=complete")
        elif target["euroleague_schema"] is None:
            raise RuntimeError(
                "euroleague schema is absent; rerun with --apply-schema"
            )

        bootstrap_result = bootstrap_game(connection, staged.bootstrap)
        backend = PostgresTransactionBackend(
            connection,
            load_run_id=bootstrap_result.load_run_id,
        )
        game_id = write_game_snapshot(backend, staged.snapshot)
        finish_load_run(
            connection,
            load_run_id=bootstrap_result.load_run_id,
            success=True,
        )
        verification = verify_trial(
            connection,
            load_run_id=bootstrap_result.load_run_id,
            game_id=game_id,
        )
        print(f"load_run_id={bootstrap_result.load_run_id}")
        print(f"game_id={game_id}")
        print(f"load_run_status={verification['load_run_status']}")
        print(
            "verified_publication_status="
            f"{verification['publication_status']}"
        )
        for table, count in verification["counts"].items():
            print(f"verified_{table}={count}")
        if args.probe_rollback:
            probe_rollback(
                connection,
                load_run_id=bootstrap_result.load_run_id,
                key=staged.snapshot.key,
                expected_pws=len(staged.snapshot.rows["pws"]),
            )
            print("rollback_probe=pass")
    except Exception as exc:
        if bootstrap_result is not None:
            finish_load_run(
                connection,
                load_run_id=bootstrap_result.load_run_id,
                success=False,
                error=f"{type(exc).__name__}: {exc}",
            )
        raise
    finally:
        connection.close()


if __name__ == "__main__":
    main()
