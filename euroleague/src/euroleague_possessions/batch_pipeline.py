"""Concurrent offline staging and guarded sequential EuroLeague publication."""

from __future__ import annotations

import argparse
import hashlib
import json
import time
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

import pandas as pd

from .boxscore_collector import GameKey
from .concurrency import TaskOutcome, run_bounded
from .postgres_backend import (
    PostgresTransactionBackend,
    assert_shadow_schema_compatible,
    bootstrap_game,
    connect_from_env_file,
    finish_load_run,
    inspect_target,
    start_load_run,
)
from .staging import GameBootstrap, StagedGame, build_staged_game, staged_counts
from .transaction_writer import GameSnapshot, NaturalGameKey, write_game_snapshot


# Increment whenever staged derived rows must be rebuilt even if the raw PBP
# and box-score fingerprints are unchanged.
STAGE_FORMAT_VERSION = 2


@dataclass(frozen=True)
class StageRecord:
    key: GameKey
    status: str
    elapsed_seconds: float
    checkpoint_path: str | None
    counts: Mapping[str, int]
    error: str | None
    staged: StagedGame | None


@dataclass(frozen=True)
class PublicationRecord:
    key: GameKey
    status: str
    load_run_id: int | None
    game_id: int | None
    elapsed_seconds: float
    error: str | None


def _json_safe(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if value is None:
        return None
    try:
        if bool(pd.isna(value)):
            return None
    except (TypeError, ValueError):
        pass
    if hasattr(value, "item"):
        try:
            return _json_safe(value.item())
        except (TypeError, ValueError):
            pass
    return value


def _atomic_json_write(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(
            _json_safe(payload),
            ensure_ascii=False,
            separators=(",", ":"),
        ),
        encoding="utf-8",
    )
    temporary.replace(path)


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _frame_sha256(frame: pd.DataFrame) -> str:
    ordered = frame.sort_values("TRUE_NUMBEROFPLAY", kind="stable")
    serialized = ordered.to_json(
        orient="table",
        index=False,
        date_format="iso",
        force_ascii=False,
    ).encode("utf-8")
    return hashlib.sha256(serialized).hexdigest()


def _checkpoint_path(
    checkpoint_dir: Path,
    key: GameKey,
    competition: str,
) -> Path:
    return checkpoint_dir / f"{competition}{key.season}_{key.gamecode}.stage.json"


def _staged_payload(
    staged: StagedGame,
    *,
    pbp_sha256: str,
    boxscore_sha256: str,
) -> dict[str, Any]:
    return {
        "format_version": STAGE_FORMAT_VERSION,
        "input_fingerprints": {
            "pbp_sha256": pbp_sha256,
            "boxscore_sha256": boxscore_sha256,
        },
        "written_at_utc": datetime.now(timezone.utc).isoformat(),
        "bootstrap": {
            "key": {
                "competition": staged.bootstrap.key.competition,
                "season": staged.bootstrap.key.season,
                "gamecode": staged.bootstrap.key.gamecode,
            },
            "package_version": staged.bootstrap.package_version,
            "collector_version": staged.bootstrap.collector_version,
            "teams": staged.bootstrap.teams,
            "players": staged.bootstrap.players,
            "schedule": staged.bootstrap.schedule,
            "source_artifacts": staged.bootstrap.source_artifacts,
        },
        "snapshot": {
            "key": {
                "competition": staged.snapshot.key.competition,
                "season": staged.snapshot.key.season,
                "gamecode": staged.snapshot.key.gamecode,
            },
            "rows": staged.snapshot.rows,
        },
    }


def _natural_key(payload: Mapping[str, Any]) -> NaturalGameKey:
    return NaturalGameKey(
        competition=str(payload["competition"]),
        season=int(payload["season"]),
        gamecode=int(payload["gamecode"]),
    )


def _load_staged_checkpoint(
    path: Path,
    *,
    pbp_sha256: str,
    boxscore_sha256: str,
) -> StagedGame | None:
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        if int(payload["format_version"]) != STAGE_FORMAT_VERSION:
            return None
        fingerprints = payload["input_fingerprints"]
        if fingerprints != {
            "pbp_sha256": pbp_sha256,
            "boxscore_sha256": boxscore_sha256,
        }:
            return None
        bootstrap_payload = payload["bootstrap"]
        snapshot_payload = payload["snapshot"]
        snapshot = GameSnapshot(
            key=_natural_key(snapshot_payload["key"]),
            rows={
                str(table): tuple(rows)
                for table, rows in snapshot_payload["rows"].items()
            },
        )
        snapshot.validate()
        bootstrap = GameBootstrap(
            key=_natural_key(bootstrap_payload["key"]),
            package_version=str(bootstrap_payload["package_version"]),
            collector_version=str(bootstrap_payload["collector_version"]),
            teams=tuple(bootstrap_payload["teams"]),
            players=tuple(bootstrap_payload["players"]),
            schedule=dict(bootstrap_payload["schedule"]),
            source_artifacts=tuple(bootstrap_payload["source_artifacts"]),
        )
        if bootstrap.key != snapshot.key:
            return None
        return StagedGame(bootstrap=bootstrap, snapshot=snapshot)
    except (OSError, KeyError, TypeError, ValueError, json.JSONDecodeError):
        return None


def game_keys_from_pbp(pbp: pd.DataFrame) -> list[GameKey]:
    missing = {"Season", "Gamecode"}.difference(pbp.columns)
    if missing:
        raise ValueError(f"PBP is missing columns: {sorted(missing)}")
    return sorted(
        {
            GameKey(int(season), int(gamecode))
            for season, gamecode in pbp[["Season", "Gamecode"]].itertuples(
                index=False,
                name=None,
            )
        }
    )


def _record_summary(record: StageRecord) -> dict[str, Any]:
    return {
        "competition": (
            record.staged.bootstrap.key.competition
            if record.staged is not None
            else None
        ),
        "season": record.key.season,
        "gamecode": record.key.gamecode,
        "status": record.status,
        "elapsed_seconds": round(record.elapsed_seconds, 6),
        "checkpoint_path": record.checkpoint_path,
        "counts": dict(record.counts),
        "error": record.error,
    }


def stage_games(
    pbp: pd.DataFrame,
    boxscore_dir: Path,
    checkpoint_dir: Path,
    *,
    keys: Sequence[GameKey] | None = None,
    competition: str = "E",
    max_workers: int = 1,
    resume: bool = True,
) -> list[StageRecord]:
    """Build independent game snapshots concurrently and checkpoint each one."""

    selected_keys = sorted(set(keys or game_keys_from_pbp(pbp)))
    checkpoint_dir.mkdir(parents=True, exist_ok=True)
    game_frames = {
        key: pbp.loc[
            pbp["Season"].eq(key.season) & pbp["Gamecode"].eq(key.gamecode)
        ].copy()
        for key in selected_keys
    }

    def stage_one(key: GameKey) -> StageRecord:
        started = time.perf_counter()
        game = game_frames[key]
        if game.empty:
            raise ValueError(
                f"game not found: season={key.season}, gamecode={key.gamecode}"
            )
        boxscore_path = boxscore_dir / key.filename(competition)
        if not boxscore_path.exists():
            raise FileNotFoundError(f"box score not found: {boxscore_path}")
        pbp_sha256 = _frame_sha256(game)
        boxscore_sha256 = _file_sha256(boxscore_path)
        path = _checkpoint_path(checkpoint_dir, key, competition)
        if resume:
            cached = _load_staged_checkpoint(
                path,
                pbp_sha256=pbp_sha256,
                boxscore_sha256=boxscore_sha256,
            )
            if cached is not None:
                return StageRecord(
                    key=key,
                    status="cached",
                    elapsed_seconds=time.perf_counter() - started,
                    checkpoint_path=str(path),
                    counts=staged_counts(cached),
                    error=None,
                    staged=cached,
                )

        staged = build_staged_game(
            game,
            boxscore_dir,
            season=key.season,
            gamecode=key.gamecode,
            competition=competition,
        )
        _atomic_json_write(
            path,
            _staged_payload(
                staged,
                pbp_sha256=pbp_sha256,
                boxscore_sha256=boxscore_sha256,
            ),
        )
        return StageRecord(
            key=key,
            status="staged",
            elapsed_seconds=time.perf_counter() - started,
            checkpoint_path=str(path),
            counts=staged_counts(staged),
            error=None,
            staged=staged,
        )

    records_by_key: dict[GameKey, StageRecord] = {}
    completed = 0

    def checkpoint_manifest(outcome: TaskOutcome[GameKey, StageRecord]) -> None:
        nonlocal completed
        completed += 1
        record = outcome.result
        if record is None:
            error = outcome.error or RuntimeError("unknown staging failure")
            record = StageRecord(
                key=outcome.item,
                status="failed",
                elapsed_seconds=0,
                checkpoint_path=None,
                counts={},
                error=f"{type(error).__name__}: {error}",
                staged=None,
            )
        records_by_key[outcome.item] = record
        print(
            f"[{completed}/{len(selected_keys)}] {record.status} "
            f"season={record.key.season} game={record.key.gamecode} "
            f"elapsed={record.elapsed_seconds:.3f}s"
            + (f" error={record.error}" if record.error else ""),
            flush=True,
        )
        ordered_records = [
            records_by_key[key]
            for key in selected_keys
            if key in records_by_key
        ]
        _atomic_json_write(
            checkpoint_dir / "stage_manifest.json",
            {
                "format_version": STAGE_FORMAT_VERSION,
                "competition": competition,
                "updated_at_utc": datetime.now(timezone.utc).isoformat(),
                "max_workers": max_workers,
                "records": [_record_summary(item) for item in ordered_records],
            },
        )

    run_bounded(
        selected_keys,
        stage_one,
        max_workers=max_workers,
        on_complete=checkpoint_manifest,
    )
    return [records_by_key[key] for key in selected_keys]


def publish_staged_games(
    records: Sequence[StageRecord],
    *,
    env_file: Path,
) -> list[PublicationRecord]:
    """Publish successful checkpoints in deterministic order, one at a time."""

    publishable = sorted(
        (record for record in records if record.staged is not None),
        key=lambda record: record.key,
    )
    connection = connect_from_env_file(env_file, direct_port=5432)
    results: list[PublicationRecord] = []
    try:
        target = inspect_target(connection)
        if int(target["server_port"]) != 5432:
            raise RuntimeError("batch publication requires direct PostgreSQL port 5432")
        if target["euroleague_schema"] is None:
            raise RuntimeError("euroleague schema is absent")
        assert_shadow_schema_compatible(connection)

        grouped: dict[tuple[str, int, str, str], list[StageRecord]] = {}
        for record in publishable:
            assert record.staged is not None
            bootstrap = record.staged.bootstrap
            group_key = (
                bootstrap.key.competition,
                bootstrap.key.season,
                bootstrap.package_version,
                bootstrap.collector_version,
            )
            grouped.setdefault(group_key, []).append(record)

        for group_key in sorted(grouped):
            group = sorted(grouped[group_key], key=lambda record: record.key)
            first = group[0]
            assert first.staged is not None
            batch_load_run_id = start_load_run(
                connection,
                first.staged.bootstrap,
                requested_games=len(group),
                request_parameters={
                    "scope": "batch",
                    "competition": group_key[0],
                    "season": group_key[1],
                    "gamecodes": [record.key.gamecode for record in group],
                    "publication_order": "season_gamecode",
                },
            )
            batch_errors: list[dict[str, Any]] = []
            successful_games = 0

            for record in group:
                started = time.perf_counter()
                try:
                    assert record.staged is not None
                    bootstrap_game(
                        connection,
                        record.staged.bootstrap,
                        load_run_id=batch_load_run_id,
                    )
                    backend = PostgresTransactionBackend(
                        connection,
                        load_run_id=batch_load_run_id,
                    )
                    game_id = write_game_snapshot(
                        backend,
                        record.staged.snapshot,
                    )
                    successful_games += 1
                    results.append(
                        PublicationRecord(
                            record.key,
                            "published",
                            batch_load_run_id,
                            game_id,
                            time.perf_counter() - started,
                            None,
                        )
                    )
                except Exception as exc:
                    error = f"{type(exc).__name__}: {exc}"
                    batch_errors.append(
                        {
                            "season": record.key.season,
                            "gamecode": record.key.gamecode,
                            "error": error,
                        }
                    )
                    results.append(
                        PublicationRecord(
                            record.key,
                            "failed",
                            batch_load_run_id,
                            None,
                            time.perf_counter() - started,
                            error,
                        )
                    )

            failed_games = len(group) - successful_games
            finish_load_run(
                connection,
                load_run_id=batch_load_run_id,
                success=failed_games == 0,
                successful_games=successful_games,
                failed_games=failed_games,
                errors=batch_errors,
            )
    finally:
        connection.close()
    return results


def _arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Stage EuroLeague games concurrently and optionally publish them "
            "through existing per-game PostgreSQL transactions."
        )
    )
    parser.add_argument("pbp_csv", type=Path)
    parser.add_argument("boxscore_dir", type=Path)
    parser.add_argument(
        "--checkpoint-dir",
        type=Path,
        default=Path("euroleague/data/staging/batch"),
    )
    parser.add_argument("--competition", default="E")
    parser.add_argument("--season", type=int)
    parser.add_argument("--gamecodes", help="Comma-separated gamecodes")
    parser.add_argument("--limit", type=int)
    parser.add_argument(
        "--stage-workers",
        type=int,
        default=1,
        help=(
            "Independent staging workers. Default 1: measured faster for the "
            "current CPU/DataFrame workload; use more only after benchmarking."
        ),
    )
    parser.add_argument("--no-resume", action="store_true")
    parser.add_argument("--execute", action="store_true")
    parser.add_argument(
        "--confirm-multiple-games",
        action="store_true",
        help="Required with --execute when more than one game is selected.",
    )
    parser.add_argument(
        "--env-file",
        type=Path,
        default=Path("etl/.Renviron"),
    )
    return parser.parse_args()


def main() -> None:
    args = _arguments()
    started = time.perf_counter()
    pbp = pd.read_csv(args.pbp_csv)
    keys = game_keys_from_pbp(pbp)
    if args.season is not None:
        keys = [key for key in keys if key.season == args.season]
    if args.gamecodes:
        selected_codes = {
            int(value.strip())
            for value in args.gamecodes.split(",")
            if value.strip()
        }
        keys = [key for key in keys if key.gamecode in selected_codes]
    if args.limit is not None:
        keys = keys[: args.limit]
    if not keys:
        raise ValueError("no games selected")

    records = stage_games(
        pbp,
        args.boxscore_dir,
        args.checkpoint_dir,
        keys=keys,
        competition=args.competition,
        max_workers=args.stage_workers,
        resume=not args.no_resume,
    )
    failed = [record for record in records if record.status == "failed"]
    available = [record for record in records if record.staged is not None]
    elapsed = time.perf_counter() - started
    print(f"stage_games={len(records)}")
    print(f"stage_available={len(available)}")
    print(f"stage_failed={len(failed)}")
    print(f"stage_elapsed_seconds={elapsed:.3f}")
    print(f"stage_games_per_second={len(records) / elapsed:.3f}")

    if failed:
        raise SystemExit(1)
    if not args.execute:
        print("database_write=skipped")
        return
    if len(available) > 1 and not args.confirm_multiple_games:
        raise ValueError(
            "multi-game publication requires --confirm-multiple-games"
        )

    publication = publish_staged_games(available, env_file=args.env_file)
    _atomic_json_write(
        args.checkpoint_dir / "publication_manifest.json",
        {
            "competition": args.competition,
            "updated_at_utc": datetime.now(timezone.utc).isoformat(),
            "records": [
                {
                    "season": item.key.season,
                    "gamecode": item.key.gamecode,
                    "status": item.status,
                    "load_run_id": item.load_run_id,
                    "game_id": item.game_id,
                    "elapsed_seconds": round(item.elapsed_seconds, 6),
                    "error": item.error,
                }
                for item in publication
            ],
        },
    )
    publication_failed = sum(item.status == "failed" for item in publication)
    print(f"publication_games={len(publication)}")
    print(f"publication_failed={publication_failed}")
    if publication_failed:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
