"""Restartable package-first play-by-play collector with bounded concurrency."""

from __future__ import annotations

import argparse
import io
import json
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from importlib.metadata import version
from pathlib import Path
from typing import Any

import pandas as pd
import requests
from euroleague_api.play_by_play_data import PlayByPlay

from .boxscore_collector import GameKey, game_keys_from_csv
from .concurrency import AdaptiveRequestGate, TaskOutcome, run_bounded


PBP_URL = "https://live.euroleague.net/api/PlaybyPlay"
REQUIRED_COLUMNS = frozenset(
    {
        "Season",
        "Gamecode",
        "PERIOD",
        "TRUE_NUMBEROFPLAY",
        "PLAYTYPE",
        "CODETEAM",
    }
)


@dataclass(frozen=True)
class PbpCollectionRecord:
    competition: str
    season: int
    gamecode: int
    status: str
    attempts: int
    retrieved_at_utc: str | None
    path: str | None
    rows: int
    error: str | None


def _filename(key: GameKey, competition: str) -> str:
    return f"{competition}{key.season}_{key.gamecode}.pbp.json"


def _atomic_json_write(path: Path, payload: Any) -> None:
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, separators=(",", ":")),
        encoding="utf-8",
    )
    temporary.replace(path)


def _frame_payload(
    frame: pd.DataFrame,
    *,
    competition: str,
    season: int,
    gamecode: int,
    retrieved_at_utc: str,
) -> dict[str, Any]:
    table = json.loads(
        frame.to_json(
            orient="table",
            index=False,
            date_format="iso",
            force_ascii=False,
        )
    )
    return {
        "metadata": {
            "competition": competition,
            "season": season,
            "gamecode": gamecode,
            "retrieved_at_utc": retrieved_at_utc,
            "package": "euroleague-api",
            "package_version": version("euroleague-api"),
            "package_method": "PlayByPlay.get_game_play_by_play_data",
            "source_endpoint": PBP_URL,
            "rows": int(len(frame)),
        },
        "frame": table,
    }


def read_cached_pbp(path: Path) -> pd.DataFrame:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict) or not isinstance(payload.get("frame"), dict):
        raise ValueError("cached PBP payload has no table frame")
    frame = pd.read_json(
        io.StringIO(json.dumps(payload["frame"], ensure_ascii=False)),
        orient="table",
    )
    missing = REQUIRED_COLUMNS.difference(frame.columns)
    if missing:
        raise ValueError(f"cached PBP is missing columns: {sorted(missing)}")
    return frame.sort_values("TRUE_NUMBEROFPLAY", kind="stable").reset_index(
        drop=True
    )


def _valid_cached_payload(path: Path, key: GameKey) -> tuple[bool, int]:
    if not path.exists():
        return False, 0
    try:
        frame = read_cached_pbp(path)
    except (OSError, ValueError, TypeError, json.JSONDecodeError):
        return False, 0
    valid_key = bool(
        not frame.empty
        and frame["Season"].eq(key.season).all()
        and frame["Gamecode"].eq(key.gamecode).all()
        and not frame["TRUE_NUMBEROFPLAY"].duplicated().any()
    )
    return valid_key, int(len(frame))


def _retry_delay(
    error: Exception,
    attempt: int,
    rate_limit_backoff_seconds: float,
) -> tuple[float, bool]:
    response = error.response if isinstance(error, requests.HTTPError) else None
    if response is not None and response.status_code == 429:
        retry_after = response.headers.get("Retry-After")
        if retry_after:
            try:
                return max(float(retry_after), rate_limit_backoff_seconds), True
            except ValueError:
                pass
        return min(rate_limit_backoff_seconds * (2 ** (attempt - 1)), 300), True
    return min(2 ** (attempt - 1), 8), False


def collect_play_by_play(
    keys: list[GameKey],
    output_dir: Path,
    *,
    competition: str = "E",
    max_attempts: int = 4,
    throttle_seconds: float = 0.75,
    rate_limit_backoff_seconds: float = 60.0,
    max_workers: int = 1,
    package_factory: Any = PlayByPlay,
) -> list[PbpCollectionRecord]:
    """Fetch package-normalized games with ordered manifests and checkpoints."""

    if max_attempts < 1:
        raise ValueError("max_attempts must be at least 1")
    output_dir.mkdir(parents=True, exist_ok=True)
    ordered_keys = sorted(set(keys))
    gate = AdaptiveRequestGate(throttle_seconds)

    def collect_one(key: GameKey) -> PbpCollectionRecord:
        path = output_dir / _filename(key, competition)
        cached, rows = _valid_cached_payload(path, key)
        if cached:
            return PbpCollectionRecord(
                competition,
                key.season,
                key.gamecode,
                "cached",
                0,
                None,
                str(path),
                rows,
                None,
            )

        error: str | None = None
        attempts_used = 0
        rate_limited = False
        for attempt in range(1, max_attempts + 1):
            attempts_used = attempt
            try:
                gate.wait()
                package = package_factory(competition=competition)
                frame = package.get_game_play_by_play_data(
                    key.season,
                    key.gamecode,
                    include_ishometeam=True,
                )
                if frame.empty:
                    raise ValueError("package returned no play-by-play rows")
                missing = REQUIRED_COLUMNS.difference(frame.columns)
                if missing:
                    raise ValueError(
                        f"package PBP is missing columns: {sorted(missing)}"
                    )
                frame = frame.sort_values(
                    "TRUE_NUMBEROFPLAY", kind="stable"
                ).reset_index(drop=True)
                if frame["TRUE_NUMBEROFPLAY"].duplicated().any():
                    raise ValueError("package source event order is not unique")
                retrieved_at = datetime.now(timezone.utc).isoformat()
                _atomic_json_write(
                    path,
                    _frame_payload(
                        frame,
                        competition=competition,
                        season=key.season,
                        gamecode=key.gamecode,
                        retrieved_at_utc=retrieved_at,
                    ),
                )
                return PbpCollectionRecord(
                    competition,
                    key.season,
                    key.gamecode,
                    "fetched",
                    attempts_used,
                    retrieved_at,
                    str(path),
                    int(len(frame)),
                    None,
                )
            except (requests.RequestException, KeyError, TypeError, ValueError) as exc:
                error = f"{type(exc).__name__}: {exc}"
                if attempt < max_attempts:
                    delay, is_rate_limit = _retry_delay(
                        exc,
                        attempt,
                        rate_limit_backoff_seconds,
                    )
                    rate_limited = rate_limited or is_rate_limit
                    if is_rate_limit:
                        gate.penalize(delay)
                    else:
                        time.sleep(delay)

        if rate_limited:
            gate.penalize(min(rate_limit_backoff_seconds * 2, 300))
        return PbpCollectionRecord(
            competition,
            key.season,
            key.gamecode,
            "failed",
            attempts_used,
            None,
            None,
            0,
            error,
        )

    records_by_key: dict[GameKey, PbpCollectionRecord] = {}
    completed = 0

    def checkpoint(outcome: TaskOutcome[GameKey, PbpCollectionRecord]) -> None:
        nonlocal completed
        completed += 1
        record = outcome.result
        if record is None:
            error = outcome.error or RuntimeError("unknown package failure")
            record = PbpCollectionRecord(
                competition,
                outcome.item.season,
                outcome.item.gamecode,
                "failed",
                0,
                None,
                None,
                0,
                f"{type(error).__name__}: {error}",
            )
        records_by_key[outcome.item] = record
        print(
            f"[{completed}/{len(ordered_keys)}] {record.status} "
            f"season={record.season} game={record.gamecode} "
            f"rows={record.rows} attempts={record.attempts}"
            + (f" error={record.error}" if record.error else ""),
            flush=True,
        )
        ordered_records = [
            records_by_key[key]
            for key in ordered_keys
            if key in records_by_key
        ]
        _atomic_json_write(
            output_dir / "manifest.json",
            {
                "competition": competition,
                "updated_at_utc": datetime.now(timezone.utc).isoformat(),
                "package": "euroleague-api",
                "package_version": version("euroleague-api"),
                "package_method": "PlayByPlay.get_game_play_by_play_data",
                "source_url": PBP_URL,
                "max_workers": max_workers,
                "minimum_request_interval_seconds": throttle_seconds,
                "records": [asdict(item) for item in ordered_records],
            },
        )

    run_bounded(
        ordered_keys,
        collect_one,
        max_workers=max_workers,
        on_complete=checkpoint,
    )
    return [records_by_key[key] for key in ordered_keys]


def combined_cached_pbp(
    records: list[PbpCollectionRecord],
) -> pd.DataFrame:
    frames = [
        read_cached_pbp(Path(record.path))
        for record in records
        if record.status in {"cached", "fetched"} and record.path
    ]
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True).sort_values(
        ["Season", "Gamecode", "TRUE_NUMBEROFPLAY"], kind="stable"
    ).reset_index(drop=True)


def _arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Collect per-game PBP through euroleague-api."
    )
    parser.add_argument("games_csv", type=Path)
    parser.add_argument("output_dir", type=Path)
    parser.add_argument("--competition", default="E")
    parser.add_argument("--max-attempts", type=int, default=4)
    parser.add_argument("--throttle", type=float, default=0.75)
    parser.add_argument("--rate-limit-backoff", type=float, default=60.0)
    parser.add_argument("--workers", type=int, default=2)
    parser.add_argument("--limit", type=int)
    parser.add_argument("--combined-output", type=Path)
    parser.add_argument("--batch-size", type=int, default=20)
    parser.add_argument("--batch-sleep", type=float, default=30.0)
    return parser.parse_args()


def main() -> None:
    args = _arguments()
    keys = game_keys_from_csv(args.games_csv)
    if args.limit is not None:
        keys = keys[: args.limit]
    if args.batch_size < 1 or args.batch_sleep < 0:
        raise SystemExit("--batch-size must be positive and --batch-sleep non-negative")
    records = []
    for offset in range(0, len(keys), args.batch_size):
        batch = keys[offset : offset + args.batch_size]
        print(f"fetch batch {offset // args.batch_size + 1}: "
              f"games={batch[0].gamecode}-{batch[-1].gamecode}", flush=True)
        records.extend(collect_play_by_play(
            batch, args.output_dir, competition=args.competition,
            max_attempts=args.max_attempts, throttle_seconds=args.throttle,
            rate_limit_backoff_seconds=args.rate_limit_backoff,
            max_workers=args.workers,
        ))
        if offset + args.batch_size < len(keys):
            print(f"API cooldown: sleeping {args.batch_sleep:.1f}s", flush=True)
            time.sleep(args.batch_sleep)
    failed = sum(record.status == "failed" for record in records)
    if args.combined_output is not None:
        combined = combined_cached_pbp(records)
        args.combined_output.parent.mkdir(parents=True, exist_ok=True)
        combined.to_csv(args.combined_output, index=False)
    print(
        f"complete games={len(records)} failed={failed} "
        f"available={len(records) - failed}",
        flush=True,
    )
    if failed:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
