"""Restartable per-game collector for official EuroLeague box-score payloads."""

from __future__ import annotations

import argparse
import json
import threading
import time
from collections.abc import Callable
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import requests

from .concurrency import AdaptiveRequestGate, TaskOutcome, run_bounded


BOXSCORE_URL = "https://live.euroleague.net/api/Boxscore"


@dataclass(frozen=True, order=True)
class GameKey:
    season: int
    gamecode: int

    def filename(self, competition: str) -> str:
        return f"{competition}{self.season}_{self.gamecode}.json"


@dataclass(frozen=True)
class CollectionRecord:
    competition: str
    season: int
    gamecode: int
    status: str
    attempts: int
    retrieved_at_utc: str | None
    path: str | None
    error: str | None


def game_keys_from_csv(path: Path) -> list[GameKey]:
    frame = pd.read_csv(path)
    season_column = "season" if "season" in frame.columns else "Season"
    game_column = "gamecode" if "gamecode" in frame.columns else "Gamecode"
    missing = [
        name
        for name, column in (("season", season_column), ("gamecode", game_column))
        if column not in frame.columns
    ]
    if missing:
        raise ValueError(f"Input CSV is missing columns: {', '.join(missing)}")
    keys = {
        GameKey(int(season), int(gamecode))
        for season, gamecode in frame[[season_column, game_column]].itertuples(
            index=False, name=None
        )
    }
    return sorted(keys)


def _valid_cached_payload(path: Path) -> bool:
    if not path.exists():
        return False
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return False
    return isinstance(payload, dict) and isinstance(payload.get("Stats"), list)


def _atomic_json_write(path: Path, payload: Any) -> None:
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, separators=(",", ":")),
        encoding="utf-8",
    )
    temporary.replace(path)


def _retry_after_seconds(
    response: requests.Response | None,
    attempt: int,
    base_seconds: float,
) -> float:
    if response is not None:
        raw_header = response.headers.get("Retry-After")
        if raw_header:
            try:
                return max(float(raw_header), base_seconds)
            except ValueError:
                pass
    return min(base_seconds * (2 ** (attempt - 1)), 300.0)


def collect_boxscores(
    keys: list[GameKey],
    output_dir: Path,
    *,
    competition: str = "E",
    timeout_seconds: float = 60.0,
    max_attempts: int = 4,
    throttle_seconds: float = 0.75,
    rate_limit_backoff_seconds: float = 60.0,
    max_workers: int = 1,
    request_get: Callable[..., requests.Response] | None = None,
) -> list[CollectionRecord]:
    """Fetch games concurrently while preserving per-game checkpoints."""

    if max_attempts < 1:
        raise ValueError("max_attempts must be at least 1")
    output_dir.mkdir(parents=True, exist_ok=True)
    ordered_keys = sorted(set(keys))
    gate = AdaptiveRequestGate(throttle_seconds)
    thread_state = threading.local()

    def get_response(**kwargs: Any) -> requests.Response:
        if request_get is not None:
            return request_get(BOXSCORE_URL, **kwargs)
        session = getattr(thread_state, "session", None)
        if session is None:
            session = requests.Session()
            session.headers.update({"Accept": "application/json"})
            thread_state.session = session
        return session.get(BOXSCORE_URL, **kwargs)

    def collect_one(key: GameKey) -> CollectionRecord:
        path = output_dir / key.filename(competition)
        if _valid_cached_payload(path):
            return CollectionRecord(
                competition=competition,
                season=key.season,
                gamecode=key.gamecode,
                status="cached",
                attempts=0,
                retrieved_at_utc=None,
                path=str(path),
                error=None,
            )

        error: str | None = None
        retrieved_at: str | None = None
        attempts_used = 0
        rate_limited = False
        last_rate_limit_delay = rate_limit_backoff_seconds
        for attempt in range(1, max_attempts + 1):
            attempts_used = attempt
            try:
                gate.wait()
                response = get_response(
                    params={
                        "gamecode": key.gamecode,
                        "seasoncode": f"{competition}{key.season}",
                    },
                    timeout=timeout_seconds,
                )
                response.raise_for_status()
                payload = response.json()
                if not isinstance(payload, dict) or not isinstance(
                    payload.get("Stats"), list
                ):
                    raise ValueError("response does not contain a Stats list")
                _atomic_json_write(path, payload)
                retrieved_at = datetime.now(timezone.utc).isoformat()
                error = None
                break
            except (requests.RequestException, ValueError, json.JSONDecodeError) as exc:
                error = f"{type(exc).__name__}: {exc}"
                if attempt < max_attempts:
                    response = exc.response if isinstance(
                        exc, requests.HTTPError
                    ) else None
                    if response is not None and response.status_code == 429:
                        rate_limited = True
                        last_rate_limit_delay = _retry_after_seconds(
                            response, attempt, rate_limit_backoff_seconds
                        )
                        gate.penalize(last_rate_limit_delay)
                    else:
                        time.sleep(min(2 ** (attempt - 1), 8))

        status = "fetched" if error is None else "failed"
        if status == "failed" and rate_limited:
            gate.penalize(min(last_rate_limit_delay * 2, 300.0))
        return CollectionRecord(
            competition=competition,
            season=key.season,
            gamecode=key.gamecode,
            status=status,
            attempts=attempts_used,
            retrieved_at_utc=retrieved_at,
            path=str(path) if error is None else None,
            error=error,
        )

    records_by_key: dict[GameKey, CollectionRecord] = {}
    completed = 0

    def checkpoint(outcome: TaskOutcome[GameKey, CollectionRecord]) -> None:
        nonlocal completed
        completed += 1
        key = outcome.item
        record = outcome.result
        if record is None:
            error = outcome.error or RuntimeError("unknown collection failure")
            record = CollectionRecord(
                competition=competition,
                season=key.season,
                gamecode=key.gamecode,
                status="failed",
                attempts=0,
                retrieved_at_utc=None,
                path=None,
                error=f"{type(error).__name__}: {error}",
            )
        records_by_key[key] = record
        print(
            f"[{completed}/{len(ordered_keys)}] {record.status} "
            f"season={key.season} game={key.gamecode} "
            f"attempts={record.attempts}"
            + (f" error={record.error}" if record.error else ""),
            flush=True,
        )
        ordered_records = [
            records_by_key[item]
            for item in ordered_keys
            if item in records_by_key
        ]
        _atomic_json_write(
            output_dir / "manifest.json",
            {
                "competition": competition,
                "updated_at_utc": datetime.now(timezone.utc).isoformat(),
                "source_url": BOXSCORE_URL,
                "max_workers": max_workers,
                "minimum_request_interval_seconds": throttle_seconds,
                "records": [asdict(item) for item in ordered_records],
            },
        )

    outcomes = run_bounded(
        ordered_keys,
        collect_one,
        max_workers=max_workers,
        on_complete=checkpoint,
    )
    # ``checkpoint`` converts unexpected worker exceptions into failed records.
    if len(outcomes) != len(records_by_key):
        raise RuntimeError("collector outcome accounting mismatch")
    return [records_by_key[key] for key in ordered_keys]


def _arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("games_csv", type=Path)
    parser.add_argument("output_dir", type=Path)
    parser.add_argument("--competition", default="E")
    parser.add_argument("--max-attempts", type=int, default=4)
    parser.add_argument("--timeout", type=float, default=60.0)
    parser.add_argument("--throttle", type=float, default=0.75)
    parser.add_argument("--rate-limit-backoff", type=float, default=60.0)
    parser.add_argument("--workers", type=int, default=1)
    parser.add_argument("--limit", type=int)
    return parser.parse_args()


def main() -> None:
    args = _arguments()
    keys = game_keys_from_csv(args.games_csv)
    if args.limit is not None:
        keys = keys[: args.limit]
    records = collect_boxscores(
        keys,
        args.output_dir,
        competition=args.competition,
        timeout_seconds=args.timeout,
        max_attempts=args.max_attempts,
        throttle_seconds=args.throttle,
        rate_limit_backoff_seconds=args.rate_limit_backoff,
        max_workers=args.workers,
    )
    failed = sum(record.status == "failed" for record in records)
    print(
        f"complete games={len(records)} failed={failed} "
        f"available={len(records) - failed}",
        flush=True,
    )
    if failed:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
