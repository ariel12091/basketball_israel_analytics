"""Small deterministic concurrency helpers for independent per-game work."""

from __future__ import annotations

import threading
import time
from collections.abc import Callable, Iterable
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Generic, TypeVar


ItemT = TypeVar("ItemT")
ResultT = TypeVar("ResultT")


@dataclass(frozen=True)
class TaskOutcome(Generic[ItemT, ResultT]):
    """One isolated task result; output ordering is assigned by the caller."""

    item: ItemT
    result: ResultT | None
    error: Exception | None


class AdaptiveRequestGate:
    """Serialize request starts and share provider cooldowns across workers."""

    def __init__(
        self,
        minimum_interval_seconds: float,
        *,
        monotonic: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        if minimum_interval_seconds < 0:
            raise ValueError("minimum request interval cannot be negative")
        self._minimum_interval = float(minimum_interval_seconds)
        self._monotonic = monotonic
        self._sleep = sleep
        self._lock = threading.Lock()
        self._next_request_at = 0.0
        self._cooldown_until = 0.0

    def wait(self) -> None:
        """Wait for the next globally permitted request-start slot."""

        while True:
            with self._lock:
                now = self._monotonic()
                permitted_at = max(
                    self._next_request_at,
                    self._cooldown_until,
                )
                delay = permitted_at - now
                if delay <= 0:
                    self._next_request_at = now + self._minimum_interval
                    return
            self._sleep(delay)

    def penalize(self, seconds: float) -> None:
        """Pause every worker after a provider throttling response."""

        if seconds < 0:
            raise ValueError("cooldown cannot be negative")
        with self._lock:
            cooldown_until = self._monotonic() + float(seconds)
            self._cooldown_until = max(self._cooldown_until, cooldown_until)
            self._next_request_at = max(
                self._next_request_at,
                self._cooldown_until,
            )


def run_bounded(
    items: Iterable[ItemT],
    worker: Callable[[ItemT], ResultT],
    *,
    max_workers: int,
    on_complete: Callable[[TaskOutcome[ItemT, ResultT]], None] | None = None,
) -> list[TaskOutcome[ItemT, ResultT]]:
    """Run isolated tasks concurrently and return results in input order."""

    if max_workers < 1:
        raise ValueError("max_workers must be at least 1")
    ordered_items = list(items)
    if not ordered_items:
        return []

    indexed: dict[Future[ResultT], int] = {}
    outcomes: list[TaskOutcome[ItemT, ResultT] | None] = [
        None for _ in ordered_items
    ]
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        for index, item in enumerate(ordered_items):
            indexed[executor.submit(worker, item)] = index
        for future in as_completed(indexed):
            index = indexed[future]
            item = ordered_items[index]
            try:
                outcome = TaskOutcome(
                    item=item,
                    result=future.result(),
                    error=None,
                )
            except Exception as exc:  # isolated batch failure by design
                outcome = TaskOutcome(item=item, result=None, error=exc)
            outcomes[index] = outcome
            if on_complete is not None:
                on_complete(outcome)

    return [outcome for outcome in outcomes if outcome is not None]
