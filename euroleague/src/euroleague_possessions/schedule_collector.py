"""Package-first collector for per-game schedule metadata.

The cached box score and play-by-play carry no round, phase or tip-off time, so
a game staged from them alone lands in ``euroleague.schedule`` with those three
columns NULL. Every downstream date/round/phase predicate then evaluates to
NULL and filters the game out, which silently empties any filtered query.

This module fetches the season schedule once and exposes it keyed by gamecode,
so ``build_staged_game`` can be handed the metadata for the game it is staging
without making a network call itself.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import pandas as pd
from euroleague_api.schedule import Schedule


# Provider column -> our field. The provider ships both ``gameCode`` (an int
# within the season) and ``gamecode`` (the "E2025_1" string); we key on the int
# because that is what ``euroleague.schedule.gamecode`` stores.
_DATE_FORMAT = "%b %d, %Y"


def _parse_tipoff(date_str: Any, time_str: Any) -> datetime | None:
    """Combine the provider's separate date and time columns into a UTC-naive
    timestamp. Returns None rather than raising: a missing tip-off must not
    fail a load, it just leaves the column NULL as before."""
    if not isinstance(date_str, str) or not date_str.strip():
        return None
    try:
        day = datetime.strptime(date_str.strip(), _DATE_FORMAT)
    except ValueError:
        return None
    if isinstance(time_str, str) and ":" in time_str:
        try:
            hour, minute = (int(x) for x in time_str.strip().split(":")[:2])
            day = day.replace(hour=hour, minute=minute)
        except ValueError:
            pass
    return day.replace(tzinfo=timezone.utc)


def _coerce_round(value: Any) -> int | None:
    try:
        if value is None or (isinstance(value, float) and pd.isna(value)):
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


def fetch_season_schedule_meta(
    competition: str = "E",
    season: int = 2025,
) -> dict[int, dict[str, Any]]:
    """Return ``{gamecode: {round_number, phase, scheduled_at}}`` for a season.

    One request per competition-season. Callers should fetch once and reuse
    across every game in the batch.
    """
    frame = Schedule(competition=competition).get_gamecodes_season(season)
    out: dict[int, dict[str, Any]] = {}
    for row in frame.to_dict("records"):
        code = _coerce_round(row.get("gameCode"))
        if code is None:
            continue
        phase = row.get("Phase")
        out[code] = {
            "round_number": _coerce_round(row.get("Round")),
            "phase": str(phase).strip() if isinstance(phase, str) and phase.strip() else None,
            "scheduled_at": _parse_tipoff(row.get("date"), row.get("time")),
        }
    return out
