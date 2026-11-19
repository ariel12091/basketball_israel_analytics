"""Player height, position and headshot from the provider's season roster.

The euroleague_api package (0.1.1) does not wrap this endpoint, so it is
called directly, as migration 065 does for club crests:

    https://api-live.euroleague.net/v2/competitions/{c}/seasons/{c}{season}/people?personType=J

One roster row per player and club; a mid-season transfer has a row for each
club. Rows are written to euroleague.player_season_profiles (migration 067),
matched to euroleague.players on provider_player_id = 'P' + person.code and to
euroleague.teams on provider_team_code = club.code. A player with no row in
euroleague.players (not yet in a loaded game) is skipped and counted.
"""

from __future__ import annotations

import json
import urllib.request
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Iterable

PEOPLE_URL = (
    "https://api-live.euroleague.net/v2/competitions/{c}/seasons/{c}{s}/people"
    "?personType=J&limit=1000"
)

# The provider sends three positions; basket.co.il's codes for them.
POSITION_CODES = {"Guard": "G", "Forward": "F", "Center": "C"}


@dataclass(frozen=True)
class Profile:
    provider_player_id: str
    provider_team_code: str
    height_m: Decimal | None
    position_en: str | None
    position_name: str | None
    headshot_url: str | None


def _https(url: Any) -> str | None:
    return url if isinstance(url, str) and url.startswith("https://") else None


def parse_roster(rows: Iterable[dict[str, Any]]) -> list[Profile]:
    """Provider roster rows -> one Profile per (player, club)."""
    out: dict[tuple[str, str], Profile] = {}
    for row in rows:
        person = row.get("person") or {}
        club = row.get("club") or {}
        code, club_code = person.get("code"), club.get("code")
        if not code or not club_code:
            continue
        height = person.get("height")
        height_m = (Decimal(int(height)) / 100).quantize(Decimal("0.01")) \
            if isinstance(height, (int, float)) and 150 <= height <= 240 else None
        name = row.get("positionName") or None
        headshot = _https((row.get("images") or {}).get("headshot")) \
            or _https((person.get("images") or {}).get("headshot"))
        out[("P" + str(code), str(club_code))] = Profile(
            provider_player_id="P" + str(code),
            provider_team_code=str(club_code),
            height_m=height_m,
            position_en=POSITION_CODES.get(name) if name else None,
            position_name=name,
            headshot_url=headshot,
        )
    return list(out.values())


def fetch_roster(competition: str, season: int) -> list[dict[str, Any]]:
    req = urllib.request.Request(PEOPLE_URL.format(c=competition, s=season),
                                 headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=60) as r:
        return json.loads(r.read().decode("utf-8"))["data"]


UPSERT_SQL = """
INSERT INTO euroleague.player_season_profiles
  (season, player_id, team_id, height_m, position_en, position_name, headshot_url, fetched_at)
SELECT %(season)s, p.player_id, t.team_id, %(height_m)s, %(position_en)s,
       %(position_name)s, %(headshot_url)s, now()
FROM euroleague.players p
JOIN euroleague.teams t
  ON t.competition = p.competition AND t.provider_team_code = %(team_code)s
WHERE p.competition = %(competition)s AND p.provider_player_id = %(player_code)s
ON CONFLICT (season, team_id, player_id) DO UPDATE SET
  height_m      = COALESCE(EXCLUDED.height_m, player_season_profiles.height_m),
  position_en   = COALESCE(EXCLUDED.position_en, player_season_profiles.position_en),
  position_name = COALESCE(EXCLUDED.position_name, player_season_profiles.position_name),
  headshot_url  = COALESCE(EXCLUDED.headshot_url, player_season_profiles.headshot_url),
  fetched_at    = now()
"""


def write_profiles(cursor: Any, competition: str, season: int,
                   profiles: Iterable[Profile]) -> dict[str, int]:
    """Upsert inside the caller's transaction; returns written / skipped counts."""
    written = skipped = 0
    for p in profiles:
        cursor.execute(UPSERT_SQL, {
            "season": season, "competition": competition,
            "player_code": p.provider_player_id, "team_code": p.provider_team_code,
            "height_m": p.height_m, "position_en": p.position_en,
            "position_name": p.position_name, "headshot_url": p.headshot_url,
        })
        if cursor.rowcount:
            written += 1
        else:
            skipped += 1
    return {"written": written, "skipped": skipped}


def refresh(cursor: Any, competition: str, season: int) -> dict[str, int]:
    """Fetch one competition-season's roster and write it."""
    profiles = parse_roster(fetch_roster(competition, season))
    stats = write_profiles(cursor, competition, season, profiles)
    stats["roster"] = len(profiles)
    return stats
