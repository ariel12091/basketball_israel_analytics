#!/usr/bin/env python
"""Load every EuroLeague/EuroCup game that has been played but not yet loaded.

The scheduled (weekly) entry point. It decides WHICH gamecodes to load and then
hands them to ``load_games.py``, so collection, staging, publication and
verification are exactly the sanctioned manual path -- nothing is reimplemented.

Discovery uses the euroleague_api package: ``Schedule.get_gamecodes_season()``
lists a season's *results*, i.e. played games only. That is also the call
staging uses for round / phase / tip-off, so every game selected here is one
whose schedule metadata is already available -- a game the results feed does
not list yet waits for the next run rather than landing with NULL metadata.

Insert-only by construction: a gamecode already present in
``euroleague.schedule`` for the (competition, season) is never requested, so an
existing game is never republished. Force a reload with ``load_games.py``.

    # what would be loaded (collects + stages, writes nothing)
    python scripts/load_new_games.py --competition E

    # load it
    python scripts/load_new_games.py --competition E --execute

Exit code: 0 when there was nothing to load or the load verified clean; the
``load_games.py`` exit code otherwise.
"""
from __future__ import annotations

import argparse
import subprocess
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "src"))
ENV_FILE = REPO.parent / "etl" / ".Renviron"


def current_provider_season(today: date | None = None) -> int:
    """Provider season for a date: 2026 = the 2026-27 season.

    A season runs from late September to the following June, so August onward
    belongs to the season starting that year.
    """
    today = today or date.today()
    return today.year if today.month >= 8 else today.year - 1


def played_gamecodes(competition: str, season: int) -> list[int]:
    """Gamecodes the provider lists as played for a competition-season."""
    from euroleague_api.schedule import Schedule

    try:
        frame = Schedule(competition=competition).get_gamecodes_season(season)
    except KeyError:
        # The results feed is an empty <results/> document until the first game
        # of the season is final; the package then fails on the missing 'game'.
        return []
    return select_played(frame.to_dict("records"))


# The package derives ``played`` with ``astype(bool)`` on the raw strings, so
# "false" becomes True; the flag cannot tell a live game from a final one. The
# feed does appear to list finished games only, but require the tip-off to be
# well in the past as well. Provider times are local (CET/EET), so the margin
# also absorbs the UTC offset.
FINAL_AFTER = timedelta(hours=6)


def select_played(records: list[dict], now: datetime | None = None) -> list[int]:
    from euroleague_possessions.schedule_collector import _parse_tipoff

    now = now or datetime.now(timezone.utc)
    out: set[int] = set()
    for r in records:
        if not bool(r.get("played")):
            continue
        tipoff = _parse_tipoff(r.get("date"), r.get("time"))
        if tipoff is None or datetime.fromisoformat(tipoff) + FINAL_AFTER > now:
            continue
        out.add(int(r["gameCode"]))
    return sorted(out)


def loaded_gamecodes(competition: str, season: int) -> set[int]:
    from euroleague_possessions.postgres_backend import connect_from_env_file

    conn = connect_from_env_file(ENV_FILE)
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT gamecode FROM euroleague.schedule "
            "WHERE competition = %s AND season = %s",
            (competition, season),
        )
        return {int(r[0]) for r in cur.fetchall()}
    finally:
        conn.close()


def new_gamecodes(played: list[int], loaded: set[int]) -> list[int]:
    return [code for code in played if code not in loaded]


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--competition", default="E", help="E=EuroLeague, U=EuroCup")
    ap.add_argument("--season", type=int, default=None,
                    help="provider season (2026 = 2026-27); default: current")
    ap.add_argument("--execute", action="store_true",
                    help="publish; without it load_games.py only collects and stages")
    ap.add_argument("--max-games", type=int, default=60,
                    help="refuse to load more than this many in one run (default 60); "
                         "a larger backlog is a manual two-phase load")
    args = ap.parse_args()

    season = args.season if args.season is not None else current_provider_season()
    played = played_gamecodes(args.competition, season)
    loaded = loaded_gamecodes(args.competition, season)
    todo = new_gamecodes(played, loaded)

    print(f"competition={args.competition} season={season}: "
          f"{len(played)} played, {len(loaded)} loaded, {len(todo)} new")
    if not todo:
        print("nothing to load")
        return
    print("new gamecodes: " + ",".join(str(c) for c in todo))
    if len(todo) > args.max_games:
        raise SystemExit(
            f"{len(todo)} new games exceeds --max-games {args.max_games}; "
            "load this backlog by hand (RUNBOOK: two-phase large load)"
        )

    cmd = [
        sys.executable, str(REPO / "scripts" / "load_games.py"),
        "--games", ",".join(str(c) for c in todo),
        "--season", str(season),
        "--competition", args.competition,
        "--collect-workers", "1",
        "--stage-workers", "1",
    ]
    if args.execute:
        cmd.append("--execute")
    raise SystemExit(subprocess.run(cmd, cwd=REPO).returncode)


if __name__ == "__main__":
    main()
