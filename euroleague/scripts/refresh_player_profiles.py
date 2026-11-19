#!/usr/bin/env python
"""Refresh euroleague.player_season_profiles (migration 067) from the roster.

Run after each load (.github/workflows/euroleague-load.yml): it picks up
players who appeared for the first time and mid-season transfers. Upsert only;
a value the provider stops sending is kept.

    python scripts/refresh_player_profiles.py                  # dry run, current season, E and U
    python scripts/refresh_player_profiles.py --execute
    python scripts/refresh_player_profiles.py --season 2025 --competition E --execute
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "src"))
sys.path.insert(0, str(REPO / "scripts"))

from euroleague_possessions import roster_collector  # noqa: E402
from euroleague_possessions.postgres_backend import connect_from_env_file  # noqa: E402
from load_new_games import current_provider_season  # noqa: E402

ENV_PATH = Path(os.environ.get("EL_ENV_PATH", REPO.parent / "etl" / ".Renviron"))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--season", type=int, default=current_provider_season())
    parser.add_argument("--competition", action="append", choices=["E", "U"])
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args(argv)
    connection = connect_from_env_file(ENV_PATH, direct_port=5432)
    cursor = connection.cursor()
    try:
        cursor.execute("BEGIN")
        cursor.execute("SET LOCAL lock_timeout='5s'")
        for competition in args.competition or ["E", "U"]:
            stats = roster_collector.refresh(cursor, competition, args.season)
            print(f"{competition}/{args.season}: roster={stats['roster']} "
                  f"written={stats['written']} not-yet-played={stats['skipped']}")
        cursor.execute("COMMIT" if args.execute else "ROLLBACK")
        print("committed" if args.execute else "rolled back (dry run; --execute to commit)")
        return 0
    except Exception:
        cursor.execute("ROLLBACK")
        raise
    finally:
        connection.close()


if __name__ == "__main__":
    raise SystemExit(main())
