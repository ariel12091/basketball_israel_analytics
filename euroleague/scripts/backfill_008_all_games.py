"""Task 4: backfill the corrected four-factor grain and the event fact, all 84 games.

Order matters. `player_four_factors_by_game` still holds 81 games built by
migration 007 revision 1, whose unconditional cross join generated rows for
combinations that never occurred. The gate diffs the fact against that table, so
the reference must be rebuilt on the corrected function before a full-season gate
means anything -- otherwise the 81 stale games fail on phantom rows that were
already known to be wrong.

Each refresh runs in its own transaction so a failure in one does not roll back
the other. Both functions are per-game DELETE/INSERT and idempotent, so a rerun
is safe.

Run from euroleague/:  ./.venv/Scripts/python.exe scripts/backfill_008_all_games.py
"""
import sys
import time
from pathlib import Path

sys.path.insert(0, "src")
from euroleague_possessions.postgres_backend import connect_from_env_file

STEPS = [
    ("player_four_factors_by_game (corrected 007)",
     "SELECT euroleague.refresh_player_four_factors_by_game_for_games(NULL::bigint[])"),
    ("action_team_context + matchup_segments (008)",
     "SELECT euroleague.refresh_action_team_context_for_games(NULL::bigint[])"),
]

SIZES = [
    "euroleague.action_team_context",
    "euroleague.matchup_segments",
    "euroleague.player_four_factors_by_game",
]


def main() -> None:
    conn = connect_from_env_file(Path("../etl/.Renviron"), direct_port=5432)
    cur = conn.cursor()
    for label, sql in STEPS:
        print(f"\n=== {label} ===", flush=True)
        t = time.perf_counter()
        cur.execute("BEGIN")
        cur.execute("SET LOCAL statement_timeout = '60min'")
        cur.execute(sql)
        rows = cur.fetchone()[0]
        cur.execute("COMMIT")
        print(f"  rows: {rows:,}   in {time.perf_counter() - t:.1f}s", flush=True)

    print("\n=== resulting populations ===", flush=True)
    for rel in SIZES:
        cur.execute(f"SELECT count(*) FROM {rel}")
        n = cur.fetchone()[0]
        cur.execute("SELECT pg_size_pretty(pg_total_relation_size(%s))", (rel,))
        size = cur.fetchone()[0]
        print(f"  {rel:45} {n:>9,} rows   {size}")

    cur.execute("SELECT count(DISTINCT game_id) FROM euroleague.action_team_context")
    print(f"\n  games covered by the fact: {cur.fetchone()[0]}")
    conn.close()
    print("\nBackfill complete. Next: scripts/verify_action_team_context.py (all games)")


if __name__ == "__main__":
    main()
