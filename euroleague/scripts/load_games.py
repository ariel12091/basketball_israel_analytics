#!/usr/bin/env python
"""One command to take EuroLeague games from the provider into the schema.

Wraps the four steps that a load actually is -- collect box scores, collect
play-by-play, stage canonical actions offline, publish -- and then
verifies the result. It shells out to the existing module CLIs rather than
reimplementing them, so the sanctioned retry, throttle and load-run logic is
exactly what runs.

Safe by default: without --execute it collects and stages but does NOT write to
the database, and tells you what it would have published.

Every step is resumable. Both collectors skip payloads already cached on disk
and staging reuses checkpoints, so re-running after a failure only does the
work that is actually missing.

    # see what 85-150 would do; no database writes
    python scripts/load_games.py --games 85-150

    # actually load them
    python scripts/load_games.py --games 85-150 --execute

    # re-check a load that already happened
    python scripts/load_games.py --games 1-84 --verify-only

Run from the euroleague/ directory with its virtualenv:
    .venv/Scripts/python.exe scripts/load_games.py ...
"""
from __future__ import annotations

import argparse
import csv
import subprocess
import sys
import time
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "src"))


def parse_games(spec: str) -> list[int]:
    """Accept '85-150', '1,2,3', or a mix: '1-10,25,40-42'."""
    out: set[int] = set()
    for part in spec.split(","):
        part = part.strip()
        if not part:
            continue
        if "-" in part:
            lo, hi = part.split("-", 1)
            lo_i, hi_i = int(lo), int(hi)
            if lo_i > hi_i:
                raise SystemExit(f"bad range (start > end): {part}")
            out.update(range(lo_i, hi_i + 1))
        else:
            out.add(int(part))
    if not out:
        raise SystemExit("no gamecodes selected")
    return sorted(out)


def run(label: str, cmd: list[str]) -> None:
    print(f"\n=== {label} ===")
    print("  $ " + " ".join(str(c) for c in cmd))
    started = time.perf_counter()
    result = subprocess.run(cmd, cwd=REPO)
    elapsed = time.perf_counter() - started
    if result.returncode != 0:
        raise SystemExit(f"{label} FAILED (exit {result.returncode}) after {elapsed:.1f}s")
    print(f"  {label} ok ({elapsed:.1f}s)")


# ---------------------------------------------------------------------------
# Verification
# ---------------------------------------------------------------------------

def verify(competition: str, season: int, gamecodes: list[int]) -> int:
    """Check a published load. Returns the number of FAILED checks."""
    from euroleague_possessions.postgres_backend import connect_from_env_file

    conn = connect_from_env_file(REPO.parent / "etl" / ".Renviron")
    cur = conn.cursor()

    def q(sql, params=None):
        cur.execute(sql, params)
        return cur.fetchall()

    failures = 0

    def check(name: str, ok: bool, detail: str = "") -> None:
        nonlocal failures
        if not ok:
            failures += 1
        print(f"  {'PASS' if ok else 'FAIL'}  {name:44s} {detail}")

    print("\n=== verification ===")

    # Load run must not be left mid-flight or partial.
    runs = q(
        "SELECT load_run_id, status, requested_games, successful_games, failed_games "
        "FROM euroleague.load_runs ORDER BY load_run_id DESC LIMIT 1"
    )
    if runs:
        rid, status, req, okc, badc = runs[0]
        check(
            f"latest load run ({rid}) completed",
            status == "completed",
            f"status={status} requested={req} ok={okc} failed={badc}",
        )
    else:
        check("a load run exists", False)

    loaded = q(
        "SELECT count(*) FROM euroleague.schedule WHERE competition=%s AND season=%s",
        (competition, season),
    )[0][0]
    check("games present for season", loaded > 0, f"{loaded} games")

    missing = q(
        "SELECT count(*) FROM euroleague.schedule "
        "WHERE competition=%s AND season=%s AND gamecode = ANY(%s) ",
        (competition, season, gamecodes),
    )[0][0]
    check(
        "every requested gamecode present",
        missing == len(gamecodes),
        f"{missing}/{len(gamecodes)}",
    )

    # Schedule metadata drives every date / round / phase filter. NULLs here
    # silently empty those filters rather than erroring.
    nulls = q(
        "SELECT count(*) FROM euroleague.schedule "
        "WHERE competition=%s AND season=%s "
        "AND (round_number IS NULL OR phase IS NULL OR scheduled_at IS NULL)",
        (competition, season),
    )[0][0]
    check("schedule metadata populated", nulls == 0, f"{nulls} rows missing round/phase/tipoff")

    # One derivation version across the schema, or aggregates mix logic.
    versions = [r[0] for r in q("SELECT DISTINCT parser_version FROM euroleague.actions")]
    check("single parser version", len(versions) <= 1, f"versions={versions}")

    # Points and possessions must match the official box score.
    bad_pts = q(
        "SELECT count(*) FROM euroleague.team_game_ratings_mv g "
        "JOIN euroleague.final_schedule fs "
        "  ON fs.game_id=g.game_id AND fs.team_id=g.team_id "
        "WHERE g.off_pts <> fs.team_points OR g.def_pts <> fs.opp_points"
    )[0][0]
    check("team points match box score", bad_pts == 0, f"{bad_pts} mismatched team-games")

    asym = q(
        "SELECT count(*) FROM euroleague.team_game_ratings_mv a "
        "JOIN euroleague.team_game_ratings_mv b "
        "  ON b.game_id=a.game_id AND b.team_id=a.opp_team_id "
        "WHERE a.off_poss <> b.def_poss OR a.def_poss <> b.off_poss"
    )[0][0]
    check("possessions symmetric between opponents", asym == 0, f"{asym} asymmetric rows")

    # Team-grain four factors, derived independently, must agree with the
    # player-grain fact divided by 5 (5 on-court players per possession).
    ff_bad = q(
        """
        WITH team AS (
          SELECT game_id, team_id, sum(off_ts_poss) ts, sum(off_oreb) oreb,
                 sum(off_tov) tov, sum(off_fga) fga, sum(off_fgm) fgm
            FROM euroleague.team_four_factors_by_game GROUP BY 1,2),
        ply AS (
          SELECT game_id, team_id, sum(ts_poss_count)/5 ts, sum(oreb_count)/5 oreb,
                 sum(tov_count)/5 tov, sum(total_fga)/5 fga, sum(total_fgm)/5 fgm
            FROM euroleague.player_game_context
           WHERE type_lineup='offense' AND is_on_key=1 GROUP BY 1,2)
        SELECT count(*) FROM team t JOIN ply p
          ON p.game_id=t.game_id AND p.team_id=t.team_id
         WHERE t.ts<>p.ts OR t.oreb<>p.oreb OR t.tov<>p.tov
            OR t.fga<>p.fga OR t.fgm<>p.fgm
        """
    )[0][0]
    check("team four factors match player fact", ff_bad == 0, f"{ff_bad} disagreeing team-games")

    # Every published game needs team analytics, not just player analytics.
    orphan = q(
        "SELECT count(*) FROM euroleague.schedule s "
        "WHERE NOT EXISTS (SELECT 1 FROM euroleague.team_four_factors_by_game f "
        "                   WHERE f.game_id = s.game_id)"
    )[0][0]
    check("all games have team analytics", orphan == 0, f"{orphan} games missing")

    orphan_fact = q(
        "SELECT count(*) FROM euroleague.schedule s "
        "WHERE NOT EXISTS (SELECT 1 FROM euroleague.action_team_context_actions a "
        "                   WHERE a.game_id = s.game_id)"
    )[0][0]
    check("all games have the event fact", orphan_fact == 0, f"{orphan_fact} games missing")

    canonical_missing = q(
        "SELECT count(*) FROM euroleague.actions_raw ar "
        "FULL JOIN euroleague.actions a "
        "  ON a.game_id=ar.game_id "
        " AND a.source_event_order=ar.source_event_order "
        "WHERE ar.game_id IS NULL OR a.game_id IS NULL"
    )[0][0]
    check(
        "canonical actions cover raw PBP exactly",
        canonical_missing == 0,
        f"{canonical_missing} missing or extra events",
    )

    package_column_mismatch = q(
        "SELECT count(*) FROM euroleague.actions a "
        "JOIN euroleague.actions_raw ar "
        "  ON ar.game_id=a.game_id "
        " AND ar.source_event_order=a.source_event_order "
        "WHERE jsonb_build_object("
        " 'Season',a.season, 'Gamecode',a.gamecode, "
        " 'TYPE',a.provider_event_type, 'NUMBEROFPLAY',a.provider_play_number, "
        " 'CODETEAM',a.provider_team_code, 'PLAYER_ID',a.provider_player_id, "
        " 'PLAYTYPE',a.play_type, 'PLAYER',a.player_name, 'TEAM',a.team_name, "
        " 'DORSAL',a.jersey_number, 'MINUTE',a.minute, "
        " 'MARKERTIME',a.marker_time, 'POINTS_A',a.points_a, "
        " 'POINTS_B',a.points_b, 'COMMENT',a.comment, 'PLAYINFO',a.play_info, "
        " 'PERIOD',a.period, 'TRUE_NUMBEROFPLAY',a.source_event_order, "
        " 'Lineup_A',a.lineup_a, 'Lineup_B',a.lineup_b, "
        " 'IsHomeTeam',a.is_home_team, "
        " 'validate_on_court_player',a.validate_on_court_player"
        ") IS DISTINCT FROM ar.raw_event"
    )[0][0]
    check(
        "all package fields match canonical columns",
        package_column_mismatch == 0,
        f"{package_column_mismatch} mismatched events",
    )

    endpoint_mismatch = q(
        "WITH numbered AS ("
        " SELECT a.*, "
        "   row_number() OVER (PARTITION BY game_id ORDER BY source_event_order) "
        "     AS expected_game_number, "
        "   row_number() OVER (PARTITION BY game_id, possession_offense_team_id "
        "                      ORDER BY source_event_order) AS expected_team_number "
        " FROM euroleague.actions a WHERE a.end_possession"
        ") SELECT count(*) FROM numbered "
        "WHERE game_possession_number IS DISTINCT FROM expected_game_number "
        "   OR team_possession_number IS DISTINCT FROM expected_team_number"
    )[0][0]
    check(
        "canonical possession numbers are gap-free",
        endpoint_mismatch == 0,
        f"{endpoint_mismatch} mismatched events",
    )

    # The team grain must still be reproducible from the fact it is derived
    # from. This replaces the per-game row-count expectation that validate_game
    # used to carry: that comparison derived its expectation from
    # matchup_segments, which was a genuine cross-check only while the refresh
    # derived its own segments. Migration 009 pointed both at the same source,
    # so it became a value compared against itself.
    #
    # Deliberately the team grain, not the player grain. The team side needs no
    # roster fan-out -- a player-level equivalent would fan every event across
    # the roster, over a million rows for a season -- so this is cheap enough to
    # run on every load while exercising the same fact columns.
    fact_only = q(
        "WITH from_fact AS ("
        "  SELECT atc.game_id, atc.team_id, atc.own_starters, atc.opp_starters, "
        "         coalesce(sum(atc.points) FILTER "
        "                  (WHERE atc.type_lineup = 'offense'), 0) AS off_pts "
        "    FROM euroleague.action_team_context_actions atc "
        "   GROUP BY atc.game_id, atc.team_id, atc.own_starters, atc.opp_starters"
        ") SELECT count(*) FROM ("
        "  SELECT game_id, team_id, own_starters, opp_starters, off_pts::numeric "
        "    FROM from_fact "
        "  EXCEPT ALL "
        "  SELECT game_id, team_id, own_starters, opp_starters, off_pts::numeric "
        "    FROM euroleague.team_four_factors_by_game) x"
    )[0][0]
    check(
        "team grain reproduces from the fact",
        fact_only == 0,
        f"{fact_only} rows differ",
    )

    # Fast path must agree with the filtered path.
    parity = q(
        """SELECT count(*) FROM (
             SELECT team_id, off_ppp, def_ppp FROM euroleague.team_ppp_ratings_mv
              WHERE competition=%s AND game_year=%s
             EXCEPT
             SELECT team_id, off_ppp, def_ppp
               FROM euroleague.get_team_ratings_dynamic(%s,%s)) x""",
        (competition, season, competition, season),
    )[0][0]
    check("ratings MV matches dynamic function", parity == 0, f"{parity} differing rows")

    qa = q(
        "SELECT publication_status, count(*) FROM euroleague.game_qa "
        "GROUP BY 1 ORDER BY 1"
    )
    print(f"        game_qa publication_status: {dict(qa)}")

    size = q(
        """SELECT pg_size_pretty(sum(pg_total_relation_size(c.oid))),
                  sum(pg_total_relation_size(c.oid))
             FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
            WHERE n.nspname='euroleague'"""
    )[0]
    print(f"        euroleague schema: {size[0]} ({size[1]/max(loaded,1)/1024:.0f} kB/game)")

    cur.close()
    conn.close()
    return failures


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Collect, stage, publish and verify EuroLeague games.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    ap.add_argument("--games", required=True,
                    help="gamecodes: '85-150', '1,2,3', or '1-10,25,40-42'")
    ap.add_argument("--season", type=int, default=2025,
                    help="provider season; 2025 = the 2025-26 season (default: 2025)")
    ap.add_argument("--competition", default="E", help="E=EuroLeague, U=EuroCup (default: E)")
    ap.add_argument("--data-dir", type=Path, default=REPO / "data")
    ap.add_argument("--execute", action="store_true",
                    help="actually publish to the database (default: collect+stage only)")
    ap.add_argument("--verify-only", action="store_true",
                    help="skip collection/staging/publication, just re-run the checks")
    ap.add_argument("--collect-workers", type=int, default=2)
    ap.add_argument("--stage-workers", type=int, default=2)
    ap.add_argument("--throttle", type=float, default=0.75)
    args = ap.parse_args()

    codes = parse_games(args.games)
    tag = f"{args.competition}{args.season}_{codes[0]}_{codes[-1]}"
    print(f"competition={args.competition} season={args.season} "
          f"games={len(codes)} ({codes[0]}..{codes[-1]})")

    if args.verify_only:
        failures = verify(args.competition, args.season, codes)
        print(f"\n{'ALL CHECKS PASSED' if not failures else f'{failures} CHECK(S) FAILED'}")
        raise SystemExit(1 if failures else 0)

    raw = args.data_dir / "raw"
    raw.mkdir(parents=True, exist_ok=True)
    games_csv = raw / f"games_{tag}.csv"
    with games_csv.open("w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(["season", "gamecode"])
        for code in codes:
            w.writerow([args.season, code])
    print(f"wrote {games_csv} ({len(codes)} games)")

    py = sys.executable
    boxscores = raw / "boxscores"
    pbp_dir = raw / "pbp"
    pbp_csv = raw / f"pbp_{tag}.csv"
    checkpoints = args.data_dir / "staging" / f"batch_{tag}"

    run("collect box scores", [
        py, "-m", "euroleague_possessions.boxscore_collector",
        str(games_csv), str(boxscores),
        "--competition", args.competition,
        "--workers", str(args.collect_workers), "--throttle", str(args.throttle),
    ])
    run("collect play-by-play", [
        py, "-m", "euroleague_possessions.pbp_collector",
        str(games_csv), str(pbp_dir),
        "--competition", args.competition,
        "--workers", str(args.collect_workers), "--throttle", str(args.throttle),
        "--combined-output", str(pbp_csv),
    ])

    stage_cmd = [
        py, "-m", "euroleague_possessions.batch_pipeline",
        str(pbp_csv), str(boxscores),
        "--checkpoint-dir", str(checkpoints),
        "--competition", args.competition,
        "--season", str(args.season),
        "--stage-workers", str(args.stage_workers),
    ]
    if args.execute:
        stage_cmd += ["--execute", "--confirm-multiple-games",
                      "--env-file", str(REPO.parent / "etl" / ".Renviron")]
        run("stage + publish", stage_cmd)
    else:
        run("stage (no publish)", stage_cmd)
        print(f"\nDRY RUN: {len(codes)} games staged in {checkpoints}, nothing written to "
              f"the database.\nRe-run with --execute to publish.")
        raise SystemExit(0)

    failures = verify(args.competition, args.season, codes)
    print(f"\n{'ALL CHECKS PASSED' if not failures else f'{failures} CHECK(S) FAILED'}")
    raise SystemExit(1 if failures else 0)


if __name__ == "__main__":
    main()
