# Migration 2026-09-05 --- remove the redundant offense filter from lineup minutes.
#
# WHAT IT FIXES
#   mv_lineup_totals_by_day and lineup_four_factors_by_game summed window
#   seconds as
#       SUM(wt.window_seconds) FILTER (WHERE wt.has_offense)
#   so a lineup on the floor for a stretch WITHOUT an offensive possession
#   contributed zero minutes. Measured 2026-09-05: 39.421 min/team-game against
#   a base-table truth of 40.006 -- 0.586 lost on average, up to 2.633, on 804
#   of 878 team-games.
#
# WHY REMOVING THE FILTER DOES NOT DOUBLE-COUNT
#   window_bounds already groups by
#       (team_id, lineup_hash, game_id, segment_id, opp_starters, opp_island)
#   with type_lineup absent, so the offense and defense rows are collapsed
#   before has_offense is consulted. Verified: zero window keys carry more than
#   one row, zero segments have windows summing past their own duration, and
#   summing every window gives 40.006 -- not ~80, which is what an actual
#   perspective double-count would produce. The single-count guard that IS
#   load-bearing lives downstream, in
#       CASE WHEN ds.type_lineup = 'offense' THEN wm.minutes END.
#   This is also what the file header has always specified: "computed across
#   ALL rows (no type_lineup filter) to capture full floor time, then attached
#   to offense rows only", and what PROJECT.md:180 and :518 describe.
#
# NOT IN SCOPE
#   onoff_default_mv is FIXED in this migration too. Its player_segments CTE
#   kept type_lineup in the GROUP BY, so its offense filter was load-bearing and
#   could not simply be deleted -- removing it alone would have double-counted.
#   The perspective is now collapsed in the GROUP BY, matching
#   player_traditional_stats_mv.segment_times, and the filter goes with it.
#   Verified read-only against the committed CTE chain before editing:
#   197.15 -> 200.09 player-minutes per team-game, landing exactly on
#   player_traditional_stats_mv, the one relation that always got this right.
#
#   player_four_factors_by_game's PUBLISHED `minutes` is fixed too. Its
#   segment_times CTE was already collapsed, but segment_stats drove the join,
#   so a segment in which the lineup had no offensive possession produced no
#   offense row and its seconds were never reached. Times are now aggregated on
#   their own grain (player_minutes) and attached to the offense row. Verified
#   read-only: 197.10 -> 200.04 player-minutes per team-game, i.e. 5 x the
#   40.006 canonical team total. (An earlier note said 195.30; that was an
#   artefact of the gate deduplicating with DISTINCT on the minutes VALUE,
#   which collapsed distinct slices holding equal numbers. Corrected.)
#
#   Its other column, `onoff_minutes`, is fixed as well. Measured first:
#   197.10 player-minutes per team-game, which is 39.42 x 5 -- the same
#   lineup-level shortfall seen through the player grain. onoff_lineup_segments
#   now collapses the perspective and the single-count guard moves to the join
#   (CASE WHEN lt.type_lineup = 'offense'), the same shape sub_lineups_by_day
#   uses. Verified read-only: team minutes 39.421 -> 40.006, i.e. player
#   minutes 197.10 -> 200.03.
#
#   All four measured minute defects are now addressed. Nothing in the schema
#   is known to undercount minutes after this migration runs.
#
# USAGE
#   Rehearsal (default) -- measures, prints the plan, changes nothing:
#     Rscript scripts/apply_minutes_offense_filter_fix.R
#   Dry run -- the ENTIRE apply inside the transaction, then rollback. Same
#   wall clock and locks as a real run; nothing is kept. Every defect this
#   migration has, it found:
#     Rscript scripts/apply_minutes_offense_filter_fix.R --dry-run
#   Apply:
#     Rscript scripts/apply_minutes_offense_filter_fix.R --apply
#
# Needs WRITE credentials (etl/.Renviron). DDL goes to port 5432, not the
# 6543 transaction pooler.

suppressMessages({library(DBI); library(RPostgres)})

APPLY  <- "--apply"   %in% commandArgs(trailingOnly = TRUE)
# --dry-run performs the ENTIRE apply -- rebuild, grants, audit, all five
# gates -- inside the transaction and then rolls it back. Same wall clock and
# the same locks as a real run, but nothing is kept. It is the only way to
# learn that the apply path works without spending a maintenance window on it.
# Precedent: etl/backfill_canonical_segment_minutes.R.
DRY    <- "--dry-run" %in% commandArgs(trailingOnly = TRUE)
args <- commandArgs(trailingOnly = TRUE)
if (any(!args %in% c("--apply", "--dry-run")) || (APPLY && DRY))
  stop("Use no flag, --dry-run, or --apply (mutually exclusive)")
SCHEMA <- "basketball_test"

say <- function(...) cat(sprintf(...), "\n", sep = "")

connect_ddl <- function() {
  readRenviron("etl/.Renviron")
  dbConnect(RPostgres::Postgres(),
    host = Sys.getenv("PG_HOST"),
    port = 5432L,                       # direct, not the pooler: this is DDL
    dbname = Sys.getenv("PG_DB"),
    user = Sys.getenv("PG_USER"),
    password = Sys.getenv("PG_PASS"),
    sslmode = Sys.getenv("PG_SSLMODE", "require"),
    connect_timeout = 15L, bigint = "numeric")
}

# ---- invariants -------------------------------------------------------------
# Truth is one duration per segment, summed per team-game: a 40 minute game.
TRUTH_SQL <- sprintf("
  with seg as (select game_id, team_id, segment_id, max(segment_seconds) s
               from %s.df_pts_poss_lineups_longer_mv
               where segment_id is not null group by 1,2,3),
       t as (select game_id, team_id, sum(s)/60.0 m from seg group by 1,2)
  select round(avg(m)::numeric,3) v from t", SCHEMA)

MV_SQL <- sprintf("
  with t as (select game_id, team_id, sum(minutes) m
             from %s.mv_lineup_totals_by_day
             where type_lineup='offense' group by 1,2)
  select round(avg(m)::numeric,3) v from t", SCHEMA)

measure <- function(con) {
  list(truth = dbGetQuery(con, TRUTH_SQL)$v[[1]],
       mv    = dbGetQuery(con, MV_SQL)$v[[1]])
}

source("scripts/minutes_migration_helpers.R")

main <- function() {
# ---- preflight --------------------------------------------------------------
# Each edited definition, with the pattern that must be gone from it. A stale
# file here would rebuild the old behaviour and still report success.
edited <- list(
  list(f = "sql/materialized_views/sub_lineups_by_day.sql",          gone = "has_offense"),
  list(f = "sql/materialized_views/lineup_four_factors_by_game.sql", gone = "has_offense"),
  list(f = "sql/materialized_views/onoff_mv.sql",                    gone = "player_segments.type_lineup = 'offense'"),
  list(f = "sql/materialized_views/player_four_factors_by_game.sql", gone = "SUM(st.stint_seconds) FILTER")
)
for (e in edited) {
  src <- paste(readLines(e$f, warn = FALSE), collapse = "
")
  if (grepl(e$gone, src, fixed = TRUE))
    stop("Preflight failed: ", e$f, " still contains the old expression. ",
         "Apply the source edit before running this migration.")
}
say("preflight: all %d edited definitions are free of the offense filter", length(edited))

con <- connect_ddl()
on.exit(try(dbDisconnect(con), silent = TRUE), add = TRUE)

before <- measure(con)
say("")
say("BEFORE   canonical truth        : %7.3f min/team-game", before$truth)
say("         mv_lineup_totals_by_day: %7.3f  (short %.3f)",
    before$mv, before$truth - before$mv)

# The remaining paths are player-grain: five on court, so 5x the floor time.
target <- before$truth * 5
others <- list(
  list(lbl = "onoff_default_mv        ", sql = "
    with g as (select team_id, game_year, count(distinct game_id) games
               from basketball_test.mv_lineup_totals_by_day group by 1,2),
         p as (select team_id, \"Year\"::int yr, sum(minutes) m
               from basketball_test.onoff_default_mv group by 1,2)
    select avg(p.m/g.games) v from p join g on g.team_id=p.team_id and g.game_year=p.yr"),
  list(lbl = "pff minutes             ", sql = "
    select avg(m) v from (select game_id, team_id, sum(minutes) m
      from basketball_test.player_four_factors_by_game
      where is_on_key = 1 and type_lineup = 'offense' and minutes is not null
      group by 1,2) t"),
  list(lbl = "pff onoff_minutes       ", sql = "
    select avg(m) v from (select game_id, team_id, sum(onoff_minutes) m
      from basketball_test.player_four_factors_by_game
      where is_on_key = 1 group by 1,2) t"),
  list(lbl = "player_traditional_stats", sql = "
    with g as (select team_id, game_year, count(distinct game_id) games
               from basketball_test.mv_lineup_totals_by_day group by 1,2),
         p as (select team_id, game_year, sum(minutes) m
               from basketball_test.player_traditional_stats_mv group by 1,2)
    select avg(p.m/g.games) v from p join g using (team_id, game_year)")
)
say("")
say("         player-minutes per team-game, target %.2f (5 x floor time):", target)
for (o in others) {
  v <- tryCatch(dbGetQuery(con, o$sql)$v[[1]], error = function(e) NA_real_)
  say("         %s: %7.2f  (short %.2f)", o$lbl, v, target - v)
}

if (!APPLY && !DRY) {
  say("")
  say("REHEARSAL ONLY -- nothing changed. --dry-run rehearses the whole")
  say("apply and rolls back; --apply executes it. Both will:")
  say("  1. Rebuild 7 relations atomically, in registry order.")
  say("     REFRESH is NOT enough: it re-runs the stored definition.")
  say("  2. Restore grants and audit access on the same connection.")
  say("  3. Five verification gates: both lineup MVs, the team dependents,")
  say("     published player minutes, onoff_minutes, and a cross-check that")
  say("     onoff_default_mv agrees with player_traditional_stats_mv, which is")
  say("     NOT rebuilt here and so stays an independent reference.")
  say("     Commit only if all pass; otherwise roll back everything.")
  say("")
  say("AFTER APPLYING, still to do by hand:")
  say("  - DQ checks T and X, and test-clock-minute-contracts.R, expect the old")
  say("    numbers; game 115 moves from 39.867 toward 40.0.")
  say("  - PROJECT.md:1390's ETL warning threshold (minutes < 39.0) exists")
  say("    because of this undercount and should be raised.")
  return(invisible(NULL))
}

# ---- apply ------------------------------------------------------------------
say("")
say(if (DRY) "DRY RUN -- full apply, then rollback" else "APPLYING -- rebuilding 7 relations")
registry_env <- new.env(parent = globalenv())
sys.source("sql/rebuild_all_mvs.R", envir = registry_env)
registry_env$validate_mv_registry()
affected <- c("mv_lineup_totals_by_day", "onoff_default_mv",
              "player_four_factors_by_game", "lineup_four_factors_by_game",
              "team_metrics_by_game_mv", "team_metrics_rolling_mv",
              "team_four_factors_mv")
targets <- Filter(function(x) x$name %in% affected, registry_env$MV_REGISTRY)
# Read every input before taking locks or dropping anything.
definitions <- lapply(targets, function(x) paste(readLines(x$file, warn = FALSE), collapse = "\n"))
refresh_paths <- file.path("sql", "functions", c(
  "refresh_player_four_factors_by_game_for_games.sql",
  "refresh_onoff_default_for_games.sql",
  "refresh_team_metrics_by_game_for_games.sql",
  "refresh_sub_lineups.sql", "refresh_sub_lineups_incremental.sql"))
refresh_definitions <- lapply(refresh_paths, function(f)
  paste(readLines(f, warn = FALSE), collapse = "\n"))
hardening <- paste(readLines("sql/security/enable_readonly_rls.sql", warn = FALSE), collapse = "\n")
audit <- paste(readLines("sql/security/audit_app_access.sql", warn = FALSE), collapse = "\n")
outcome <- tryCatch({
DBI::dbWithTransaction(con, {
  dbExecute(con, "SET LOCAL search_path TO basketball_test, public")
  dbExecute(con, "SET LOCAL lock_timeout = '15s'")
  dbExecute(con, "SET LOCAL statement_timeout = '30min'")
  # Freeze the truth used throughout verification, including concurrent ETL.
  dbExecute(con, "LOCK TABLE basketball_test.df_pts_poss_lineups_longer_mv IN SHARE MODE")
  rebuild_minutes_relations(con, targets, definitions)
  for (i in seq_along(refresh_definitions)) {
    say("Installing %s", refresh_paths[[i]])
    dbExecute(con, refresh_definitions[[i]], immediate = TRUE)
  }
  say("Refreshing sub-lineup season totals")
  dbExecute(con, "SELECT basketball_test.refresh_sub_lineups_stats()")
  dbExecute(con, hardening, immediate = TRUE)  # 76 statements
  violations <- dbGetQuery(con, audit)
  if (nrow(violations)) {
    print(violations)
    stop("Database access audit failed")
  }
  verify_minutes_migration(con)
  verify_minutes_refresh_parity(con)
  after <- measure(con)
  say("")
  say("AFTER    canonical truth        : %7.3f min/team-game", after$truth)
  say("         mv_lineup_totals_by_day: %7.3f  (gap %.3f)",
      after$mv, abs(after$truth - after$mv))
  if (DRY) stop(structure(class = c("ibpl_dry_rollback", "error", "condition"),
                          list(message = "dry-run rollback", call = NULL)))
  "committed"
})
}, ibpl_dry_rollback = function(e) "rolled_back")
if (identical(outcome, "rolled_back")) {
  say("")
  say("DRY RUN COMPLETE -- every step ran, all five gates passed, and the")
  say("transaction was rolled back. Nothing changed. Re-run with --apply.")
} else {
  say("")
  say("VERIFY PASSED -- rebuild, grants and all five gates committed together.")
}
}

main()
