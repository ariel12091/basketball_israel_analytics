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
#   player_four_factors_by_game (195.30 vs 200) is NOT fixed here. It has TWO
#   independent minute paths -- the published `minutes` at line 273
#   (SUM(stint_seconds) FILTER (type_lineup = 'offense')) and `onoff_minutes`
#   fed from onoff_lineup_minutes at 104/129 -- and its downstream join keys on
#   type_lineup, so collapsing the grain means restructuring the join rather
#   than deleting a clause. It needs its own design and its own before/after
#   measurement.
#
# USAGE
#   Rehearsal (default) -- measures, prints the plan, changes nothing:
#     Rscript scripts/apply_minutes_offense_filter_fix.R
#   Apply:
#     Rscript scripts/apply_minutes_offense_filter_fix.R --apply
#
# Needs WRITE credentials (etl/.Renviron). DDL goes to port 5432, not the
# 6543 transaction pooler.

suppressMessages({library(DBI); library(RPostgres)})

APPLY  <- "--apply" %in% commandArgs(trailingOnly = TRUE)
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
edited <- c("sql/materialized_views/sub_lineups_by_day.sql",
            "sql/materialized_views/lineup_four_factors_by_game.sql")
for (f in edited) {
  src <- paste(readLines(f, warn = FALSE), collapse = "\n")
  if (grepl("has_offense", src, fixed = TRUE))
    stop("Preflight failed: ", f, " still references has_offense. ",
         "Apply the source edit before running this migration.")
}
say("preflight: both view definitions are free of has_offense")

con <- connect_ddl()
on.exit(try(dbDisconnect(con), silent = TRUE), add = TRUE)

before <- measure(con)
say("")
say("BEFORE   base-table truth : %.3f min/team-game", before$truth)
say("         mv as deployed   : %.3f min/team-game", before$mv)
say("         shortfall        : %.3f", before$truth - before$mv)

if (!APPLY) {
  say("")
  say("REHEARSAL ONLY -- nothing changed. Re-run with --apply to execute:")
  say("  1. Rebuild both lineup MVs and their three team dependents atomically.")
  say("     REFRESH is NOT enough: it re-runs the stored definition.")
  say("  2. Restore grants and audit access on the same connection.")
  say("  3. Verify both MVs per team-game, downstream minutes, and indexes.")
  say("     Commit only if all checks pass; otherwise roll back everything.")
  say("")
  say("AFTER APPLYING, still to do by hand:")
  say("  - CLAUDE.md:387 still tells the next person to add the filter back.")
  say("  - DQ checks T and X, and test-clock-minute-contracts.R, expect the old")
  say("    numbers; game 115 moves from 39.867 toward 40.0.")
  say("  - PROJECT.md:1390's ETL warning threshold (minutes < 39.0) exists")
  say("    because of this undercount and should be raised.")
  return(invisible(NULL))
}

# ---- apply ------------------------------------------------------------------
say("")
say("APPLYING -- rebuilding lineup MVs and their team dependents")
registry_env <- new.env(parent = globalenv())
sys.source("sql/rebuild_all_mvs.R", envir = registry_env)
registry_env$validate_mv_registry()
affected <- c("mv_lineup_totals_by_day", "lineup_four_factors_by_game",
              "team_metrics_by_game_mv", "team_metrics_rolling_mv", "team_four_factors_mv")
targets <- Filter(function(x) x$name %in% affected, registry_env$MV_REGISTRY)
# Read every input before taking locks or dropping anything.
definitions <- lapply(targets, function(x) paste(readLines(x$file, warn = FALSE), collapse = "\n"))
hardening <- paste(readLines("sql/security/enable_readonly_rls.sql", warn = FALSE), collapse = "\n")
audit <- paste(readLines("sql/security/audit_app_access.sql", warn = FALSE), collapse = "\n")
DBI::dbWithTransaction(con, {
  dbExecute(con, "SET LOCAL search_path TO basketball_test, public")
  dbExecute(con, "SET LOCAL lock_timeout = '15s'")
  dbExecute(con, "SET LOCAL statement_timeout = '30min'")
  # Freeze the truth used throughout verification, including concurrent ETL.
  dbExecute(con, "LOCK TABLE basketball_test.df_pts_poss_lineups_longer_mv IN SHARE MODE")
  rebuild_minutes_relations(con, targets, definitions)
  dbExecute(con, hardening)
  violations <- dbGetQuery(con, audit)
  if (nrow(violations)) {
    print(violations)
    stop("Database access audit failed")
  }
  verify_minutes_migration(con)
})
say("VERIFY PASSED -- rebuild, grants and minute checks committed together.")
}

main()
