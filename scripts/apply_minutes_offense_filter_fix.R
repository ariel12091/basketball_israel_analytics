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
#   onoff_default_mv (197.16 vs 200) and player_four_factors_by_game (195.30 vs
#   200) undercount for a RELATED BUT DIFFERENT reason: their segment CTEs keep
#   type_lineup IN the GROUP BY, so their offense filter is load-bearing and
#   deleting it WOULD double-count. They need the perspective collapsed in the
#   GROUP BY first, matching player_traditional_stats_mv -- the one relation
#   that gets this right (200.09) because its segment_times CTE groups by
#   (game_year, game_id, team_id, lineup_hash, segment_id) and filters nothing.
#   That is a separate migration and needs its own before/after measurement.
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
  say("  1. rebuild_all_mvs(from_level = 2)   DROP+CREATE, L2 through L5 in order.")
  say("     REFRESH is NOT enough: it re-runs the stored definition.")
  say("  2. scripts/apply_db_security.R with CONFIRM_DB_SECURITY_APPLY=1.")
  say("     DROP+CREATE wipes GRANTs -- the app connects as app_readonly and")
  say("     will 403 on every rebuilt relation until this runs.")
  say("  3. this script re-measures and asserts the MV now matches truth.")
  say("")
  say("AFTER APPLYING, still to do by hand:")
  say("  - CLAUDE.md:387 still tells the next person to add the filter back.")
  say("  - DQ checks T and X, and test-clock-minute-contracts.R, expect the old")
  say("    numbers; game 115 moves from 39.867 toward 40.0.")
  say("  - PROJECT.md:1390's ETL warning threshold (minutes < 39.0) exists")
  say("    because of this undercount and should be raised.")
  quit(save = "no", status = 0)
}

# ---- apply ------------------------------------------------------------------
say("")
say("APPLYING -- rebuilding L2 through L5 in dependency order")
source("sql/rebuild_all_mvs.R")
rebuild_all_mvs(from_level = 2)

say("")
say("re-granting (DROP+CREATE wipes GRANTs)")
Sys.setenv(CONFIRM_DB_SECURITY_APPLY = "1")
source("scripts/apply_db_security.R")

# ---- verify -----------------------------------------------------------------
con2 <- connect_ddl(); on.exit(try(dbDisconnect(con2), silent = TRUE), add = TRUE)
after <- measure(con2)
say("")
say("AFTER    base-table truth : %.3f", after$truth)
say("         mv rebuilt       : %.3f", after$mv)
gap <- abs(after$truth - after$mv)
say("         gap              : %.3f", gap)
if (gap > 0.01) {
  stop(sprintf("VERIFY FAILED: mv still %.3f off truth. Investigate before deploying.", gap))
}
say("")
say("VERIFY PASSED -- lineup minutes now match the canonical segment total.")
