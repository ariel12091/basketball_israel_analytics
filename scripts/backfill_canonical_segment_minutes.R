# Backfill canonical lineup-boundary timing and rebuild minutes-dependent data.
# Usage:
#   Rscript scripts/backfill_canonical_segment_minutes.R          # validate + rollback
#   Rscript scripts/backfill_canonical_segment_minutes.R --apply  # resumable online apply
#
# The rollback rehearsal retains its ALTER TABLE lock until rollback. It is a
# validation tool for a maintenance window, not the production apply path.

readRenviron("etl/.Renviron")

suppressPackageStartupMessages({
  library(DBI)
  library(RPostgres)
})

apply_changes <- "--apply" %in% commandArgs(trailingOnly = TRUE)
schema <- "basketball_test"

if (apply_changes) {
  # The rehearsal intentionally remains one rollback-only transaction. The
  # production apply must not retain the ALTER TABLE lock across downstream
  # rebuilds, so delegate to the phased, resumable online path before opening
  # this script's transaction.
  sys.source(
    "scripts/apply_canonical_segment_minutes_online.R",
    envir = new.env(parent = globalenv())
  )
  quit(save = "no", status = 0L)
}

read_sql <- function(path) {
  paste(readLines(path, warn = FALSE), collapse = "\n")
}

execute_file <- function(con, path) {
  message("  ", path)
  DBI::dbExecute(con, read_sql(path), immediate = TRUE)
}

con <- dbConnect(
  Postgres(),
  host = Sys.getenv("PG_HOST"),
  port = 5432L,
  dbname = Sys.getenv("PG_DB"),
  user = Sys.getenv("PG_USER"),
  password = Sys.getenv("PG_PASS"),
  sslmode = "require"
)
on.exit(dbDisconnect(con), add = TRUE)

dbExecute(con, "BEGIN")
committed <- FALSE
on.exit({
  if (!committed) try(dbExecute(con, "ROLLBACK"), silent = TRUE)
}, add = TRUE)

dbExecute(con, sprintf("SET LOCAL search_path TO %s, public", schema))
dbExecute(con, "SET LOCAL statement_timeout TO '15min'")
dbExecute(con, "SET LOCAL lock_timeout TO '30s'")

message("Validating canonical-minute backfill (rollback mode)")

message("Adding canonical timing columns")
execute_file(con, "sql/migrations/2026-07-21_canonical_segment_clock_minutes.sql")
dbExecute(con, "
  CREATE INDEX IF NOT EXISTS idx_df_longer_game_team_segment
    ON basketball_test.df_pts_poss_lineups_longer_mv (game_id, team_id, segment_id)
")

message("Deploying canonical timing refresh")
execute_file(con, "sql/functions/refresh_segment_clock_fields_for_games.sql")
# Refresh in game batches with explicit game-id arrays. A single NULL
# (whole-table) call makes the planner drop the game_id filter (the
# `game_ids IS NULL OR ...` predicate) and full-scan the table. Explicit
# arrays keep the scans indexed; the function's own `SET enable_nestloop = off`
# keeps the final UPDATE join on a stable hash/merge plan (it otherwise flips
# to a catastrophic nested loop at scale or once a prior batch has bloated the
# table mid-transaction). as.numeric() coerces the bigint (integer64) count so
# sum()/format() report the true value rather than the raw 64-bit pattern.
game_ids <- dbGetQuery(
  con,
  "SELECT DISTINCT game_id
     FROM basketball_test.df_pts_poss_lineups_longer_mv
    WHERE game_id IS NOT NULL
    ORDER BY game_id"
)$game_id
batches <- split(game_ids, ceiling(seq_along(game_ids) / 100))
updated_rows <- sum(vapply(batches, function(ids) {
  as.numeric(dbGetQuery(
    con,
    sprintf(
      "SELECT basketball_test.refresh_segment_clock_fields_for_games(ARRAY[%s]::int4[]) AS n",
      paste(ids, collapse = ",")
    )
  )$n[[1]])
}, numeric(1)))
message("  canonical rows updated: ", format(updated_rows, big.mark = ","),
        " across ", length(batches), " game batch(es)")

timing_quality <- dbGetQuery(con, "
WITH segments AS (
  SELECT
    game_id,
    team_id,
    lineup_hash,
    segment_id,
    count(DISTINCT segment_seconds)::int AS duration_values,
    min(segment_seconds)::numeric AS segment_seconds
  FROM basketball_test.df_pts_poss_lineups_longer_mv
  WHERE lineup_hash IS NOT NULL AND segment_id IS NOT NULL
  GROUP BY game_id, team_id, lineup_hash, segment_id
),
teams AS (
  SELECT game_id, team_id, sum(segment_seconds)::numeric AS segment_total
  FROM segments
  GROUP BY game_id, team_id
),
ends AS (
  SELECT game_id, team_id, max(event_elapsed_seconds)::numeric AS game_end
  FROM basketball_test.df_pts_poss_lineups_longer_mv
  GROUP BY game_id, team_id
)
SELECT
  (SELECT count(*) FROM basketball_test.df_pts_poss_lineups_longer_mv
    WHERE segment_id IS NOT NULL
      AND (event_elapsed_seconds IS NULL
        OR clock_regression_seconds IS NULL
        OR segment_start_elapsed_seconds IS NULL
        OR segment_end_elapsed_seconds IS NULL
        OR segment_seconds IS NULL))::bigint AS missing_rows,
  (SELECT count(*) FROM segments
    WHERE duration_values <> 1 OR segment_seconds IS NULL OR segment_seconds < 0)::bigint AS invalid_segments,
  (SELECT count(*) FROM teams t JOIN ends e USING (game_id, team_id)
    WHERE abs(t.segment_total - e.game_end) > 5)::bigint AS conservation_failures
")

if (any(unlist(timing_quality[1, ]) > 0)) {
  stop("Canonical timing validation failed: ", paste(names(timing_quality), unlist(timing_quality[1, ]), collapse = ", "))
}

message("Deploying minutes-dependent refresh/query functions")
function_files <- c(
  "sql/functions/refresh_df_pts_poss_lineups_longer_for_games.sql",
  "sql/functions/refresh_player_four_factors_by_game_for_games.sql",
  "sql/functions/refresh_onoff_default_for_games.sql",
  "sql/functions/refresh_sub_lineups.sql",
  "sql/functions/refresh_sub_lineups_incremental.sql",
  "sql/functions/fetch_lineups_all.sql",
  "sql/functions/fetch_lineups_four_factors.sql",
  "sql/functions/get_player_traditional_dynamic.sql"
)
invisible(lapply(function_files, function(path) execute_file(con, path)))

message("Rebuilding minutes-dependent materialized views")
dbExecute(con, "DROP MATERIALIZED VIEW basketball_test.team_four_factors_mv")
dbExecute(con, "DROP MATERIALIZED VIEW basketball_test.lineup_four_factors_by_game")
dbExecute(con, "DROP MATERIALIZED VIEW basketball_test.mv_lineup_totals_by_day")
dbExecute(con, "DROP MATERIALIZED VIEW basketball_test.player_traditional_stats_mv")

execute_file(con, "sql/materialized_views/sub_lineups_by_day.sql")
execute_file(con, "sql/materialized_views/lineup_four_factors_by_game.sql")
execute_file(con, "sql/materialized_views/team_four_factors_mv.sql")
execute_file(con, "sql/materialized_views/player_traditional_stats_mv.sql")

message("Refreshing incrementally maintained minutes tables")
# as.numeric() coerces the bigint (integer64) counts so the summary message
# prints the true values rather than the raw 64-bit pattern.
refresh_counts <- list(
  player_four_factors = as.numeric(dbGetQuery(
    con,
    "SELECT basketball_test.refresh_player_four_factors_by_game_for_games(NULL) AS n"
  )$n[[1]]),
  team_metrics = as.numeric(dbGetQuery(
    con,
    "SELECT basketball_test.refresh_team_metrics_by_game_for_games(NULL) AS n"
  )$n[[1]]),
  onoff_default = as.numeric(dbGetQuery(
    con,
    "SELECT basketball_test.refresh_onoff_default_for_games(NULL) AS n"
  )$n[[1]])
)
invisible(dbGetQuery(con, "SELECT basketball_test.refresh_sub_lineups_stats()"))
dbExecute(con, "REFRESH MATERIALIZED VIEW basketball_test.team_metrics_rolling_mv")
message("  ", paste(names(refresh_counts), unlist(refresh_counts), sep = "=", collapse = ", "))

game_115 <- dbGetQuery(con, "
WITH segments AS (
  SELECT
    game_id,
    team_id,
    lineup_hash,
    segment_id,
    max(segment_seconds)::numeric AS segment_seconds
  FROM basketball_test.df_pts_poss_lineups_longer_mv
  WHERE game_id = 115
  GROUP BY game_id, team_id, lineup_hash, segment_id
)
SELECT game_id, team_id, round(sum(segment_seconds) / 60.0, 3) AS minutes
FROM segments
GROUP BY game_id, team_id
ORDER BY team_id
")
if (nrow(game_115) != 2L || any(abs(game_115$minutes - 39.867) > 0.002)) {
  stop("Game 115 canonical-minute regression failed")
}
print(game_115, row.names = FALSE)

if (apply_changes) {
  dbExecute(con, "COMMIT")
  committed <- TRUE
  message("Canonical-minute backfill committed")
} else {
  dbExecute(con, "ROLLBACK")
  committed <- TRUE
  message("Validation passed; transaction rolled back (use --apply to commit)")
}
