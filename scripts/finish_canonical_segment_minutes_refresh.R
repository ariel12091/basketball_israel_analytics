# Resume only the final consumers after the canonical base/MV/player cutover.
# This is intentionally safe to rerun: season-level sub-lineup rows are
# upserted, and the rolling materialized view is refreshed concurrently.
#
# Usage:
#   Rscript scripts/finish_canonical_segment_minutes_refresh.R --apply

readRenviron("etl/.Renviron")

suppressPackageStartupMessages({
  library(DBI)
  library(RPostgres)
})

finish_canonical_segment_minutes_refresh <- function() {
  if (!"--apply" %in% commandArgs(trailingOnly = TRUE)) {
    stop("Refusing to change the database without --apply")
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

  dbExecute(con, "SET search_path TO basketball_test, public")
  dbExecute(con, "SET application_name = 'canonical_segment_minutes_finish'")
  dbExecute(con, "SET statement_timeout = '10min'")
  dbExecute(con, "SET lock_timeout = '5s'")

  advisory_key <- "basketball_test.canonical_segment_minutes.2026_07_21"
  acquired <- isTRUE(dbGetQuery(
    con,
    sprintf(
      "SELECT pg_try_advisory_lock(hashtext(%s)) AS acquired",
      dbQuoteString(con, advisory_key)
    )
  )$acquired[[1]])
  if (!acquired) stop("Another canonical-minute apply session is active")
  on.exit(try(dbGetQuery(
    con,
    sprintf(
      "SELECT pg_advisory_unlock(hashtext(%s))",
      dbQuoteString(con, advisory_key)
    )
  ), silent = TRUE), add = TRUE)

  preflight <- dbGetQuery(con, "
    SELECT
      (SELECT count(*) FROM information_schema.columns
        WHERE table_schema = 'basketball_test'
          AND table_name = 'df_pts_poss_lineups_longer_mv'
          AND column_name IN (
            'event_elapsed_seconds', 'clock_regression_seconds',
            'segment_start_elapsed_seconds', 'segment_end_elapsed_seconds',
            'segment_seconds'
          ))::int AS canonical_columns,
      (SELECT count(*) FROM basketball_test.df_pts_poss_lineups_longer_mv
        WHERE segment_id IS NOT NULL AND segment_seconds IS NULL)::bigint AS missing_segment_rows,
      (SELECT count(*) FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'basketball_test'
          AND c.relname IN (
            'mv_lineup_totals_by_day', 'lineup_four_factors_by_game',
            'team_four_factors_mv', 'player_traditional_stats_mv'
          ))::int AS live_minute_mvs
  ")
  if (preflight$canonical_columns[[1]] != 5L ||
      as.numeric(preflight$missing_segment_rows[[1]]) != 0 ||
      preflight$live_minute_mvs[[1]] != 4L) {
    stop("Canonical base/MV preflight failed; do not refresh final consumers")
  }

  # Keep the planner guard inside the deployed function so ETL and future
  # callers receive the same stable plan, not only this recovery session.
  incremental_sql <- paste(
    readLines("sql/functions/refresh_sub_lineups_incremental.sql", warn = FALSE),
    collapse = "\n"
  )
  dbExecute(con, incremental_sql, immediate = TRUE)

  season_games <- dbGetQuery(con, "
    SELECT DISTINCT s.game_year, d.game_id
    FROM basketball_test.df_pts_poss_lineups_longer_mv d
    JOIN basketball_test.schedule s USING (game_id)
    ORDER BY s.game_year, d.game_id
  ")
  total_rows <- 0
  for (year in unique(season_games$game_year)) {
    ids <- as.integer(season_games$game_id[season_games$game_year == year])
    batches <- split(ids, ceiling(seq_along(ids) / 50L))
    for (i in seq_along(batches)) {
      batch_ids <- batches[[i]]
      result <- dbGetQuery(
        con,
        sprintf(
          "SELECT basketball_test.refresh_sub_lineups_stats_for_games(ARRAY[%s]::int4[]) AS n",
          paste(batch_ids, collapse = ",")
        )
      )$n[[1]]
      touched <- as.numeric(result)
      total_rows <- total_rows + touched
      message(
        "sub-lineups season ", year, " batch ", i, "/", length(batches),
        ": games=", length(batch_ids), ", rows=", format(touched, big.mark = ",")
      )
    }
  }

  dbExecute(
    con,
    "REFRESH MATERIALIZED VIEW CONCURRENTLY basketball_test.team_metrics_rolling_mv"
  )

  rolling_mismatches <- as.numeric(dbGetQuery(con, "
    SELECT count(*)::bigint AS n
    FROM basketball_test.team_metrics_rolling_mv r
    JOIN basketball_test.team_metrics_by_game_mv t
      USING (game_year, game_id, team_id)
    WHERE r.off_minutes IS DISTINCT FROM t.off_minutes
       OR r.def_minutes IS DISTINCT FROM t.def_minutes
  ")$n[[1]])
  if (rolling_mismatches != 0) {
    stop("Rolling team metrics still contain ", rolling_mismatches, " minute mismatches")
  }

  message(
    "Final canonical-minute consumers refreshed successfully; sub-lineup rows=",
    format(total_rows, big.mark = ","), ", rolling mismatches=0"
  )
}

finish_canonical_segment_minutes_refresh()
