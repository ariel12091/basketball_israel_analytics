# Resumable, low-lock production apply for canonical lineup timing/minutes.
#
# This script deliberately does not use one migration-wide transaction:
#   1. commit the metadata-only columns immediately;
#   2. build the supporting index concurrently;
#   3. backfill canonical timing and the large player table in game batches;
#   4. build replacement materialized views under temporary names;
#   5. cut them over with a short, retryable rename transaction;
#   6. refresh each remaining consumer in its own transaction.
#
# Usage (normally delegated by backfill_canonical_segment_minutes.R):
#   Rscript scripts/apply_canonical_segment_minutes_online.R --apply

readRenviron("etl/.Renviron")

suppressPackageStartupMessages({
  library(DBI)
  library(RPostgres)
})

apply_canonical_segment_minutes_online <- function() {

if (!"--apply" %in% commandArgs(trailingOnly = TRUE)) {
  stop("Refusing to change the database without --apply")
}

schema <- "basketball_test"
base_table <- "basketball_test.df_pts_poss_lineups_longer_mv"
advisory_key <- "basketball_test.canonical_segment_minutes.2026_07_21"

read_sql <- function(path) {
  paste(readLines(path, warn = FALSE), collapse = "\n")
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

dbExecute(con, sprintf("SET search_path TO %s, public", schema))
dbExecute(con, "SET application_name = 'canonical_segment_minutes_online'")

lock_acquired <- isTRUE(dbGetQuery(
  con,
  sprintf("SELECT pg_try_advisory_lock(hashtext(%s)) AS acquired", dbQuoteString(con, advisory_key))
)$acquired[[1]])
if (!lock_acquired) {
  stop("Another canonical-minute apply session holds the advisory lock")
}
on.exit(try(dbGetQuery(
  con,
  sprintf("SELECT pg_advisory_unlock(hashtext(%s))", dbQuoteString(con, advisory_key))
), silent = TRUE), add = TRUE)

set_session_limits <- function(statement_timeout = "15min", lock_timeout = "5s") {
  dbExecute(con, sprintf("SET statement_timeout = %s", dbQuoteString(con, statement_timeout)))
  dbExecute(con, sprintf("SET lock_timeout = %s", dbQuoteString(con, lock_timeout)))
}

run_transaction <- function(label, code, statement_timeout = "15min", lock_timeout = "5s") {
  message("[TX] ", label)
  dbExecute(con, "BEGIN")
  committed <- FALSE
  on.exit({
    if (!committed) try(dbExecute(con, "ROLLBACK"), silent = TRUE)
  }, add = TRUE)
  dbExecute(con, sprintf("SET LOCAL search_path TO %s, public", schema))
  dbExecute(con, sprintf("SET LOCAL statement_timeout = %s", dbQuoteString(con, statement_timeout)))
  dbExecute(con, sprintf("SET LOCAL lock_timeout = %s", dbQuoteString(con, lock_timeout)))
  value <- force(code)
  dbExecute(con, "COMMIT")
  committed <- TRUE
  value
}

execute_file <- function(path) {
  message("  ", path)
  dbExecute(con, read_sql(path), immediate = TRUE)
}

execute_sql <- function(label, sql) {
  message("  ", label)
  dbExecute(con, sql, immediate = TRUE)
}

sql_int_array <- function(ids) {
  paste(as.integer(ids), collapse = ",")
}

split_batches <- function(ids, batch_size) {
  if (!length(ids)) return(list())
  split(ids, ceiling(seq_along(ids) / batch_size))
}

call_game_batches <- function(function_name, ids, batch_size, label) {
  batches <- split_batches(ids, batch_size)
  total <- 0
  if (!length(batches)) {
    message("  ", label, ": no games to refresh")
    return(total)
  }
  for (i in seq_along(batches)) {
    batch_ids <- batches[[i]]
    result <- dbGetQuery(
      con,
      sprintf(
        "SELECT basketball_test.%s(ARRAY[%s]::int4[]) AS n",
        function_name,
        sql_int_array(batch_ids)
      )
    )$n[[1]]
    touched <- as.numeric(result)
    total <- total + touched
    message(
      "  ", label, " batch ", i, "/", length(batches),
      ": games=", length(batch_ids), ", rows=", format(touched, big.mark = ",")
    )
  }
  total
}

validate_base_timing <- function() {
  quality <- dbGetQuery(con, sprintf("
    WITH segments AS (
      SELECT
        game_id,
        team_id,
        lineup_hash,
        segment_id,
        count(DISTINCT segment_seconds)::int AS duration_values,
        min(segment_seconds)::numeric AS segment_seconds
      FROM %s
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
      FROM %s
      GROUP BY game_id, team_id
    )
    SELECT
      (SELECT count(*) FROM %s
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
  ", base_table, base_table, base_table))

  values <- vapply(quality[1, ], as.numeric, numeric(1))
  if (any(values > 0)) {
    stop(
      "Canonical timing validation failed: ",
      paste(names(values), values, collapse = ", ")
    )
  }
  message("  canonical timing: missing=0, invalid=0, conservation failures=0")

  game_115 <- dbGetQuery(con, sprintf("
    WITH segments AS (
      SELECT
        game_id,
        team_id,
        lineup_hash,
        segment_id,
        max(segment_seconds)::numeric AS segment_seconds
      FROM %s
      WHERE game_id = 115
      GROUP BY game_id, team_id, lineup_hash, segment_id
    )
    SELECT game_id, team_id, round(sum(segment_seconds) / 60.0, 3) AS minutes
    FROM segments
    GROUP BY game_id, team_id
    ORDER BY team_id
  ", base_table))
  if (nrow(game_115) != 2L || any(abs(as.numeric(game_115$minutes) - 39.867) > 0.002)) {
    stop("Game 115 canonical-minute regression failed")
  }
  print(game_115, row.names = FALSE)
  invisible(quality)
}

message("Starting resumable online canonical-minute apply")

# Phase 1: the ALTER is metadata-only. Commit it before any table scan so its
# ACCESS EXCLUSIVE lock lasts seconds, not the duration of the migration.
run_transaction(
  "add canonical columns",
  execute_file("sql/migrations/2026-07-21_canonical_segment_clock_minutes.sql"),
  statement_timeout = "30s",
  lock_timeout = "3s"
)

# Phase 2: construct the supporting index without blocking app reads/writes.
set_session_limits(statement_timeout = "15min", lock_timeout = "5s")
index_state <- dbGetQuery(con, "
  SELECT i.indisvalid
  FROM pg_class c
  JOIN pg_namespace n ON n.oid = c.relnamespace
  JOIN pg_index i ON i.indexrelid = c.oid
  WHERE n.nspname = 'basketball_test'
    AND c.relname = 'idx_df_longer_game_team_segment'
")
if (nrow(index_state) && !isTRUE(index_state$indisvalid[[1]])) {
  execute_sql(
    "drop invalid concurrent index",
    "DROP INDEX CONCURRENTLY IF EXISTS basketball_test.idx_df_longer_game_team_segment"
  )
  index_state <- index_state[0, , drop = FALSE]
}
if (!nrow(index_state)) {
  execute_sql(
    "create supporting index concurrently",
    "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_df_longer_game_team_segment
       ON basketball_test.df_pts_poss_lineups_longer_mv (game_id, team_id, segment_id)"
  )
} else {
  message("  supporting index already valid")
}

# Phase 3: helper deployment and base timing backfill. Each game batch commits,
# so a disconnected client cannot retain the schema lock or lose all progress.
set_session_limits(statement_timeout = "5min", lock_timeout = "5s")
execute_file("sql/functions/refresh_segment_clock_fields_for_games.sql")
all_game_ids <- dbGetQuery(con, sprintf("
  SELECT DISTINCT game_id
  FROM %s
  WHERE game_id IS NOT NULL
  ORDER BY game_id
", base_table))$game_id
canonical_rows <- call_game_batches(
  "refresh_segment_clock_fields_for_games",
  all_game_ids,
  batch_size = 100L,
  label = "canonical timing"
)
message("  canonical rows updated: ", format(canonical_rows, big.mark = ","))

# Install the incremental maintainer as soon as the initial backfill is ready,
# then close any race where a normal ETL inserted rows during phases 1-3.
execute_file("sql/functions/refresh_df_pts_poss_lineups_longer_for_games.sql")
missing_game_ids <- dbGetQuery(con, sprintf("
  SELECT DISTINCT game_id
  FROM %s
  WHERE segment_id IS NOT NULL
    AND segment_seconds IS NULL
  ORDER BY game_id
", base_table))$game_id
if (length(missing_game_ids)) {
  call_game_batches(
    "refresh_segment_clock_fields_for_games",
    missing_game_ids,
    batch_size = 100L,
    label = "canonical timing race closure"
  )
}
validate_base_timing()

# Phase 4: build changed MVs beside the live copies. Their only inter-MV
# dependency is team_four_factors -> lineup_four_factors; the replacement SQL
# points that dependency at the staged lineup MV before the atomic rename.
mv_specs <- list(
  list(
    name = "mv_lineup_totals_by_day",
    file = "sql/materialized_views/sub_lineups_by_day.sql"
  ),
  list(
    name = "lineup_four_factors_by_game",
    file = "sql/materialized_views/lineup_four_factors_by_game.sql"
  ),
  list(
    name = "team_four_factors_mv",
    file = "sql/materialized_views/team_four_factors_mv.sql"
  ),
  list(
    name = "player_traditional_stats_mv",
    file = "sql/materialized_views/player_traditional_stats_mv.sql"
  )
)

relation_replacements <- c(
  "basketball_test.mv_lineup_totals_by_day" = "basketball_test.mv_lineup_totals_by_day__clock_next",
  "basketball_test.lineup_four_factors_by_game" = "basketball_test.lineup_four_factors_by_game__clock_next",
  "basketball_test.team_four_factors_mv" = "basketball_test.team_four_factors_mv__clock_next",
  "basketball_test.player_traditional_stats_mv" = "basketball_test.player_traditional_stats_mv__clock_next"
)
index_replacements <- c(
  "idx_mv_ltotals_day_date" = "idx_mv_ltotals_day_date__clock_next",
  "idx_mv_ltotals_day_pk" = "idx_mv_ltotals_day_pk__clock_next",
  "idx_lff_game_id" = "idx_lff_game_id__clock_next",
  "idx_lff_lineup_hash" = "idx_lff_lineup_hash__clock_next",
  "idx_lff_pk" = "idx_lff_pk__clock_next",
  "idx_tffmv_gy" = "idx_tffmv_gy__clock_next",
  "idx_tffmv_pk" = "idx_tffmv_pk__clock_next",
  "player_traditional_stats_mv_uq" = "player_traditional_stats_mv_uq__clock_next",
  "player_traditional_stats_mv_year_team_name_idx" = "player_traditional_stats_mv_year_team_name_idx__clock_next"
)

transform_mv_sql <- function(path) {
  sql <- read_sql(path)
  replacements <- c(relation_replacements, index_replacements)
  for (from in names(replacements)) {
    sql <- gsub(from, replacements[[from]], sql, fixed = TRUE)
  }
  sql
}

canonical_view_state <- function() {
  dbGetQuery(con, "
    SELECT
      c.relname,
      position('segment_seconds' IN pg_get_viewdef(c.oid, true)) > 0 AS canonical
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = 'basketball_test'
      AND c.relname IN (
        'mv_lineup_totals_by_day',
        'lineup_four_factors_by_game',
        'player_traditional_stats_mv'
      )
    ORDER BY c.relname
  ")
}

view_state <- canonical_view_state()
views_already_canonical <- nrow(view_state) == 3L && all(view_state$canonical)

if (!views_already_canonical) {
  old_leftovers <- dbGetQuery(con, "
    SELECT c.relname
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = 'basketball_test'
      AND right(c.relname, 11) = '__clock_old'
  ")$relname
  if (length(old_leftovers)) {
    stop("Found prior __clock_old relations before cutover: ", paste(old_leftovers, collapse = ", "))
  }

  set_session_limits(statement_timeout = "15min", lock_timeout = "5s")
  execute_sql("drop stale staged team MV", "DROP MATERIALIZED VIEW IF EXISTS basketball_test.team_four_factors_mv__clock_next")
  for (spec in rev(mv_specs[c(1, 2, 4)])) {
    execute_sql(
      paste("drop stale staged MV", spec$name),
      sprintf("DROP MATERIALIZED VIEW IF EXISTS basketball_test.%s__clock_next", spec$name)
    )
  }

  for (spec in mv_specs) {
    execute_sql(paste("build staged MV", spec$name), transform_mv_sql(spec$file))
    execute_sql(
      paste("grant staged MV", spec$name),
      sprintf(
        "GRANT SELECT ON basketball_test.%s__clock_next TO service_role, app_readonly",
        spec$name
      )
    )
    execute_sql(
      paste("analyze staged MV", spec$name),
      sprintf("ANALYZE basketball_test.%s__clock_next", spec$name)
    )
  }

  for (spec in mv_specs) {
    counts <- dbGetQuery(con, sprintf("
      SELECT
        (SELECT count(*) FROM basketball_test.%s)::bigint AS old_rows,
        (SELECT count(*) FROM basketball_test.%s__clock_next)::bigint AS new_rows
    ", spec$name, spec$name))
    if (as.numeric(counts$old_rows[[1]]) != as.numeric(counts$new_rows[[1]])) {
      stop(
        "Staged MV row-count mismatch for ", spec$name, ": ",
        counts$old_rows[[1]], " vs ", counts$new_rows[[1]]
      )
    }
    message("  staged ", spec$name, ": ", format(as.numeric(counts$new_rows[[1]]), big.mark = ","), " rows")
  }

  cutover_once <- function() {
    run_transaction(
      "atomic materialized-view cutover",
      {
        # Rename old relations first. Dependencies follow object identity, so
        # the old team MV continues to reference the old lineup MV and the new
        # team MV continues to reference the staged/new lineup MV.
        for (spec in mv_specs[c(3, 2, 1, 4)]) {
          dbExecute(con, sprintf(
            "ALTER MATERIALIZED VIEW basketball_test.%s RENAME TO %s__clock_old",
            spec$name,
            spec$name
          ))
        }
        for (spec in mv_specs[c(2, 3, 1, 4)]) {
          dbExecute(con, sprintf(
            "ALTER MATERIALIZED VIEW basketball_test.%s__clock_next RENAME TO %s",
            spec$name,
            spec$name
          ))
        }
      },
      statement_timeout = "30s",
      lock_timeout = "2s"
    )
  }

  cutover_ok <- FALSE
  last_error <- NULL
  for (attempt in seq_len(12L)) {
    tryCatch({
      cutover_once()
      cutover_ok <- TRUE
    }, error = function(e) {
      last_error <<- e
      message("  cutover attempt ", attempt, "/12 deferred: ", conditionMessage(e))
    })
    if (cutover_ok) break
    Sys.sleep(5)
  }
  if (!cutover_ok) {
    stop("Could not obtain a short MV cutover window: ", conditionMessage(last_error))
  }
} else {
  message("  materialized-view definitions are already canonical; cutover skipped")
}

view_state <- canonical_view_state()
if (nrow(view_state) != 3L || !all(view_state$canonical)) {
  stop("Canonical materialized-view definitions are not all live after cutover")
}

# Old MVs are no longer reachable by app queries. Drop them dependency-first,
# then restore conventional index names on the live replacements. This cleanup
# is idempotent and does not lock the base event table.
set_session_limits(statement_timeout = "2min", lock_timeout = "5s")
run_transaction(
  "remove old MVs and normalize replacement index names",
  {
    dbExecute(con, "DROP MATERIALIZED VIEW IF EXISTS basketball_test.team_four_factors_mv__clock_old")
    dbExecute(con, "DROP MATERIALIZED VIEW IF EXISTS basketball_test.lineup_four_factors_by_game__clock_old")
    dbExecute(con, "DROP MATERIALIZED VIEW IF EXISTS basketball_test.mv_lineup_totals_by_day__clock_old")
    dbExecute(con, "DROP MATERIALIZED VIEW IF EXISTS basketball_test.player_traditional_stats_mv__clock_old")
    for (from in names(index_replacements)) {
      staged_name <- index_replacements[[from]]
      exists <- dbGetQuery(con, sprintf(
        "SELECT to_regclass('basketball_test.%s') IS NOT NULL AS present",
        staged_name
      ))$present[[1]]
      if (isTRUE(exists)) {
        dbExecute(con, sprintf(
          "ALTER INDEX basketball_test.%s RENAME TO %s",
          staged_name,
          from
        ))
      }
    }
  }
)

# Phase 5: deploy consumers only after both the canonical base fields and the
# replacement MVs are live.
set_session_limits(statement_timeout = "2min", lock_timeout = "5s")
function_files <- c(
  "sql/functions/refresh_player_four_factors_by_game_for_games.sql",
  "sql/functions/refresh_onoff_default_for_games.sql",
  "sql/functions/refresh_sub_lineups.sql",
  "sql/functions/refresh_sub_lineups_incremental.sql",
  "sql/functions/fetch_lineups_all.sql",
  "sql/functions/fetch_lineups_four_factors.sql",
  "sql/functions/get_player_traditional_dynamic.sql"
)
invisible(lapply(function_files, execute_file))

# Phase 6: each persisted consumer commits independently. App SELECTs continue
# to see their previous MVCC snapshot while DELETE/INSERT refreshes run.
set_session_limits(statement_timeout = "15min", lock_timeout = "5s")
player_rows <- call_game_batches(
  "refresh_player_four_factors_by_game_for_games",
  all_game_ids,
  batch_size = 50L,
  label = "player four factors"
)
execute_sql("analyze player four factors", "ANALYZE basketball_test.player_four_factors_by_game")

team_metrics_rows <- as.numeric(dbGetQuery(
  con,
  "SELECT basketball_test.refresh_team_metrics_by_game_for_games(NULL) AS n"
)$n[[1]])
onoff_rows <- as.numeric(dbGetQuery(
  con,
  "SELECT basketball_test.refresh_onoff_default_for_games(NULL) AS n"
)$n[[1]])

# The full sub-lineups refresh can exceed the connection's practical lifetime.
# The incremental function recomputes complete totals for every lineup touched
# by its game IDs. Fifty-game batches keep each statement bounded; their union
# touches every retained lineup in both seasons, and each batch commits alone.
season_games <- dbGetQuery(con, sprintf("
  SELECT DISTINCT s.game_year, d.game_id
  FROM %s d
  JOIN basketball_test.schedule s USING (game_id)
  ORDER BY s.game_year, d.game_id
", base_table))
sub_lineup_rows <- 0
for (year in unique(season_games$game_year)) {
  ids <- season_games$game_id[season_games$game_year == year]
  batches <- split_batches(ids, 50L)
  for (i in seq_along(batches)) {
    batch_ids <- batches[[i]]
    touched <- as.numeric(dbGetQuery(
      con,
      sprintf(
        "SELECT basketball_test.refresh_sub_lineups_stats_for_games(ARRAY[%s]::int4[]) AS n",
        sql_int_array(batch_ids)
      )
    )$n[[1]])
    sub_lineup_rows <- sub_lineup_rows + touched
    message(
      "  sub-lineups season ", year, " batch ", i, "/", length(batches),
      ": games=", length(batch_ids), ", rows=", format(touched, big.mark = ",")
    )
  }
}

# This MV has a valid unique index; concurrent refresh avoids blocking readers.
execute_sql(
  "refresh team metrics rolling concurrently",
  "REFRESH MATERIALIZED VIEW CONCURRENTLY basketball_test.team_metrics_rolling_mv"
)
message(
  "  persisted refresh counts: player_four_factors=", format(player_rows, big.mark = ","),
  ", team_metrics=", format(team_metrics_rows, big.mark = ","),
  ", onoff_default=", format(onoff_rows, big.mark = ","),
  ", sub_lineups=", format(sub_lineup_rows, big.mark = ",")
)

validate_base_timing()

privileges_ok <- dbGetQuery(con, "
  SELECT bool_and(
    has_table_privilege('service_role', format('basketball_test.%I', relname), 'SELECT')
    AND has_table_privilege('app_readonly', format('basketball_test.%I', relname), 'SELECT')
  ) AS ok
  FROM (VALUES
    ('mv_lineup_totals_by_day'),
    ('lineup_four_factors_by_game'),
    ('team_four_factors_mv'),
    ('player_traditional_stats_mv')
  ) AS v(relname)
")$ok[[1]]
if (!isTRUE(privileges_ok)) {
  stop("One or more replacement MVs are missing app SELECT grants")
}

message("Online canonical-minute apply completed successfully")
}

apply_canonical_segment_minutes_online()
