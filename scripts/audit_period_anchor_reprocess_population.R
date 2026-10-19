#!/usr/bin/env Rscript

# Read-only audit for ribbon Workstream A's historical reprocessing population.
# A faulty straddle is a team segment that crosses a regulation boundary while
# that team records a substitution inside the segment's post-boundary overhang.

suppressPackageStartupMessages({
  library(DBI)
  library(RPostgres)
})

show_details <- "--details" %in% commandArgs(trailingOnly = TRUE)

readRenviron("etl/.Renviron")

schema <- Sys.getenv("APP_SCHEMA", "basketball_test")
stopifnot(identical(schema, "basketball_test"))

pg <- dbConnect(
  Postgres(),
  host = Sys.getenv("PG_HOST"),
  port = as.integer(Sys.getenv("PG_PORT", "6543")),
  dbname = Sys.getenv("PG_DB"),
  user = Sys.getenv("PG_USER"),
  password = Sys.getenv("PG_PASS"),
  sslmode = Sys.getenv("PG_SSLMODE", "require"),
  connect_timeout = 15L
)
on.exit(dbDisconnect(pg), add = TRUE)

sql <- sprintf(
  "
  WITH segment_rows AS (
    SELECT DISTINCT
      game_id,
      team_id,
      segment_id,
      lineup_hash,
      segment_start_elapsed_seconds AS start_elapsed,
      segment_end_elapsed_seconds AS end_elapsed
    FROM %1$s.df_pts_poss_lineups_longer_mv
    WHERE segment_start_elapsed_seconds IS NOT NULL
      AND segment_end_elapsed_seconds IS NOT NULL
  ),
  boundaries AS (
    SELECT unnest(ARRAY[600, 1200, 1800])::double precision AS boundary_elapsed
  ),
  straddles AS (
    SELECT
      s.*,
      b.boundary_elapsed,
      s.end_elapsed - b.boundary_elapsed AS overhang_seconds,
      (b.boundary_elapsed / 600 + 1)::integer AS opening_quarter
    FROM segment_rows s
    CROSS JOIN boundaries b
    WHERE s.start_elapsed < b.boundary_elapsed
      AND s.end_elapsed > b.boundary_elapsed
  ),
  substitution_rows AS (
    SELECT
      game_id,
      team_id,
      id,
      quarter,
      quarter * 600.0 - end_quarter_seconds_remaining AS elapsed
    FROM %1$s.subs
    WHERE quarter BETWEEN 2 AND 4
      AND end_quarter_seconds_remaining IS NOT NULL
  ),
  faulty AS (
    SELECT
      s.game_id,
      s.team_id,
      s.segment_id,
      s.lineup_hash,
      s.opening_quarter,
      s.start_elapsed,
      s.boundary_elapsed,
      s.end_elapsed,
      s.overhang_seconds,
      count(*)::integer AS substitution_count,
      string_agg(sub.id::text, ',' ORDER BY sub.id) AS substitution_ids,
      min(sub.elapsed) AS first_sub_elapsed,
      max(sub.elapsed) AS last_sub_elapsed
    FROM straddles s
    JOIN substitution_rows sub
      ON sub.game_id = s.game_id
     AND sub.team_id = s.team_id
     AND sub.quarter = s.opening_quarter
     AND sub.elapsed >= s.boundary_elapsed
     -- A substitution exactly at the segment endpoint creates that endpoint;
     -- the carried lineup is valid until then and is not a faulty straddle.
     AND sub.elapsed < s.end_elapsed
    GROUP BY
      s.game_id, s.team_id, s.segment_id, s.lineup_hash,
      s.opening_quarter, s.start_elapsed, s.boundary_elapsed,
      s.end_elapsed, s.overhang_seconds
  )
  SELECT *
  FROM faulty
  ORDER BY overhang_seconds DESC, game_id, team_id, opening_quarter
  ",
  dbQuoteIdentifier(pg, schema)
)

faulty <- dbGetQuery(pg, sql)

original_gap_games <- c(
  66L, 68L, 88L, 156L, 177L, 184L, 223L, 360L, 406L,
  62449L, 62506L, 62512L, 62526L, 62530L, 62534L, 62537L,
  62540L, 62581L, 64902L
)
excluded_games <- c(211L, 406L)

faulty$original_gap_population <- faulty$game_id %in% original_gap_games
faulty$excluded_by_design <- faulty$game_id %in% excluded_games
faulty$three_seconds_or_less <- faulty$overhang_seconds <= 3

print(faulty, row.names = FALSE)

candidate_games <- sort(unique(faulty$game_id[!faulty$excluded_by_design]))
missing_from_first_pass <- setdiff(candidate_games, original_gap_games)

cat("\nFaulty straddles:", nrow(faulty), "\n")
cat("Candidate games excluding 211/406:", paste(candidate_games, collapse = ","), "\n")
cat("Missing from original gap-selected population:",
    paste(missing_from_first_pass, collapse = ","), "\n")

if (requireNamespace("arrow", quietly = TRUE)) {
  cold_path <- file.path("exports", "cold", "actions_clean.parquet")
  if (file.exists(cold_path)) {
    archived_game_ids <- arrow::read_parquet(cold_path, col_select = "game_id")
    archived_games <- unique(as.integer(archived_game_ids$game_id))
    missing_archive <- setdiff(candidate_games, archived_games)
    cat("Candidate games absent from actions archive:",
        if (length(missing_archive)) paste(missing_archive, collapse = ",") else "none",
        "\n")

    if (show_details) {
      archive_actions <- arrow::read_parquet(
        cold_path,
        col_select = c(
          "game_id", "id", "quarter", "quarter_time", "type", "team_id",
          "player_id", "parameters_player_in", "parameters_player_out",
          "end_game_seconds_remaining"
        )
      )
      archive_actions <- as.data.frame(archive_actions)
      archive_actions <- archive_actions[
        archive_actions$game_id == 62452L & archive_actions$quarter == 4L,
        , drop = FALSE
      ]
      archive_actions <- archive_actions[
        order(archive_actions$id), , drop = FALSE
      ]
      cat("\nGame 62452 archived Q4 opening actions:\n")
      print(utils::head(archive_actions, 20L), row.names = FALSE)
    }
  } else {
    cat("Actions archive not found:", cold_path, "\n")
  }
}

if (show_details) {
  cat("\nCurrent boundary-state diagnostics:\n")
  diagnostics <- dbGetQuery(pg, sprintf(
  "
  SELECT DISTINCT
    game_id, team_id, quarter, id, quarter_time,
    end_game_seconds_remaining, n_on, lineup_hash
  FROM %1$s.lineups_lookup
  WHERE (game_id = 100 AND quarter = 2)
     OR (game_id = 62452 AND quarter = 4)
  ORDER BY game_id, team_id, id
  ",
  dbQuoteIdentifier(pg, schema)
  ))
  print(diagnostics, row.names = FALSE)

  segment_diagnostics <- dbGetQuery(pg, sprintf(
  "
  SELECT DISTINCT
    game_id, team_id, segment_id, lineup_hash,
    segment_start_elapsed_seconds AS start_elapsed,
    segment_end_elapsed_seconds AS end_elapsed,
    segment_seconds
  FROM %1$s.df_pts_poss_lineups_longer_mv
  WHERE game_id IN (100, 62452)
    AND segment_start_elapsed_seconds < 1900
    AND segment_end_elapsed_seconds > 500
  ORDER BY game_id, team_id, start_elapsed, segment_id
  ",
  dbQuoteIdentifier(pg, schema)
  ))
  cat("\nCurrent ribbon-segment diagnostics:\n")
  print(segment_diagnostics, row.names = FALSE)
}

last_success <- dbGetQuery(pg, sprintf(
  "SELECT value FROM %s.app_meta WHERE key = 'etl_full_last_success'",
  dbQuoteIdentifier(pg, schema)
))
cat("\napp_meta.etl_full_last_success:",
    if (nrow(last_success)) as.character(last_success$value[[1]]) else "missing",
    "\n")
