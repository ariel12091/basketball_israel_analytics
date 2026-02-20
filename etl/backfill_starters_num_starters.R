#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(DBI)
  library(dplyr)
  library(purrr)
  library(tibble)
})

source_etl_onoff_definitions <- function(path = "etl/etl_onoff.R") {
  etl_lines <- readLines(path, warn = FALSE)
  usage_marker <- grep("^#\\s*Usage", etl_lines)
  if (length(usage_marker)) {
    cut_at <- usage_marker[1] - 2
    etl_lines <- etl_lines[seq_len(cut_at)]
  }
  eval(parse(text = etl_lines), envir = .GlobalEnv)
}

ensure_starter_columns <- function(pg, schema) {
  DBI::dbExecute(
    pg,
    sprintf('ALTER TABLE "%s"."full_rosters" ADD COLUMN IF NOT EXISTS starter boolean;', schema)
  )
  DBI::dbExecute(
    pg,
    sprintf('ALTER TABLE "%s"."lineups_lookup" ADD COLUMN IF NOT EXISTS num_starters integer;', schema)
  )
}

fetch_rosters_for_games <- function(ids, sched_feed) {
  if (!length(ids)) return(tibble())

  req_df <- tibble(game_id = as.integer(ids)) |>
    left_join(
      sched_feed |> select(game_id, box_url),
      by = "game_id"
    ) |>
    mutate(
      box_url = if_else(
        is.na(box_url) | box_url == "",
        paste0("https://stats.segevstats.com/realtimestat_heb/get_team_score.php?game_id=", game_id),
        box_url
      )
    )

  pbps <- purrr::map2(req_df$game_id, req_df$box_url, fetch_game_box)

  purrr::map(pbps, extract_roster) |>
    purrr::list_rbind() |>
    dplyr::rename_with(tolower)
}

refresh_num_starters_for_games <- function(pg, schema, ids) {
  if (!length(ids)) return(invisible(0L))
  ids_csv <- paste(as.integer(ids), collapse = ",")

  sql <- sprintf(
    "WITH lineup_counts AS (
       SELECT
         ll.game_id,
         ll.team_id,
         ll.quarter,
         ll.id,
         SUM(
           CASE
             WHEN ll.is_on_verdict = 1 AND COALESCE(fr.starter, FALSE) THEN 1
             ELSE 0
           END
         )::int AS num_starters
       FROM \"%s\".\"lineups_lookup\" ll
       LEFT JOIN \"%s\".\"full_rosters\" fr
         ON fr.game_id = ll.game_id
        AND fr.team_id = ll.team_id
        AND fr.player_id = ll.player_id
       WHERE ll.game_id IN (%s)
       GROUP BY ll.game_id, ll.team_id, ll.quarter, ll.id
     )
     UPDATE \"%s\".\"lineups_lookup\" ll
        SET num_starters = lc.num_starters
       FROM lineup_counts lc
      WHERE ll.game_id = lc.game_id
        AND ll.team_id = lc.team_id
        AND ll.quarter = lc.quarter
        AND ll.id = lc.id;",
    schema, schema, ids_csv, schema
  )

  DBI::dbExecute(pg, sql)
}

validate_games <- function(pg, schema, ids, label) {
  if (!length(ids)) {
    message(sprintf("[%s] no game_ids to validate", label))
    return(invisible(NULL))
  }

  ids_csv <- paste(as.integer(ids), collapse = ",")
  q <- sprintf(
    "SELECT
       game_id,
       team_id,
       MIN(num_starters) AS min_num_starters,
       MAX(num_starters) AS max_num_starters,
       COUNT(*) FILTER (WHERE num_starters IS NULL) AS null_rows
     FROM \"%s\".\"lineups_lookup\"
     WHERE game_id IN (%s)
     GROUP BY game_id, team_id
     ORDER BY game_id, team_id;",
    schema, ids_csv
  )
  out <- DBI::dbGetQuery(pg, q)
  message(sprintf("[%s] lineup num_starters summary rows: %d", label, nrow(out)))
  print(out, row.names = FALSE)
  invisible(out)
}

run_stage <- function(pg, schema, sched_feed, ids, label) {
  if (!length(ids)) {
    message(sprintf("[%s] no games to process", label))
    return(invisible(NULL))
  }

  message(sprintf("[%s] processing %d games", label, length(ids)))
  rosters <- fetch_rosters_for_games(ids, sched_feed)
  if (!nrow(rosters)) {
    stop(sprintf("[%s] no roster rows fetched", label), call. = FALSE)
  }

  ids_csv <- paste(as.integer(ids), collapse = ",")
  years <- DBI::dbGetQuery(
    pg,
    sprintf('SELECT game_id, game_year FROM "%s"."schedule" WHERE game_id IN (%s)', schema, ids_csv)
  )

  rosters <- rosters |>
    left_join(years, by = "game_id", suffix = c("", "_db")) |>
    mutate(starter = dplyr::coalesce(as.logical(starter), FALSE))

  if ("game_year_db" %in% names(rosters) && "game_year" %in% names(rosters)) {
    rosters <- rosters |>
      mutate(game_year = dplyr::coalesce(game_year_db, game_year, as.integer(format(Sys.Date(), "%Y")))) |>
      select(-game_year_db)
  } else if ("game_year_db" %in% names(rosters)) {
    rosters <- rosters |>
      mutate(game_year = dplyr::coalesce(game_year_db, as.integer(format(Sys.Date(), "%Y")))) |>
      select(-game_year_db)
  } else if (!"game_year" %in% names(rosters)) {
    rosters <- rosters |>
      mutate(game_year = as.integer(format(Sys.Date(), "%Y")))
  }

  if (exists("enrich_roster_names_from_existing", mode = "function")) {
    rosters <- enrich_roster_names_from_existing(pg, schema, rosters)
  }

  upsert_by_like(pg, schema, "full_rosters", rosters)
  refresh_num_starters_for_games(pg, schema, ids)
  validate_games(pg, schema, ids, label)

  message(sprintf(
    "[%s] complete: upserted %d roster rows and refreshed num_starters",
    label, nrow(rosters)
  ))
}

set.seed(20260220)

env_file <- file.path("etl", ".Renviron")
if (file.exists(env_file)) readRenviron(env_file)
Sys.setenv(APP_ENV = Sys.getenv("APP_ENV", "test"))

source_etl_onoff_definitions("etl/etl_onoff.R")

on.exit({
  if (exists("pg") && DBI::dbIsValid(pg)) DBI::dbDisconnect(pg)
}, add = TRUE)

ensure_starter_columns(pg, SCHEMA)

all_ids <- DBI::dbGetQuery(
  pg, sprintf('SELECT DISTINCT game_id FROM "%s"."lineups_lookup" ORDER BY game_id', SCHEMA)
)$game_id |>
  as.integer()

if (!length(all_ids)) stop("No game_ids found in lineups_lookup.", call. = FALSE)

sched_feed <- fetch_israel_schedule() |>
  filter(score_team1 > 0) |>
  select(game_id, box_url)

stage1 <- 95L
stage2_pool <- setdiff(all_ids, stage1)
stage2 <- if (length(stage2_pool) > 0) sample(stage2_pool, min(30L, length(stage2_pool))) else integer(0)
stage3 <- setdiff(all_ids, c(stage1, stage2))

stage_mode <- tolower(Sys.getenv("STAGE_MODE", "all"))

if (stage_mode %in% c("all", "stage1")) {
  run_stage(pg, SCHEMA, sched_feed, stage1, "stage1_game95")
}
if (stage_mode %in% c("all", "stage2")) {
  run_stage(pg, SCHEMA, sched_feed, stage2, "stage2_random30")
}
if (stage_mode %in% c("all", "stage3")) {
  run_stage(pg, SCHEMA, sched_feed, stage3, "stage3_remaining")
}

message("Starter backfill + lineup num_starters refresh complete.")
