#!/usr/bin/env Rscript

# Read-only offline report for game 406's period relabel and approximate Q4
# wall-clock reconstruction. It evaluates only the correction/cleaning helpers
# from etl_onoff.R, so it does not connect to or write the database.

suppressPackageStartupMessages({
  library(arrow)
  library(dplyr)
})

exprs <- parse("etl/etl_onoff.R", keep.source = FALSE)
wanted <- c(
  "KNOWN_CLOCK_STAMP_CORRECTIONS",
  "KNOWN_PERIOD_LABEL_CORRECTIONS",
  "apply_known_period_label_corrections",
  "apply_known_clock_corrections",
  "apply_known_game406_wall_clock_correction"
)
assignments <- Filter(function(expr) {
  is.call(expr) && identical(expr[[1]], as.name("<-")) &&
    as.character(expr[[2]]) %in% wanted
}, exprs)
stopifnot(identical(
  vapply(assignments, function(x) as.character(x[[2]]), character(1)),
  wanted
))
env <- new.env(parent = globalenv())
invisible(lapply(assignments, eval, envir = env))

action_cols <- c(
  "game_id", "id", "quarter", "quarter_time", "type", "team_id",
  "player_id", "parameters_player_in", "parameters_player_out",
  "parameters_points", "parameters_made", "score", "user_time"
)
actions <- as.data.frame(read_parquet(
  "exports/cold/actions_clean.parquet",
  col_select = tidyselect::all_of(action_cols)
)) |>
  filter(game_id == 406L)

# Before the scoped database reprocess this archive contained the broken feed;
# afterwards it contains the corrected clean-actions output. Keep the report
# useful in both states without asking the guarded corrections to overwrite
# their own already-corrected result (which intentionally emits warnings).
row_matches <- function(action_id, quarter, quarter_time = NULL) {
  row <- actions[actions$id == action_id, , drop = FALSE]
  nrow(row) == 1L && identical(as.integer(row$quarter), as.integer(quarter)) &&
    (is.null(quarter_time) || identical(as.character(row$quarter_time), quarter_time))
}
archive_is_reprocessed <-
  row_matches(4060230L, 2L, "10:00") &&
  row_matches(4060417L, 2L, "00:00") &&
  row_matches(4060419L, 3L) &&
  row_matches(4060592L, 4L, "10:00") &&
  row_matches(4060617L, 4L, "09:11")

if (!archive_is_reprocessed) {
  actions <- env$apply_known_period_label_corrections(actions, 406L)
  actions <- env$apply_known_clock_corrections(actions, 406L)
  actions <- env$apply_known_game406_wall_clock_correction(actions, 406L)
}
actions <- actions |>
  filter(!(id %in% c(4060238L, 4060239L) & type == "quarter")) |>
  mutate(
    team_score = if_else(
      parameters_made == "made",
      as.integer(parameters_points),
      NA_integer_
    )
  ) |>
  arrange(id)

# Frozen provider box minutes captured 2026-09-20. They are useful only as a
# before/after reference: the same five home players were credited the full
# clockless Q4, so these are not constraints on the approximation.
box_players_source <- data.frame(
  team_id = c(rep(6L, 11), rep(14L, 11)),
  player_id = c(
    2069L, 1162L, 2329L, 1456L, 2317L, 1249L, 2281L, 1982L, 2491L, 1077L, 1380L,
    2071L, 1012L, 1377L, 1005L, 2286L, 2287L, 1312L, 2318L, 2285L, 1085L, 1198L
  ),
  provider_minutes = c(
    "25:22", "26:16", "00:00", "00:00", "29:00", "32:21", "36:00", "15:12", "00:00", "35:48", "00:00",
    "27:13", "21:24", "06:35", "00:00", "20:45", "18:09", "12:10", "32:56", "17:20", "12:13", "26:53"
  )
)

clock_seconds <- function(x) {
  pieces <- strsplit(as.character(x), ":", fixed = TRUE)
  vapply(pieces, function(p) {
    if (length(p) != 2L) return(NA_real_)
    as.numeric(p[[1]]) * 60 + as.numeric(p[[2]])
  }, numeric(1))
}

quarter_summary <- actions |>
  mutate(clock_seconds = clock_seconds(quarter_time)) |>
  group_by(quarter) |>
  summarise(
    actions = n(),
    min_id = min(id),
    max_id = max(id),
    opening_clock = max(clock_seconds),
    closing_clock = min(clock_seconds),
    clock_increases = sum(diff(clock_seconds) > 0),
    team_6_points = sum(if_else(team_id == 6L, team_score, 0L), na.rm = TRUE),
    team_14_points = sum(if_else(team_id == 14L, team_score, 0L), na.rm = TRUE),
    .groups = "drop"
  )

expected <- data.frame(
  quarter = 1:4,
  expected_team_6_points = c(28L, 26L, 19L, 26L),
  expected_team_14_points = c(12L, 24L, 18L, 25L)
)
quarter_summary <- left_join(quarter_summary, expected, by = "quarter")

cat("=== Corrected period summary ===\n")
cat("archive already reprocessed:", archive_is_reprocessed, "\n")
print(quarter_summary, n = Inf, width = Inf)

cat("\n=== Approximate Q4 scoring clocks ===\n")
q4_scores <- actions |>
  filter(quarter == 4L, !is.na(team_score)) |>
  select(id, user_time, quarter_time, team_id, player_id, team_score, score) |>
  as_tibble()
print(q4_scores, n = Inf, width = Inf)

cat("\n=== Approximate Q4 substitution clocks ===\n")
q4_subs <- actions |>
  filter(quarter == 4L, type == "substitution") |>
  select(
    id, user_time, quarter_time, team_id, player_id,
    parameters_player_in, parameters_player_out
  ) |>
  as_tibble()
print(q4_subs, n = Inf, width = Inf)

apply_substitution <- function(state, row) {
  pid <- as.integer(row$player_id[[1]])
  if (!is.na(row$parameters_player_out[[1]])) state <- setdiff(state, pid)
  if (!is.na(row$parameters_player_in[[1]])) state <- union(state, pid)
  state
}

period_exposure <- function(rows, initial_state) {
  rows$clock_seconds <- clock_seconds(rows$quarter_time)
  times <- sort(unique(rows$clock_seconds), decreasing = TRUE)
  if (!length(times) || tail(times, 1L) != 0) times <- c(times, 0)
  state <- initial_state
  seconds <- numeric(0)
  for (i in seq_len(length(times) - 1L)) {
    at_time <- rows[
      rows$clock_seconds == times[[i]] & rows$type == "substitution",
      , drop = FALSE
    ]
    if (nrow(at_time)) {
      for (j in seq_len(nrow(at_time))) state <- apply_substitution(state, at_time[j, ])
    }
    duration <- times[[i]] - times[[i + 1L]]
    if (duration > 0 && length(state)) {
      keys <- as.character(state)
      seconds[keys] <- ifelse(is.na(seconds[keys]), 0, seconds[keys]) + duration
    }
  }
  seconds[is.na(seconds)] <- 0
  list(seconds = seconds, final_state = state)
}

estimated <- numeric(0)
state <- integer(0)
for (q in 1:4) {
  result <- period_exposure(actions[actions$quarter == q, , drop = FALSE], state)
  state <- result$final_state
  keys <- union(names(estimated), names(result$seconds))
  next_estimated <- setNames(numeric(length(keys)), keys)
  next_estimated[names(estimated)] <- estimated
  next_estimated[names(result$seconds)] <-
    next_estimated[names(result$seconds)] + result$seconds
  estimated <- next_estimated
}

box_players <- box_players_source |>
  transmute(
    team_id,
    player_id,
    provider_minutes,
    provider_seconds = clock_seconds(provider_minutes),
    estimated_seconds = estimated[as.character(player_id)],
    estimated_seconds = coalesce(estimated_seconds, 0),
    estimated_minutes = sprintf(
      "%02d:%02d",
      as.integer(estimated_seconds) %/% 60L,
      as.integer(estimated_seconds) %% 60L
    ),
    delta_seconds = estimated_seconds - provider_seconds
  ) |>
  as_tibble()

cat("\n=== Player-minute impact (estimated minus broken provider box) ===\n")
print(box_players, n = Inf, width = Inf)

cat("\n=== Team minute totals ===\n")
print(
  box_players |>
    group_by(team_id) |>
    summarise(
      provider_seconds = sum(provider_seconds),
      estimated_seconds = sum(estimated_seconds),
      expected_seconds = 12000,
      .groups = "drop"
    ),
  n = Inf,
  width = Inf
)
