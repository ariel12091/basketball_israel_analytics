# Rank games by shots with impossible derived shot-clock values.
#
# The archived marks are produced by scripts/mark_shot_clock.R. A negative
# value can also mean that the source feed omitted a reset event, so the
# high-confidence report is intentionally narrower than the full report.

suppressPackageStartupMessages({
  library(arrow)
  library(DBI)
  library(dplyr)
  library(RPostgres)
})

archive_path <- Sys.getenv(
  "SHOT_CLOCK_ARCHIVE",
  unset = "output/archive/pbp_shot_clock_marks_2026-07-15.parquet"
)
all_games_path <- Sys.getenv(
  "SHOT_CLOCK_ALL_GAMES_OUTPUT",
  unset = "output/shot_clock_impossible_games.csv"
)
high_confidence_path <- Sys.getenv(
  "SHOT_CLOCK_HIGH_CONFIDENCE_OUTPUT",
  unset = "output/shot_clock_high_confidence_games.csv"
)
actions_path <- Sys.getenv(
  "SHOT_CLOCK_ACTIONS_OUTPUT",
  unset = "output/shot_clock_impossible_actions.csv"
)
game_year_text <- Sys.getenv("SHOT_CLOCK_GAME_YEAR", unset = "")
target_game_year <- if (nzchar(game_year_text)) {
  suppressWarnings(as.integer(game_year_text))
} else {
  NA_integer_
}
if (nzchar(game_year_text) && is.na(target_game_year)) {
  stop("SHOT_CLOCK_GAME_YEAR must be an integer game_year.", call. = FALSE)
}

if (!file.exists(archive_path)) {
  stop(sprintf("Shot-clock archive not found: %s", archive_path), call. = FALSE)
}

marks <- read_parquet(archive_path)

required_columns <- c(
  "game_id",
  "type",
  "parameters_type",
  "possession_seq",
  "shot_clock_seconds",
  "shot_clock_segment_max_seconds"
)
missing_columns <- setdiff(required_columns, names(marks))
if (length(missing_columns)) {
  stop(
    sprintf("Archive is missing columns: %s", paste(missing_columns, collapse = ", ")),
    call. = FALSE
  )
}

format_period_clock <- function(seconds) {
  out <- rep(NA_character_, length(seconds))
  valid <- is.finite(seconds)
  rounded <- pmax(0, round(seconds[valid]))
  out[valid] <- sprintf("%02d:%02d", rounded %/% 60, rounded %% 60)
  out
}

marked_actions <- marks |>
  arrange(game_id, id) |>
  group_by(game_id) |>
  mutate(
    previous_action_id = lag(id),
    previous_action_quarter = lag(quarter),
    previous_action_end_game_seconds_remaining =
      lag(end_game_seconds_remaining),
    previous_action_type = lag(type),
    previous_action_parameters_type = lag(parameters_type),
    previous_action_team_id = lag(team_id),
    previous_action_player_id = lag(player_id),
    previous_action_final_end_poss = lag(final_end_poss),
    next_action_id = lead(id),
    next_action_end_game_seconds_remaining = lead(end_game_seconds_remaining),
    next_action_type = lead(type),
    next_action_parameters_type = lead(parameters_type),
    is_new_shot_clock_segment =
      row_number() == 1L |
      possession_seq != lag(possession_seq, default = first(possession_seq)) |
      shot_clock_segment_start_secs != lag(
        shot_clock_segment_start_secs,
        default = first(shot_clock_segment_start_secs)
      ) |
      segment_start_reason != lag(
        segment_start_reason,
        default = first(segment_start_reason)
      ),
    shot_clock_segment_number = cumsum(is_new_shot_clock_segment),
    segment_start_action_seed = if_else(
      is_new_shot_clock_segment & segment_start_reason != "period_start",
      previous_action_id,
      NA_integer_
    )
  ) |>
  group_by(game_id, shot_clock_segment_number) |>
  mutate(segment_start_action_id = first(segment_start_action_seed)) |>
  ungroup() |>
  mutate(
    is_shot = type == "shot",
    is_negative_shot = is_shot & shot_clock_seconds < -0.01,
    is_above_max_shot =
      is_shot &
      shot_clock_seconds > shot_clock_segment_max_seconds + 0.01,
    is_impossible_shot = is_negative_shot | is_above_max_shot,
    # Below -24 means more than 48 seconds elapsed without an inferred reset.
    # This is used only as a high-confidence triage threshold.
    is_severe_negative_shot = is_shot & shot_clock_seconds < -24.01,
    is_clock_reversal =
      shot_clock_seconds > shot_clock_segment_max_seconds + 0.01,
    is_high_clock_24s_violation =
      parameters_type == "24-seconds-violation" &
      shot_clock_seconds > 3,
    period_clock_seconds = if_else(
      quarter <= 4L,
      end_game_seconds_remaining - (4L - quarter) * 600,
      end_game_seconds_remaining
    ),
    period_clock = format_period_clock(period_clock_seconds)
  )

ranked_games <- marked_actions |>
  group_by(game_id) |>
  summarise(
    total_shots = sum(is_shot, na.rm = TRUE),
    impossible_shots = sum(is_impossible_shot, na.rm = TRUE),
    negative_shots = sum(is_negative_shot, na.rm = TRUE),
    above_max_shots = sum(is_above_max_shot, na.rm = TRUE),
    severe_negative_shots = sum(is_severe_negative_shot, na.rm = TRUE),
    impossible_possessions =
      n_distinct(possession_seq[is_impossible_shot %in% TRUE]),
    clock_reversal_rows = sum(is_clock_reversal, na.rm = TRUE),
    high_clock_24s_violations =
      sum(is_high_clock_24s_violation, na.rm = TRUE),
    min_shot_clock = min(shot_clock_seconds[is_shot %in% TRUE], na.rm = TRUE),
    max_shot_clock = max(shot_clock_seconds[is_shot %in% TRUE], na.rm = TRUE),
    .groups = "drop"
  ) |>
  mutate(
    impossible_shot_pct = round(100 * impossible_shots / total_shots, 2),
    high_confidence_source_anomaly =
      above_max_shots > 0 | severe_negative_shots > 0,
    anomaly_reason = case_when(
      above_max_shots > 0 & severe_negative_shots > 0 ~
        "shot above segment max; shot below -24",
      above_max_shots > 0 ~ "shot above segment max",
      severe_negative_shots > 0 ~ "shot below -24",
      TRUE ~ NA_character_
    )
  )

readRenviron("app/.Renviron")
con <- dbConnect(
  Postgres(),
  host = Sys.getenv("PG_HOST"),
  port = as.integer(Sys.getenv("PG_PORT")),
  dbname = Sys.getenv("PG_DB"),
  user = Sys.getenv("PG_USER"),
  password = Sys.getenv("PG_PASS"),
  sslmode = Sys.getenv("PG_SSLMODE"),
  connect_timeout = 15L
)
on.exit(dbDisconnect(con), add = TRUE)

schedule <- dbGetQuery(
  con,
  paste(
    "select game_id, game_year, gn, game_date,",
    "team_name_eng_1, team_name_eng_2, score_team1, score_team2",
    "from basketball_test.schedule"
  )
)

rosters <- dbGetQuery(
  con,
  paste(
    "select distinct game_id, team_id, player_id, firstname, lastname,",
    "jerseynumber, team_name",
    "from basketball_test.full_rosters"
  )
)

ranked_games <- ranked_games |>
  left_join(schedule, by = "game_id")

if (!is.na(target_game_year)) {
  ranked_games <- ranked_games |>
    filter(game_year == target_game_year)
}

ranked_games <- ranked_games |>
  arrange(desc(impossible_shots), desc(impossible_shot_pct), game_id) |>
  mutate(impossible_shot_rank = row_number()) |>
  relocate(
    impossible_shot_rank,
    game_year,
    gn,
    game_date,
    team_name_eng_1,
    team_name_eng_2,
    score_team1,
    score_team2,
    .after = game_id
  )

high_confidence_games <- ranked_games |>
  filter(high_confidence_source_anomaly) |>
  arrange(
    desc(severe_negative_shots),
    desc(above_max_shots),
    min_shot_clock,
    game_id
  )

segment_start_context <- marked_actions |>
  transmute(
    game_id,
    segment_start_action_id = id,
    segment_start_action_quarter = quarter,
    segment_start_action_end_game_seconds_remaining =
      end_game_seconds_remaining,
    segment_start_action_type = type,
    segment_start_action_parameters_type = parameters_type,
    segment_start_action_team_id = team_id,
    segment_start_action_player_id = player_id,
    segment_start_action_final_end_poss = final_end_poss
  )

impossible_actions <- marked_actions |>
  filter(is_impossible_shot) |>
  left_join(schedule, by = "game_id")

if (!is.na(target_game_year)) {
  impossible_actions <- impossible_actions |>
    filter(game_year == target_game_year)
}

impossible_actions <- impossible_actions |>
  left_join(
    rosters,
    by = c("game_id", "team_id", "player_id")
  ) |>
  left_join(
    segment_start_context,
    by = c("game_id", "segment_start_action_id")
  ) |>
  left_join(
    ranked_games |>
      select(
        game_id,
        impossible_shots_in_game = impossible_shots,
        impossible_shot_pct_in_game = impossible_shot_pct,
        game_has_extreme_clock_anomaly = high_confidence_source_anomaly
      ),
    by = "game_id"
  ) |>
  arrange(game_id, id) |>
  group_by(game_id) |>
  mutate(impossible_shot_number_in_game = row_number()) |>
  ungroup() |>
  transmute(
    game_id,
    game_year,
    gn,
    game_date,
    team_name_eng_1,
    team_name_eng_2,
    score_team1,
    score_team2,
    impossible_shot_number_in_game,
    impossible_shots_in_game,
    impossible_shot_pct_in_game,
    game_has_extreme_clock_anomaly,
    action_id = id,
    parent_action_id,
    quarter,
    period_clock,
    period_clock_seconds,
    end_game_seconds_remaining,
    action_team_id = team_id,
    action_player_id = player_id,
    player_firstname = firstname,
    player_lastname = lastname,
    player_jersey_number = jerseynumber,
    player_team_name = team_name,
    action_type = type,
    shot_type = parameters_type,
    shot_points = parameters_points,
    shot_result = parameters_made,
    final_end_poss,
    possession_seq,
    segment_start_reason,
    segment_start_action_id,
    segment_start_action_quarter,
    segment_start_action_end_game_seconds_remaining,
    segment_start_action_type,
    segment_start_action_parameters_type,
    segment_start_action_team_id,
    segment_start_action_player_id,
    segment_start_action_final_end_poss,
    shot_clock_segment_start_seconds = shot_clock_segment_start_secs,
    shot_clock_segment_max_seconds,
    elapsed_since_segment_start_seconds,
    derived_shot_clock_seconds = shot_clock_seconds,
    is_negative_shot_clock = is_negative_shot,
    is_above_segment_max = is_above_max_shot,
    is_extreme_clock_anomaly = is_severe_negative_shot | is_above_max_shot,
    fault_reason,
    previous_action_id,
    previous_action_quarter,
    previous_action_end_game_seconds_remaining,
    previous_action_type,
    previous_action_parameters_type,
    previous_action_team_id,
    previous_action_player_id,
    previous_action_final_end_poss,
    next_action_id,
    next_action_end_game_seconds_remaining,
    next_action_type,
    next_action_parameters_type
  )

dir.create(dirname(all_games_path), recursive = TRUE, showWarnings = FALSE)
dir.create(dirname(high_confidence_path), recursive = TRUE, showWarnings = FALSE)
dir.create(dirname(actions_path), recursive = TRUE, showWarnings = FALSE)
write.csv(ranked_games, all_games_path, row.names = FALSE, na = "")
write.csv(high_confidence_games, high_confidence_path, row.names = FALSE, na = "")
write.csv(impossible_actions, actions_path, row.names = FALSE, na = "")

cat(sprintf(
  "Wrote %s games to %s\n",
  nrow(ranked_games),
  normalizePath(all_games_path, winslash = "/", mustWork = FALSE)
))
cat(sprintf(
  "Wrote %s high-confidence games to %s\n",
  nrow(high_confidence_games),
  normalizePath(high_confidence_path, winslash = "/", mustWork = FALSE)
))
cat(sprintf(
  "Wrote %s impossible shot actions to %s\n",
  nrow(impossible_actions),
  normalizePath(actions_path, winslash = "/", mustWork = FALSE)
))
cat(sprintf(
  "Impossible shots: %s total (%s negative, %s above segment max)\n",
  sum(ranked_games$impossible_shots),
  sum(ranked_games$negative_shots),
  sum(ranked_games$above_max_shots)
))
if (!is.na(target_game_year)) {
  cat(sprintf("Filtered to game_year=%s\n", target_game_year))
}
