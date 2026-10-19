readRenviron("etl/.Renviron")

suppressPackageStartupMessages({
  library(DBI)
  library(RPostgres)
})

schema <- Sys.getenv("SHOT_CLOCK_SCHEMA", unset = "")
if (!nzchar(schema)) {
  app_env <- tolower(Sys.getenv("APP_ENV", "test"))
  schema <- if (identical(app_env, "prod")) "basketball" else "basketball_test"
}
if (!grepl("^[A-Za-z_][A-Za-z0-9_]*$", schema)) {
  stop("Invalid SHOT_CLOCK_SCHEMA", call. = FALSE)
}

dry_run <- tolower(Sys.getenv("SHOT_CLOCK_DRY_RUN", "false")) %in% c("1", "true", "yes")

con <- dbConnect(
  Postgres(),
  host = Sys.getenv("PG_HOST"),
  port = as.integer(Sys.getenv("PG_PORT", "6543")),
  dbname = Sys.getenv("PG_DB"),
  user = Sys.getenv("PG_USER"),
  password = Sys.getenv("PG_PASS"),
  sslmode = Sys.getenv("PG_SSLMODE", "require")
)
on.exit(dbDisconnect(con), add = TRUE)

qi <- function(x) as.character(dbQuoteIdentifier(con, x))
qt <- function(schema, table) paste(qi(schema), qi(table), sep = ".")

marks_table <- qt(schema, "pbp_shot_clock_marks")
faults_view <- qt(schema, "pbp_shot_clock_faults_v")
source_table <- qt(schema, "df_pts_poss_lineups_longer_mv")

source_exists <- dbGetQuery(
  con,
  "select to_regclass($1) is not null as exists",
  params = list(paste(schema, "df_pts_poss_lineups_longer_mv", sep = "."))
)$exists[[1]]
if (!isTRUE(source_exists)) {
  stop(sprintf("Missing source table %s.df_pts_poss_lineups_longer_mv", schema), call. = FALSE)
}

message(sprintf("Reading offense action stream from %s ...", source_table))
actions <- dbGetQuery(con, sprintf("
select
  quarter::integer as quarter,
  parameters_type,
  parameters_points,
  parameters_made,
  id::integer as id,
  parent_action_id::integer as parent_action_id,
  type,
  player_id::integer as player_id,
  team_id::integer as team_id,
  game_id::integer as game_id,
  end_game_seconds_remaining::double precision as end_game_seconds_remaining,
  coalesce(final_end_poss, false) as final_end_poss
from %s
where type_lineup = 'offense'
order by game_id, id
", source_table))

n <- nrow(actions)
if (!n) stop("No offense action rows found.", call. = FALSE)

quarter_start_secs <- ifelse(
  actions$quarter <= 4L,
  (5L - actions$quarter) * 600,
  300
)

possession_seq <- integer(n)
segment_start_secs <- numeric(n)
segment_max_seconds <- numeric(n)
segment_start_reason <- character(n)
elapsed_since_segment_start <- numeric(n)
shot_clock_seconds <- numeric(n)
shot_clock_after_action <- numeric(n)
reset_after_action <- logical(n)
reset_after_reason <- rep(NA_character_, n)
fault_reason <- rep(NA_character_, n)

for (i in seq_len(n)) {
  if (i == 1L || actions$game_id[[i]] != actions$game_id[[i - 1L]]) {
    possession_seq[[i]] <- 1L
    segment_start_secs[[i]] <- quarter_start_secs[[i]]
    segment_max_seconds[[i]] <- 24
    segment_start_reason[[i]] <- "period_start"
  } else if (actions$quarter[[i]] != actions$quarter[[i - 1L]]) {
    possession_seq[[i]] <- possession_seq[[i - 1L]] + 1L
    segment_start_secs[[i]] <- quarter_start_secs[[i]]
    segment_max_seconds[[i]] <- 24
    segment_start_reason[[i]] <- "period_start"
  } else if (isTRUE(actions$final_end_poss[[i - 1L]])) {
    possession_seq[[i]] <- possession_seq[[i - 1L]] + 1L
    segment_start_secs[[i]] <- actions$end_game_seconds_remaining[[i - 1L]]
    segment_max_seconds[[i]] <- 24
    segment_start_reason[[i]] <- "previous_possession_end"
  } else if (isTRUE(reset_after_action[[i - 1L]])) {
    possession_seq[[i]] <- possession_seq[[i - 1L]]
    segment_start_secs[[i]] <- actions$end_game_seconds_remaining[[i - 1L]]
    segment_max_seconds[[i]] <- 14
    segment_start_reason[[i]] <- reset_after_reason[[i - 1L]]
  } else {
    possession_seq[[i]] <- possession_seq[[i - 1L]]
    segment_start_secs[[i]] <- segment_start_secs[[i - 1L]]
    segment_max_seconds[[i]] <- segment_max_seconds[[i - 1L]]
    segment_start_reason[[i]] <- segment_start_reason[[i - 1L]]
  }

  elapsed_since_segment_start[[i]] <- segment_start_secs[[i]] - actions$end_game_seconds_remaining[[i]]
  shot_clock_seconds[[i]] <- segment_max_seconds[[i]] - elapsed_since_segment_start[[i]]

  is_oreb <- identical(actions$type[[i]], "rebound") &&
    identical(actions$parameters_type[[i]], "offensive")
  is_def_foul_reset <- identical(actions$type[[i]], "foul") &&
    is.finite(shot_clock_seconds[[i]]) &&
    shot_clock_seconds[[i]] < 14

  reset_after_action[[i]] <- is_oreb || is_def_foul_reset
  if (is_oreb) {
    reset_after_reason[[i]] <- "offensive_rebound"
  } else if (is_def_foul_reset) {
    reset_after_reason[[i]] <- "defensive_foul_lt14"
  }

  shot_clock_after_action[[i]] <- if (reset_after_action[[i]]) 14 else shot_clock_seconds[[i]]

  if (!is.finite(actions$end_game_seconds_remaining[[i]]) || !is.finite(segment_start_secs[[i]])) {
    fault_reason[[i]] <- "missing_clock"
  } else if (elapsed_since_segment_start[[i]] < -0.01) {
    fault_reason[[i]] <- "event_before_segment_start"
  } else if (shot_clock_seconds[[i]] < -0.01) {
    fault_reason[[i]] <- "negative_shot_clock"
  } else if (shot_clock_seconds[[i]] > segment_max_seconds[[i]] + 0.01) {
    fault_reason[[i]] <- "shot_clock_above_segment_max"
  } else if (identical(actions$parameters_type[[i]], "24-seconds-violation") &&
             shot_clock_seconds[[i]] > 3) {
    fault_reason[[i]] <- "24s_violation_clock_high"
  }
}

marks <- data.frame(
  game_id = actions$game_id,
  id = actions$id,
  quarter = actions$quarter,
  team_id = actions$team_id,
  player_id = actions$player_id,
  type = actions$type,
  parameters_type = actions$parameters_type,
  parameters_made = actions$parameters_made,
  parameters_points = actions$parameters_points,
  parent_action_id = actions$parent_action_id,
  end_game_seconds_remaining = actions$end_game_seconds_remaining,
  possession_seq = possession_seq,
  shot_clock_segment_start_secs = segment_start_secs,
  shot_clock_segment_max_seconds = segment_max_seconds,
  segment_start_reason = segment_start_reason,
  elapsed_since_segment_start_seconds = elapsed_since_segment_start,
  shot_clock_seconds = shot_clock_seconds,
  shot_clock_after_action_seconds = shot_clock_after_action,
  reset_after_reason = reset_after_reason,
  final_end_poss = actions$final_end_poss,
  is_faulty_shot_clock = !is.na(fault_reason),
  fault_reason = fault_reason,
  stringsAsFactors = FALSE
)

summary_df <- data.frame(
  rows_marked = nrow(marks),
  faulty_rows = sum(marks$is_faulty_shot_clock, na.rm = TRUE),
  shot_clock_violations = sum(marks$parameters_type == "24-seconds-violation", na.rm = TRUE),
  games_marked = length(unique(marks$game_id))
)

fault_summary <- as.data.frame(table(
  fault_reason = ifelse(is.na(marks$fault_reason), "ok", marks$fault_reason),
  useNA = "no"
))
fault_summary <- fault_summary[order(-fault_summary$Freq, fault_summary$fault_reason), , drop = FALSE]
names(fault_summary) <- c("fault_reason", "rows")

if (dry_run) {
  message(sprintf("[DRY RUN] Would refresh %s from %s", marks_table, source_table))
  print(summary_df, row.names = FALSE)
  print(fault_summary, row.names = FALSE)
  quit(save = "no", status = 0)
}

create_sql <- sprintf("
create table if not exists %s (
  game_id integer not null,
  id integer not null,
  quarter integer,
  team_id integer,
  player_id integer,
  type text,
  parameters_type text,
  parameters_made text,
  parameters_points numeric,
  parent_action_id integer,
  end_game_seconds_remaining double precision,
  possession_seq integer,
  shot_clock_segment_start_secs double precision,
  shot_clock_segment_max_seconds double precision,
  segment_start_reason text,
  elapsed_since_segment_start_seconds double precision,
  shot_clock_seconds double precision,
  shot_clock_after_action_seconds double precision,
  reset_after_reason text,
  final_end_poss boolean,
  is_faulty_shot_clock boolean not null,
  fault_reason text,
  updated_at timestamptz not null default now(),
  primary key (game_id, id)
)
", marks_table)

stage <- sprintf("stage_pbp_shot_clock_marks_%s", Sys.getpid())
stage_q <- qi(stage)

dbBegin(con)
tryCatch({
  dbExecute(con, create_sql)
  dbWriteTable(con, stage, marks, temporary = TRUE, row.names = FALSE)
  dbExecute(con, sprintf("delete from %s", marks_table))
  inserted <- dbExecute(con, sprintf("
    insert into %s (
      game_id,
      id,
      quarter,
      team_id,
      player_id,
      type,
      parameters_type,
      parameters_made,
      parameters_points,
      parent_action_id,
      end_game_seconds_remaining,
      possession_seq,
      shot_clock_segment_start_secs,
      shot_clock_segment_max_seconds,
      segment_start_reason,
      elapsed_since_segment_start_seconds,
      shot_clock_seconds,
      shot_clock_after_action_seconds,
      reset_after_reason,
      final_end_poss,
      is_faulty_shot_clock,
      fault_reason,
      updated_at
    )
    select
      game_id,
      id,
      quarter,
      team_id,
      player_id,
      type,
      parameters_type,
      parameters_made,
      parameters_points,
      parent_action_id,
      end_game_seconds_remaining,
      possession_seq,
      shot_clock_segment_start_secs,
      shot_clock_segment_max_seconds,
      segment_start_reason,
      elapsed_since_segment_start_seconds,
      shot_clock_seconds,
      shot_clock_after_action_seconds,
      reset_after_reason,
      final_end_poss,
      is_faulty_shot_clock,
      fault_reason,
      now()
    from %s
  ", marks_table, stage_q))
  dbExecute(con, sprintf(
    "create index if not exists %s on %s (is_faulty_shot_clock, game_id)",
    qi("pbp_shot_clock_marks_fault_idx"),
    marks_table
  ))
  dbExecute(con, sprintf(
    "create index if not exists %s on %s (game_id, possession_seq)",
    qi("pbp_shot_clock_marks_possession_idx"),
    marks_table
  ))
  dbExecute(con, sprintf(
    "create or replace view %s as
     select *
     from %s
     where is_faulty_shot_clock",
    faults_view,
    marks_table
  ))
  try(dbExecute(con, sprintf("grant select on table %s to app_readonly", marks_table)), silent = TRUE)
  try(dbExecute(con, sprintf("grant select on table %s to app_readonly", faults_view)), silent = TRUE)
  dbCommit(con)
}, error = function(e) {
  try(dbRollback(con), silent = TRUE)
  stop(e)
})

message(sprintf("Refreshed %s (%s rows inserted)", marks_table, format(inserted, big.mark = ",")))
print(summary_df, row.names = FALSE)
print(fault_summary, row.names = FALSE)
