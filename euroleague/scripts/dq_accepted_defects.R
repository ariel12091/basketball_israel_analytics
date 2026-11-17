# Register of accepted defects: blocked games a person has looked at.
#
# A game is stored with publication_status = 'blocked' when one of four staging
# checks fails (staging.py), but the loader publishes it regardless: the status
# is a label, not a gate. euroleague/data_quality/accepted_defects.csv holds one
# row per (game, failed check) that was looked at and accepted, with the reason.
# The EuroLeague data quality report reads it:
#   N1_publication_blocked_games  error   blocked checks the register does not cover
#   N1b_accepted_defects          listed  blocked checks the register covers
#   N1c_stale_accepted_defects    warning register rows that no longer match the data
# so N1 fails on a NEW defect only, and the register cannot silently go stale.

# The four checks that make staging.py mark a game blocked, as conditions on a
# latest game_qa row aliased `l`.
ACCEPTED_DEFECT_GATES <- c(
  possession_structure = "l.possession_structural_status = 'fail'",
  boxscore_metrics     = "NOT l.boxscore_metrics_exact",
  score_progression    = "NOT l.score_progression_reconciled",
  lineup_structure     = "NOT l.lineup_structure_valid"
)

ACCEPTED_DEFECT_COLUMNS <- c(
  "competition", "season", "gamecode", "gate",
  "accepted_on", "accepted_by", "reason", "reference"
)

ACCEPTED_DEFECT_KEY <- c("competition", "season", "gamecode", "gate")

accepted_defects_path <- function(root) {
  file.path(root, "data_quality", "accepted_defects.csv")
}

# Reads and validates the register; any malformed row stops the check (fail
# closed), since a row the report cannot read would hide a defect.
read_accepted_defects <- function(path) {
  reg <- utils::read.csv(
    path, colClasses = "character", na.strings = character(),
    encoding = "UTF-8", check.names = FALSE
  )
  missing <- setdiff(ACCEPTED_DEFECT_COLUMNS, names(reg))
  if (length(missing)) {
    stop(sprintf("accepted_defects.csv is missing column(s): %s", paste(missing, collapse = ", ")))
  }
  reg <- reg[ACCEPTED_DEFECT_COLUMNS]
  problems <- character()
  bad_gate <- setdiff(unique(reg$gate), names(ACCEPTED_DEFECT_GATES))
  if (length(bad_gate)) {
    problems <- c(problems, sprintf("unknown gate(s) %s", paste(bad_gate, collapse = ", ")))
  }
  for (col in setdiff(ACCEPTED_DEFECT_COLUMNS, "reference")) {
    blank <- which(!nzchar(trimws(reg[[col]])))
    if (length(blank)) {
      problems <- c(problems, sprintf("blank %s in row(s) %s", col, paste(blank, collapse = ", ")))
    }
  }
  for (col in c("season", "gamecode")) {
    value <- suppressWarnings(as.integer(reg[[col]]))
    bad <- which(is.na(value) | as.character(value) != trimws(reg[[col]]))
    if (length(bad)) {
      problems <- c(problems, sprintf("%s is not a whole number in row(s) %s", col, paste(bad, collapse = ", ")))
    }
    reg[[col]] <- value
  }
  bad_date <- which(is.na(as.Date(reg$accepted_on, format = "%Y-%m-%d")))
  if (length(bad_date)) {
    problems <- c(problems, sprintf("accepted_on is not YYYY-MM-DD in row(s) %s", paste(bad_date, collapse = ", ")))
  }
  dup <- which(duplicated(reg[ACCEPTED_DEFECT_KEY]))
  if (length(dup)) {
    problems <- c(problems, sprintf("duplicate game and gate in row(s) %s", paste(dup, collapse = ", ")))
  }
  if (length(problems)) {
    stop(sprintf("accepted_defects.csv: %s", paste(problems, collapse = "; ")))
  }
  reg
}

# One row per failed check of every blocked game (latest load run per game).
# A blocked game that fails none of the four known checks gets gate
# 'unexplained', so a fifth reason added to staging can never be accepted by
# accident.
blocked_gates_sql <- function(latest_qa, schedule) {
  gate_rows <- c(
    sprintf(
      "SELECT l.game_id, '%s'::text AS gate FROM latest l WHERE l.publication_status = 'blocked' AND %s",
      names(ACCEPTED_DEFECT_GATES), ACCEPTED_DEFECT_GATES
    ),
    sprintf(
      "SELECT l.game_id, 'unexplained'::text AS gate FROM latest l WHERE l.publication_status = 'blocked' AND NOT ((%s))",
      paste(ACCEPTED_DEFECT_GATES, collapse = ") OR (")
    )
  )
  sprintf(
    "WITH latest AS (%s),
     gates AS (%s)
     SELECT s.competition, s.season, s.gamecode, g.gate, s.round_number, s.phase,
            l.load_run_id, l.possession_structural_status, l.boxscore_metrics_exact,
            l.score_progression_reconciled, l.lineup_structure_valid,
            l.unresolved_ft_rows, l.lineup_invalid_actor_rows, g.game_id
       FROM gates g
       JOIN latest l ON l.game_id = g.game_id
       LEFT JOIN %s s ON s.game_id = g.game_id
      ORDER BY s.competition, s.season, s.gamecode, g.gate",
    latest_qa, paste(gate_rows, collapse = "\n UNION ALL "), schedule
  )
}

# Splits blocked gates against the register:
#   open     blocked gates with no register row (N1, an error)
#   accepted blocked gates with a register row, joined to it (N1b)
#   stale    register rows whose game no longer fails that gate (N1c)
classify_blocked_gates <- function(blocked, register) {
  key <- function(df) {
    paste(df$competition, as.integer(df$season), as.integer(df$gamecode), df$gate, sep = "|")
  }
  blocked_key <- key(blocked)
  register_key <- key(register)
  accepted <- merge(
    blocked, register,
    by = ACCEPTED_DEFECT_KEY, sort = FALSE
  )
  list(
    open = blocked[!blocked_key %in% register_key, , drop = FALSE],
    accepted = cbind(accepted, open = rep(0L, nrow(accepted))),
    stale = register[!register_key %in% blocked_key, , drop = FALSE]
  )
}

# Runner shared by the three checks: `part` picks which split it returns.
accepted_defects_details <- function(con, latest_qa, schedule, root, part) {
  register <- read_accepted_defects(accepted_defects_path(root))
  blocked <- DBI::dbGetQuery(con, blocked_gates_sql(latest_qa, schedule))
  blocked$season <- as.integer(blocked$season)
  blocked$gamecode <- as.integer(blocked$gamecode)
  classify_blocked_gates(blocked, register)[[part]]
}
