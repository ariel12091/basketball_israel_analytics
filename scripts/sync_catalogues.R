# Keep the catalogue tables and the app's pinned copy of them in step.
#
#   1. season_teams: insert any (game_year, team_id) that full_rosters holds
#      and the table lacks, under the roster's name. Insert-only, so a display
#      name corrected by hand in the table is never overwritten.
#   2. Regenerate app/R/catalogues_generated.R from season_teams and
#      game_types. The app sources that file for STATIC_TEAM_ROSTERS,
#      GAME_TYPE_LABELS and GAME_TYPE_CHOICES_UI, because its UI is built
#      before any database connection exists.
#   3. Fail if schedule holds a game_type the game_types table cannot name.
#      A new provider code needs a human to give it a label.
#
# Run from the repository root. Step 1 only reports unless
# CONFIRM_CATALOGUE_SYNC=1. The nightly catalogue-sync workflow runs this after
# the ETL and commits the generated file when it changed.

confirm_apply <- identical(Sys.getenv("CONFIRM_CATALOGUE_SYNC", "0"), "1")
schema <- "basketball_test"
generated_path <- file.path("app", "R", "catalogues_generated.R")

if (file.exists("etl/.Renviron")) {
  readRenviron("etl/.Renviron")
}
if (!dir.exists(dirname(generated_path))) {
  stop("Run this script from the repository root")
}

required_env <- c("PG_HOST", "PG_DB", "PG_USER", "PG_PASS")
missing_env <- required_env[!nzchar(Sys.getenv(required_env))]
if (length(missing_env)) {
  stop(
    "Missing required database environment variables: ",
    paste(missing_env, collapse = ", ")
  )
}

suppressPackageStartupMessages({
  library(DBI)
  library(RPostgres)
})

con <- dbConnect(
  Postgres(),
  host = Sys.getenv("PG_HOST"),
  port = as.integer(Sys.getenv("PG_PORT", "6543")),
  dbname = Sys.getenv("PG_DB"),
  user = Sys.getenv("PG_USER"),
  password = Sys.getenv("PG_PASS"),
  sslmode = Sys.getenv("PG_SSLMODE", "require"),
  connect_timeout = 15
)
on.exit(dbDisconnect(con), add = TRUE)

# ---- 1. Teams a new season brought in ---------------------------------------

missing_teams_sql <- sprintf('
  SELECT fr.game_year, fr.team_id, MIN(fr.team_name) AS team_name
  FROM "%1$s"."full_rosters" fr
  LEFT JOIN "%1$s"."season_teams" st
    ON st.game_year = fr.game_year AND st.team_id = fr.team_id
  WHERE st.team_id IS NULL
  GROUP BY fr.game_year, fr.team_id
  ORDER BY fr.game_year, fr.team_id', schema)

missing_teams <- dbGetQuery(con, missing_teams_sql)
if (nrow(missing_teams)) {
  print(missing_teams)
  if (confirm_apply) {
    inserted <- dbExecute(con, sprintf('
      INSERT INTO "%1$s"."season_teams" (game_year, team_id, team_name)
      %2$s
      ON CONFLICT (game_year, team_id) DO NOTHING',
      schema, missing_teams_sql))
    message("season_teams: inserted ", inserted, " team(s) from full_rosters.")
  } else {
    message("season_teams: ", nrow(missing_teams),
            " team(s) missing; set CONFIRM_CATALOGUE_SYNC=1 to insert them.")
  }
} else {
  message("season_teams: covers every team in full_rosters.")
}

# ---- 2. Generated copy for the app ------------------------------------------

season_teams <- dbGetQuery(con, sprintf('
  SELECT game_year, team_id, team_name
  FROM "%s"."season_teams"
  ORDER BY game_year, team_id', schema))
game_types <- dbGetQuery(con, sprintf('
  SELECT game_type, label, short_label
  FROM "%s"."game_types"
  ORDER BY sort_order', schema))
if (!nrow(season_teams) || !nrow(game_types)) {
  stop("A catalogue table is empty; refusing to generate an empty copy")
}

# The app's R source must stay ASCII (test-source-encoding.R).
catalogue_text <- c(season_teams$team_name, game_types$label, game_types$short_label)
non_ascii <- unique(catalogue_text[grepl("[^\\x01-\\x7F]", catalogue_text, perl = TRUE)])
if (length(non_ascii)) {
  stop("Non-ASCII catalogue text cannot be written to R source: ",
       paste(encodeString(non_ascii, quote = '"'), collapse = ", "))
}

quoted <- function(x) encodeString(as.character(x), quote = '"')

named_vector_lines <- function(name, values, names) {
  entries <- paste0("  ", quoted(names), " = ", quoted(values))
  c(paste0(name, " <- c("), paste0(entries, c(rep(",", length(entries) - 1L), "")), ")")
}

roster_lines <- unlist(lapply(split(season_teams, season_teams$game_year), function(season) {
  c(
    sprintf("  `%d` = data.frame(", season$game_year[[1]]),
    sprintf("    team_id = c(%s),", paste0(as.integer(season$team_id), "L", collapse = ", ")),
    "    team_name = c(",
    paste0("      ", quoted(season$team_name),
           c(rep(",", nrow(season) - 1L), "")),
    "    ),",
    "    stringsAsFactors = FALSE",
    "  ),"
  )
}), use.names = FALSE)
roster_lines[length(roster_lines)] <- "  )"

# Codes sharing a label are one dropdown choice ("10,34"), in sort_order.
choice_labels <- unique(game_types$label)
choice_codes <- vapply(choice_labels, function(lbl) {
  paste(game_types$game_type[game_types$label == lbl], collapse = ",")
}, character(1))
# Chips look a selected choice up by its value, so a multi-code choice needs
# its own key next to the single codes.
multi <- grepl(",", choice_codes, fixed = TRUE)
multi_short <- vapply(choice_labels[multi], function(lbl) {
  short <- unique(game_types$short_label[game_types$label == lbl])
  if (length(short) != 1L) {
    stop("Codes sharing the label ", quoted(lbl), " must share one short_label")
  }
  short
}, character(1))

generated <- c(
  "# GENERATED by scripts/sync_catalogues.R -- do not edit by hand.",
  "#",
  "# A copy of basketball_test.season_teams and basketball_test.game_types, which",
  "# are the source of truth. The copy exists because the UI is built before any",
  "# database connection. To change a team name or a game-type label, change the",
  "# table row and re-run the script (the nightly catalogue-sync workflow does).",
  "",
  "STATIC_TEAM_ROSTERS <- list(",
  roster_lines,
  ")",
  "",
  named_vector_lines(
    "GAME_TYPE_LABELS",
    values = c(game_types$short_label, multi_short),
    names = c(game_types$game_type, choice_codes[multi])
  ),
  "",
  named_vector_lines(
    "GAME_TYPE_CHOICES_UI",
    values = c("", choice_codes),
    names = c("All", choice_labels)
  )
)

current <- if (file.exists(generated_path)) readLines(generated_path, warn = FALSE) else character(0)
if (identical(current, generated)) {
  message(generated_path, ": already up to date.")
} else {
  out <- file(generated_path, open = "wb")
  writeLines(generated, out, sep = "\n")
  close(out)
  message(generated_path, ": rewritten.")
}

# ---- 3. Game types the catalogue cannot name --------------------------------

uncatalogued <- dbGetQuery(con, sprintf('
  SELECT s.game_year, s.game_type, COUNT(*) AS games
  FROM "%1$s"."schedule" s
  LEFT JOIN "%1$s"."game_types" gt ON gt.game_type = s.game_type
  WHERE gt.game_type IS NULL
  GROUP BY s.game_year, s.game_type
  ORDER BY s.game_year, s.game_type', schema))
if (nrow(uncatalogued)) {
  print(uncatalogued)
  stop("schedule holds game_type code(s) missing from game_types. ",
       "Add a row for each (label, short_label, sort_order) and re-run.")
}
message("game_types: covers every game_type in schedule.")
