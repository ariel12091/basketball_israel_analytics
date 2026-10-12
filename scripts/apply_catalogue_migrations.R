# Create and seed the catalogue tables: basketball_test.game_types and
# basketball_test.season_teams. Both migrations are idempotent.
#
# Dry run by default: everything runs in one transaction and rolls back.
# Set CONFIRM_CATALOGUE_MIGRATION=1 to commit. Afterwards re-run
# scripts/apply_db_security.R (CONFIRM_DB_SECURITY_APPLY=1) so app_readonly
# gets its SELECT grant and RLS policy on a new table.

confirm_apply <- identical(Sys.getenv("CONFIRM_CATALOGUE_MIGRATION", "0"), "1")

if (file.exists("etl/.Renviron")) {
  readRenviron("etl/.Renviron")
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

ddl_port <- suppressWarnings(as.integer(Sys.getenv("PG_DDL_PORT", "5432")))
if (!is.finite(ddl_port) || ddl_port <= 0L) {
  stop("PG_DDL_PORT must be a valid PostgreSQL port")
}

# One entry per catalogue: the migration, how to show the result, and a query
# returning the rows of live data the catalogue fails to cover (must be empty).
catalogues <- list(
  game_types = list(
    migration = "2026-10-12_game_types.sql",
    show = "
      SELECT game_type, label, short_label, sort_order
      FROM basketball_test.game_types
      ORDER BY sort_order",
    uncovered = "
      SELECT s.game_type, COUNT(*) AS games
      FROM basketball_test.schedule s
      LEFT JOIN basketball_test.game_types gt ON gt.game_type = s.game_type
      WHERE gt.game_type IS NULL
      GROUP BY s.game_type
      ORDER BY s.game_type"
  ),
  season_teams = list(
    migration = "2026-10-12_season_teams.sql",
    show = "
      SELECT game_year, COUNT(*) AS teams
      FROM basketball_test.season_teams
      GROUP BY game_year
      ORDER BY game_year",
    uncovered = "
      SELECT fr.game_year, fr.team_id, MIN(fr.team_name) AS roster_name
      FROM basketball_test.full_rosters fr
      LEFT JOIN basketball_test.season_teams st
        ON st.game_year = fr.game_year AND st.team_id = fr.team_id
      WHERE st.team_id IS NULL
      GROUP BY fr.game_year, fr.team_id
      ORDER BY fr.game_year, fr.team_id"
  )
)

migration_paths <- file.path(
  "sql", "migrations",
  vapply(catalogues, function(x) x$migration, character(1))
)
if (!all(file.exists(migration_paths))) {
  stop("Run this script from the repository root")
}

con <- dbConnect(
  Postgres(),
  host = Sys.getenv("PG_HOST"),
  port = ddl_port,
  dbname = Sys.getenv("PG_DB"),
  user = Sys.getenv("PG_USER"),
  password = Sys.getenv("PG_PASS"),
  sslmode = Sys.getenv("PG_SSLMODE", "require"),
  connect_timeout = 10
)
on.exit(dbDisconnect(con), add = TRUE)

message("Database role: ", dbGetQuery(con, "SELECT current_user AS role")$role[[1]])
message(
  if (confirm_apply) {
    "Mode: APPLY (transaction will commit)"
  } else {
    "Mode: DRY RUN (transaction will roll back)"
  }
)

dbBegin(con)
transaction_open <- TRUE
on.exit({
  if (transaction_open) {
    try(dbRollback(con), silent = TRUE)
  }
}, add = TRUE)

invisible(dbExecute(con, "SET LOCAL client_min_messages = warning"))

for (name in names(catalogues)) {
  catalogue <- catalogues[[name]]
  path <- file.path("sql", "migrations", catalogue$migration)
  sql <- paste(readLines(path, warn = FALSE), collapse = "\n")
  invisible(dbExecute(con, sql, immediate = TRUE))

  message(name, ":")
  print(dbGetQuery(con, catalogue$show))

  uncovered <- dbGetQuery(con, catalogue$uncovered)
  if (nrow(uncovered)) {
    print(uncovered)
    stop(name, " does not cover the live data above; transaction will roll back")
  }
}

if (confirm_apply) {
  dbCommit(con)
  transaction_open <- FALSE
  message("Catalogue migrations committed.")
} else {
  dbRollback(con)
  transaction_open <- FALSE
  message("Catalogue migrations validated and rolled back.")
}
