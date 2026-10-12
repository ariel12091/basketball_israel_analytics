# Create and seed basketball_test.game_types.
#
# Dry run by default: the migration runs in a transaction and rolls back.
# Set CONFIRM_GAME_TYPES_MIGRATION=1 to commit. Afterwards re-run
# scripts/apply_db_security.R (CONFIRM_DB_SECURITY_APPLY=1) so app_readonly
# gets its SELECT grant and RLS policy on the new table.

confirm_apply <- identical(Sys.getenv("CONFIRM_GAME_TYPES_MIGRATION", "0"), "1")

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

migration_path <- file.path("sql", "migrations", "2026-10-12_game_types.sql")
if (!file.exists(migration_path)) {
  stop("Run this script from the repository root")
}
migration_sql <- paste(readLines(migration_path, warn = FALSE), collapse = "\n")

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
invisible(dbExecute(con, migration_sql, immediate = TRUE))

print(dbGetQuery(con, "
  SELECT game_type, label, short_label, sort_order
  FROM basketball_test.game_types
  ORDER BY sort_order"))

# Every code the schedule holds must be catalogued, or a game would carry a
# game_type the table cannot name.
uncatalogued <- dbGetQuery(con, "
  SELECT s.game_type, COUNT(*) AS games
  FROM basketball_test.schedule s
  LEFT JOIN basketball_test.game_types gt ON gt.game_type = s.game_type
  WHERE gt.game_type IS NULL
  GROUP BY s.game_type
  ORDER BY s.game_type")
if (nrow(uncatalogued)) {
  print(uncatalogued)
  stop("schedule holds game_type codes missing from game_types; transaction will roll back")
}

if (confirm_apply) {
  dbCommit(con)
  transaction_open <- FALSE
  message("game_types migration committed.")
} else {
  dbRollback(con)
  transaction_open <- FALSE
  message("game_types migration validated and rolled back.")
}
