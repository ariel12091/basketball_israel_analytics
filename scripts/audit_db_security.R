if (file.exists("app/.Renviron")) {
  readRenviron("app/.Renviron")
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

audit_path <- file.path("sql", "security", "audit_app_access.sql")
if (!file.exists(audit_path)) {
  stop("Run this script from the repository root")
}

audit_sql <- paste(readLines(audit_path, warn = FALSE), collapse = "\n")
port <- suppressWarnings(as.integer(Sys.getenv("PG_PORT", "6543")))

con <- dbConnect(
  Postgres(),
  host = Sys.getenv("PG_HOST"),
  port = port,
  dbname = Sys.getenv("PG_DB"),
  user = Sys.getenv("PG_USER"),
  password = Sys.getenv("PG_PASS"),
  sslmode = Sys.getenv("PG_SSLMODE", "require"),
  connect_timeout = 10
)
on.exit(dbDisconnect(con), add = TRUE)

violations <- dbGetQuery(con, audit_sql)
if (!nrow(violations)) {
  message("Database security audit passed.")
  quit(save = "no", status = 0L)
}

summary <- as.data.frame(
  table(violations$violation),
  stringsAsFactors = FALSE
)
names(summary) <- c("violation", "count")

print(summary, row.names = FALSE)
message("Database security audit failed with ", nrow(violations), " violation(s).")
quit(save = "no", status = 1L)
