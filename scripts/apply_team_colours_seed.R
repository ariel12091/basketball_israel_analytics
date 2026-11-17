# Writes the approved club colours into basketball_test.season_teams and
# euroleague.teams (sql/migrations/2026-11-17_team_colours_seed.sql,
# euroleague/sql/066_team_colours_seed.sql). Run after the catalogue migration
# (season_teams_identity) and EuroLeague migration 065 added the columns.
#
# Dry run by default: both seeds run in one transaction, the checks run, and
# it rolls back. Set CONFIRM_TEAM_COLOURS=1 to commit. Re-runnable.
#
# Checks: every season_teams row and every euroleague.teams row has two
# '#RRGGBB' colours afterwards, and every UPDATE matched at least one row (a
# line that matched nothing is a renamed club or a typo).

confirm_apply <- identical(Sys.getenv("CONFIRM_TEAM_COLOURS", "0"), "1")
if (file.exists("etl/.Renviron")) readRenviron("etl/.Renviron")
suppressPackageStartupMessages({
  library(DBI)
  library(RPostgres)
})

seeds <- c(
  file.path("sql", "migrations", "2026-11-17_team_colours_seed.sql"),
  file.path("euroleague", "sql", "066_team_colours_seed.sql")
)
if (!all(file.exists(seeds))) stop("Run this script from the repository root")

con <- dbConnect(
  Postgres(),
  host = Sys.getenv("PG_HOST"),
  port = as.integer(Sys.getenv("PG_DDL_PORT", "5432")),
  dbname = Sys.getenv("PG_DB"),
  user = Sys.getenv("PG_USER"),
  password = Sys.getenv("PG_PASS"),
  sslmode = Sys.getenv("PG_SSLMODE", "require"),
  connect_timeout = 10
)
on.exit(dbDisconnect(con), add = TRUE)
message(if (confirm_apply) "Mode: APPLY" else "Mode: DRY RUN (CONFIRM_TEAM_COLOURS=1 to commit)")

dbBegin(con)
done <- FALSE
on.exit(if (!done) try(dbRollback(con), silent = TRUE), add = TRUE)

unmatched <- character(0)
for (path in seeds) {
  statements <- grep("^UPDATE ", readLines(path, warn = FALSE), value = TRUE)
  for (s in statements) {
    if (dbExecute(con, sub(";.*$", "", s)) < 1) unmatched <- c(unmatched, s)
  }
  message(path, ": ", length(statements), " update(s)")
}
if (length(unmatched)) {
  print(unmatched)
  stop("UPDATE(s) above matched no row; rolling back")
}

hex <- "'^#[0-9A-F]{6}$'"
missing <- rbind(
  dbGetQuery(con, sprintf("
    SELECT 'il' AS league, game_year::text AS season, team_name AS club
    FROM basketball_test.season_teams
    WHERE primary_color IS NULL OR secondary_color IS NULL
       OR primary_color !~ %1$s OR secondary_color !~ %1$s", hex)),
  dbGetQuery(con, sprintf("
    SELECT competition AS league, '' AS season, display_name AS club
    FROM euroleague.teams
    WHERE primary_color IS NULL OR secondary_color IS NULL
       OR primary_color !~ %1$s OR secondary_color !~ %1$s", hex))
)
if (nrow(missing)) {
  print(missing)
  stop("clubs above have no colours; rolling back")
}
print(dbGetQuery(con, "
  SELECT 'il' AS league, count(*) AS rows, count(primary_color) AS coloured, count(logo_url) AS with_logo
  FROM basketball_test.season_teams
  UNION ALL
  SELECT competition, count(*), count(primary_color), count(crest_url)
  FROM euroleague.teams GROUP BY competition ORDER BY 1"))

if (confirm_apply) {
  dbCommit(con)
  message("Club colours committed.")
} else {
  dbRollback(con)
  message("Club colours validated and rolled back.")
}
done <- TRUE
