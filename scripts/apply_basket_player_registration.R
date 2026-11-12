# Create and fill basketball_test.basket_player_registration
# (sql/migrations/2026-11-12_basket_player_registration.sql) and prove the fill
# is a faithful copy: every stats row equals its basket_player_season row and
# every roster-only row its basket_player_roster row, column by column.
#
# Dry run by default: one transaction, every gate, then ROLLBACK.
# CONFIRM_BASKET_REGISTRATION=1 commits. Afterwards re-run
# CONFIRM_DB_SECURITY_APPLY=1 scripts/apply_db_security.R (grant + RLS).

confirm_apply <- identical(Sys.getenv("CONFIRM_BASKET_REGISTRATION", "0"), "1")
readRenviron("etl/.Renviron")
suppressPackageStartupMessages({ library(DBI); library(RPostgres) })
con <- dbConnect(Postgres(), host = Sys.getenv("PG_HOST"), port = 5432L,
  dbname = Sys.getenv("PG_DB"), user = Sys.getenv("PG_USER"),
  password = Sys.getenv("PG_PASS"), sslmode = "require", bigint = "numeric")
on.exit(dbDisconnect(con))
q <- function(sql) dbGetQuery(con, sql)
one <- function(sql) q(sql)[[1]][1]
gate <- function(ok, what) {
  cat(if (ok) "ok  " else "FAIL", what, "\n")
  if (!ok) stop(what)
}

reg <- "basketball_test.basket_player_registration"
season <- "basketball_test.basket_player_season"
roster <- "basketball_test.basket_player_roster"
# Columns each source table shares with the registration table.
season_cols <- c("player_name", "player_name_en", "basket_team_name", "position_name",
  "position_en", "height_m", "date_of_birth", "nationality_name", "nationality_code",
  "source_url", "source_content_md5", "fetched_at", "run_id", "profile_source_url",
  "profile_source_content_md5", "profile_fetched_at", "profile_cache_hit",
  "identity_id", "canonical_player_id", "identity_match_status",
  "identity_matched_on", "identity_matched_at", "created_at", "updated_at")
roster_cols <- c("player_name", "player_name_en", "basket_team_name", "basket_team_id",
  "jersey", "position_name", "position_en", "height_m", "date_of_birth",
  "source_url", "source_content_md5", "fetched_at",
  "identity_id", "canonical_player_id", "identity_match_status",
  "identity_matched_on", "identity_matched_at", "created_at", "updated_at")
differences <- function(src, cols, source) {
  counts <- vapply(cols, function(col) as.numeric(one(sprintf(
    "SELECT count(*) FROM %s r JOIN %s s USING (game_year, basket_player_id)
      WHERE r.source = '%s' AND r.%s IS DISTINCT FROM s.%s",
    reg, src, source, col, col))), numeric(1))
  counts
}

cat("mode:", if (confirm_apply) "APPLY (commits)" else "DRY RUN (rolls back)", "\n")
sql <- paste(readLines("sql/migrations/2026-11-12_basket_player_registration.sql",
  warn = FALSE, encoding = "UTF-8"), collapse = "\n")
dbExecute(con, "BEGIN")
ok <- FALSE
tryCatch({
  dbExecute(con, "SET LOCAL lock_timeout = '5s'")
  dbExecute(con, "SET LOCAL statement_timeout = '120s'")
  dbExecute(con, sql, immediate = TRUE)

  n_season <- as.numeric(one(sprintf("SELECT count(*) FROM %s", season)))
  n_roster_only <- as.numeric(one(sprintf(
    "SELECT count(*) FROM %s r WHERE NOT EXISTS (SELECT 1 FROM %s s
      WHERE s.game_year = r.game_year AND s.basket_player_id = r.basket_player_id)", roster, season)))
  n_reg <- as.numeric(one(sprintf("SELECT count(*) FROM %s", reg)))
  cat(sprintf("rows: season %d + roster-only %d = %d; registration %d\n",
    n_season, n_roster_only, n_season + n_roster_only, n_reg))
  gate(n_reg == n_season + n_roster_only, "row count = season + roster-only")
  gate(as.numeric(one(sprintf("SELECT count(*) FROM %s s LEFT JOIN %s r USING (game_year, basket_player_id)
    WHERE r.source IS DISTINCT FROM 'stats'", season, reg))) == 0, "every season row is a stats row")
  gate(as.numeric(one(sprintf("SELECT count(*) FROM %s r JOIN %s x USING (game_year, basket_player_id)
    WHERE r.source = 'roster'", reg, roster))) == n_roster_only, "every roster-only row is a roster row")

  d_season <- differences(season, season_cols, "stats")
  d_roster <- differences(roster, roster_cols, "roster")
  gate(all(d_season == 0), sprintf("stats rows equal basket_player_season on %d columns", length(season_cols)))
  gate(all(d_roster == 0), sprintf("roster rows equal basket_player_roster on %d columns", length(roster_cols)))
  if (any(d_season != 0)) print(d_season[d_season != 0])
  if (any(d_roster != 0)) print(d_roster[d_roster != 0])

  print(q(sprintf("SELECT game_year, source, count(*) AS rows, count(height_m) AS height,
      count(*) FILTER (WHERE identity_match_status = 'verified') AS verified
    FROM %s GROUP BY 1, 2 ORDER BY 1, 2", reg)))
  ok <- TRUE
}, error = function(e) cat("FAILED:", conditionMessage(e), "\n"))

if (ok && confirm_apply) { dbExecute(con, "COMMIT"); cat("COMMITTED\n") } else { dbExecute(con, "ROLLBACK"); cat("ROLLED BACK\n") }
if (!ok) quit(status = 1)
