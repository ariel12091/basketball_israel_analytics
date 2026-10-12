library(testthat)

# basketball_test.season_teams is the source of truth for the team names the
# dropdowns show. STATIC_TEAM_ROSTERS is a copy, kept because the Home page is
# built before any database connection exists; this pins the copy to the table.

skip_if_not(Sys.getenv("RUN_DB_TESTS", "0") == "1")
skip_if_not_installed("DBI")
skip_if_not_installed("RPostgres")

pg_env <- c("PG_HOST", "PG_PORT", "PG_DB", "PG_USER", "PG_PASS")
missing_env <- pg_env[!nzchar(Sys.getenv(pg_env))]
if (length(missing_env)) {
  skip(paste("Missing DB env:", paste(missing_env, collapse = ", ")))
}

.season_team_defs <- global_defs("STATIC_TEAM_ROSTERS")

con <- DBI::dbConnect(
  RPostgres::Postgres(),
  host = Sys.getenv("PG_HOST"),
  port = as.integer(Sys.getenv("PG_PORT")),
  dbname = Sys.getenv("PG_DB"),
  user = Sys.getenv("PG_USER"),
  password = Sys.getenv("PG_PASS"),
  sslmode = Sys.getenv("PG_SSLMODE", unset = "require")
)
on.exit(DBI::dbDisconnect(con), add = TRUE)

by_team_id <- function(df) {
  out <- data.frame(
    team_id = as.integer(df$team_id),
    team_name = as.character(df$team_name),
    stringsAsFactors = FALSE
  )
  out <- out[order(out$team_id), , drop = FALSE]
  rownames(out) <- NULL
  out
}

season_teams <- DBI::dbGetQuery(con, "
  SELECT game_year, team_id, team_name
  FROM basketball_test.season_teams")
catalogued <- lapply(split(season_teams, season_teams$game_year), by_team_id)
pinned <- lapply(.season_team_defs$STATIC_TEAM_ROSTERS, by_team_id)

test_that("every pinned season roster matches the season_teams table", {
  expect_true(all(names(pinned) %in% names(catalogued)))
  for (gy in names(pinned)) {
    expect_identical(pinned[[gy]], catalogued[[gy]], info = gy)
  }
})

test_that("seasons without a pinned roster match what full_rosters serves", {
  # fetch_teams_distinct() falls back to full_rosters for these seasons.
  unpinned <- setdiff(names(catalogued), names(pinned))
  skip_if(!length(unpinned), "every catalogued season has a pinned roster")

  rosters <- DBI::dbGetQuery(con, "
    SELECT DISTINCT game_year, team_id, team_name
    FROM basketball_test.full_rosters")
  served <- lapply(split(rosters, rosters$game_year), by_team_id)

  for (gy in unpinned) {
    expect_identical(catalogued[[gy]], served[[gy]], info = gy)
  }
})
