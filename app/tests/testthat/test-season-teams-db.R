library(testthat)

# basketball_test.season_teams is the source of truth for the team names the
# dropdowns show. STATIC_TEAM_ROSTERS is a copy generated from it by
# scripts/sync_catalogues.R, kept because the Home page is built before any
# database connection exists. This fails when the nightly sync stopped landing.

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

test_that("the generated season rosters match the season_teams table", {
  expect_setequal(names(pinned), names(catalogued))
  for (gy in names(pinned)) {
    expect_identical(pinned[[gy]], catalogued[[gy]], info = gy)
  }
})

test_that("season_teams covers every team in full_rosters", {
  uncovered <- DBI::dbGetQuery(con, "
    SELECT fr.game_year, fr.team_id, MIN(fr.team_name) AS roster_name
    FROM basketball_test.full_rosters fr
    LEFT JOIN basketball_test.season_teams st
      ON st.game_year = fr.game_year AND st.team_id = fr.team_id
    WHERE st.team_id IS NULL
    GROUP BY fr.game_year, fr.team_id")
  expect_equal(nrow(uncovered), 0,
               info = paste(capture.output(print(uncovered)), collapse = "\n"))
})
