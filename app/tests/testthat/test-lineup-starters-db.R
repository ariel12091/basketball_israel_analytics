library(testthat)

# Lineups' "# Starters" (docs/shared_definitions.md, "Starters, lineup column"):
# how many of the group's OWN players started each game, weighted by the
# group's possessions. The stored path (refresh_sub_lineups*) and the two live
# paths of fetch_lineups_all compute it separately, so they are pinned against
# each other here. Until 2026-11-01 all three counted the starters among all
# five on the floor, and a 3-man group showed 4.

skip_if_not(Sys.getenv("RUN_DB_TESTS", "0") == "1")
skip_if_not_installed("DBI")
skip_if_not_installed("RPostgres")

pg_env <- c("PG_HOST", "PG_PORT", "PG_DB", "PG_USER", "PG_PASS")
missing_env <- pg_env[!nzchar(Sys.getenv(pg_env))]
if (length(missing_env)) {
  skip(paste("Missing DB env:", paste(missing_env, collapse = ", ")))
}

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

season <- 2026L

readers <- function(size, extra = "") DBI::dbGetQuery(con, sprintf(
  "SELECT team_id, sub_lineup_hash, num_starters, off_poss, def_poss
   FROM basketball_test.fetch_lineups_all(p_num_lineup := %d::int2, p_game_year := %d,
                                           p_min_poss := 0%s)", size, season, extra))

test_that("no stored group shows more starters than it has players", {
  over <- DBI::dbGetQuery(con, "
    SELECT COUNT(*) AS n FROM basketball_test.sub_lineups_stats
    WHERE starters_poss_num / NULLIF(off_poss + def_poss, 0) > num_lineup + 1e-9")
  expect_equal(as.numeric(over$n), 0)
})

test_that("the fast and filtered paths give the same # Starters", {
  # Every game type selected: the same games, through the filtered branch.
  types <- DBI::dbGetQuery(con, sprintf(
    "SELECT string_agg(DISTINCT game_type::text, ',') AS t FROM basketball_test.schedule WHERE game_year = %d",
    season))$t
  for (size in c(3L, 5L)) {
    fast <- readers(size)
    filtered <- readers(size, sprintf(", p_game_type_csv := '%s'", types))
    j <- merge(fast, filtered, by = c("team_id", "sub_lineup_hash"), suffixes = c("_fast", "_filt"))
    expect_gt(nrow(j), 0)
    same_poss <- j$off_poss_fast == j$off_poss_filt & j$def_poss_fast == j$def_poss_filt
    expect_equal(j$num_starters_fast[same_poss], j$num_starters_filt[same_poss], info = paste("size", size))
  }
})

test_that("the clutch path stays within the group size", {
  clutch <- readers(3L, ", p_max_margin := 5, p_max_time_remaining := 300")
  expect_gt(nrow(clutch), 0)
  expect_true(all(clutch$num_starters <= 3 + 1e-9, na.rm = TRUE))
})
