cache_ddl_path <- repo_file(
  "..", "sql", "materialized_views", "team_ratings_preset_cache.sql"
)
cache_refresh_path <- repo_file(
  "..", "sql", "functions", "refresh_team_ratings_preset_cache_for_games.sql"
)
etl_full_path <- repo_file("..", "etl", "etl_full.R")
rebuild_path <- repo_file("..", "sql", "rebuild_all_mvs.R")

read_sql_contract <- function(path) {
  paste(readLines(path, warn = FALSE), collapse = "\n")
}

test_that("preset cache persists every Team Hub rating variant", {
  ddl <- read_sql_contract(cache_ddl_path)
  refresh <- read_sql_contract(cache_refresh_path)
  variants <- c(
    "starters_hi",
    "starters_lo",
    "clutch",
    "last10",
    "top4",
    "bottom4"
  )

  expect_true(all(vapply(variants, grepl, logical(1), x = ddl, fixed = TRUE)))
  expect_true(all(vapply(variants, grepl, logical(1), x = refresh, fixed = TRUE)))
  expect_match(
    ddl,
    "PRIMARY KEY \\(game_year, preset_variant, team_id\\)"
  )
  expect_match(refresh, "game_ids IS NULL OR s\\.game_id = ANY\\(game_ids\\)")
})

test_that("ETL and full rebuild own the preset cache lifecycle", {
  etl <- read_sql_contract(etl_full_path)
  rebuild <- read_sql_contract(rebuild_path)

  expect_match(etl, "refresh_team_ratings_preset_cache_for_games")
  expect_match(etl, "team_ratings_preset_cache")
  expect_match(
    rebuild,
    'name = "team_ratings_preset_cache".*level = 5, type = "table"'
  )
})
