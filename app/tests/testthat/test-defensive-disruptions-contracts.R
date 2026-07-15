read_contract_file <- function(...) {
  paste(readLines(repo_file(...), warn = FALSE), collapse = "\n")
}

test_that("defensive disruption SQL uses steals plus deflections", {
  base_sql <- read_contract_file("..", "sql", "materialized_views", "player_four_factors_by_game.sql")
  compute_sql <- read_contract_file("..", "sql", "functions", "four_factors_compute.sql")

  expect_true(grepl("steal_count", base_sql, fixed = TRUE))
  expect_true(grepl("deflection_count", base_sql, fixed = TRUE))
  expect_true(grepl("(a.steal_count + a.deflection_count)::numeric / NULLIF(a.total_poss, 0)::numeric", compute_sql, fixed = TRUE))
  expect_true(grepl('"Def Disruptions/100 Diff"', compute_sql, fixed = TRUE))
})

test_that("defensive disruption migration upgrades incremental tables", {
  migration_sql <- read_contract_file("..", "sql", "migrations", "2026-07-15_add_defensive_disruptions.sql")

  expect_true(grepl("ALTER TABLE basketball_test.player_four_factors_by_game", migration_sql, fixed = TRUE))
  expect_true(grepl("ALTER TABLE basketball_test.player_advanced_stats_mv", migration_sql, fixed = TRUE))
  expect_true(grepl('ADD COLUMN IF NOT EXISTS "Def Disruptions/100 Diff" numeric', migration_sql, fixed = TRUE))
})
