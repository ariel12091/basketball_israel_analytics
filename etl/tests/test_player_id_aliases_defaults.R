# Unit tests for default_player_id_aliases() canonicalize_base flags (pure, no DB).
# Run: Rscript -e "testthat::test_file('etl/tests/test_player_id_aliases_defaults.R')"
library(testthat)
# testthat::test_file() sets the wd to this file's directory (etl/tests),
# so the implementation is sourced relative to here.
source("../player_id_aliases.R")

test_that("default aliases carry a logical canonicalize_base column", {
  aliases <- default_player_id_aliases()
  expect_true("canonicalize_base" %in% names(aliases))
  expect_type(aliases$canonicalize_base, "logical")
  expect_false(anyNA(aliases$canonicalize_base))
})

test_that("cross-team season re-mints are sync-only (canonicalize_base = FALSE)", {
  aliases <- default_player_id_aliases()
  cross_team <- aliases$alias_player_id %in% c(2046L, 2052L, 1982L)
  # Altshuler / Avivi / Burns: identity-dictionary merge only; base data
  # deliberately keeps the re-minted ids (Tabs 1-4 are team-scoped).
  expect_true(all(!aliases$canonicalize_base[cross_team]))
  # Same-team duplicates pollute that team's lineups and MUST be scrubbed.
  expect_true(all(aliases$canonicalize_base[!cross_team]))
})
