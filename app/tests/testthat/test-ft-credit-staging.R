library(testthat)

# The free-throw credit migration stages a materialized view under
# <name>__ftc_next by rewriting its definition file. The rewrite must touch
# whole identifiers only (scripts/apply_ft_credit_migration_helpers.R).

source(repo_file("..", "scripts", "apply_ft_credit_migration_helpers.R"), local = TRUE)

definition <- paste(
  "DROP MATERIALIZED VIEW IF EXISTS basketball_test.foo CASCADE;",
  "CREATE MATERIALIZED VIEW basketball_test.foo AS",
  "SELECT a.* FROM basketball_test.foo_archive a JOIN basketball_test.bar b USING (id);",
  "CREATE UNIQUE INDEX foo_pk ON basketball_test.foo (id);",
  "CREATE INDEX foo_pk_wide",
  "  ON basketball_test.foo (id, team_id);",
  sep = "\n")

test_that("staging renames the view and its indexes and removes drops", {
  out <- stage_matview_sql(definition, "basketball_test", "foo", c("foo_pk", "foo_pk_wide"))
  expect_match(out, "CREATE MATERIALIZED VIEW basketball_test.foo__ftc_next AS", fixed = TRUE)
  expect_match(out, "INDEX foo_pk__ftc_next ON basketball_test.foo__ftc_next (id)", fixed = TRUE)
  expect_match(out, "INDEX foo_pk_wide__ftc_next\n  ON basketball_test.foo__ftc_next", fixed = TRUE)
  expect_false(grepl("DROP", out, fixed = TRUE))
})

test_that("a longer name that starts with the staged name is left alone", {
  out <- stage_matview_sql(definition, "basketball_test", "foo", c("foo_pk", "foo_pk_wide"))
  expect_match(out, "FROM basketball_test.foo_archive a", fixed = TRUE)
  expect_false(grepl("foo__ftc_next_archive", out, fixed = TRUE))
  expect_false(grepl("foo_pk__ftc_next_wide", out, fixed = TRUE))
})

test_that("staging stops when the file does not create the view exactly once", {
  expect_error(stage_matview_sql("SELECT 1;", "basketball_test", "foo", character(0)), "exactly one")
  expect_error(stage_matview_sql(paste(definition, definition, sep = "\n"), "basketball_test", "foo",
                                 c("foo_pk", "foo_pk_wide")), "exactly one")
})

test_that("staging stops when a live index is not created by the file", {
  expect_error(stage_matview_sql(definition, "basketball_test", "foo", c("foo_pk", "foo_missing")),
               "foo_missing")
})

test_that("staging stops when the file creates an index the live view does not have", {
  expect_error(stage_matview_sql(definition, "basketball_test", "foo", "foo_pk"), "foo_pk_wide")
})
