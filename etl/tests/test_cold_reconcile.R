# Unit tests for etl/cold_reconcile.R (pure helpers, no DB).
# Run: Rscript -e "testthat::test_file('etl/tests/test_cold_reconcile.R')"
library(testthat)
# testthat::test_file() sets the wd to this file's directory (etl/tests),
# so the implementation is sourced relative to here.
source("../cold_reconcile.R")

test_that("cold_export_scope = run ids plus hot ids missing from parquet", {
  # run processed 300; 250+251 are leftovers from a failed prior run:
  # 250 already exported (in parquet), 251 never exported -> must be included
  expect_equal(
    cold_export_scope(run_ids = 300L, hot_ids = c(250L, 251L, 300L),
                      parquet_ids = c(100L, 250L)),
    c(251L, 300L)
  )
})

test_that("cold_export_scope with empty run still rescues unexported hot rows", {
  expect_equal(
    cold_export_scope(run_ids = integer(0), hot_ids = c(9L, 7L),
                      parquet_ids = 7L),
    9L
  )
})

test_that("cold_export_scope drops NA and duplicates, sorts ascending", {
  expect_equal(
    cold_export_scope(run_ids = c(5L, NA, 5L), hot_ids = c(5L, 3L),
                      parquet_ids = integer(0)),
    c(3L, 5L)
  )
})

test_that("cold_export_scope with no parquet file (NULL) exports all hot rows", {
  expect_equal(
    cold_export_scope(run_ids = 1L, hot_ids = c(1L, 2L), parquet_ids = NULL),
    c(1L, 2L)
  )
})

test_that("cold_coverage_gaps flags hot ids not in parquet", {
  expect_equal(cold_coverage_gaps(hot_ids = c(1L, 2L, 3L), parquet_ids = c(1L, 3L)), 2L)
  expect_equal(cold_coverage_gaps(hot_ids = c(1L, 3L), parquet_ids = c(1L, 2L, 3L)),
               integer(0))
  expect_equal(cold_coverage_gaps(hot_ids = integer(0), parquet_ids = integer(0)),
               integer(0))
})
