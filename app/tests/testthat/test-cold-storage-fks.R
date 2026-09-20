library(testthat)

source(repo_file("..", "etl", "cold_storage.R"), local = TRUE)

# `subs` was promoted out of cold storage on 2026-09-19 so the Shiny ribbon can
# read substitution evidence at request time. Cold tables are empty between ETL
# runs, which is why the reader could never use it before.

test_that("subs is not truncated by the cold purge", {
  expect_false("subs" %in% COLD_TABLES)
  expect_setequal(COLD_TABLES, c("actions_clean", "possessions", "pws", "stints"))
})

test_that("subs stays restorable from its historical parquet", {
  # The purge no longer exports it, but ~79k rows of history live in
  # exports/cold/subs.parquet and must still load through the same helper.
  expect_true("subs" %in% names(COLD_TABLE_KEYS))
})

test_that("every hot table holding an FK into cold actions_clean is registered", {
  # A hot table referencing actions_clean blocks TRUNCATE unless its
  # constraint is dropped first. Forgetting one fails Phase 7 outright.
  expect_setequal(names(HOT_FKS_INTO_COLD), c("lineups_lookup", "subs"))
})

test_that("the purge drops every registered constraint", {
  sql <- cold_fk_drop_sql("basketball_test")

  expect_equal(length(sql), length(HOT_FKS_INTO_COLD))
  expect_true(all(grepl("DROP CONSTRAINT IF EXISTS", sql)))
  expect_true(any(grepl("lineups_lookup_actions_clean_fk", sql, fixed = TRUE)))
  expect_true(any(grepl("subs_actions_clean_fk", sql, fixed = TRUE)))
})

test_that("the purge re-adds every registered constraint as NOT VALID", {
  # NOT VALID is the whole point: the referenced rows are gone by design, but
  # rows inserted by a later ETL run still validate, because Phase 2 writes
  # them while actions_clean holds that game.
  sql <- cold_fk_readd_sql("basketball_test")

  expect_equal(length(sql), length(HOT_FKS_INTO_COLD))
  expect_true(all(grepl("ADD CONSTRAINT", sql)))
  expect_true(all(grepl("NOT VALID", sql)))
})

test_that("each constraint keeps its own column order", {
  # lineups_lookup references (game_id, id); subs references (id, game_id).
  # A copy-paste between the two would be accepted by Postgres only if a
  # matching unique index existed, and would otherwise fail Phase 7 nightly.
  sql <- cold_fk_readd_sql("basketball_test")
  lineups <- sql[grepl("lineups_lookup", sql, fixed = TRUE)]
  subs <- sql[grepl('"subs"', sql, fixed = TRUE)]

  expect_match(lineups, "FOREIGN KEY (game_id, id)", fixed = TRUE)
  expect_match(subs, "FOREIGN KEY (id, game_id)", fixed = TRUE)
})

test_that("the subs constraint keeps its ON DELETE CASCADE", {
  # Preserved from the live definition rather than re-invented.
  sql <- cold_fk_readd_sql("basketball_test")
  expect_match(sql[grepl('"subs"', sql, fixed = TRUE)], "ON DELETE CASCADE", fixed = TRUE)
})

test_that("both statement builders name the schema they are given", {
  expect_true(all(grepl("other_schema", cold_fk_drop_sql("other_schema"))))
  expect_true(all(grepl("other_schema", cold_fk_readd_sql("other_schema"))))
})

test_that("restore_cold_table still accepts subs, which export no longer does", {
  # The guards deliberately differ: export walks COLD_TABLES (subs is not
  # exported any more), restore walks names(COLD_TABLE_KEYS) so the historical
  # parquet stays loadable. Reaching the missing-file error proves the name
  # guard passed; a rejected name would stop() before any file is touched.
  expect_error(
    restore_cold_table(NULL, "basketball_test", "subs", cold_dir = tempfile()),
    "No Parquet found"
  )
  expect_error(
    restore_cold_table(NULL, "basketball_test", "not_a_table", cold_dir = tempfile()),
    "table_name"
  )
})

test_that("export_cold_table refuses subs, because the purge no longer exports it", {
  expect_error(
    export_cold_table(NULL, "basketball_test", "subs", tempfile(), function(...) NULL),
    "table_name"
  )
})
