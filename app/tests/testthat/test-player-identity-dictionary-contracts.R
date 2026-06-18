project_txt <- function(...) {
  app_root <- repo_file()
  path <- normalizePath(file.path(app_root, "..", ...), winslash = "/", mustWork = FALSE)
  paste(readLines(path, warn = FALSE), collapse = "\n")
}

identity_helper_txt <- function() {
  project_txt("etl", "player_identity_dictionary.R")
}

test_that("identity dictionary is additive and exposes both resolver views", {
  txt <- identity_helper_txt()

  expect_true(grepl('CREATE TABLE IF NOT EXISTS "%s"."player_identities"', txt, fixed = TRUE))
  expect_true(grepl('CREATE TABLE IF NOT EXISTS "%s"."player_identity_map"', txt, fixed = TRUE))
  expect_true(grepl('"player_identity_aliases_v"', txt, fixed = TRUE))
  expect_true(grepl('"resolved_player_identity_v"', txt, fixed = TRUE))
  expect_true(grepl("START WITH 1000000000000", txt, fixed = TRUE))
})

test_that("identity resolution gives game mappings precedence over season mappings", {
  txt <- identity_helper_txt()

  expect_true(grepl("COALESCE(g.identity_id, y.identity_id", txt, fixed = TRUE))
  expect_true(grepl("WHEN g.map_id IS NOT NULL THEN \\'game\\'", txt, fixed = TRUE))
  expect_true(grepl("WHEN y.map_id IS NOT NULL THEN \\'season\\'", txt, fixed = TRUE))
})

test_that("legacy correction loader reads the dictionary compatibility view", {
  txt <- project_txt("etl", "player_id_aliases.R")
  loader_start <- regexpr("load_player_id_aliases <- function", txt, fixed = TRUE)[[1]]
  expect_gt(loader_start, 0)
  loader_txt <- substr(txt, loader_start, loader_start + 1800)

  expect_true(grepl("player_identity_aliases_v", loader_txt, fixed = TRUE))
  expect_false(grepl('FROM "%s"."player_id_aliases"', loader_txt, fixed = TRUE))
  expect_false(grepl('FROM "%s"."player_id_game_overrides"', loader_txt, fixed = TRUE))
})

test_that("full ETL synchronizes identity mappings before and after base loading", {
  txt <- project_txt("etl", "etl_full.R")

  expect_true(grepl('source("etl/player_identity_dictionary.R")', txt, fixed = TRUE))
  sync_calls <- lengths(regmatches(
    txt,
    gregexpr("sync_player_identity_dictionary\\(pg, SCHEMA\\)", txt)
  ))
  expect_true(sync_calls >= 2L)
  expect_true(grepl("player_identity_ambiguity_summary(pg, SCHEMA)", txt, fixed = TRUE))
})
