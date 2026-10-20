library(testthat)
# Every relation, column and function the web foundation's SQL names must
# exist (inventory defect 1: app_api.game_logs_il filtered
# df_pts_poss_lineups_longer_mv on a game_year column that view does not
# have, so the deploy would have failed).
#
# Part 1 is static (no DB). Part 2 deploys sql/publication/001_publication.sql
# and sql/app_api/010+ into ONE scratch pubtest_* schema as the object owner
# (review R3: the live basketball_test.publication and app_api are never
# touched; the schema is dropped afterwards), which validates every LANGUAGE
# sql body at CREATE, then calls every reader for a real season, which
# validates the plpgsql bodies and proves parity with Shiny's own fetchers.
# It runs before the production apply.

read_sql <- function(...) paste(readLines(repo_file("..", "sql", ...), warn = FALSE),
                                collapse = "\n")
app_api_files <- function() {
  sort(list.files(repo_file("..", "sql", "app_api"), "\\.sql$", full.names = TRUE))
}

# Quoted names in the list that follows start_marker, up to end_marker.
sql_list_between <- function(sql, start_marker, end_marker) {
  start <- regexpr(start_marker, sql, fixed = TRUE)
  expect_gt(start, 0L)
  rest <- substring(sql, start + attr(start, "match.length"))
  block <- substring(rest, 1L, regexpr(end_marker, rest, fixed = TRUE) - 1L)
  gsub("'", "", regmatches(block, gregexpr("'[a-z_]+'", block))[[1]], fixed = TRUE)
}

# CREATE FUNCTION blocks of one file: name and full text up to its $fn$ end.
sql_function_blocks <- function(text) {
  starts <- gregexpr("CREATE OR REPLACE FUNCTION [a-z_]+\\.[a-z_]+\\(", text)[[1]]
  if (starts[[1]] < 0) return(list())
  ends <- c(starts[-1] - 1L, nchar(text))
  lapply(seq_along(starts), function(i) {
    block <- substr(text, starts[[i]], ends[[i]])
    list(name = sub("^CREATE OR REPLACE FUNCTION [a-z_]+\\.([a-z_]+)\\(.*$", "\\1",
                    strsplit(block, "\n", fixed = TRUE)[[1]][[1]]),
         text = block)
  })
}

test_that("every app_api function pins search_path, plan mode and STABLE", {
  blocks <- unlist(lapply(app_api_files(), function(path) {
    sql_function_blocks(paste(readLines(path, warn = FALSE), collapse = "\n"))
  }), recursive = FALSE)
  expect_setequal(vapply(blocks, `[[`, "", "name"),
                  c("version", "scoreless_games_il", "game_logs_il", "game_logs_euro"))
  for (b in blocks) {
    expect_match(b$text, "SET search_path = ''", fixed = TRUE, info = b$name)
    expect_match(b$text, "SET plan_cache_mode = force_custom_plan", fixed = TRUE, info = b$name)
    expect_match(b$text, "\\bSTABLE\\b", info = b$name)
    expect_false(grepl("SECURITY DEFINER", b$text, fixed = TRUE), info = b$name)
  }
})

test_that("the functions app_api defines are exactly its allowlist (review R9)", {
  defined <- unlist(lapply(app_api_files(), function(path) {
    vapply(sql_function_blocks(paste(readLines(path, warn = FALSE), collapse = "\n")),
           `[[`, "", "name")
  }))
  hardening <- read_sql("security", "enable_readonly_rls.sql")
  allowlisted <- sql_list_between(hardening, "app_api_function_names constant text[] := ARRAY[",
                                  "]::text[];")
  expect_setequal(defined, allowlisted)
})

test_that("the publication readers and only they are on the Israeli allowlist", {
  pub <- read_sql("publication", "001_publication.sql")
  defined <- vapply(sql_function_blocks(gsub("__SCHEMA__", "s", pub, fixed = TRUE)),
                    `[[`, "", "name")
  expect_setequal(defined, c("publication_lock_key", "begin_publication", "fence_publication",
                             "finish_publication", "publication_status"))
  hardening <- read_sql("security", "enable_readonly_rls.sql")
  allowlisted <- sql_list_between(hardening, "  app_function_names constant text[] := ARRAY[", "];")
  expect_true(all(c("publication_status", "publication_lock_key") %in% allowlisted))
  expect_false(any(c("begin_publication", "fence_publication", "finish_publication") %in%
                     allowlisted))
})

test_that("the app_api files and apply script state the deploy order", {
  for (path in app_api_files()[-1]) {
    expect_match(paste(readLines(path, warn = FALSE), collapse = "\n"),
                 "apply_publication.R", fixed = TRUE, info = basename(path))
  }
  web <- paste(readLines(repo_file("..", "scripts", "apply_web_foundation.R")), collapse = "\n")
  pub <- paste(readLines(repo_file("..", "scripts", "apply_publication.R")), collapse = "\n")
  expect_match(web, "DEPLOY ORDER", fixed = TRUE)
  expect_match(pub, "DEPLOY ORDER", fixed = TRUE)
  # An apply never stages the publication SQL itself; the refusal runs before
  # the transaction that deploys anything.
  expect_match(web, "stage_publication <- !confirm &&", fixed = TRUE)
  expect_lt(regexpr("assert_publication_applied(con)", web, fixed = TRUE)[[1]],
            regexpr("dbBegin(con)", web, fixed = TRUE)[[1]])
})

test_that("apply_web_foundation refuses without the publication objects", {
  env <- new.env()
  sys.source(repo_file("..", "scripts", "apply_web_foundation.R"), envir = env)
  # Stand-in for DBI::dbGetQuery: answers to_regclass / to_regprocedure.
  query_with <- function(present) function(con, sql, params) {
    data.frame(ok = params[[1]] %in% present)
  }
  expect_length(env$missing_publication_objects(NULL, query_with(character(0))), 3L)
  expect_error(env$assert_publication_applied(NULL, query_with(character(0))),
               "apply_publication.R")
  all_present <- c("basketball_test.publication", "basketball_test.publication_status(text)",
                   "basketball_test.publication_lock_key(text)")
  expect_identical(env$missing_publication_objects(NULL, query_with(all_present)),
                   character(0))
  expect_identical(env$missing_publication_objects(NULL, query_with(all_present[-2])),
                   "basketball_test.publication_status(text)")
})

# ---- Part 2: deploy into a scratch schema and call everything ---------------

test_that("the SQL deploys into a scratch schema and every reader matches Shiny", {
  creds <- db_role_require("owner")
  con <- db_role_connect(creds, bigint = "numeric")
  withr::defer(DBI::dbDisconnect(con))
  schema <- paste0("pubtest_", Sys.getpid(), "_", sample.int(1e8, 1))
  qs <- as.character(DBI::dbQuoteIdentifier(con, schema))
  live_before <- DBI::dbGetQuery(con,
    "SELECT to_regclass('basketball_test.publication') IS NOT NULL AS pub,
            to_regnamespace('app_api') IS NOT NULL AS api")
  DBI::dbExecute(con, paste("CREATE SCHEMA", qs))
  withr::defer({
    DBI::dbExecute(con, paste("DROP SCHEMA", qs, "CASCADE"))
    # Nothing outside the scratch schema was created.
    expect_identical(DBI::dbGetQuery(con,
      "SELECT to_regclass('basketball_test.publication') IS NOT NULL AS pub,
              to_regnamespace('app_api') IS NOT NULL AS api"), live_before)
  })
  DBI::dbExecute(con, "SET check_function_bodies = on")
  DBI::dbExecute(con, "SET statement_timeout = '120s'")

  pub <- gsub("__SCHEMA__", qs, read_sql("publication", "001_publication.sql"), fixed = TRUE)
  DBI::dbExecute(con, pub, immediate = TRUE)

  # The app_api files, retargeted at the scratch schema. 000_schema.sql only
  # creates and grants the live app_api schema, so it is not replayed here.
  retarget <- function(text) {
    text <- gsub("basketball_test.publication", paste0(qs, ".publication"), text, fixed = TRUE)
    gsub("app_api.", paste0(qs, "."), text, fixed = TRUE)
  }
  for (path in app_api_files()[-1]) {
    sql <- retarget(paste(readLines(path, warn = FALSE), collapse = "\n"))
    expect_error(DBI::dbExecute(con, sql, immediate = TRUE), NA, info = basename(path))
  }

  # The check can fail: the defective body is rejected at CREATE.
  expect_error(DBI::dbExecute(con, paste0(
    "CREATE FUNCTION ", qs, ".broken_scoreless(p_season int) RETURNS bigint
     LANGUAGE sql STABLE AS $fn$ SELECT count(*) FROM basketball_test.df_pts_poss_lineups_longer_mv
     WHERE game_year = p_season $fn$"), immediate = TRUE), "game_year")

  # The completed seasons the goldens use (plan, Global Constraints).
  expect_app_api_game_logs_parity(con, schema, il_season = 2026L, euro_season = 2025L)
})
