source(file.path("..", "..", "..", "scripts", "minutes_migration_helpers.R"))

test_that("minute verification rejects errors hidden by an overall average", {
  truth <- data.frame(game_id = c(1, 2), team_id = c(10, 10), minutes = c(40, 40))
  expect_true(assert_minute_rows(truth, truth, "fixture"))
  bad <- truth
  bad$minutes <- c(39, 41)
  expect_error(assert_minute_rows(truth, bad, "fixture"), "2 team-games")
  expect_error(assert_minute_rows(truth, truth[1, ], "fixture"), "conservation")
  expect_error(assert_minute_rows(truth, rbind(truth, truth[1, ]), "fixture"), "duplicate")
  bad$minutes <- c(NA, 40)
  expect_error(assert_minute_rows(truth, bad, "fixture"), "conservation")
  expect_error(assert_minute_rows(truth[FALSE, ], truth, "fixture"), "empty")
  rounded <- truth
  rounded$minutes <- c(40.04, 39.96)
  expect_true(assert_minute_rows(truth, rounded, "published", tolerance = 0.050001))
  rounded$minutes[1] <- 39.9
  expect_error(assert_minute_rows(truth, rounded, "published", tolerance = 0.050001), "conservation")
})

test_that("rebuild errors propagate immediately to the transaction owner", {
  executed <- character()
  local_mocked_bindings(
    dbGetQuery = function(...) data.frame(kind = "m"),
    dbQuoteIdentifier = function(...) "basketball_test.fixture",
    dbExecute = function(con, statement, ...) {
      executed <<- c(executed, statement)
      if (grepl("CREATE", statement)) stop("injected DDL failure")
      invisible(0L)
    },
    .package = "DBI"
  )
  targets <- list(list(name = "fixture"), list(name = "later"))
  expect_error(rebuild_minutes_relations(NULL, targets,
    list("CREATE MATERIALIZED VIEW fixture", "CREATE MATERIALIZED VIEW later")),
    "injected DDL failure")
  expect_length(executed, 3L)
  expect_true(all(grepl("RESTRICT$", executed[1:2])))
})
