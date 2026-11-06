test_that("real R wrappers commit, fail, fence old writers and refresh without games", {
  f <- publication_fixture()
  on.exit(f$cleanup(), add = TRUE)
  before <- f$status()
  # No publication opened means no state/version change.
  expect_identical(f$status(), before)
  with_publication("il", schema = f$schema, fn = function(pub) {
    publication_fenced_tx(pub, f$con,
      DBI::dbExecute(f$con, paste0("INSERT INTO ", f$qs, ".fixture VALUES (1)")))
  })
  expect_equal(f$status()$state, "published")
  expect_equal(as.numeric(f$status()$version), 2)
  expect_error(with_publication("il", schema = f$schema, fn = function(pub) {
    publication_fenced_tx(pub, f$con,
      DBI::dbExecute(f$con, paste0("INSERT INTO ", f$qs, ".fixture VALUES (2)")))
    stop("Phase 6 validation failed")
  }), "Phase 6 validation failed")
  expect_equal(f$status()$state, "failed")
  expect_equal(as.numeric(f$status()$version), 3)
  pub <- publication_open("il", schema = f$schema)
  publication_fenced_tx(pub, f$con,
    DBI::dbExecute(f$con, paste0("INSERT INTO ", f$qs, ".fixture VALUES (3)")))
  # One game committed; the next game's transaction rolls back.
  expect_error(publication_fenced_tx(pub, f$con, {
    DBI::dbExecute(f$con, paste0("INSERT INTO ", f$qs, ".fixture VALUES (999)"))
    stop("base game failed")
  }), "base game failed")
  publication_close(pub, FALSE)
  expect_equal(f$status()$state, "failed")
  expect_equal(as.numeric(f$status()$version), 4)
  expect_equal(DBI::dbGetQuery(f$con, paste0("SELECT n FROM ", f$qs, ".fixture ORDER BY n"))$n, 1:3)
  old <- publication_open("il", schema = f$schema)
  DBI::dbDisconnect(old$con)
  expect_equal(f$status()$state, "stale")
  current <- publication_open("il", schema = f$schema)
  expect_error(publication_fenced_tx(old, f$con,
    DBI::dbExecute(f$con, paste0("INSERT INTO ", f$qs, ".fixture VALUES (999)"))), "fenced_out")
  old$closed <- TRUE
  publication_close(current, TRUE)
  # Execute the same extracted no-new-game refresh used by etl_full.
  DBI::dbExecute(f$con, paste0("CREATE FUNCTION ", f$qs,
    ".refresh_sub_lineups_stats() RETURNS int LANGUAGE sql AS $$ INSERT INTO ", f$qs,
    ".fixture VALUES (4) RETURNING n $$"), immediate = TRUE)
  with_publication("il", schema = f$schema, fn = function(pub) {
    publication_refresh_sub_lineups(pub, f$con)
  })
  expect_equal(f$status()$state, "published")
  expect_equal(DBI::dbGetQuery(f$con, paste0("SELECT max(n) AS n FROM ", f$qs, ".fixture"))$n, 4L)
})
