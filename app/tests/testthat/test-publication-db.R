test_that("publication lock, stale takeover and row fences serialize writers", {
  f <- publication_fixture()
  on.exit(f$cleanup(), add = TRUE)
  a <- publication_connect(); b <- publication_connect(); w <- publication_connect()
  on.exit({ for (con in list(a, b, w)) if (DBI::dbIsValid(con)) DBI::dbDisconnect(con) }, add = TRUE)
  arun <- "00000000-0000-0000-0000-000000000001"
  brun <- "00000000-0000-0000-0000-000000000002"
  call <- function(con, fn, args, params = list()) {
    DBI::dbGetQuery(con, publication_sql(con, f$schema, fn, args), params = params)
  }
  initial <- f$status()
  expect_identical(initial$state, "published")
  expect_equal(as.numeric(initial$version), 1)
  expect_error(call(a, "begin_publication", "$1, $2::uuid", list("il", arun)), "advisory lock required")
  call(a, "publication_lock_key", "$1", list("il"))
  lock <- paste0("SELECT pg_advisory_lock(", f$qs, ".publication_lock_key('il'))")
  # (h) A run that takes the lock but never begins changes nothing.
  DBI::dbGetQuery(a, lock)
  DBI::dbGetQuery(a, paste0("SELECT pg_advisory_unlock(", f$qs, ".publication_lock_key('il'))"))
  expect_identical(f$status(), initial)
  DBI::dbGetQuery(a, lock)
  call(a, "begin_publication", "$1, $2::uuid", list("il", arun))
  expect_false(DBI::dbGetQuery(b, paste0("SELECT pg_try_advisory_lock(", f$qs,
    ".publication_lock_key('il')) AS ok"))$ok[[1]])
  expect_equal(f$status()$state, "publishing")
  DBI::dbBegin(w)
  call(w, "fence_publication", "$1, $2::uuid", list("il", arun))
  DBI::dbExecute(w, paste0("INSERT INTO ", f$qs, ".fixture VALUES (1)"))
  # Losing only the session lock leaves the in-flight fenced writer alive.
  DBI::dbDisconnect(a)
  expect_equal(f$status()$state, "stale")
  DBI::dbGetQuery(b, lock)
  DBI::dbExecute(b, "SET lock_timeout = '100ms'")
  expect_error(call(b, "begin_publication", "$1, $2::uuid", list("il", brun)), "lock timeout")
  DBI::dbCommit(w)
  call(b, "begin_publication", "$1, $2::uuid", list("il", brun))
  expect_equal(as.numeric(f$status()$version), 2)
  DBI::dbBegin(w)
  expect_error(call(w, "fence_publication", "$1, $2::uuid", list("il", arun)), "fenced_out")
  DBI::dbRollback(w)
  expect_error(call(w, "finish_publication", "$1, $2::uuid, true", list("il", arun)), "fenced_out")
  call(b, "finish_publication", "$1, $2::uuid, true", list("il", brun))
  expect_equal(f$status()$state, "published")
  expect_equal(as.numeric(f$status()$version), 3)
  expect_equal(DBI::dbGetQuery(w, paste0("SELECT n FROM ", f$qs, ".fixture"))$n, 1L)
  # (g) A run that finishes not-ok ends failed and still advances the version.
  crun <- "00000000-0000-0000-0000-000000000003"
  call(b, "begin_publication", "$1, $2::uuid", list("il", crun))
  expect_equal(call(b, "finish_publication", "$1, $2::uuid, false", list("il", crun))[[1]], 4)
  expect_equal(f$status()$state, "failed")
  expect_equal(as.numeric(f$status()$version), 4)
  # Leagues are independent rows and locks.
  expect_identical(f$status("euroleague")$state, "published")
  expect_equal(as.numeric(f$status("euroleague")$version), 1)
})
