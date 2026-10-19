# global.R builds a pool, registers onStop() and prewarms a checkout at source
# time. The API and the golden harness inject a pool through the
# `ibpl.pg_pool` option instead and must get none of those side effects.

test_that("global.R uses an injected pool and has no side effects", {
  old <- options(ibpl.pg_pool = structure(list(), class = "fake_pool"))
  on.exit(options(old), add = TRUE)
  env <- new.env(parent = globalenv())
  # global.R calls dbPool() and onStop() unqualified, so a stub in the
  # sourcing environment wins over the attached packages.
  env$dbPool <- function(...) stop("called: dbPool")
  env$onStop <- function(...) stop("called: onStop")
  testthat::local_mocked_bindings(
    poolCheckout = function(...) stop("called: poolCheckout"),
    dbPool = function(...) stop("called: pool::dbPool"),
    .package = "pool"
  )
  testthat::local_mocked_bindings(
    onStop = function(...) stop("called: shiny::onStop"),
    .package = "shiny"
  )
  Sys.setenv(POOL_PREWARM = "true")
  expect_error(source(repo_file("R", "catalogues_generated.R"), local = env), NA)
  expect_error(source(repo_file("R", "global.R"), local = env), NA)
  expect_true(inherits(env$pg_pool, "fake_pool"))
})

test_that("global.R parse has exactly one dbPool call, inside the override branch", {
  exprs <- parse(repo_file("R", "global.R"), keep.source = FALSE)
  has_call <- function(e, fn) {
    if (is.call(e)) {
      if (identical(e[[1]], as.name(fn))) return(TRUE)
      return(any(vapply(as.list(e), has_call, logical(1), fn = fn)))
    }
    FALSE
  }
  count_calls <- function(e, fn) {
    if (!is.call(e)) return(0L)
    n <- as.integer(identical(e[[1]], as.name(fn)))
    n + sum(vapply(as.list(e), count_calls, integer(1), fn = fn))
  }
  expect_equal(sum(vapply(as.list(exprs), count_calls, integer(1), fn = "dbPool")), 1L)
  top_with_pool <- Filter(function(e) has_call(e, "dbPool"), as.list(exprs))
  expect_length(top_with_pool, 1L)
  e <- top_with_pool[[1]]
  expect_true(is.call(e) && identical(e[[1]], as.name("if")))
  expect_true(grepl("ibpl.pg_pool", paste(deparse(e[[2]]), collapse = ""), fixed = TRUE))
  expect_true(has_call(e[[3]], "dbPool"))
})
