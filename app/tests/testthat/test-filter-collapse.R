# global.R cannot be sourced from a test: at source time it builds the DB pool,
# registers onStop() and schedules a later() prewarm. parse() does not execute,
# so the one definition under test is lifted out and evaluated on its own. That
# keeps this an assertion about rendered output rather than about source text.
filter_chips_row <- local({
  exprs <- parse(repo_file("R", "global.R"))
  env <- new.env(parent = globalenv())
  for (e in exprs) {
    if (is.call(e) && identical(as.character(e[[1]]), "<-") &&
        identical(as.character(e[[2]]), "filter_chips_row")) {
      eval(e, envir = env)
    }
  }
  stopifnot(is.function(env$filter_chips_row))
  env$filter_chips_row
})

test_that("the chips row carries an accessible filter toggle", {
  html <- htmltools::renderTags(filter_chips_row("demo_chips"))$html

  expect_match(html, "js-filters-toggle", fixed = TRUE)
  expect_match(html, 'aria-expanded="true"', fixed = TRUE)
  expect_match(html, "Filters", fixed = TRUE)
})

test_that("the toggle does not become a Shiny input", {
  # Collapse is pure client state. A Shiny input would add a round trip and a
  # second copy of the state to keep in step with the class on <body>.
  html <- htmltools::renderTags(filter_chips_row("demo_chips"))$html

  expect_match(html, 'type="button"', fixed = TRUE)
  expect_false(grepl("action-button", html, fixed = TRUE))
})

test_that("collapse is driven by a body class over tagged columns", {
  css <- read_repo_txt("www", "app.css")
  js <- read_repo_txt("www", "app.js")

  expect_true(grepl("body.filters-collapsed", css, fixed = TRUE))
  expect_true(grepl("ibpl-filter-col", css, fixed = TRUE))
  expect_true(grepl("ibpl-main-col", css, fixed = TRUE))

  # The columns are tagged in JS by looking for the sidebar's .well, so no
  # tab file has to be edited and no reliance on :has() is needed.
  expect_true(grepl("ibpl-filter-col", js, fixed = TRUE))
  expect_true(grepl("ibpl_filters_collapsed", js, fixed = TRUE))
})

test_that("the collapse state is remembered per browser", {
  js <- read_repo_txt("www", "app.js")

  expect_true(grepl("localStorage", js, fixed = TRUE))
  # Storage can throw outright in a private window; reads must be guarded.
  expect_true(grepl("try {", js, fixed = TRUE))
})
