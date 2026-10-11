# Lifted out of global.R rather than sourced -- see helper-global-defs.R.
filter_chips_row <- global_defs("filter_chips_row")$filter_chips_row

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

test_that("only tabs with their own toggle have collapsible sidebars", {
  js <- read_repo_txt("www", "app.js")
  compare_ui <- read_repo_txt("R", "ui_tab7_compare.R")

  # Compare intentionally keeps its own large two-sided filter UI and does not
  # render filter_chips_row(). A collapse choice made elsewhere must therefore
  # not hide Compare's sidebar with no local way to restore it.
  expect_true(grepl('pane.querySelector(".js-filters-toggle")', js, fixed = TRUE))
  expect_true(grepl('uiOutput("cmp_filter_chips")', compare_ui, fixed = TRUE))
  expect_false(grepl('filter_chips_row("cmp_filter_chips")', compare_ui, fixed = TRUE))
})

test_that("the collapse state is remembered per browser", {
  js <- read_repo_txt("www", "app.js")

  expect_true(grepl("localStorage", js, fixed = TRUE))
  # Storage can throw outright in a private window; reads must be guarded.
  expect_true(grepl("try {", js, fixed = TRUE))
})
