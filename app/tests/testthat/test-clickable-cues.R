cues_css <- function() read_repo_txt("www", "app.css")

test_that("tabs with a view-mode menu carry a caret and a mode label", {
  js <- read_repo_txt("www", "app.js")
  css <- cues_css()

  expect_true(grepl('className = "tab-mode-label"', js, fixed = TRUE))
  expect_true(grepl("shiny:inputchanged", js, fixed = TRUE))
  expect_true(grepl(".nav-item.tab-has-dropdown > .nav-link::after", css, fixed = TRUE))
  expect_true(grepl(".tab-mode-label", css, fixed = TRUE))
})

test_that("a selectize-backed mode is read and written through selectize", {
  js <- read_repo_txt("www", "app.js")

  # Setting .value on a selectize <select> to an option it has not rendered
  # leaves it blank, which emptied both the menu checkmark and the tab label.
  expect_true(grepl("sel.selectize.setValue(value)", js, fixed = TRUE))
  expect_true(grepl("sel.selectize.getValue()", js, fixed = TRUE))
})

test_that("pivot cells are styled as menu triggers", {
  css <- cues_css()

  # Team and player cells both open the menu, so the caret is not first-child only.
  expect_true(grepl("td[data-pivot-trigger]::after", css, fixed = TRUE))
  expect_false(grepl("td[data-pivot-trigger]:first-child::after", css, fixed = TRUE))
  expect_true(grepl(".ibpl-row-name[data-pivot-trigger]::after", css, fixed = TRUE))
  expect_true(grepl("td[data-pivot-trigger]:hover", css, fixed = TRUE))
})

test_that("everything that opens something shares one Open cue", {
  css <- cues_css()
  open_targets <- c(".ld-lineup-link", ".euro-ld-unit", ".ribbon-link",
                    ".hub-story-line", ".hub-card-go")

  for (sel in open_targets) {
    expect_true(grepl(paste0(sel, "::after"), css, fixed = TRUE), info = sel)
  }
  expect_true(grepl("#cmp_table tbody tr", css, fixed = TRUE))
  expect_true(grepl("#cmp_team_players_table tbody tr", css, fixed = TRUE))
})

test_that("each hub card names where it goes", {
  hub <- read_repo_txt("R", "mod_team_hub.R")

  expect_equal(lengths(regmatches(hub, gregexpr('class = "hub-card-go"', hub, fixed = TRUE))), 3L)
})
