# Lifted out of global.R rather than sourced -- see helper-global-defs.R.
.chip_defs <- global_defs("make_chip", "make_season_chip")
make_chip <- .chip_defs$make_chip
make_season_chip <- .chip_defs$make_season_chip

test_that("make_chip carries a focus target when given one", {
  with_focus <- htmltools::renderTags(
    make_chip("Wins", "tr_clear_outcome", "chip-game", focus_id = "tr_outcome")
  )$html
  without <- htmltools::renderTags(
    make_chip("Wins", "tr_clear_outcome", "chip-game")
  )$html

  expect_match(with_focus, 'data-chip-focus="tr_outcome"', fixed = TRUE)
  expect_match(with_focus, "chip-focusable", fixed = TRUE)
  expect_false(grepl("data-chip-focus", without, fixed = TRUE))
})

test_that("clearing a chip is still reachable independently of focusing it", {
  html <- htmltools::renderTags(
    make_chip("Wins", "tr_clear_outcome", "chip-game", focus_id = "tr_outcome")
  )$html

  # The x keeps its own event id; focusing must not swallow the clear.
  expect_match(html, 'data-shiny-event="tr_clear_outcome"', fixed = TRUE)
})

test_that("the season chip is not focusable", {
  # Season lives in the navbar, not the filter panel, and is never cleared.
  html <- htmltools::renderTags(make_season_chip("2026"))$html

  expect_false(grepl("data-chip-focus", html, fixed = TRUE))
  expect_false(grepl("chip-x", html, fixed = TRUE))
})

test_that("app.js opens the panel before focusing a hidden control", {
  js <- read_repo_txt("www", "app.js")

  expect_true(grepl("data-chip-focus", js, fixed = TRUE))
  expect_true(grepl("chipFocus", js, fixed = TRUE))
  # A control inside a collapsed panel cannot take focus, so the panel has to
  # be opened first.
  expect_true(grepl("filters-collapsed", js, fixed = TRUE))
})
