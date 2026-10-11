hint_part <- function(html, cls) {
  m <- regmatches(html, regexec(sprintf('<span class="%s">([^<]*)</span>', cls), html))[[1L]]
  if (length(m) < 2L) NA_character_ else m[[2L]]
}

test_that("table_hint writes one phrase per key, in order", {
  pointer <- hint_part(as.character(table_hint("pivot", "sort")), "hint-pointer")

  expect_match(pointer, "Click a team or player for lineups and game logs", fixed = TRUE)
  expect_match(pointer, "Click a column header to sort", fixed = TRUE)
  expect_lt(regexpr("team or player", pointer), regexpr("to sort", pointer))
})

test_that("touch wording names the action that really explains a header", {
  html <- as.character(table_hint("lineup", "explain"))

  expect_match(hint_part(html, "hint-pointer"),
               "Hover over a column header for its definition", fixed = TRUE)
  touch <- hint_part(html, "hint-touch")
  expect_match(touch, "Tap a lineup for its game log", fixed = TRUE)
  expect_match(touch, "info button", fixed = TRUE)
  # A plain tap on a header sorts, so the touch line must not promise otherwise.
  expect_false(grepl("Tap a column header for its definition", touch, fixed = TRUE))
})

test_that("pointer-only interactions are left out of the touch line", {
  html <- as.character(table_hint("sort", "reorder"))

  expect_match(hint_part(html, "hint-pointer"), "Drag a column header to reorder", fixed = TRUE)
  expect_false(grepl("reorder", hint_part(html, "hint-touch"), fixed = TRUE))
  expect_match(hint_part(html, "hint-touch"), "Tap a column header to sort", fixed = TRUE)
})

test_that("a hint with nothing to say on touch renders no touch span", {
  expect_false(grepl("hint-touch", as.character(table_hint("reorder")), fixed = TRUE))
})

test_that("table_hint refuses a claim it has no wording for", {
  expect_error(table_hint("pivot", "teleport"), "teleport")
  expect_error(table_hint(), "at least one")
})

test_that("every interactive table has a hint, in both leagues", {
  hinted <- c("ui_tab1_onoff.R", "ui_tab8_euro.R", "ui_tab2_lineup.R",
              "ui_tab10_euro_lineups.R", "ui_tab4_gamelogs.R",
              "ui_tab11_euro_gamelogs.R", "ui_tab5_traditional.R", "ui_tab3_team.R")

  for (f in hinted) {
    expect_true(grepl("table_hint(", read_repo_txt("R", f), fixed = TRUE), info = f)
  }
  # Compare's second level is built server-side.
  expect_true(grepl('table_hint("compare_player"',
                    read_repo_txt("R", "server_tab7_compare.R"), fixed = TRUE))
})

test_that("the touch line is chosen by the app's touch class, not by width", {
  css <- read_repo_txt("www", "app.css")

  expect_true(grepl("body.ibpl-touch-tips .table-hint .hint-pointer", css, fixed = TRUE))
  expect_true(grepl("body.ibpl-touch-tips .table-hint .hint-touch", css, fixed = TRUE))
})
