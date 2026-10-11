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

test_that("a chip that opens its control says so; one that does not stays plain", {
  global_txt <- read_repo_txt("R", "global.R")

  expect_true(grepl('if (focusable) tags$i(class = "bi bi-pencil chip-edit"',
                    global_txt, fixed = TRUE))
})

test_that("the stat-filter entry point is named, everywhere it is built", {
  for (f in c("helpers.R", "server_tab5_traditional.R")) {
    txt <- read_repo_txt("R", f)
    expect_true(grepl('tags$i(class = "bi bi-plus"), " Stat filter"', txt, fixed = TRUE), info = f)
    expect_false(grepl('tags$i(class = "bi bi-plus"), " Filter"', txt, fixed = TRUE), info = f)
  }
})

test_that("explained headers and the Net cell are marked at rest, on pointer devices only", {
  css <- cues_css()

  # Headers carry their explanation in title, not data-tooltip.
  expect_match(css, "body:not\\(\\.ibpl-touch-tips\\) th\\[title\\]\\s*\\{[^}]*text-decoration:\\s*underline dotted")
  expect_match(css, "body:not\\(\\.ibpl-touch-tips\\) \\.onoff-net-tip\\s*\\{[^}]*text-decoration:\\s*underline dotted")
  expect_false(grepl("th[data-tooltip] {", css, fixed = TRUE))
})

test_that("Gameflow tells the user what selecting players reveals, in R and in JS", {
  wording <- "Select two or more players, by their names or bars, to see their shared court time and lineups."

  expect_true(grepl(wording, read_repo_txt("R", "mod_ribbon_modal.R"), fixed = TRUE))
  expect_true(grepl(wording, read_repo_txt("www", "app.js"), fixed = TRUE))
  expect_false(grepl("Select players by clicking or tapping their bars or names.",
                     read_repo_txt("www", "app.js"), fixed = TRUE))
})

test_that("the navbar shows the update date short, keeping the full stamp for the tooltip", {
  # Measured 2026-10-11: at 1440px the tab row has ~55px to spare for this
  # text. "Oct 10" (38px) fits; anything carrying the time (74px+) costs a
  # second navbar row, whose tabs then cover the first row's mode menus.
  expect_identical(last_updated_short("Last updated: 2026-10-10 22:33:06"), "Oct 10")
  expect_identical(last_updated_short("Last updated: 2026-01-05 00:00:00"), "Jan 5")
  expect_identical(last_updated_short("Last updated: unavailable"), "n/a")
  expect_identical(last_updated_short(NULL), "n/a")
  expect_identical(last_updated_short("Last updated: 2026-13-40 00:00:00"), "n/a")

  # A bare date has no context, so it is always labelled: stacked above the
  # date where width is short (a caption adds height, not width), inline from
  # 1600px.
  css <- cues_css()
  expect_match(css, "#last_updated > span::before\\s*\\{[^}]*content:\\s*\"Updated\"[^}]*display:\\s*block")
  expect_match(css, "min-width:\\s*1600px[^@]*#last_updated > span::before\\s*\\{[^}]*display:\\s*inline")

  # Measured 2026-10-11 at 1440px: the row wraps once this text block passes
  # 38px WITH the status dot beside it. Dates run to 43px ("May 28"), so the
  # dot (10px with its gap) gives way below 1600px to leave real slack.
  expect_match(css, "\\.nav-updated-dot\\s*\\{[^}]*display:\\s*none")
  expect_true(grepl('class = "nav-updated-dot"', read_repo_txt("app.R"), fixed = TRUE))

  app_txt <- read_repo_txt("app.R")
  expect_true(grepl("last_updated_short(", app_txt, fixed = TRUE))
  expect_true(grepl('uiOutput("last_updated", inline = TRUE)', app_txt, fixed = TRUE))
})

test_that("each Gameflow player row is a button, not an underlined name", {
  css <- cues_css()
  helpers_txt <- read_repo_txt("R", "helpers.R")

  # The underline read as decoration, not as "selectable" (user feedback
  # 2026-10-11). The row button carries data-clip, which is what app.js keys
  # clicks, hover and the is-pair-selected class on, so it needs no JS.
  expect_false(grepl("\\.ibpl-ribbon-name\\s*\\{[^}]*text-decoration:\\s*underline", css))
  expect_true(grepl('tags$rect(class = "ibpl-ribbon-row-btn"', helpers_txt, fixed = TRUE))
  expect_match(css, "\\.ibpl-ribbon-row-btn\\s*\\{[^}]*stroke:")
  expect_true(grepl(".ibpl-ribbon-row-btn:hover", css, fixed = TRUE))
  expect_true(grepl(".ibpl-ribbon-row-btn.is-pair-selected", css, fixed = TRUE))
})

test_that("Gameflow bars are outlined at rest so they read as pressable", {
  expect_match(cues_css(), "\\.ibpl-ribbon-lane rect\\s*\\{[^}]*stroke:[^}]*filter:\\s*drop-shadow")
})

test_that("the row button is drawn once per player, behind that player's labels", {
  src <- read_repo_txt("R", "helpers.R")
  btn_at <- regexpr('tags$rect(class = "ibpl-ribbon-row-btn"', src, fixed = TRUE)
  name_at <- regexpr('list(class = "ibpl-ribbon-name"', src, fixed = TRUE)
  expect_gt(btn_at, 0)
  # SVG paints in document order: the button must come before the name text.
  expect_lt(btn_at, name_at)
})
