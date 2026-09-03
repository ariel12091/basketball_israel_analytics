test_that("row identity travels on the tr, never as injected HTML", {
  helpers_txt <- read_repo_txt("R", "helpers.R")

  # createdRow, not rowCallback: DT implements formatStyle() by writing
  # options$rowCallback, so a hand-written one there is silently overwritten
  # and the table stops rendering entirely.
  expect_true(grepl("createdRow = pivot_cb", helpers_txt, fixed = TRUE))
  expect_false(grepl("rowCallback = pivot_cb", helpers_txt, fixed = TRUE))
  expect_true(grepl("data-pivot-team", helpers_txt, fixed = TRUE))
  expect_true(grepl("data-pivot-player", helpers_txt, fixed = TRUE))
})

test_that("the ids are carried but never rendered as a column", {
  helpers_txt <- read_repo_txt("R", "helpers.R")

  # Carried through keep_cols to reach the browser, and hidden again so they
  # never occupy a cell. Both halves must be present or the pivot either has
  # no identity or shows two id columns.
  expect_true(grepl('"team_id", "player_id"', helpers_txt, fixed = TRUE))
  expect_true(grepl('"team_id", "player_id")', helpers_txt, fixed = TRUE))
})

test_that("the DT escaping contract is untouched", {
  # The pivot must not have widened any escape allowlist: entity names stay
  # escaped text and identity rides on the row's data attributes.
  server_files <- list.files(repo_file("R"), pattern = "^server_tab.*\\.R$", full.names = TRUE)
  code <- paste(unlist(lapply(server_files, readLines, warn = FALSE)), collapse = "\n")

  expect_false(grepl("escape\\s*=\\s*FALSE", code))
})

test_that("the pivot menu is keyboard dismissable and sends one event", {
  js <- read_repo_txt("www", "app.js")

  expect_true(grepl("pivot_action", js, fixed = TRUE))
  expect_true(grepl("data-pivot-team", js, fixed = TRUE))
  expect_true(grepl("Escape", js, fixed = TRUE))
  expect_true(grepl('role", "menu"', js, fixed = TRUE))
})

test_that("the menu only opens on the identity columns", {
  js <- read_repo_txt("www", "app.js")

  # Team and Player are the first two cells. Opening anywhere would swallow
  # clicks on the shot-split and heat cells.
  expect_true(grepl("cellIndex > 1", js, fixed = TRUE))
})

test_that("the dispatcher routes every advertised target", {
  app_txt <- read_repo_txt("app.R")

  expect_true(grepl("input$pivot_action", app_txt, fixed = TRUE))
  expect_true(grepl('"lineup_data"', app_txt, fixed = TRUE))
  expect_true(grepl('"game_logs"', app_txt, fixed = TRUE))
  # The trigger must be a plain input, never a data reactive: observers are
  # not suspended by tab visibility (4487c2f).
  expect_false(grepl("observeEvent\\(\\s*\\{?\\s*[a-z_]+\\(\\)\\s*,\\s*\\{[^}]*pivot_action", app_txt))
})

test_that("destination tabs read the generalised payload", {
  tab2 <- read_repo_txt("R", "server_tab2.R")
  tab4 <- read_repo_txt("R", "server_tab4.R")

  expect_true(grepl('consume_pending_nav(shared, "lineup_data")', tab2, fixed = TRUE))
  expect_true(grepl('consume_pending_nav(shared, "game_logs")', tab4, fixed = TRUE))

  # The Home cards' own values keep working alongside it.
  expect_true(grepl("pending_ld_team", tab2, fixed = TRUE))
  expect_true(grepl("pending_gl_team", tab4, fixed = TRUE))
})

test_that("a player pivot carries the player, not just the team", {
  tab2 <- read_repo_txt("R", "server_tab2.R")
  mod <- read_repo_txt("R", "mod_lineup_player_filter.R")

  # The menu offers "Lineups with this player". Reading only nav$team_id made
  # that item behave exactly like "Lineups for this team".
  expect_true(grepl("nav$player_id", tab2, fixed = TRUE))
  expect_true(grepl("players_on = pending_player", tab2, fixed = TRUE))

  # The module has to accept the selection; it otherwise only preserves one.
  expect_true(grepl("function(team_value, players_on = NULL)", mod, fixed = TRUE))
  # Team-scoped choices: a player not on this roster must not be forced in.
  expect_true(grepl("unname(choices))", mod, fixed = TRUE))
})

test_that("a team pivot clears a player left by an earlier one", {
  js <- read_repo_txt("www", "app.js")
  mod <- read_repo_txt("R", "mod_lineup_player_filter.R")
  tab2 <- read_repo_txt("R", "server_tab2.R")

  # A row carries both ids, so the menu has to send only the one its action
  # is about, or "Lineups for this team" behaves like the player action.
  expect_true(grepl('action.needs === "player" ?', js, fixed = TRUE))

  # NULL means no pivot (preserve); a value states the selection, and
  # character(0) from a team pivot must clear rather than preserve.
  expect_true(grepl("if (is.null(players_on))", mod, fixed = TRUE))
  expect_true(grepl("pending_player <- if (is.null(nav)) NULL else {", tab2, fixed = TRUE))
})
