test_that("EuroLeague clutch controls reuse the Israeli UI and parameter helpers", {
  team_ui <- read_repo_txt("R", "ui_tab9_euro_team.R")
  lineup_ui <- read_repo_txt("R", "ui_tab10_euro_lineups.R")
  team_server <- read_repo_txt("R", "server_tab9_euro_team.R")
  lineup_server <- read_repo_txt("R", "server_tab10_euro_lineups.R")

  expect_match(team_ui, 'clutch_filter_ui("euroteam")', fixed = TRUE)
  expect_match(lineup_ui, 'clutch_filter_ui("euro_ld")', fixed = TRUE)
  expect_match(team_server, "resolve_clutch_params(", fixed = TRUE)
  expect_match(lineup_server, "resolve_clutch_params(", fixed = TRUE)
  expect_match(team_server, 'clutch_enabled_id = "euroteam_clutch_enabled"', fixed = TRUE)
  expect_match(lineup_server, 'clutch_enabled_id = "euro_ld_clutch_enabled"', fixed = TRUE)
})

test_that("EuroLeague clutch parameters reach ratings, factors, minutes, and lineups", {
  team_server <- read_repo_txt("R", "server_tab9_euro_team.R")
  lineup_server <- read_repo_txt("R", "server_tab10_euro_lineups.R")

  for (parameter in c(
    "max_margin", "margin_status", "max_time_remaining", "ot_margin_filter"
  )) {
    expect_match(team_server, paste0("p$", parameter), fixed = TRUE)
    expect_match(lineup_server, paste0("a$", parameter), fixed = TRUE)
  }
  # Ratings and Four Factors compose the reader name from a base plus the kind
  # clutch_reader_kind() picks, so assert the pieces: the literal
  # get_team_ratings_direct no longer appears in the source.
  expect_match(team_server, 'paste0("SELECT * FROM euroleague.", base, "_", kind, "("',
               fixed = TRUE)
  expect_match(team_server, 'team_reader_call("get_team_ratings"', fixed = TRUE)
  expect_match(team_server, 'team_reader_call("get_team_four_factors"', fixed = TRUE)
  # Both tabs route through the one shared classifier rather than each carrying
  # its own copy of the same three-way test.
  expect_match(team_server, "clutch_reader_kind(p)", fixed = TRUE)
  expect_match(lineup_server, "clutch_reader_kind(a)", fixed = TRUE)
  # The per-game branch must not carry the four clutch parameters: its tail is
  # seven int4 slots, not the text/bool margin-status/OT pair the other two use.
  expect_match(team_server,
               "$13::int4,$14::int4,$15::int4,$16::int4,$17::int4,$18::int4,$19::int4",
               fixed = TRUE)
  # Same contract on the lineups reader, whose per-game signature is 23 slots
  # against the clutch-capable 27.
  expect_match(lineup_server,
               "$13::int4,$14::int4,$15::int4,",
               fixed = TRUE)
  expect_match(team_server, "get_team_minutes_dynamic", fixed = TRUE)
  expect_match(team_server, "get_team_minutes_direct", fixed = TRUE)
  expect_match(lineup_server, "fetch_lineups_pergame", fixed = TRUE)
  expect_match(lineup_server, "fetch_lineups_dynamic", fixed = TRUE)
  expect_match(lineup_server, "fetch_lineups_direct", fixed = TRUE)
  expect_match(lineup_server, "isTRUE(input$euro_ld_clutch_enabled)", fixed = TRUE)
})

test_that("clutch_reader_kind routes by what the request actually asks for", {
  # No margin or time predicate at all: the per-game facts answer it, and this
  # is the branch both Tab 9 and Tab 10 used to miss.
  expect_identical(
    clutch_reader_kind(list(max_margin = NA_integer_,
                            max_time_remaining = NA_integer_,
                            margin_status = NA_character_,
                            ot_margin_filter = FALSE)),
    "pergame"
  )
  # A blank single-select status is the "all" sentinel, not a filter.
  expect_identical(
    clutch_reader_kind(list(max_margin = NA_integer_,
                            max_time_remaining = NA_integer_,
                            margin_status = "",
                            ot_margin_filter = FALSE)),
    "pergame"
  )
  # Exactly the 5 / all / 5:00 preset, which has an incremental cache.
  expect_identical(
    clutch_reader_kind(list(max_margin = 5L, margin_status = "all",
                            max_time_remaining = 300L,
                            ot_margin_filter = FALSE)),
    "dynamic"
  )
  # OT bypass is part of that preset's identity, so flipping it is a scan.
  expect_identical(
    clutch_reader_kind(list(max_margin = 5L, margin_status = "all",
                            max_time_remaining = 300L,
                            ot_margin_filter = TRUE)),
    "direct"
  )
  # Any other clutch window scans.
  expect_identical(
    clutch_reader_kind(list(max_margin = 3L, margin_status = "all",
                            max_time_remaining = 240L,
                            ot_margin_filter = FALSE)),
    "direct"
  )
  # A margin status on its own is still a clutch predicate: the per-game facts
  # have no margin dimension and must not be asked.
  expect_identical(
    clutch_reader_kind(list(max_margin = NA_integer_,
                            max_time_remaining = NA_integer_,
                            margin_status = "leading",
                            ot_margin_filter = FALSE)),
    "direct"
  )
})

test_that("Tab 5's EuroLeague reader routes through the shared classifier", {
  traditional <- read_repo_txt("R", "server_tab5_traditional.R")

  # Tab 5 inlined its own copy of the three-way test, down to a private
  # has_int_value() character-identical to the helper's is_set(). The copies
  # agreed only because the clutch status select uses a literal "all" value
  # rather than the project's "" blank sentinel; adopting that convention
  # would have split them silently.
  expect_match(traditional, "clutch_reader_kind(list(", fixed = TRUE)
  expect_no_match(traditional, "clutch_active <-", fixed = TRUE)
  expect_no_match(traditional, "has_int_value <- function", fixed = TRUE)

  # Kind names describe the request, reader names the SQL function, and the
  # two vocabularies cross over: pergame -> _dynamic, dynamic -> _standard_clutch.
  expect_match(traditional, 'pergame = "get_player_traditional_dynamic"', fixed = TRUE)
  expect_match(traditional, 'dynamic = "get_player_traditional_standard_clutch"', fixed = TRUE)
  expect_match(traditional, '"get_player_traditional_custom_clutch"', fixed = TRUE)

  # The standard-clutch reader bakes the preset in and takes none of the four
  # clutch parameters, so reader and parameter list must be chosen together.
  expect_match(traditional, "takes_clutch <- !identical(reader,", fixed = TRUE)
  expect_match(traditional, '"$13::int4,$14::int4,$15::int4"', fixed = TRUE)

  # Israel now shares the request classifier: non-clutch uses the per-game
  # fact, while clutch retains the action-level dynamic reader.
  expect_match(traditional, '"get_player_traditional_from_games"', fixed = TRUE)
  expect_match(traditional, '"get_player_traditional_dynamic"', fixed = TRUE)
  expect_match(traditional, 'identical(clutch_reader_kind(list(', fixed = TRUE)
})
