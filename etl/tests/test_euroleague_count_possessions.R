# Deterministic possession-count tests using the labelled EuroLeague fixtures.
# Run: Rscript -e "testthat::test_file('etl/tests/test_euroleague_count_possessions.R')"
library(testthat)
source(file.path("..", "euroleague", "count_possessions.R"))

labelled <- read.csv(
  file.path(
    "..", "euroleague", "fixtures", "event_grouping_edge_cases.csv"
  ),
  na.strings = c("", "NA"),
  stringsAsFactors = FALSE,
  check.names = FALSE
)
raw_columns <- c(
  "season", "gamecode", "source_event_order", "provider_number_of_play",
  "period", "clock", "team_code", "play_type", "player_id", "player_name",
  "play_info", "score_a", "score_b", "comment"
)

first_run <- count_euroleague_possessions(labelled[raw_columns])
second_run <- count_euroleague_possessions(labelled[raw_columns])

test_that("possession counting is deterministic", {
  expect_identical(first_run, second_run)
})

test_that("one possession row is emitted for every labelled endpoint", {
  labelled_key <- paste(
    labelled$season,
    labelled$gamecode,
    labelled$period,
    labelled$source_event_order,
    sep = ":"
  )
  event_key <- paste(
    first_run$events$season,
    first_run$events$gamecode,
    first_run$events$period,
    first_run$events$source_event_order,
    sep = ":"
  )
  aligned <- first_run$events[match(labelled_key, event_key), , drop = FALSE]
  expect_equal(
    aligned$final_end_poss,
    labelled$expected_possession_end
  )
  expect_equal(
    nrow(first_run$possessions),
    sum(labelled$expected_possession_end)
  )
  possession_key <- paste(
    first_run$possessions$season,
    first_run$possessions$gamecode,
    first_run$possessions$period,
    first_run$possessions$source_event_order,
    sep = ":"
  )
  expected_endpoint_key <- labelled_key[labelled$expected_possession_end]
  expect_equal(
    first_run$possessions$end_reason[match(
      expected_endpoint_key,
      possession_key
    )],
    labelled$expected_end_reason[labelled$expected_possession_end]
  )
})

test_that("game and team possession numbers are gap-free", {
  game_groups <- split(
    first_run$possessions,
    interaction(
      first_run$possessions$season,
      first_run$possessions$gamecode,
      drop = TRUE
    )
  )
  for (events in game_groups) {
    expect_equal(events$game_possession_number, seq_len(nrow(events)))
  }

  team_groups <- split(
    first_run$possessions,
    interaction(
      first_run$possessions$season,
      first_run$possessions$gamecode,
      first_run$possessions$offense_team,
      drop = TRUE
    )
  )
  for (events in team_groups) {
    expect_equal(events$team_possession_number, seq_len(nrow(events)))
  }
})

test_that("team totals reconcile exactly to possession rows", {
  expected <- aggregate(
    rep(1L, nrow(first_run$possessions)),
    by = first_run$possessions[c("season", "gamecode", "offense_team")],
    FUN = sum
  )
  names(expected)[names(expected) == "x"] <- "possessions"
  actual <- first_run$team_totals[
    first_run$team_totals$possessions > 0L,
    c(
    "season", "gamecode", "offense_team", "possessions"
    ),
    drop = FALSE
  ]
  expected <- expected[do.call(order, expected[c(
    "season", "gamecode", "offense_team"
  )]), ]
  actual <- actual[do.call(order, actual[c(
    "season", "gamecode", "offense_team"
  )]), ]
  rownames(expected) <- NULL
  rownames(actual) <- NULL
  expect_equal(actual, expected)
})

test_that("period-end offensive rebound emits one traceable endpoint", {
  period_end <- first_run$events[
    first_run$events$gamecode == 300L &
      first_run$events$source_event_order %in% 238:240,
    , drop = FALSE
  ]
  expect_equal(which(period_end$final_end_poss), 2L)
  expect_equal(
    period_end$end_reason[period_end$final_end_poss],
    "period_end_offensive_rebound"
  )
})

test_that("fixture games have no structural counting failures", {
  expect_true(all(first_run$game_qa$structural_status == "pass"))
  expect_true(all(first_run$game_qa$unresolved_ft_rows == 0L))
  expect_true(all(first_run$game_qa$duplicate_endpoint_incidents == 0L))
  expect_true(all(first_run$game_qa$missing_parent_targets == 0L))
})
