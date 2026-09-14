# Effectiveness tests for the pure EuroLeague event-grouping state machine.
# Run: Rscript -e "testthat::test_file('etl/tests/test_euroleague_group_events.R')"
library(testthat)
source(file.path("..", "euroleague", "group_events.R"))

fixture_path <- file.path(
  "..", "euroleague", "fixtures", "event_grouping_edge_cases.csv"
)
labelled <- read.csv(
  fixture_path,
  na.strings = c("", "NA"),
  stringsAsFactors = FALSE,
  check.names = FALSE
)

raw_columns <- c(
  "season", "gamecode", "source_event_order", "provider_number_of_play",
  "period", "clock", "team_code", "play_type", "player_id", "player_name",
  "play_info", "score_a", "score_b", "comment"
)
predicted <- group_euroleague_events(labelled[raw_columns])
effectiveness <- evaluate_euroleague_grouping(predicted, labelled)

test_that("state machine does not consume fixture expectation columns", {
  expect_false(any(grepl("^expected_", names(predicted))))
  expect_equal(nrow(predicted), nrow(labelled))
})

test_that("synthetic parents reproduce labelled incident membership", {
  expect_equal(
    predicted$synthetic_parent_order,
    labelled$expected_parent_order
  )
})

test_that("FT trips reproduce labelled partitions without using clock as key", {
  ft <- labelled$play_type %in% c("FTA", "FTM")
  expect_false(anyNA(predicted$synthetic_ft_trip_id[ft]))
  expect_equal(effectiveness$ft_parent_accuracy, 1)
  expect_equal(effectiveness$ft_trip_partition_accuracy, 1)
  expect_equal(effectiveness$ft_resolved_rate, 1)
})

test_that("possession endpoints and reasons reproduce fixture labels", {
  expect_equal(
    predicted$final_end_poss,
    labelled$expected_possession_end
  )
  endpoint <- labelled$expected_possession_end
  expect_equal(
    predicted$end_reason[endpoint],
    labelled$expected_end_reason[endpoint]
  )
  expect_equal(effectiveness$endpoint_precision, 1)
  expect_equal(effectiveness$endpoint_recall, 1)
})

test_that("special penalty clusters span administrative rows", {
  play_type <- c(
    "CMU", "RV", "OUT", "IN", "OUT", "IN", "OUT", "IN", "OUT", "IN",
    "FTM", "AS", "FTM", "2FGM"
  )
  events <- data.frame(
    season = 2025L,
    gamecode = 901L,
    source_event_order = seq_along(play_type),
    period = 2L,
    play_type = play_type,
    team_code = c(
      "B", "A", "A", "A", "B", "B", "A", "A", "B", "B", "A", "A",
      "A", "B"
    ),
    player_id = c(
      "b1", "a1", "a2", "a3", "b2", "b3", "a4", "a5", "b4", "b5",
      "a1", "a2", "a1", "b1"
    ),
    stringsAsFactors = FALSE
  )

  grouped <- group_euroleague_events(events)
  free_throws <- grouped[grouped$play_type == "FTM", ]

  expect_equal(free_throws$grouping_status, c("provisional", "provisional"))
  expect_equal(free_throws$synthetic_parent_order, c(1L, 1L))
  expect_equal(length(unique(free_throws$synthetic_ft_trip_id)), 1L)
})

test_that("a live boundary separates an and-one from a later special penalty", {
  events <- data.frame(
    season = 2025L,
    gamecode = 902L,
    source_event_order = 1:8,
    period = 4L,
    play_type = c("2FGM", "CM", "RV", "OUT", "IN", "FTM", "2FGA", "CMU"),
    team_code = c("A", "B", "A", "B", "B", "A", "B", "A"),
    player_id = c("a1", "b1", "a1", "b2", "b3", "a1", "b4", "a2"),
    stringsAsFactors = FALSE
  )

  grouped <- group_euroleague_events(events)
  free_throw <- grouped[grouped$source_event_order == 6L, ]

  expect_equal(free_throw$grouping_status, "confirmed")
  expect_equal(free_throw$synthetic_parent_order, 1L)
  expect_true(free_throw$final_end_poss)
  expect_equal(free_throw$end_reason, "and_one_final_ft")
})
