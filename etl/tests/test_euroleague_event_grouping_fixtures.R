# Structural tests for the exploratory EuroLeague event-grouping fixtures.
# Run: Rscript -e "testthat::test_file('etl/tests/test_euroleague_event_grouping_fixtures.R')"
library(testthat)

fixture_path <- file.path(
  "..", "euroleague", "fixtures", "event_grouping_edge_cases.csv"
)
fixtures <- read.csv(
  fixture_path,
  na.strings = c("", "NA"),
  stringsAsFactors = FALSE,
  check.names = FALSE
)

required_columns <- c(
  "fixture_id", "season", "gamecode", "source_event_order",
  "provider_number_of_play", "period", "clock", "team_code", "play_type",
  "player_id", "player_name", "play_info", "score_a", "score_b", "comment",
  "expected_incident_id", "expected_parent_order", "expected_ft_trip_id",
  "expected_possession_end", "expected_end_reason", "review_status",
  "label_confidence_pct"
)

required_fixture_ids <- c(
  "clock_shifted_and_one",
  "and_one_with_substitutions",
  "four_point_play",
  "technical_after_made_basket",
  "personal_then_technical_same_clock",
  "opposing_ft_trips_same_clock",
  "interleaved_unsportsmanlike",
  "clock_shifted_coach_technical",
  "blocked_shot_defensive_rebound",
  "period_end_blocked_shot",
  "technical_then_personal_ft_trips",
  "offsetting_technicals_personal_trip",
  "throw_in_then_personal_ft_trips",
  "retained_possession_after_made_basket",
  "retained_possession_after_challenge",
  "period_end_offensive_rebound",
  "made_basket_dead_ball_ft",
  "new_foul_closes_special_ft_trip",
  "unsportsmanlike_turnover_then_retained_ball",
  "compound_penalty_after_turnover_single_endpoint",
  "foul_offensive_rebound_before_free_throws"
)

test_that("fixture schema and edge-case coverage are stable", {
  expect_true(file.exists(fixture_path))
  expect_setequal(names(fixtures), required_columns)
  expect_setequal(unique(fixtures$fixture_id), required_fixture_ids)
  expect_equal(
    anyDuplicated(fixtures[c("season", "gamecode", "source_event_order")]),
    0L
  )
  expect_true(all(fixtures$label_confidence_pct >= 0 &
                    fixtures$label_confidence_pct <= 100))
  expect_true(all(fixtures$review_status %in%
                    c("confirmed", "provisional", "unresolved")))
})

test_that("all synthetic parents resolve inside the same fixture game and period", {
  row_key <- paste(
    fixtures$fixture_id,
    fixtures$season,
    fixtures$gamecode,
    fixtures$period,
    fixtures$source_event_order,
    sep = ":"
  )
  parent_key <- paste(
    fixtures$fixture_id,
    fixtures$season,
    fixtures$gamecode,
    fixtures$period,
    fixtures$expected_parent_order,
    sep = ":"
  )

  expect_false(anyNA(fixtures$expected_parent_order))
  expect_true(all(parent_key %in% row_key))

  incident_rows <- !is.na(fixtures$expected_incident_id)
  parent_rows <- fixtures$source_event_order == fixtures$expected_parent_order
  incident_roots <- unique(fixtures$expected_incident_id[incident_rows & parent_rows])
  expect_setequal(unique(fixtures$expected_incident_id[incident_rows]), incident_roots)
})

test_that("every free throw belongs to one labelled trip and incident", {
  ft_rows <- fixtures$play_type %in% c("FTA", "FTM")
  expect_true(all(!is.na(fixtures$expected_ft_trip_id[ft_rows])))
  expect_true(all(!is.na(fixtures$expected_incident_id[ft_rows])))

  trip_incident_counts <- aggregate(
    expected_incident_id ~ expected_ft_trip_id,
    data = fixtures[ft_rows, ],
    FUN = function(x) length(unique(x))
  )
  expect_true(all(trip_incident_counts$expected_incident_id == 1L))
})

test_that("an incident never emits more than one possession endpoint", {
  incident_rows <- !is.na(fixtures$expected_incident_id)
  endpoint_counts <- aggregate(
    expected_possession_end ~ fixture_id + expected_incident_id,
    data = fixtures[incident_rows, ],
    FUN = function(x) sum(x, na.rm = TRUE)
  )
  expect_true(all(endpoint_counts$expected_possession_end <= 1L))

  endpoint_rows <- fixtures$expected_possession_end
  expect_true(all(!is.na(fixtures$expected_end_reason[endpoint_rows])))
  expect_true(all(is.na(fixtures$expected_end_reason[!endpoint_rows])))
})

test_that("fixtures encode the important grouping distinctions", {
  delayed <- fixtures[fixtures$fixture_id == "clock_shifted_and_one", ]
  expect_equal(
    delayed$expected_parent_order[delayed$play_type == "FTM"],
    36L
  )
  expect_true(delayed$expected_possession_end[delayed$play_type == "FTM"])
  expect_false(
    delayed$clock[delayed$play_type == "FTM"] ==
      delayed$clock[delayed$source_event_order == 36L]
  )

  same_clock <- fixtures[
    fixtures$fixture_id == "opposing_ft_trips_same_clock" &
      fixtures$play_type %in% c("FTA", "FTM"),
  ]
  expect_equal(length(unique(same_clock$expected_ft_trip_id)), 2L)
  expect_equal(length(unique(same_clock$clock)), 1L)

  bench_technical <- fixtures[
    fixtures$fixture_id == "technical_after_made_basket",
  ]
  expect_true(bench_technical$expected_possession_end[
    bench_technical$source_event_order == 56L
  ])
  expect_false(bench_technical$expected_possession_end[
    bench_technical$source_event_order == 65L
  ])

  dead_ball <- fixtures[
    fixtures$fixture_id == "made_basket_dead_ball_ft",
  ]
  expect_true(all(
    dead_ball$expected_parent_order[
      dead_ball$source_event_order %in% 271:276
    ] == 271L
  ))
  expect_true(dead_ball$expected_possession_end[
    dead_ball$source_event_order == 276L
  ])
  expect_equal(
    dead_ball$expected_end_reason[
      dead_ball$source_event_order == 276L
    ],
    "made_basket_dead_ball_ft"
  )

  split_trip <- fixtures[
    fixtures$fixture_id == "new_foul_closes_special_ft_trip",
  ]
  expect_equal(
    split_trip$expected_parent_order[
      split_trip$source_event_order == 498L
    ],
    497L
  )
  expect_true(all(
    split_trip$expected_parent_order[
      split_trip$source_event_order %in% 501:504
    ] == 499L
  ))
  expect_equal(length(unique(
    split_trip$expected_ft_trip_id[
      split_trip$play_type %in% c("FTA", "FTM")
    ]
  )), 2L)

  unsports <- fixtures[
    fixtures$fixture_id ==
      "unsportsmanlike_turnover_then_retained_ball",
  ]
  expect_true(all(
    unsports$expected_parent_order[
      unsports$source_event_order %in% c(117L, 118L, 119L, 125L, 126L)
    ] == 117L
  ))
  expect_true(unsports$expected_possession_end[
    unsports$source_event_order == 118L
  ])
  expect_false(any(unsports$expected_possession_end[
    unsports$source_event_order %in% c(125L, 126L)
  ]))

  rebound_before_ft <- fixtures[
    fixtures$fixture_id == "foul_offensive_rebound_before_free_throws",
  ]
  expect_true(all(
    rebound_before_ft$expected_parent_order[
      rebound_before_ft$source_event_order %in% 287:288
    ] == 284L
  ))
})
