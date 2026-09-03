source(repo_file("R", "helpers.R"), local = TRUE)

# isolate() rather than testServer(): reading a reactiveVal needs a reactive
# context, but nothing here needs a session. testServer() with the work inside
# the server function and no expr argument fails with "object '' not found".

test_that("consume_pending_nav returns and clears a matching payload", {
  shiny::isolate({
    shared <- list(pending_nav = shiny::reactiveVal(NULL))
    shared$pending_nav(list(target = "lineup_data", team_id = "7", player_id = "42"))

    got <- consume_pending_nav(shared, "lineup_data")

    expect_equal(got$team_id, "7")
    expect_equal(got$player_id, "42")
    expect_null(shared$pending_nav())
  })
})

test_that("consume_pending_nav leaves another tab's payload alone", {
  shiny::isolate({
    shared <- list(pending_nav = shiny::reactiveVal(NULL))
    shared$pending_nav(list(target = "game_logs", team_id = "7"))

    expect_null(consume_pending_nav(shared, "lineup_data"))
    expect_equal(shared$pending_nav()$target, "game_logs")
  })
})

test_that("consume_pending_nav is safe when nothing is pending", {
  shiny::isolate({
    shared <- list(pending_nav = shiny::reactiveVal(NULL))
    expect_null(consume_pending_nav(shared, "lineup_data"))
  })
})

test_that("consume_pending_nav tolerates a shared list without the value", {
  # Server tests build partial shared lists; a missing value must not error.
  expect_null(consume_pending_nav(list(), "lineup_data"))
})

test_that("a payload is consumed once, not on every visit", {
  # The destination reads it in an init observer that can re-run; a second
  # read must return NULL rather than re-applying the filter.
  shiny::isolate({
    shared <- list(pending_nav = shiny::reactiveVal(NULL))
    shared$pending_nav(list(target = "lineup_data", team_id = "7"))

    expect_equal(consume_pending_nav(shared, "lineup_data")$team_id, "7")
    expect_null(consume_pending_nav(shared, "lineup_data"))
  })
})
