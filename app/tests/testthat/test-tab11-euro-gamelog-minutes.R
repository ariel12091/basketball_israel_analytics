# Tab 11 (EuroLeague game logs) must show a Min column at parity with its
# Israeli companion, Tab 4.
#
# The shared calculator gl_build_ff_metrics() already owns the minutes
# contract: it defaults the column when absent, sums it per
# (game_id, team_id, type_lineup), and reports the OFFENSE perspective via
# coalesce(off_minutes, 0). EuroLeague matches that shape in the database --
# euroleague.lineup_totals_by_game stores `seconds` on offense rows and NULL
# on defense (57,203 of 114,406 rows measured 2026-09-05) -- so Tab 11 only
# has to supply the column, never to reimplement the aggregation.
#
# EuroLeague's per-game fact is a starters cross-tab: one row per
# (game_id, team_id, own_starters, opp_starters), ~22 rows per game-team.
# Minutes therefore have to sum ACROSS that cross-tab to reach game minutes.

euro_shaped_metric_rows <- function() {
  # One game-team whose cross-tab splits 40 minutes three ways.
  cross_tab <- data.frame(
    game_id = 101L,
    team_id = 1L,
    own_starters = c(5L, 5L, 3L),
    opp_starters = c(5L, 4L, 5L),
    minutes = c(10, 5, 25)
  )

  shared_counts <- data.frame(
    total_points = c(20, 10, 50),
    total_poss = c(20, 10, 50),
    ts_poss_count = c(10, 5, 25),
    oreb_count = c(2, 1, 5),
    oreb_opportunities = c(5, 2, 10),
    tov_count = c(2, 1, 5),
    total_ft_attempts = c(4, 2, 10),
    total_fga = c(18, 9, 45),
    total_fgm = c(8, 4, 20),
    total_fg3_made = c(2, 1, 5)
  )

  offense <- cbind(
    cross_tab[, c("game_id", "team_id")],
    type_lineup = "offense",
    num_starters = cross_tab$own_starters,
    shared_counts,
    minutes = cross_tab$minutes
  )
  # Defense carries no duration, exactly as the EuroLeague fact does.
  defense <- cbind(
    cross_tab[, c("game_id", "team_id")],
    type_lineup = "defense",
    num_starters = cross_tab$opp_starters,
    shared_counts,
    minutes = NA_real_
  )
  rbind(offense, defense)
}

euro_schedule_rows <- function() data.frame(game_id = 101L, team_id = 1L)

test_that("gl_build_ff_metrics sums EuroLeague minutes across the starters cross-tab", {
  out <- gl_build_ff_metrics(euro_shaped_metric_rows(), euro_schedule_rows())

  expect_equal(nrow(out), 1L)
  expect_equal(out$minutes, 40)
})

test_that("EuroLeague defense rows carrying NULL duration do not zero the Min column", {
  rows <- euro_shaped_metric_rows()
  expect_true(all(is.na(rows$minutes[rows$type_lineup == "defense"])))

  out <- gl_build_ff_metrics(rows, euro_schedule_rows())
  # Offense perspective only -- NA defense rows must not drag the total down.
  expect_equal(out$minutes, 40)
})

test_that("an offense starters filter narrows EuroLeague minutes", {
  out <- gl_build_ff_metrics(
    euro_shaped_metric_rows(), euro_schedule_rows(),
    starters_bounds = list(off_min = 5, off_max = NA, def_min = NA, def_max = NA)
  )

  # Only the two own_starters == 5 cross-tab cells survive: 10 + 5.
  expect_equal(out$minutes, 15)
})

test_that("Tab 11 reads canonical offense seconds from lineup_totals_by_game", {
  src <- paste(
    readLines(repo_file("R", "server_tab11_euro_gamelogs.R"), warn = FALSE),
    collapse = "\n"
  )

  expect_match(src, "lineup_totals_by_game", fixed = TRUE)
  # Canonical wall-clock seconds, not the action-span convention Israel adopted
  # for a clock defect EuroLeague's running-max clamp makes impossible.
  expect_false(grepl("action_span_seconds", src, fixed = TRUE))
  expect_match(src, "type_lineup = 'offense'", fixed = TRUE)
  expect_match(src, "minutes", fixed = TRUE)
})

test_that("Tab 11 sorts newest game first, like Tab 4", {
  src <- paste(
    readLines(repo_file("R", "server_tab11_euro_gamelogs.R"), warn = FALSE),
    collapse = "\n"
  )

  # Without an explicit order DataTables sorts ascending on column 0 (Rd),
  # showing the OLDEST game first and silently discarding the R-side arrange().
  expect_match(src, "order = list(list(2, \"desc\")", fixed = TRUE)
})
