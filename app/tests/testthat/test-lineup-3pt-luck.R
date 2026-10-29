# 3PT luck on a lineup's Net RTG (Lineups Summary, both leagues): the On/Off
# rule applied to one unit. A lineup is tagged when removing the 3PT shooting
# above or below the league rate, on both ends, flips the sign of its net.

lineup_luck_fixture <- function() {
  # League: 70 of 200 from three = 0.35.
  data.frame(
    team_id = c(1, 1, 2),
    net_rtg = c(4, 10, -3),
    off_poss = c(100, 100, 100), def_poss = c(100, 100, 100),
    off_fg3_made = c(30, 20, 20), off_fg3_att = c(60, 60, 80),
    def_fg3_made = c(10, 20, 20), def_fg3_att = c(40, 40, 50),
    check.names = FALSE
  )
}

test_that("lineup_league_3p is the population's made over attempted", {
  expect_equal(lineup_league_3p(lineup_luck_fixture()), 0.35)
  df <- lineup_luck_fixture(); df$off_fg3_att <- NULL
  expect_true(is.na(lineup_league_3p(df)))
  expect_true(is.na(lineup_league_3p(lineup_luck_fixture()[0, ])))
})

test_that("lineup_3pt_luck splits our and opponents' shooting and tags sign flips", {
  out <- lineup_3pt_luck(lineup_luck_fixture())
  # Row 1 at p = 0.35: ours 30 made vs 21 expected over 100 poss = +27;
  # opponents 10 vs 14 = -12 per 100, which counts for the lineup: +12.
  expect_equal(out$luck_3pt_ours[1], 27)
  expect_equal(out$luck_3pt_theirs[1], 12)
  expect_equal(out$net_without_3pt_luck[1], 4 - 27 - 12)
  # Row 2: ours 20 vs 21 = -3; opponents 20 vs 14 = +18 against: -18.
  # Without luck 10 + 3 + 18 = +31: same sign, no tag.
  expect_equal(out$net_without_3pt_luck[2], 31)
  # Row 3: ours 20 vs 28 = -24; opponents 20 vs 17.5 = -7.5. -3 + 24 + 7.5 > 0.
  expect_equal(out$luck_3pt_flag, c(TRUE, FALSE, TRUE))
})

test_that("lineup_3pt_luck leaves a table without shooting columns alone", {
  df <- lineup_luck_fixture()[, c("team_id", "net_rtg", "off_poss", "def_poss")]
  expect_identical(lineup_3pt_luck(df), df)
  zero <- lineup_luck_fixture(); zero$off_poss[1] <- 0
  out <- lineup_3pt_luck(zero)
  expect_true(is.na(out$luck_3pt_ours[1]))
  expect_false(out$luck_3pt_flag[1])
})

test_that("the Summary table carries the luck columns hidden and renders the tag on Net RTG", {
  df <- lineup_3pt_luck(lineup_luck_fixture())
  df$Team <- "T"; df$Players <- c("A", "B", "C"); df$minutes <- 10
  df$total_poss <- df$off_poss + df$def_poss; df$plus_minus <- 0
  df$off_pts <- 100; df$def_pts <- 100; df$off_ppp <- 100; df$def_ppp <- 100
  df$off_fg2_made <- 10; df$off_fg2_att <- 20; df$def_fg2_made <- 10; df$def_fg2_att <- 20
  df$num_starters <- 3; df$sub_lineup_hash <- c("h1", "h2", "h3")
  dt <- lineup_summary_datatable(df, list(), LD_LINEUP_TABLE_SPEC)
  defs <- dt$x$options$columnDefs
  net_idx <- which(names(dt$x$data) == "net_rtg") - 1L
  net_def <- Filter(function(d) identical(as.integer(d$targets), as.integer(net_idx)) && !is.null(d$render), defs)
  expect_length(net_def, 1L)
  expect_match(as.character(net_def[[1]]$render), "3PT luck", fixed = TRUE)
  hidden <- unlist(Filter(function(d) isFALSE(d$visible), defs)[[1]]$targets)
  luck_idx <- which(names(dt$x$data) %in% c("luck_3pt_flag", "net_without_3pt_luck", "luck_3pt_ours", "luck_3pt_theirs")) - 1L
  expect_length(luck_idx, 4L)
  expect_true(all(luck_idx %in% hidden))
})
