# "Too close to call" on the On/Off Net RTG Diff: the range that 3-point
# shooting luck alone could produce, given the 3s taken with a player on and
# off. 25-26 analysis (2026-10-04): opponent 3P% on/off is indistinguishable
# from chance, and our own 3P% nearly so.

luck_fixture <- function() {
  # Team 1: 40/100 from three; team 2: 30/100. For every player, on + off
  # is his team's total, so the league rate is 70/200 = 0.35. Summing all
  # rows instead would count team 1 twice (110/300).
  data.frame(
    team_id = c(1, 1, 2),
    `ON Poss` = c(400, 400, 400), `OFF Poss` = c(300, 300, 300),
    off_on_fg3_made  = c(25, 20, 21), off_on_fg3_att  = c(60, 50, 70),
    off_off_fg3_made = c(15, 20, 9),  off_off_fg3_att = c(40, 50, 30),
    def_on_fg3_att   = c(50, 50, 50), def_off_fg3_att = c(40, 40, 40),
    check.names = FALSE
  )
}

test_that("onoff_3pt_luck_range takes the league rate once per team", {
  df <- luck_fixture()
  p <- 0.35
  expected <- 1.96 * 300 * sqrt(p * (1 - p) *
    ((df$off_on_fg3_att + df$def_on_fg3_att) / 400^2 +
     (df$off_off_fg3_att + df$def_off_fg3_att) / 300^2))
  expect_equal(onoff_3pt_luck_range(df), expected)
})

test_that("onoff_3pt_luck_range is NULL without the 3PA columns", {
  df <- luck_fixture()
  df$def_off_fg3_att <- NULL
  expect_null(onoff_3pt_luck_range(df))
})

test_that("onoff_3pt_luck_range is NA where possessions are missing", {
  df <- luck_fixture()
  df$`OFF Poss`[2] <- 0
  out <- onoff_3pt_luck_range(df)
  expect_true(is.na(out[2]))
  expect_false(anyNA(out[-2]))
})

test_that("onoff_summary_datatable flags Net RTG Diff inside the luck range", {
  CUTS <- seq(0.05, 0.95, by = 0.05)
  HEADER_TOOLTIP_JS <- DT::JS("function(thead, data, start, end, display) {}")

  df <- luck_fixture()
  for (side in c("off_on", "off_off", "def_on", "def_off")) {
    df[[paste0(side, "_fg2_made")]] <- 20
    df[[paste0(side, "_fg2_att")]] <- 40
  }
  df$def_on_fg3_made <- 17
  df$def_off_fg3_made <- 14
  df$Team <- c("A", "A", "B")
  df$Player <- c("P1", "P2", "P3")
  df$player_id <- 1:3
  df$`Net RTG Diff` <- c(2, 40, -3)
  df$`Off ON Diff` <- 1; df$`Def ON Diff` <- -1
  df$`Off ON PPP` <- 110; df$`Def ON PPP` <- 105; df$`On Net RTG` <- 5
  df$`Off OFF PPP` <- 108; df$`Def OFF PPP` <- 107; df$`Off Net RTG` <- 1
  df$minutes <- 500
  df$pr_net <- c(0.2, 0.9, 0.6)

  w <- onoff_summary_datatable(df, NULL)
  d <- w$x$data
  range <- onoff_3pt_luck_range(df)

  # The range rides along as a hidden column the Net cell reads.
  expect_equal(d$luck_3pt_range, range)
  idx <- which(names(d) == "luck_3pt_range") - 1
  hidden <- unlist(lapply(w$x$options$columnDefs,
                          function(cd) if (isFALSE(cd$visible)) cd$targets))
  expect_true(idx %in% hidden)

  # The flag is display-only: percentile shading stays as it was.
  close <- abs(df$`Net RTG Diff`) < range
  expect_identical(close, c(TRUE, FALSE, TRUE))
  expect_equal(d$pr_net, df$pr_net)

  net_idx <- which(names(d) == "Net RTG Diff") - 1
  net_render <- Filter(function(cd) !is.null(cd$render) && net_idx %in% cd$targets,
                       w$x$options$columnDefs)
  expect_length(net_render, 1L)
  js <- as.character(net_render[[1]]$render)
  expect_match(js, "too close to call", fixed = TRUE)
  expect_match(js, sprintf("row[%d]", idx), fixed = TRUE)
})

test_that("onoff_summary_datatable is unchanged when 3PA columns are absent", {
  CUTS <- seq(0.05, 0.95, by = 0.05)
  HEADER_TOOLTIP_JS <- DT::JS("function(thead, data, start, end, display) {}")
  df <- data.frame(Team = "A", Player = "P1", `Net RTG Diff` = 2,
                   `Off ON PPP` = 110, `Def ON PPP` = 105,
                   `Off OFF PPP` = 108, `Def OFF PPP` = 107,
                   `ON Poss` = 400, `OFF Poss` = 300, pr_net = 0.2,
                   check.names = FALSE)
  w <- onoff_summary_datatable(df, NULL)
  expect_false("luck_3pt_range" %in% names(w$x$data))
  expect_equal(w$x$data$pr_net, 0.2)
})
