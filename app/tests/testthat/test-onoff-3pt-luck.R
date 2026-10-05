# 3PT luck on the On/Off Net RTG Diff: how much of the number came from 3P%
# differing from the league average, on both ends, with the player on vs off.
# 3P% is treated as luck, 2P% as real (decided 2026-10-05). A player is tagged
# when removing that luck flips the sign of his Net RTG Diff.

luck_fixture <- function() {
  # Team 1: 40/100 from three; team 2: 30/100. For every player, on + off
  # is his team's total, so the league rate is 70/200 = 0.35. Summing all
  # rows instead would count team 1 twice (110/300).
  data.frame(
    team_id = c(1, 1, 2),
    `ON Poss` = c(400, 400, 400), `OFF Poss` = c(300, 300, 300),
    off_on_fg3_made  = c(25, 20, 21), off_on_fg3_att  = c(60, 50, 70),
    off_off_fg3_made = c(15, 20, 9),  off_off_fg3_att = c(40, 50, 30),
    def_on_fg3_made  = c(17, 17, 17), def_on_fg3_att  = c(50, 50, 50),
    def_off_fg3_made = c(14, 14, 14), def_off_fg3_att = c(40, 40, 40),
    check.names = FALSE
  )
}

test_that("onoff_league_3p takes each team's total once", {
  expect_equal(onoff_league_3p(luck_fixture()), 0.35)
  # On + off covers only the games a player appeared in, so a player who
  # missed games holds less than his team's total. Take each team's largest.
  partial <- luck_fixture()[2, ]
  partial$off_off_fg3_made <- 0; partial$off_off_fg3_att <- 5
  expect_equal(onoff_league_3p(rbind(partial, luck_fixture())), 0.35)
  df <- luck_fixture()
  df$off_off_fg3_att <- NULL
  expect_true(is.na(onoff_league_3p(df)))
})

test_that("onoff_3pt_luck splits the luck into our and opponents' shooting", {
  luck <- onoff_3pt_luck(luck_fixture())
  # Row 1 by hand at p = 0.35. Ours: on 25 made vs 21 expected over 400
  # poss = +3.0, off 15 vs 14 over 300 = +1.0, so +2.0. Opponents: on 17 vs
  # 17.5 = -0.375, off 14 vs 14 = 0; their luck counts against him, so +0.375.
  expect_equal(luck$ours[1], 2)
  expect_equal(luck$theirs[1], 0.375)
  # Row 3: ours on 21 vs 24.5 = -2.625, off 9 vs 10.5 = -1.5, so -1.125.
  expect_equal(luck$ours[3], -1.125)
})

test_that("onoff_3pt_luck uses a supplied league rate", {
  luck <- onoff_3pt_luck(luck_fixture(), league_3p = 0.4)
  # Ours row 1: on 25 vs 24 = +0.75, off 15 vs 16 = -1, so +1.75.
  expect_equal(luck$ours[1], 1.75)
})

test_that("onoff_3pt_luck is NULL without the 3P columns and NA without possessions", {
  df <- luck_fixture()
  df$def_off_fg3_made <- NULL
  expect_null(onoff_3pt_luck(df))
  df <- luck_fixture()
  df$`OFF Poss`[2] <- 0
  luck <- onoff_3pt_luck(df)
  expect_true(is.na(luck$ours[2]))
  expect_false(anyNA(luck$ours[-2]))
})

summary_fixture <- function() {
  df <- luck_fixture()
  for (side in c("off_on", "off_off", "def_on", "def_off")) {
    df[[paste0(side, "_fg2_made")]] <- 20
    df[[paste0(side, "_fg2_att")]] <- 40
  }
  df$Team <- c("A", "A", "B")
  df$Player <- c("P1", "P2", "P3")
  df$player_id <- 1:3
  # 3PT luck totals at p = 0.35: row 1 +2.375, row 3 -0.75. Row 1's +2 is
  # more than all luck (flagged); row 2's +40 and row 3's -3 survive it.
  df$`Net RTG Diff` <- c(2, 40, -3)
  df$`Off ON Diff` <- 1; df$`Def ON Diff` <- -1
  df$`Off ON PPP` <- 110; df$`Def ON PPP` <- 105; df$`On Net RTG` <- 5
  df$`Off OFF PPP` <- 108; df$`Def OFF PPP` <- 107; df$`Off Net RTG` <- 1
  df$minutes <- 500
  df$pr_net <- c(0.2, 0.9, 0.6)
  df
}

test_that("onoff_summary_datatable tags a Net RTG Diff that 3PT luck flips", {
  CUTS <- seq(0.05, 0.95, by = 0.05)
  HEADER_TOOLTIP_JS <- DT::JS("function(thead, data, start, end, display) {}")
  df <- summary_fixture()
  w <- onoff_summary_datatable(df, NULL)
  d <- w$x$data
  luck <- onoff_3pt_luck(df)

  expect_equal(d$luck_3pt_ours, luck$ours)
  expect_equal(d$luck_3pt_theirs, luck$theirs)
  expect_identical(d$luck_3pt_flag, c(TRUE, FALSE, FALSE))

  hidden <- unlist(lapply(w$x$options$columnDefs,
                          function(cd) if (isFALSE(cd$visible)) cd$targets))
  for (col in c("luck_3pt_ours", "luck_3pt_theirs", "luck_3pt_flag")) {
    expect_true((which(names(d) == col) - 1) %in% hidden, info = col)
  }

  # Percentile shading is untouched.
  expect_equal(d$pr_net, df$pr_net)

  net_idx <- which(names(d) == "Net RTG Diff") - 1
  net_render <- Filter(function(cd) !is.null(cd$render) && net_idx %in% cd$targets,
                       w$x$options$columnDefs)
  expect_length(net_render, 1L)
  js <- as.character(net_render[[1]]$render)
  expect_match(js, "3PT luck", fixed = TRUE)
  expect_match(js, "Without 3-point luck", fixed = TRUE)
  expect_match(js, "League 3P% 35.0%", fixed = TRUE)
  expect_match(js, sprintf("row[%d]", which(names(d) == "luck_3pt_flag") - 1), fixed = TRUE)
})

test_that("onoff_summary_datatable takes the league rate from league_3p when given", {
  CUTS <- seq(0.05, 0.95, by = 0.05)
  HEADER_TOOLTIP_JS <- DT::JS("function(thead, data, start, end, display) {}")
  df <- summary_fixture()
  w <- onoff_summary_datatable(df, NULL, league_3p = 0.4)
  expect_equal(w$x$data$luck_3pt_ours, onoff_3pt_luck(df, league_3p = 0.4)$ours)
  js <- vapply(w$x$options$columnDefs,
               function(cd) paste(as.character(cd$render), collapse = ""), character(1))
  expect_true(any(grepl("League 3P% 40.0%", js, fixed = TRUE)))
})

test_that("onoff_summary_datatable is unchanged when 3P columns are absent", {
  CUTS <- seq(0.05, 0.95, by = 0.05)
  HEADER_TOOLTIP_JS <- DT::JS("function(thead, data, start, end, display) {}")
  df <- data.frame(Team = "A", Player = "P1", `Net RTG Diff` = 2,
                   `Off ON PPP` = 110, `Def ON PPP` = 105,
                   `Off OFF PPP` = 108, `Def OFF PPP` = 107,
                   `ON Poss` = 400, `OFF Poss` = 300, pr_net = 0.2,
                   check.names = FALSE)
  w <- onoff_summary_datatable(df, NULL)
  expect_false(any(grepl("^luck_3pt", names(w$x$data))))
  expect_equal(w$x$data$pr_net, 0.2)
})

test_that("the luck explainer is a Summary-only popover with the given example", {
  html <- as.character(onoff_luck_explainer_ui("onoff_view_mode", "EXAMPLE TEXT"))
  # htmltools escapes ' inside attributes but not in text.
  expect_match(html, "input.onoff_view_mode == &#39;Summary&#39;", fixed = TRUE)
  expect_match(html, "What does '3PT luck' mean?", fixed = TRUE)
  expect_match(html, "bslib-popover", fixed = TRUE)
  expect_match(html, "EXAMPLE TEXT", fixed = TRUE)
})

test_that("both on/off tabs show the luck explainer and pass a season league rate", {
  src <- function(f) paste(readLines(testthat::test_path("..", "..", "R", f),
                                     warn = FALSE), collapse = "\n")
  expect_match(src("ui_tab1_onoff.R"), 'onoff_luck_explainer_ui("onoff_view_mode"',
               fixed = TRUE)
  expect_match(src("ui_tab8_euro.R"), 'onoff_luck_explainer_ui("euro_view_mode"',
               fixed = TRUE)
  for (f in c("server_tab1.R", "server_tab8_euro.R")) {
    expect_match(src(f), "league_3p = onoff_league_3p(mv_result_df())", fixed = TRUE,
                 info = f)
  }
})
