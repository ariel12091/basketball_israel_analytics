# Sample-size adjustment on the On/Off Net RTG Diff (decided 2026-10-06).
# Each side (on, off) is padded with ONOFF_PAD_POSS possessions at the team's
# own net rating -- the padding approach, with the padding fitted on 2025-2026
# Israeli data to predict rest-of-season on/off. A row is tagged "small
# sample" when its raw number is in the table's top or bottom 20% and it rests
# on far less evidence (shrink under 0.6x the table median) than most rows.

test_that("onoff_padded_net matches padding each side toward the team net", {
  # On +10 over 1500 poss, off -10 over 500: team net = +5. Padded on =
  # (10*1500 + 5*4000) / 5500, padded off = (-10*500 + 5*4000) / 4500.
  df <- data.frame(`Net RTG Diff` = 20, `ON Poss` = 1500, `OFF Poss` = 500,
                   check.names = FALSE)
  by_hand <- (10 * 1500 + 5 * 4000) / 5500 - (-10 * 500 + 5 * 4000) / 4500
  expect_equal(onoff_padded_net(df, pad = 4000), by_hand)
  # Equal samples: neff 500 * (2 / 5000) = 0.2 of the raw number.
  df <- data.frame(`Net RTG Diff` = 20, `ON Poss` = 1000, `OFF Poss` = 1000,
                   check.names = FALSE)
  expect_equal(onoff_padded_net(df, pad = 4000), 4)
  expect_equal(onoff_padded_net(df, pad = 0), 20)
})

test_that("onoff_padded_net uses ONOFF_PAD_POSS by default", {
  df <- data.frame(`Net RTG Diff` = 20, `ON Poss` = 1000, `OFF Poss` = 1000,
                   check.names = FALSE)
  expect_equal(onoff_padded_net(df), onoff_padded_net(df, pad = ONOFF_PAD_POSS))
})

test_that("onoff_padded_net is NA without possessions and NULL without columns", {
  df <- data.frame(`Net RTG Diff` = c(20, 20, NA), `ON Poss` = c(1000, 1000, 1000),
                   `OFF Poss` = c(1000, 0, 1000), check.names = FALSE)
  out <- onoff_padded_net(df)
  expect_false(is.na(out[1]))
  expect_true(all(is.na(out[2:3])))
  df$`OFF Poss` <- NULL
  expect_null(onoff_padded_net(df))
})

test_that("onoff_pad_shrink is the share of the raw number padding keeps", {
  df <- data.frame(`Net RTG Diff` = c(20, 50), `ON Poss` = c(1000, 1000),
                   `OFF Poss` = c(1000, 0), check.names = FALSE)
  s <- onoff_pad_shrink(df)
  expect_equal(s[1], 0.2)
  expect_true(is.na(s[2]))
  expect_equal(onoff_padded_net(df), df$`Net RTG Diff` * s)
  expect_null(onoff_pad_shrink(df[, "Net RTG Diff", drop = FALSE]))
})

test_that("onoff_sample_flag tags standouts that rest on thin evidence", {
  net <- seq(-45, 45, by = 10)          # 10 rows; top/bottom 20% = 2 each end
  shrink <- rep(0.2, 10)
  expect_false(any(onoff_sample_flag(net, shrink)))
  shrink[c(1, 10)] <- 0.05              # standouts on thin evidence
  shrink[5] <- 0.05                     # thin, but middling: not tagged
  expect_identical(which(onoff_sample_flag(net, shrink)), c(1L, 10L))
  # Rank alone never tags: a standout on typical evidence stays untagged.
  shrink[10] <- 0.15
  expect_identical(which(onoff_sample_flag(net, shrink)), 1L)
})

test_that("onoff_sample_flag never tags small tables and skips NA rows", {
  expect_false(any(onoff_sample_flag(1:9 * 10, c(rep(0.2, 8), 0.01))))
  net <- c(seq(-45, 45, by = 10), NA)
  shrink <- c(rep(0.2, 9), 0.05, 0.01)
  f <- onoff_sample_flag(net, shrink)
  expect_identical(which(f), 10L)
  expect_false(f[11])
})

test_that("onoff_summary_datatable carries the padded net and the tag", {
  CUTS <- seq(0.05, 0.95, by = 0.05)
  HEADER_TOOLTIP_JS <- DT::JS("function(thead, data, start, end, display) {}")
  df <- data.frame(Team = "A", Player = paste0("P", 1:10),
                   `Net RTG Diff` = seq(-45, 45, by = 10),
                   `Off ON PPP` = 110, `Def ON PPP` = 105,
                   `Off OFF PPP` = 108, `Def OFF PPP` = 107,
                   `ON Poss` = 1000, `OFF Poss` = 1000, pr_net = 0.5,
                   check.names = FALSE)
  df$`OFF Poss`[10] <- 20               # +45 on a tiny off-court sample
  w <- onoff_summary_datatable(df, NULL)
  d <- w$x$data
  expect_equal(d$net_padded, onoff_padded_net(df))
  expect_identical(which(d$sample_flag), 10L)

  hidden <- unlist(lapply(w$x$options$columnDefs,
                          function(cd) if (isFALSE(cd$visible)) cd$targets))
  for (col in c("net_padded", "sample_flag")) {
    expect_true((which(names(d) == col) - 1) %in% hidden, info = col)
  }

  net_idx <- which(names(d) == "Net RTG Diff") - 1
  net_render <- Filter(function(cd) !is.null(cd$render) && net_idx %in% cd$targets,
                       w$x$options$columnDefs)
  expect_length(net_render, 1L)
  js <- as.character(net_render[[1]]$render)
  expect_match(js, "attr('padded', p)", fixed = TRUE)
  expect_match(js, "small sample", fixed = TRUE)
  expect_match(js, sprintf("row[%d]", which(names(d) == "net_padded") - 1), fixed = TRUE)
  expect_match(js, sprintf("row[%d]", which(names(d) == "sample_flag") - 1), fixed = TRUE)
  # The tag carries its own hover kind, so it shows only the sample section.
  expect_match(js, "tagHtml('small sample', 'sample')", fixed = TRUE)
  # The template is pasted onto one line, so a // comment would eat the rest.
  expect_false(grepl("//", js, fixed = TRUE))
  for (col in c("ON Poss", "OFF Poss")) {
    expect_match(js, sprintf("row[%d]", which(names(d) == col) - 1), fixed = TRUE, info = col)
  }
})

test_that("the Net cell keeps both luck and sample-size parts when 3P data exists", {
  CUTS <- seq(0.05, 0.95, by = 0.05)
  HEADER_TOOLTIP_JS <- DT::JS("function(thead, data, start, end, display) {}")
  df <- data.frame(
    team_id = c(1, 1, 2), Team = c("A", "A", "B"), Player = c("P1", "P2", "P3"),
    `Net RTG Diff` = c(2, 40, -3), `ON Poss` = 400, `OFF Poss` = 300,
    `Off ON PPP` = 110, `Def ON PPP` = 105, `Off OFF PPP` = 108, `Def OFF PPP` = 107,
    off_on_fg3_made = c(25, 20, 21), off_on_fg3_att = c(60, 50, 70),
    off_off_fg3_made = c(15, 20, 9), off_off_fg3_att = c(40, 50, 30),
    def_on_fg3_made = 17, def_on_fg3_att = 50, def_off_fg3_made = 14, def_off_fg3_att = 40,
    pr_net = 0.5, check.names = FALSE)
  w <- onoff_summary_datatable(df, NULL)
  js <- as.character(Filter(function(cd) !is.null(cd$render) &&
                              (which(names(w$x$data) == "Net RTG Diff") - 1) %in% cd$targets,
                            w$x$options$columnDefs)[[1]]$render)
  expect_match(js, "attr('luck-ours', o)", fixed = TRUE)
  expect_match(js, "attr('padded', p)", fixed = TRUE)
  # Three rows: too few to rank, so nothing is tagged.
  expect_false(any(w$x$data$sample_flag))
})

test_that("app.js draws the Net cell tooltip from the renderer's data attributes", {
  js <- paste(readLines(test_path("..", "..", "www", "app.js"), warn = FALSE), collapse = "\n")
  for (s in c(".onoff-net-tip", "d.luckOurs", "d.padded", "d.possOn", "d.sampleFlag",
              "Without 3PT luck", "Adjusted for sample size",
              "far fewer than most rows in this table")) {
    expect_match(js, s, fixed = TRUE, info = s)
  }
})

test_that("the explainer covers the small-sample tag", {
  html <- as.character(onoff_luck_explainer_ui("onoff_view_mode", "EXAMPLE TEXT"))
  expect_match(html, "small sample", fixed = TRUE)
  expect_match(html, "Adjusted for sample size", fixed = TRUE)
})
