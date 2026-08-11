# Helpers extracted from the per-league tab files so both leagues share one
# implementation. These tests exist because the extractions are byte-identical
# MOVES: they must keep behaving exactly as they did inside the server bodies,
# including when called from file scope rather than from inside a server
# function (where dplyr was already attached by global.R).

test_that("auto_min_on_from_df keeps the top AUTO_TOP_PCT by usage", {
  expect_identical(AUTO_TOP_PCT, 0.35)

  # 10 rows, top 35% = ceiling(3.5) = 4 rows -> lowest of those is 70,
  # floored to the step.
  df <- data.frame(poss = c(100, 90, 80, 70, 60, 50, 40, 30, 20, 10))
  expect_identical(auto_min_on_from_df(df, "poss", step = 10L), 70L)

  # Floors to the step rather than rounding up.
  df2 <- data.frame(poss = c(100, 90, 80, 77, 60, 50, 40, 30, 20, 10))
  expect_identical(auto_min_on_from_df(df2, "poss", step = 10L), 70L)

  # Degenerate inputs return NA, never an error.
  expect_true(is.na(auto_min_on_from_df(NULL, "poss")))
  expect_true(is.na(auto_min_on_from_df(data.frame(), "poss")))
  expect_true(is.na(auto_min_on_from_df(data.frame(other = 1:3), "poss")))
})

test_that("auto_min_all_from_df requires both on and off to clear the bar", {
  # Ordered by usage; the top 4 rows' pmin(on, off) low-water mark is 20.
  df <- data.frame(
    poss = c(100, 90, 80, 70, 60, 50, 40, 30, 20, 10),
    on   = c(100, 90, 80, 70, 60, 50, 40, 30, 20, 10),
    off  = c( 50, 40, 30, 20, 60, 50, 40, 30, 20, 10)
  )
  expect_identical(auto_min_all_from_df(df, "poss", "on", "off", step = 10L), 20L)

  # A missing column is NA, not an error.
  expect_true(is.na(auto_min_all_from_df(df, "poss", "on", "nope")))
})

test_that("resolve_poss_cols picks the columns for the active view mode", {
  summary_df <- data.frame(`ON Poss` = 1, `OFF Poss` = 1, check.names = FALSE)
  expect_identical(resolve_poss_cols(summary_df, "Summary"),
                   list(on = "ON Poss", off = "OFF Poss"))

  ff_df <- data.frame(off_on_poss = 1, off_off_poss = 1)
  expect_identical(resolve_poss_cols(ff_df, "Four Factors"),
                   list(on = "off_on_poss", off = "off_off_poss"))

  # Summary falls back to the raw column names when the display ones are absent.
  expect_identical(resolve_poss_cols(ff_df, "Summary"),
                   list(on = "off_on_poss", off = "off_off_poss"))

  # Four Factors does NOT fall back to the display names.
  expect_identical(resolve_poss_cols(summary_df, "Four Factors"),
                   list(on = NA_character_, off = NA_character_))
})

test_that("onoff_add_ff_ranks derives the display columns and ranks the full population", {
  raw_cols <- as.vector(outer(c("off_on", "off_off", "def_on", "def_off"),
                              c("efg", "oreb", "tov", "ftr"), paste, sep = "_"))

  n <- 8L
  df <- data.frame(
    off_on_poss = c(400, 350, 300, 250, 200, 150, 120, 110),
    `Off ON Diff` = as.character(seq_len(n)),
    `Def ON Diff` = as.character(-seq_len(n)),
    `Net RTG Diff` = seq(1.04, by = 1, length.out = n),
    check.names = FALSE
  )
  for (col in raw_cols) df[[col]] <- seq(0.1, 0.8, length.out = n)

  out <- onoff_add_ff_ranks(df)

  # Derived display columns: the rating diffs are coerced to numeric (the
  # filtered path returns them as text) and Net Diff is rounded to 1dp.
  expect_identical(out$`Off Rtg Diff`, as.numeric(seq_len(n)))
  expect_identical(out$`Def Rtg Diff`, -as.numeric(seq_len(n)))
  expect_identical(out$`Net Diff`, round(df$`Net RTG Diff`, 1))

  # A pr_ colour rank per rated column, and a _rank dot position per raw
  # column -- the two things the diff cell renderer reads.
  expect_true(all(c("pr_net_diff", "pr_off_rtg", "pr_def_rtg") %in% names(out)))
  expect_true(all(paste0("pr_diff_", as.vector(outer(c("off", "def"),
                                                     c("efg", "oreb", "tov", "ftr"),
                                                     paste, sep = "_"))) %in% names(out)))
  expect_true(all(paste0(raw_cols, "_rank") %in% names(out)))
  expect_true(all(out$off_on_efg_rank >= 0 & out$off_on_efg_rank <= 100))

  # Defense polarity is inverted at the coalesce default: a missing Def Rtg
  # Diff ranks as the worst value (999), a missing Off one as the best (-999).
  gaps <- df
  gaps$`Off ON Diff`[[1]] <- NA
  gaps$`Def ON Diff`[[1]] <- NA
  g <- onoff_add_ff_ranks(gaps)
  expect_identical(g$pr_off_rtg[[1]], 0)
  expect_identical(g$pr_def_rtg[[1]], 1)

  # Rows under the ranking threshold come out unranked (NA), not zero -- that
  # gate is what makes these "full population" ranks rather than "whatever
  # survived the filters". The mocks stub adaptive_baseline() to 0 so every row
  # ranks, so install a real threshold for this assertion only.
  env <- environment(onoff_add_ff_ranks)
  old_baseline <- get("adaptive_baseline", envir = env)
  assign("adaptive_baseline", function(poss_vec) 250, envir = env)
  withr::defer(assign("adaptive_baseline", old_baseline, envir = env))

  s <- onoff_add_ff_ranks(df)
  ranked <- df$off_on_poss >= 250
  expect_identical(is.na(s$pr_net_diff), !ranked)
  expect_identical(is.na(s$off_on_efg_rank), !ranked)
  # The surviving percentiles span the ranked rows only, not all eight.
  expect_identical(sum(!is.na(s$pr_net_diff)), sum(ranked))
  expect_identical(range(s$pr_net_diff, na.rm = TRUE), c(0, 1))
})

test_that("onoff_filter_summary_rows floors on the weaker side and honours the team filter", {
  df <- data.frame(
    team_id = c(1L, 1L, 2L, 2L, 3L),
    `ON Poss`  = c(500, 400, 300, 90, 200),
    `OFF Poss` = c(500,  80, 300, 400, 200),
    check.names = FALSE
  )

  # min_all is a bar on BOTH sides: row 2 clears it on ON but not on OFF,
  # row 4 the other way round. Both drop.
  out <- onoff_filter_summary_rows(df, NULL, 100, 0)
  expect_identical(out$team_id, c(1L, 2L, 3L))

  # min_on applies to the ON side only.
  expect_identical(nrow(onoff_filter_summary_rows(df, NULL, 0, 250)), 3L)

  # Team filter first; NULL and empty both mean "all teams".
  expect_identical(unique(onoff_filter_summary_rows(df, 2L, 0, 0)$team_id), 2L)
  expect_identical(nrow(onoff_filter_summary_rows(df, integer(0), 0, 0)), nrow(df))
  expect_identical(nrow(onoff_filter_summary_rows(df, NULL, 0, 0)), nrow(df))

  # NA possessions count as zero rather than dropping out of the comparison.
  na_df <- df
  na_df$`OFF Poss`[[1]] <- NA_real_
  expect_identical(nrow(onoff_filter_summary_rows(na_df, NULL, 100, 0)), 2L)

  # A frame without the possession columns is returned untouched, not errored:
  # only the team filter can apply.
  bare <- data.frame(team_id = c(1L, 2L))
  expect_identical(nrow(onoff_filter_summary_rows(bare, NULL, 999, 999)), 2L)
})

test_that("onoff_filter_ff_rows floors on the weakest of all four possession columns", {
  df <- data.frame(
    team_id = c(1L, 1L, 2L, 2L),
    off_on_poss  = c(500, 400, 300, 500),
    off_off_poss = c(500, 400, 300, 500),
    def_on_poss  = c(500, 400, 300, 500),
    def_off_poss = c(500,  80, 300, 500)
  )

  # Row 2 fails on the defense-off side alone, and that is enough.
  expect_identical(nrow(onoff_filter_ff_rows(df, NULL, 100, 0)), 3L)

  # min_on reads the offense-on column, matching the Summary view's "ON Poss".
  expect_identical(nrow(onoff_filter_ff_rows(df, NULL, 0, 350)), 3L)

  expect_identical(unique(onoff_filter_ff_rows(df, 2L, 0, 0)$team_id), 2L)

  # Missing four-factor columns leave the possession bars inert.
  bare <- data.frame(team_id = c(1L, 2L))
  expect_identical(nrow(onoff_filter_ff_rows(bare, NULL, 999, 999)), 2L)
})

test_that("onoff_fallback_needed sends only real filter narrowing to the SQL path", {
  bounds <- list(start = as.Date("2025-10-01"), end = as.Date("2026-07-01"))
  full_range <- c(bounds$start, bounds$end)
  no_filters <- list(game_type = NULL, opp_ids = NULL, home_away = "", outcome = "",
                     rank_side = "", num_starters_off_mode = "", num_starters_off = "",
                     num_starters_def_mode = "", num_starters_def = "")
  no_gn <- list(min_gn = NA_integer_, max_gn = NA_integer_, last_n = NA_integer_)
  no_input <- list()

  # The untouched full-season window stays on the materialized view.
  expect_false(onoff_fallback_needed(full_range, bounds, no_filters, no_gn, no_input, "on"))

  # A narrowed date window does not.
  expect_true(onoff_fallback_needed(c(bounds$start + 7, bounds$end), bounds,
                                    no_filters, no_gn, no_input, "on"))

  # Missing or unparseable dates fall back to the MV rather than firing a query.
  expect_false(onoff_fallback_needed(NULL, bounds, no_filters, no_gn, no_input, "on"))
  expect_false(onoff_fallback_needed(c(NA, NA), bounds, no_filters, no_gn, no_input, "on"))

  # Each filter on its own is enough to leave the fast path.
  each <- list(
    list(game_type = "1"), list(opp_ids = c("4", "5")), list(home_away = "home"),
    list(outcome = "win"), list(rank_side = "top"),
    list(num_starters_off_mode = "gte", num_starters_off = "3")
  )
  for (one in each) {
    f <- modifyList(no_filters, one)
    expect_true(onoff_fallback_needed(full_range, bounds, f, no_gn, no_input, "on"))
  }

  # A starters mode without its value is incomplete, so it does not count.
  expect_false(onoff_fallback_needed(
    full_range, bounds, modifyList(no_filters, list(num_starters_def_mode = "lte")),
    no_gn, no_input, "on"))

  # Resolved GN params leave the fast path...
  expect_true(onoff_fallback_needed(full_range, bounds, no_filters,
                                    list(min_gn = 5L, max_gn = NA_integer_, last_n = NA_integer_),
                                    no_input, "on"))

  # ...and so do raw prefixed inputs the debounce has not resolved yet, which is
  # what stops the tab serving MV numbers mid-keystroke. The prefix selects the
  # league's own input ids.
  expect_true(onoff_fallback_needed(full_range, bounds, no_filters, no_gn,
                                    list(euro_last_n = "5"), "euro"))
  expect_false(onoff_fallback_needed(full_range, bounds, no_filters, no_gn,
                                     list(euro_last_n = "5"), "on"))
})

test_that("onoff input and query mapping is shared without erasing league dimensions", {
  input <- list(
    on_game_type = c("5", "16"), on_opponents = c("2", "3"),
    on_home_away = "home", on_outcome = "win",
    on_opp_rank_side = "top", on_opp_rank_n = "4", on_opp_rank_metric = "net",
    on_num_starters_off_mode = "gte", on_num_starters_off = "3",
    on_num_starters_def_mode = "lte", on_num_starters_def = "2",
    euro_phase = c("RS", "PO"), euro_opponents = "9",
    euro_home_away = "away", euro_outcome = "loss",
    euro_opp_rank_side = "bottom", euro_opp_rank_n = "6", euro_opp_rank_metric = "def",
    euro_num_starters_off_mode = "", euro_num_starters_off = "",
    euro_num_starters_def_mode = "gte", euro_num_starters_def = "4"
  )

  il <- onoff_filter_values(input, "on")
  el <- onoff_filter_values(input, "euro", game_type_id = "euro_phase")
  expect_identical(il$game_type, c("5", "16"))
  expect_identical(el$game_type, c("RS", "PO"))
  expect_identical(el$opp_ids, "9")

  gn <- list(min_gn = 2L, max_gn = 10L, last_n = NA_integer_)
  il_args <- onoff_db_args(il, gn, opponent_ids = c("7", "8"))
  el_args <- onoff_db_args(el, gn)
  expect_identical(il_args$game_type_csv, "5,16")
  expect_identical(il_args$opp_ids_csv, "7,8")
  expect_identical(il_args$num_starters_off_min, 3L)
  expect_identical(il_args$num_starters_def_max, 2L)
  expect_identical(el_args$game_type_csv, "RS,PO")
  expect_identical(el_args$opp_ids_csv, "9")
  expect_true(is.na(el_args$num_starters_off_min))
  expect_identical(el_args$num_starters_def_min, 4L)
  expect_identical(el_args$last_n_games, NA_integer_)
})

test_that("the shared stat-filter menus cover every column both leagues filter on", {
  # Summary: the nine rating/usage entries plus a shot-split group per context.
  expect_identical(
    unname(ONOFF_SUMMARY_FILTERABLE_COLS[c("Net", "Off", "Def")]),
    c("Net RTG Diff", "Off ON Diff", "Def ON Diff")
  )
  for (p in c("on_off", "on_def", "off_off", "off_def")) {
    expect_true(all(shot_split_metric_cols(p, p) %in% ONOFF_SUMMARY_FILTERABLE_COLS))
  }
  expect_true(all(c("minutes", "ON Poss", "OFF Poss") %in% ONOFF_SUMMARY_FILTERABLE_COLS))

  # Four Factors: a diff column per factor per side, plus the rating diffs.
  expect_true(all(
    as.vector(outer(c("Off ", "Def "), c("eFG% Diff", "OREB% Diff", "TOV% Diff", "FTR Diff"), paste0))
      %in% ONOFF_FF_FILTERABLE_COLS
  ))
  expect_true(all(c("Net Diff", "Off Rtg Diff", "Def Rtg Diff") %in% ONOFF_FF_FILTERABLE_COLS))

  # Both menus are name -> column maps with no blank or duplicated labels: the
  # stat-filter UI keys its controls on the names.
  for (menu in list(ONOFF_SUMMARY_FILTERABLE_COLS, ONOFF_FF_FILTERABLE_COLS)) {
    expect_true(all(nzchar(names(menu))))
    expect_false(anyDuplicated(names(menu)) > 0)
  }
})

test_that("ff_diff_cell_js emits the same JS both leagues rendered before extraction", {
  # These are the exact strings server_tab1.R and server_tab8_euro.R produced
  # from their own inline copies, captured before the extraction. They pin the
  # move: any change to the template shows up here rather than in the browser.
  js_on  <- ff_diff_cell_js(11L, 22L, 33L, 44L, 1.45, " pts allowed", "TIPTEXT",
                            show_impact = TRUE)
  js_off <- ff_diff_cell_js(11L, 22L, 33L, 44L, 1.45, " pts allowed", "TIPTEXT",
                            show_impact = FALSE)

  # The Israeli guard runs the estimate; the EuroLeague one compiles it out.
  expect_match(js_on, "if (data !== null && data !== '' && !isNaN(parseFloat(data))) {",
               fixed = TRUE)
  expect_match(js_off, "if (false) {", fixed = TRUE)

  # Everything else is identical between the two.
  expect_identical(
    sub("if (data !== null && data !== '' && !isNaN(parseFloat(data))) {", "if (false) {",
        js_on, fixed = TRUE),
    js_off
  )

  # Column indices land in the right slots, in order.
  expect_match(js_off, "var onVal   = row[11]", fixed = TRUE)
  expect_match(js_off, "var offVal  = row[22]", fixed = TRUE)
  expect_match(js_off, "var onPct   = row[33]", fixed = TRUE)
  expect_match(js_off, "var offPct  = row[44]", fixed = TRUE)

  # The weight and its wording reach the estimate line.
  expect_match(js_on, "var w = 1.450000;", fixed = TRUE)
  expect_match(js_on, "title=\"TIPTEXT\"", fixed = TRUE)
  expect_match(js_on, " pts allowed</div>", fixed = TRUE)

  # Defaults keep a caller that supplies only indices safe.
  expect_match(ff_diff_cell_js(1L, 2L, 3L, 4L), "var w = 0.000000;", fixed = TRUE)
})

test_that("onoff_summary_datatable builds the shared Summary widget", {
  # Globals the function reads from global.R, which the mocks do not load.
  CUTS <- seq(0.05, 0.95, by = 0.05)
  COLS_GRAD <- colorRampPalette(c("#8b2020", "#6b5a20", "#1a6b38"))(20)
  COLS_REV <- rev(COLS_GRAD)
  HEADER_TOOLTIP_JS <- DT::JS("function(thead, data, start, end, display) {}")

  shot_raw_cols <- c(
    "off_on_fg2_made", "off_on_fg2_att", "off_on_fg3_made", "off_on_fg3_att",
    "def_on_fg2_made", "def_on_fg2_att", "def_on_fg3_made", "def_on_fg3_att",
    "off_off_fg2_made", "off_off_fg2_att", "off_off_fg3_made", "off_off_fg3_att",
    "def_off_fg2_made", "def_off_fg2_att", "def_off_fg3_made", "def_off_fg3_att")

  n <- 6L
  mk <- function(v) rep(v, n)
  df <- data.frame(
    Team = mk("T"), Player = paste0("P", seq_len(n)),
    `Net RTG Diff` = mk(1.5), `Off ON Diff` = mk(1), `Def ON Diff` = mk(-1),
    `Off ON PPP` = mk(110), `Def ON PPP` = mk(105), `On Net RTG` = mk(5),
    `Off Shot ON` = mk(120), `Def Shot ON` = mk(120),
    `Off OFF PPP` = mk(108), `Def OFF PPP` = mk(107), `Off Net RTG` = mk(1),
    `Off Shot OFF` = mk(120), `Def Shot OFF` = mk(120),
    minutes = mk(500), `ON Poss` = mk(900), `OFF Poss` = mk(800),
    check.names = FALSE)
  for (col in shot_raw_cols) df[[col]] <- mk(60L)
  for (p in c("pr_net", "pr_off_on_d", "pr_def_on_d", "pr_off_on",
              "pr_def_on_inv", "pr_on_net", "pr_off_off", "pr_def_off_inv",
              "pr_off_net")) {
    df[[p]] <- seq(0, 1, length.out = n)
  }

  w <- onoff_summary_datatable(df, NULL)
  expect_s3_class(w, "datatables")
  expect_identical(nrow(w$x$data), n)

  # The grouped two-row header both leagues rely on.
  expect_true(grepl("group-head", as.character(w$x$container), fixed = TRUE))
  expect_true(grepl("On Court Stats", as.character(w$x$container), fixed = TRUE))

  # Percentile-rank columns are data, not display: they must be hidden.
  expect_true(any(vapply(w$x$options$columnDefs,
                         function(d) isFALSE(d$visible), logical(1))))

  # One render per shot column, on both ends and both on/off contexts.
  expect_gte(sum(vapply(w$x$options$columnDefs,
                        function(d) !is.null(d$render), logical(1))), 4L)
})

test_that("onoff_four_factors_datatable differs between leagues only in the impact estimate", {
  # Globals the function reads from global.R, which the mocks do not load.
  CUTS <- seq(0.05, 0.95, by = 0.05)
  COLS_GRAD <- colorRampPalette(c("#8b2020", "#6b5a20", "#1a6b38"))(20)
  COLS_REV <- rev(COLS_GRAD)
  HEADER_TOOLTIP_JS <- DT::JS("function(thead, data, start, end, display) {}")
  OFF_OREB_TOOLTIP <- "off oreb"
  DEF_OREB_TOOLTIP <- "def oreb"

  raw_cols <- as.vector(outer(c("off_on", "off_off", "def_on", "def_off"),
                              c("efg", "oreb", "tov", "ftr"), paste, sep = "_"))
  diff_cols <- c("Off eFG% Diff", "Off OREB% Diff", "Off TOV% Diff", "Off FTR Diff",
                 "Def eFG% Diff", "Def OREB% Diff", "Def TOV% Diff", "Def FTR Diff")

  n <- 6L
  df <- data.frame(Team = rep("T", n), Player = paste0("P", seq_len(n)),
                   `Net Diff` = rep(1.5, n), `Off Rtg Diff` = rep(1, n),
                   `Def Rtg Diff` = rep(-1, n), minutes = rep(500, n),
                   off_on_poss = rep(900, n), off_off_poss = rep(800, n),
                   check.names = FALSE)
  for (col in raw_cols) {
    df[[col]] <- rep(0.5, n)
    df[[paste0(col, "_rank")]] <- seq(0, 100, length.out = n)
  }
  for (col in diff_cols) df[[col]] <- rep(2.5, n)
  for (col in c("pr_net_diff", "pr_off_rtg", "pr_def_rtg",
                paste0("pr_diff_", c("off_efg", "off_oreb", "off_tov", "off_ftr",
                                     "def_efg", "def_oreb", "def_tov", "def_ftr")))) {
    df[[col]] <- seq(0, 1, length.out = n)
  }

  il <- onoff_four_factors_datatable(df, NULL, show_impact = TRUE)
  el <- onoff_four_factors_datatable(df, NULL, show_impact = FALSE)

  expect_s3_class(il, "datatables")
  expect_s3_class(el, "datatables")
  expect_identical(nrow(il$x$data), n)

  # Same table on both sides: same columns, in the same order.
  expect_identical(names(il$x$data), names(el$x$data))
  expect_identical(as.character(il$x$container), as.character(el$x$container))

  # One diff-cell renderer per four-factor column, on both sides.
  n_render <- function(w) sum(vapply(w$x$options$columnDefs,
                                     function(d) !is.null(d$render), logical(1)))
  expect_identical(n_render(il), n_render(el))
  expect_gte(n_render(il), length(diff_cols))

  js <- function(w) paste(vapply(w$x$options$columnDefs,
                                 function(d) if (is.null(d$render)) "" else as.character(d$render),
                                 character(1)), collapse = "\n")

  # Israeli cells carry the est. +/-X pts annotation with the fitted weights and
  # the defense wording; EuroLeague cells compile it out entirely.
  expect_match(js(il), sprintf("var w = %f;", FF_IMPACT_WEIGHTS[["efg"]]), fixed = TRUE)
  expect_match(js(il), " pts allowed</div>", fixed = TRUE)
  expect_match(js(il), FF_IMPACT_EST_TITLE, fixed = TRUE)
  expect_false(grepl("if (false) {", js(il), fixed = TRUE))

  expect_match(js(el), "if (false) {", fixed = TRUE)
  expect_match(js(el), "var w = 0.000000;", fixed = TRUE)
  expect_false(grepl(" pts allowed", js(el), fixed = TRUE))
  expect_false(grepl(FF_IMPACT_EST_TITLE, js(el), fixed = TRUE))
})
