source(repo_file("R", "helpers.R"), local = TRUE)

# These assertions are chosen to fail on the ways an "extraction" can quietly
# lose something: the baseline track, the weighting that separates the on-court
# value from the off-court one, and the impact estimate that only one caller
# passes. A test that merely checks the container classes are present would
# pass while all three were gone.

test_that("range_cell_js emits both branches with the full markup", {
  js <- range_cell_js("diffVal", "onVal", "offVal")

  # unranked branch
  expect_true(grepl("diff-val unranked", js, fixed = TRUE))
  expect_true(grepl("rank-bar-container hidden", js, fixed = TRUE))

  # ranked branch
  expect_true(grepl("rank-bar-container", js, fixed = TRUE))
  expect_true(grepl("rank-track", js, fixed = TRUE))
  expect_true(grepl("range-connect", js, fixed = TRUE))
  expect_true(grepl("dot-off", js, fixed = TRUE))
  expect_true(grepl("dot-on", js, fixed = TRUE))

  # The on dot must paint after the off dot so it wins the overlap.
  expect_lt(
    regexpr("dot-off", js, fixed = TRUE),
    regexpr("dot-on", js, fixed = TRUE)
  )
})

test_that("the sub-text keeps on-court primary and off-court secondary", {
  js <- range_cell_js("diffVal", "onVal", "offVal")

  # Flattening this to plain text would render both values with equal weight
  # and lose which one is the on-court figure.
  expect_true(grepl("font-weight:700", js, fixed = TRUE))
  expect_true(grepl("opacity:0.6", js, fixed = TRUE))

  # Weight separates the on-court value from the off-court one. Their colours
  # remain separate semantic tokens, but the current heat ramp requires both
  # to resolve to white to retain normal-text contrast at its brightest green.
  expect_true(grepl("font-weight:700; color:var(--ibpl-cell-text)", js, fixed = TRUE))
  expect_true(grepl("color:var(--ibpl-cell-text-2)", js, fixed = TRUE))
  expect_false(grepl("#666", js, fixed = TRUE))
  expect_false(grepl("#222", js, fixed = TRUE))
})

test_that("extra_expr reaches both branches, and is absent when not passed", {
  with_extra <- range_cell_js("diffVal", "onVal", "offVal", extra_expr = "estLine")
  without <- range_cell_js("head", "onTxt", "offTxt")

  # Four Factors appends its est. +/-X pts line to the unranked branch as well
  # as the ranked one; dropping either would blank the annotation on some rows.
  expect_equal(length(gregexpr("estLine", with_extra, fixed = TRUE)[[1]]), 2)
  expect_false(grepl("estLine", without, fixed = TRUE))
})

test_that("the unranked test keys on the on-court percentile alone", {
  js <- range_cell_js("diffVal", "onVal", "offVal")

  # A row with a known on-court rank and a missing off-court rank renders the
  # ranked branch. Widening this condition would change which rows show dots.
  expect_true(grepl("onPct === null || onPct === undefined", js, fixed = TRUE))
  expect_false(grepl("isNaN(offPct)", js, fixed = TRUE))
})

test_that("both call sites go through the shared builder", {
  helpers_txt <- read_repo_txt("R", "helpers.R")
  tab1_txt <- read_repo_txt("R", "server_tab1.R")

  expect_true(grepl("range_cell_js <- function", helpers_txt, fixed = TRUE))
  expect_true(grepl("range_cell_js(", tab1_txt, fixed = TRUE))

  # The markup exists once: inside the builder, and nowhere by hand.
  expect_equal(length(gregexpr("rank-track", helpers_txt, fixed = TRUE)[[1]]), 1)
  expect_false(grepl("rank-track", tab1_txt, fixed = TRUE))
})

# --- Summary verdict columns -------------------------------------------------

# Isolate onoff_summary_datatable's body so these assertions cannot be
# satisfied by the four-factors renderer further down the same file.
summary_body <- function() {
  helpers_txt <- read_repo_txt("R", "helpers.R")
  start <- regexpr("onoff_summary_datatable <- function", helpers_txt, fixed = TRUE)
  end <- regexpr("onoff_four_factors_datatable <- function", helpers_txt, fixed = TRUE)
  substring(helpers_txt, start, end)
}

test_that("the summary verdict columns stay percentile-ranked heat cells", {
  # The range track was tried here and reverted: against nine ranked columns it
  # read as busier, not clearer. Summary encodes rank as colour; the range
  # track is the Four Factors grammar and stays there.
  expect_false(grepl("range_cell_js(", summary_body(), fixed = TRUE))
})

test_that("every ranked summary column carries its heat colour", {
  body <- summary_body()

  styled <- regmatches(body, gregexpr('formatStyle\\(dt, "[^"]+"', body))[[1]]
  styled <- sub('formatStyle\\(dt, "', "", styled)
  styled <- sub('"$', "", styled)

  # The three verdict columns and the six context columns beside them. Dropping
  # any of these is what the revert put back.
  expect_setequal(styled, c(
    "Net RTG Diff", "Off ON Diff", "Def ON Diff",
    "Off ON PPP", "Def ON PPP", "On Net RTG",
    "Off OFF PPP", "Def OFF PPP", "Off Net RTG"
  ))
})

test_that("the summary table emits no HTML, so it needs no escape allowlist", {
  # Every summary cell is plain text now. Nothing here may relax DT's escaping.
  expect_false(grepl("escape =", summary_body(), fixed = TRUE))
})

test_that("Net RTG Diff runs through the shared plain diff formatter again", {
  body <- summary_body()

  expect_true(grepl("idx_diff <- which(names(df) %in% diff_cols) - 1", body, fixed = TRUE))
  expect_false(grepl('setdiff(diff_cols, "Net RTG Diff")', body, fixed = TRUE))
})

test_that("percent signs match how the call site consumes the JS", {
  # A call site that passes the result through sprintf() needs %% ; one that
  # concatenates straight into DT::JS() needs % , or the browser is handed
  # "left:42%%" and positions nothing.
  escaped <- range_cell_js("v", "on", "off")
  plain <- range_cell_js("v", "on", "off", sprintf_escaped = FALSE)

  expect_true(grepl("left:' + rangeLineLeft + '%%;", escaped, fixed = TRUE))
  expect_true(grepl("left:' + rangeLineLeft + '%;", plain, fixed = TRUE))
  expect_false(grepl("%%", plain, fixed = TRUE))
})

test_that("the rating columns render through the shared range cell", {
  helpers_txt <- read_repo_txt("R", "helpers.R")
  start <- regexpr("onoff_four_factors_datatable <- function", helpers_txt, fixed = TRUE)
  ff <- substring(helpers_txt, start)

  # Off/Def Rtg Diff take the four-factor grammar. They are deliberately NOT in
  # metric_map: vis_cols already names them, and a second mention there would
  # select the same column twice.
  expect_true(grepl("rtg_map <- list(", ff, fixed = TRUE))
  expect_true(grepl('"Off Rtg Diff" = c("off_on_ppp", "off_off_ppp")', ff, fixed = TRUE))
  expect_true(grepl('"Def Rtg Diff" = c("def_on_ppp", "def_off_ppp")', ff, fixed = TRUE))
  expect_true(grepl("cell_map <- c(metric_map, rtg_map)", ff, fixed = TRUE))
  # metric_map itself must stay the eight four-factor columns: a "Rtg Diff"
  # entry there would put the name into vis_cols twice.
  mm_at <- regexpr("metric_map <- list(", ff, fixed = TRUE)
  expect_false(grepl("Rtg Diff", substring(ff, mm_at, mm_at + 420L), fixed = TRUE))
})

test_that("a rating cell carries no estimated-points line", {
  helpers_txt <- read_repo_txt("R", "helpers.R")
  start <- regexpr("onoff_four_factors_datatable <- function", helpers_txt, fixed = TRUE)
  ff <- substring(helpers_txt, start)

  # The rating diff IS the points. An est. line derived from it would restate
  # it, and FF_METRIC_FACTOR has no entry for these two names anyway.
  expect_true(grepl("isTRUE(show_impact) && !is_rating", ff, fixed = TRUE))
  expect_true(grepl("show_impact = isTRUE(show_impact) && !is_rating", ff, fixed = TRUE))
})

test_that("the rating components are carried but never shown", {
  helpers_txt <- read_repo_txt("R", "helpers.R")
  start <- regexpr("onoff_four_factors_datatable <- function", helpers_txt, fixed = TRUE)
  ff <- substring(helpers_txt, start)

  # Selected so the renderer can index them, hidden so they are not four extra
  # columns of raw ratings. They rendered visibly in the browser when the second
  # half of this was missing, with the suite green.
  expect_true(grepl("any_of(rtg_cols_all)", ff, fixed = TRUE))
  expect_true(grepl("hide_cols <- c(rank_cols, raw_cols_all, rtg_cols_all,", ff, fixed = TRUE))
})

test_that("only the rating columns without a range cell keep the plain + renderer", {
  helpers_txt <- read_repo_txt("R", "helpers.R")
  start <- regexpr("onoff_four_factors_datatable <- function", helpers_txt, fixed = TRUE)
  ff <- substring(helpers_txt, start)

  # Two renderers on one column would leave the outcome to DataTables'
  # columnDefs precedence -- the trap Task 9 hit on the Summary view.
  expect_true(grepl('plain_rtg <- setdiff(c("Off Rtg Diff", "Def Rtg Diff"), rendered_rtg)', ff, fixed = TRUE))
  expect_true(grepl("rtg_diff_idx <- which(names(df_final) %in% plain_rtg) - 1L", ff, fixed = TRUE))
})

test_that("both leagues supply the rating components on the MV path", {
  for (f in c("server_tab1.R", "server_tab8_euro.R")) {
    txt <- read_repo_txt("R", f)
    expect_true(grepl("off_on_ppp = `Off ON PPP`", txt, fixed = TRUE), info = f)
    expect_true(grepl("def_off_ppp = `Def OFF PPP`", txt, fixed = TRUE), info = f)
  }
})

test_that("the summary sketch's headers map to the first 18 columns", {
  body <- summary_body()

  # The container assigns its <th> positionally across the first 18 columns,
  # visible or not, so a HIDDEN column among them shifts every header one to
  # the left. Putting the pivot ids at position 3 did exactly that: the header
  # row read Team, Player, Def, ... with "Net" and "Off" consumed by team_id
  # and player_id, and the body drifted 109px out of line. Browser-only defect;
  # the suite was green throughout.
  n_sub_head <- length(gregexpr('class="sub-head', body, fixed = TRUE)[[1]])
  expect_equal(n_sub_head, 18L)

  # Everything hidden must be listed after the 18 display columns. regexpr
  # takes the first hit, which is the keep_cols one rather than hide_cols.
  ids_at <- regexpr('"team_id", "player_id"', body, fixed = TRUE)
  usage_at <- regexpr('"minutes", "ON Poss", "OFF Poss"', body, fixed = TRUE)
  expect_gt(as.integer(ids_at), as.integer(usage_at))
})
