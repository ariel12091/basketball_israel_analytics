test_that("both euro read-layer enumerations list the ribbon views", {
  # The curated grant list is duplicated in two files. They must agree, or
  # apply_db_security.R grants a relation the audit then reports as unexpected.
  grant_sql <- paste(readLines(
    testthat::test_path("..", "..", "..", "sql", "security", "enable_readonly_rls.sql"),
    warn = FALSE), collapse = "\n")
  audit_sql <- paste(readLines(
    testthat::test_path("..", "..", "..", "sql", "security", "audit_app_access.sql"),
    warn = FALSE), collapse = "\n")

  for (view in c("ribbon_segments_v", "ribbon_margin_v")) {
    expect_match(grant_sql, view, fixed = TRUE)
    expect_match(audit_sql, view, fixed = TRUE)
  }
})

test_that("the two euro read-layer lists contain exactly the same relations", {
  extract <- function(path, pattern) {
    txt <- paste(readLines(testthat::test_path("..", "..", "..", "sql", "security", path),
                           warn = FALSE), collapse = "\n")
    block <- regmatches(txt, regexpr(pattern, txt, perl = TRUE))
    sort(unique(gsub("'", "", regmatches(block, gregexpr("'[a-z_]+'", block))[[1]])))
  }
  grants <- extract("enable_readonly_rls.sql",
                    "(?s)euro_app_relations constant text\\[\\] := ARRAY\\[.*?\\];")
  audit <- extract("audit_app_access.sql",
                   "(?s)euro_app_relations\\(relation_name\\) AS \\(.*?\\n\\),")
  expect_identical(grants, audit)
})

# Pure transforms behind the stint ribbon. These run without a database:
# the readers hand them plain data frames, so every geometry rule and every
# merge rule is checked here rather than through a query.

lane_row <- function(side, player_key, start_elapsed, end_elapsed,
                     is_starter = FALSE, player_label = NULL) {
  data.frame(
    side = side,
    player_key = player_key,
    player_label = player_label %||% paste("Player", player_key),
    is_starter = is_starter,
    start_elapsed = start_elapsed,
    end_elapsed = end_elapsed,
    stringsAsFactors = FALSE
  )
}

test_that("merge_adjacent_stints collapses a contiguous run into one bar", {
  lanes <- rbind(
    lane_row("own", "7", 0, 242),
    lane_row("own", "7", 242, 337),
    lane_row("own", "7", 337, 410)
  )
  out <- merge_adjacent_stints(lanes)
  expect_identical(nrow(out), 1L)
  expect_identical(out$start_elapsed, 0)
  expect_identical(out$end_elapsed, 410)
})

test_that("merge_adjacent_stints splits on a real gap", {
  lanes <- rbind(
    lane_row("own", "7", 0, 242),
    lane_row("own", "7", 600, 700)
  )
  out <- merge_adjacent_stints(lanes)
  expect_identical(nrow(out), 2L)
  expect_identical(out$end_elapsed, c(242, 700))
})

test_that("merge_adjacent_stints never merges across players or sides", {
  lanes <- rbind(
    lane_row("own", "7", 0, 242),
    lane_row("own", "8", 242, 337),
    lane_row("opp", "7", 337, 410)
  )
  out <- merge_adjacent_stints(lanes)
  expect_identical(nrow(out), 3L)
})

test_that("merge_adjacent_stints tolerates unsorted input", {
  lanes <- rbind(
    lane_row("own", "7", 242, 337),
    lane_row("own", "7", 0, 242)
  )
  out <- merge_adjacent_stints(lanes)
  expect_identical(nrow(out), 1L)
  expect_identical(out$start_elapsed, 0)
  expect_identical(out$end_elapsed, 337)
})

test_that("merge_adjacent_stints returns empty input unchanged", {
  empty <- lane_row("own", "7", 0, 1)[0, , drop = FALSE]
  expect_identical(nrow(merge_adjacent_stints(empty)), 0L)
})



test_that("ribbon_mark_starters flags whoever is on the floor in the first segment", {
  lanes <- rbind(
    lane_row("own", "starter_a", 0, 300),
    lane_row("own", "starter_b", 0, 300),
    lane_row("own", "bench", 300, 600)
  )
  out <- ribbon_mark_starters(lanes)
  flag <- setNames(out$is_starter, out$player_key)
  expect_true(flag[["starter_a"]])
  expect_true(flag[["starter_b"]])
  expect_false(flag[["bench"]])
})

test_that("ribbon_mark_starters resolves each side independently", {
  # The two sides always share a segment timeline, but a side whose first
  # segment starts later must still get its own starters.
  lanes <- rbind(
    lane_row("own", "a", 0, 300),
    lane_row("opp", "b", 0, 300),
    lane_row("opp", "c", 300, 600)
  )
  out <- ribbon_mark_starters(lanes)
  flag <- setNames(out$is_starter, paste(out$side, out$player_key))
  expect_true(flag[["own a"]])
  expect_true(flag[["opp b"]])
  expect_false(flag[["opp c"]])
})

test_that("ribbon_mark_starters keeps a player who returns later flagged once", {
  lanes <- rbind(
    lane_row("own", "a", 0, 300),
    lane_row("own", "a", 900, 1200)
  )
  out <- ribbon_mark_starters(lanes)
  expect_true(all(out$is_starter))
})




test_that("ribbon_lane_index orders starters first, then by floor time", {
  lanes <- rbind(
    lane_row("own", "sub", 0, 100, is_starter = FALSE),
    lane_row("own", "big_sub", 0, 900, is_starter = FALSE),
    lane_row("own", "starter", 0, 200, is_starter = TRUE)
  )
  out <- ribbon_lane_index(lanes)
  idx <- setNames(out$lane_index, out$player_key)
  expect_identical(unname(idx[["starter"]]), 1L)
  expect_identical(unname(idx[["big_sub"]]), 2L)
  expect_identical(unname(idx[["sub"]]), 3L)
})

test_that("ribbon_lane_index restarts numbering per side", {
  lanes <- rbind(
    lane_row("own", "a", 0, 100),
    lane_row("opp", "b", 0, 100)
  )
  out <- ribbon_lane_index(lanes)
  expect_identical(sort(out$lane_index), c(1L, 1L))
})

test_that("ribbon_lane_index sums floor time across a player's stints", {
  lanes <- rbind(
    lane_row("own", "split", 0, 100),
    lane_row("own", "split", 500, 700),   # 300s total
    lane_row("own", "solid", 0, 250)      # 250s total
  )
  out <- ribbon_lane_index(lanes)
  idx <- setNames(out$lane_index, out$player_key)
  expect_identical(unname(idx[["split"]][1]), 1L)
})

test_that("ribbon_geometry scales into the plot area, after the label gutter", {
  lanes <- lane_row("own", "7", 0, 1200)
  lanes$lane_index <- 1L
  out <- ribbon_geometry(lanes, total_seconds = 2400, width = 1000, gutter = 150)
  expect_identical(out$x, 150)              # t=0 sits at the gutter edge
  expect_identical(out$w, (1000 - 150) / 2) # half the game = half the plot area
})

test_that("ribbon_geometry gives a short stint a visible minimum width", {
  lanes <- lane_row("own", "7", 100, 101)
  lanes$lane_index <- 1L
  out <- ribbon_geometry(lanes, total_seconds = 2400, width = 1000)
  expect_gte(out$w, 0.75)
})

test_that("ribbon_geometry stacks lanes by index", {
  lanes <- rbind(lane_row("own", "a", 0, 10), lane_row("own", "b", 0, 10))
  lanes$lane_index <- c(1L, 2L)
  out <- ribbon_geometry(lanes, total_seconds = 2400,
                         lane_height = 14, lane_gap = 3)
  expect_identical(out$y, c(0, 17))
  expect_identical(out$h, c(14, 14))
})


test_that("merge_adjacent_stints does not merge across a one-second gap", {
  # Proves a minimal one-second gap is still treated as a real gap (guards an
  # off-by-one threshold bug that requires gap > 1 rather than gap > 0). It
  # does NOT distinguish `>` from `>=` in the adjacency check -- 242 and 243
  # are never equal, so that operator can't matter here. The touching/zero-gap
  # case (`start_elapsed == prev_end`) is already covered by "collapses a
  # contiguous run into one bar" above, which is what would catch a `>=` vs
  # `>` regression.
  lanes <- rbind(
    lane_row("own", "7", 0, 242),
    lane_row("own", "7", 243, 337)
  )
  out <- merge_adjacent_stints(lanes)
  expect_identical(nrow(out), 2L)
  expect_identical(out$end_elapsed, c(242, 337))
})

test_that("ribbon_period_bounds uses nominal lengths, not observed data", {
  # Regulation: four 10-minute quarters.
  expect_identical(ribbon_period_bounds(4), c(600, 1200, 1800, 2400))
  # One overtime adds 5 minutes, matching the measured EuroLeague max of 2700.
  expect_identical(ribbon_period_bounds(5), c(600, 1200, 1800, 2400, 2700))
  expect_identical(ribbon_period_bounds(6), c(600, 1200, 1800, 2400, 2700, 3000))
})

test_that("ribbon_period_bounds floors at regulation for truncated games", {
  # A game whose actions stop early (one Israeli game ends at 961s) must still
  # be drawn on a full regulation axis, or its lanes read at the wrong scale.
  expect_identical(ribbon_period_bounds(2), c(600, 1200, 1800, 2400))
  expect_identical(ribbon_period_bounds(NA), c(600, 1200, 1800, 2400))
})

test_that("ribbon_complete_margin opens the game at zero", {
  # The first scoring event can be a minute in. Without a leading (0, 0) the
  # curve starts mid-air and the first stint has no baseline behind it.
  m <- data.frame(elapsed = c(60, 120), margin = c(2, 5), order_key = c(1, 2))
  out <- ribbon_complete_margin(m, total_seconds = 2400)
  expect_identical(out$elapsed[1], 0)
  expect_identical(out$margin[1], 0)
})

test_that("ribbon_complete_margin extends the final score to the game end", {
  m <- data.frame(elapsed = c(0, 1200), margin = c(0, 7), order_key = c(1, 2))
  out <- ribbon_complete_margin(m, total_seconds = 2400)
  expect_identical(out$elapsed[nrow(out)], 2400)
  expect_identical(out$margin[nrow(out)], 7)
})

test_that("ribbon_complete_margin collapses ties by order_key, keeping the last", {
  # Several scoring records share one elapsed second (an and-1, or a made shot
  # and the ensuing free throw). DISTINCT + ORDER BY elapsed is not
  # deterministic; the last state at that second is the true one.
  m <- data.frame(elapsed = c(0, 600, 600, 600), margin = c(0, 3, 5, 4),
                  order_key = c(1, 10, 11, 12))
  out <- ribbon_complete_margin(m, total_seconds = 2400)
  expect_identical(sum(out$elapsed == 600), 1L)
  expect_identical(out$margin[out$elapsed == 600], 4)
})

test_that("ribbon_complete_margin clamps elapsed into the nominal frame", {
  # elapsed 0 and -10 both clamp to 0 -- clamping deliberately manufactures a
  # tie at 0, which the tie-collapse must then resolve to the highest
  # order_key (3, margin 1), same as any other same-second tie. Asserting
  # only the elapsed range would still pass if the tie-collapse were broken
  # (e.g. kept the first record instead of the last, or dropped the clamped
  # row), since pmin/pmax alone already guarantees the range.
  m <- data.frame(elapsed = c(0, 2500, -10), margin = c(0, 9, 1),
                  order_key = c(1, 2, 3))
  out <- ribbon_complete_margin(m, total_seconds = 2400)
  expect_true(all(out$elapsed >= 0 & out$elapsed <= 2400))
  expect_identical(nrow(out), 2L)
  expect_identical(out$margin[out$elapsed == 0], 1)
  expect_identical(out$margin[out$elapsed == 2400], 9)
})

test_that("ribbon_complete_margin returns a usable series from no data", {
  m <- data.frame(elapsed = numeric(0), margin = numeric(0), order_key = numeric(0))
  out <- ribbon_complete_margin(m, total_seconds = 2400)
  expect_identical(out$elapsed, c(0, 2400))
  expect_identical(out$margin, c(0, 0))
})

ribbon_fixture <- function() {
  lanes <- rbind(
    # Player "1" is split into two TOUCHING stints (0-600, 600-1200) rather
    # than one 0-1200 row on purpose: build_stint_ribbon_svg must call
    # merge_adjacent_stints() internally to collapse them back into one bar.
    # A single unsplit row here would let that call be deleted with the whole
    # suite staying green (found in the 2026-09-05 final review, I6).
    lane_row("own", "1", 0, 600, is_starter = TRUE, player_label = "A Cohen"),
    lane_row("own", "1", 600, 1200, player_label = "A Cohen"),
    lane_row("own", "2", 1200, 2400, player_label = "B Levy"),
    lane_row("opp", "9", 0, 2400, is_starter = TRUE, player_label = "C Katz")
  )
  margin <- data.frame(elapsed = c(0, 600, 1200), margin = c(0, 5, -3))
  meta <- list(game_label = "Team A vs Team B", n_periods = 4L,
               own_team = "Team A", opp_team = "Team B")
  list(lanes = lanes, margin = margin, meta = meta)
}

test_that("ribbon_margin_path emits a stepped path, not a diagonal one", {
  margin <- data.frame(elapsed = c(0, 1200), margin = c(0, 10))
  d <- ribbon_margin_path(margin, total_seconds = 2400, width = 1000,
                          top = 0, height = 100)
  expect_match(d, "^M ")
  expect_match(d, "H .* V ")
})

test_that("ribbon_margin_path returns an empty string for no data", {
  expect_identical(
    ribbon_margin_path(data.frame(elapsed = numeric(0), margin = numeric(0)),
                       2400, 1000, 0, 100),
    ""
  )
})

test_that("ribbon_margin_path never emits NA or NaN into the path, even with bad input", {
  # Defence in depth (2026-09-05 final review): a single non-finite margin
  # value must never blank the whole curve. An SVG path `d` attribute
  # containing "NaN" is invalid, and the browser drops the ENTIRE path rather
  # than just the bad segment -- this is exactly how the EuroLeague margin
  # curve went blank on every one of 593 games (the provider leaves the
  # running score NULL until a side has scored; that NULL propagated to NA
  # margin, then to "V NaN" here).
  m <- data.frame(elapsed = c(0, 60, 120), margin = c(0, NA, 4))
  d <- ribbon_margin_path(m, total_seconds = 2400, width = 1000, top = 0, height = 100)
  expect_false(grepl("NA", d, fixed = TRUE))
  expect_false(grepl("NaN", d, fixed = TRUE))
  expect_match(d, "^M ")
})

# ---- ribbon_margin_scale(): the shared max_abs/interval/ticks helper ------
# (added M7, 2026-09-06) -- ribbon_margin_path() and the scale gridlines in
# build_stint_ribbon_svg() both consume this ONE function, so alignment
# between the curve and the gridlines is structural, not coincidental.

margin_of <- function(mx) data.frame(elapsed = c(0, 1), margin = c(0, mx))

test_that("ribbon_margin_scale picks the smallest ladder rung with max_abs/interval <= 4", {
  # Mutation check: hardcode `interval <- 25` unconditionally in
  # ribbon_margin_scale() -- the 6/18/45 cases below fail (still pass the
  # >100 case by coincidence), confirming this test actually pins the ladder
  # logic rather than just checking "some interval was returned".
  expect_identical(ribbon_margin_scale(margin_of(6))$interval, 2)
  expect_identical(ribbon_margin_scale(margin_of(18))$interval, 5)
  expect_identical(ribbon_margin_scale(margin_of(45))$interval, 20)
  # 150 / 25 = 6 > 4 -- no ladder rung satisfies the <=4 target, so this
  # must fall back to 25 rather than silently picking a rung that violates
  # the "2-4 gridlines per side" spec.
  expect_identical(ribbon_margin_scale(margin_of(150))$interval, 25)
})

test_that("ribbon_margin_scale ticks never exceed max_abs and never include zero", {
  # Mutation check: drop the "<= max_abs" filter (e.g. use
  # `seq_len(ceiling(max_abs / interval))` instead of `floor`) -- the 6 and 45
  # cases below then emit a tick past max_abs and this fails.
  for (mx in c(6, 18, 45, 150, 0.5)) {
    scale <- ribbon_margin_scale(margin_of(mx))
    expect_true(all(abs(scale$ticks) <= scale$max_abs))
    expect_false(0 %in% scale$ticks)
  }
})

test_that("ribbon_margin_scale returns no ticks and max_abs = 1 for empty/all-zero margin", {
  # Mirrors ribbon_margin_path's own empty/all-zero guard exactly -- these
  # two functions must never compute a different max_abs from the same data.
  empty <- ribbon_margin_scale(data.frame(elapsed = numeric(0), margin = numeric(0)))
  expect_identical(empty$max_abs, 1)
  expect_length(empty$ticks, 0)

  zero <- ribbon_margin_scale(data.frame(elapsed = c(0, 600, 1200), margin = c(0, 0, 0)))
  expect_identical(zero$max_abs, 1)
  expect_length(zero$ticks, 0)
})

test_that("ribbon_margin_path and ribbon_margin_scale agree on max_abs for the same data", {
  # The extraction's whole point: a single source of truth. This directly
  # guards against a future edit reintroducing a second, independent max_abs
  # computation in either function.
  m <- data.frame(elapsed = c(0, 600, 1200), margin = c(0, 17, -9))
  expect_identical(ribbon_margin_scale(m)$max_abs, 17)
})

test_that("ribbon_complete_margin drops non-finite margins rather than propagating them", {
  m <- data.frame(elapsed = c(0, 60, 120), margin = c(0, NA, 4), order_key = c(1, 2, 3))
  out <- ribbon_complete_margin(m, total_seconds = 2400)
  expect_true(all(is.finite(out$margin)))
})

test_that("ribbon_complete_margin falls back to a flat series when every margin is non-finite", {
  m <- data.frame(elapsed = c(0, 60), margin = c(NA_real_, NaN), order_key = c(1, 2))
  out <- ribbon_complete_margin(m, total_seconds = 2400)
  expect_identical(out$elapsed, c(0, 2400))
  expect_identical(out$margin, c(0, 0))
})

test_that("build_stint_ribbon_svg draws one rect per merged stint", {
  f <- ribbon_fixture()
  html <- as.character(build_stint_ribbon_svg(f$lanes, f$margin, f$meta))
  expect_identical(lengths(regmatches(html, gregexpr("ibpl-ribbon-lane", html))), 3L)
})

test_that("build_stint_ribbon_svg emits one clipPath per player", {
  f <- ribbon_fixture()
  html <- as.character(build_stint_ribbon_svg(f$lanes, f$margin, f$meta))
  expect_identical(lengths(regmatches(html, gregexpr("<clipPath", html))), 3L)
})

test_that("every lane's data-clip matches a clipPath id that exists", {
  f <- ribbon_fixture()
  html <- as.character(build_stint_ribbon_svg(f$lanes, f$margin, f$meta))
  clips <- regmatches(html, gregexpr('data-clip="[^"]+"', html))[[1]]
  clips <- sub('data-clip="', "", sub('"$', "", clips))
  ids <- regmatches(html, gregexpr('id="[^"]+"', html))[[1]]
  ids <- sub('id="', "", sub('"$', "", ids))
  # all(character(0) %in% ids) is vacuously TRUE -- a renamed data-clip
  # attribute would make `clips` empty and this assertion pass on nothing.
  expect_gt(length(clips), 0)
  expect_true(all(clips %in% ids))
})

test_that("the viewBox width comes from nominal period length", {
  f <- ribbon_fixture()
  html <- as.character(build_stint_ribbon_svg(f$lanes, f$margin, f$meta))
  expect_match(html, 'viewBox="0 0 1070 ')
})

test_that("an overtime game gets more period gridlines than regulation", {
  f <- ribbon_fixture()
  reg <- as.character(build_stint_ribbon_svg(f$lanes, f$margin, f$meta))
  f$meta$n_periods <- 5L
  ot <- as.character(build_stint_ribbon_svg(f$lanes, f$margin, f$meta))
  n_reg <- lengths(regmatches(reg, gregexpr("ibpl-ribbon-period", reg)))
  n_ot <- lengths(regmatches(ot, gregexpr("ibpl-ribbon-period", ot)))
  expect_gt(n_ot, n_reg)
})

test_that("the margin curve is drawn twice: a base copy and a focus copy", {
  f <- ribbon_fixture()
  html <- as.character(build_stint_ribbon_svg(f$lanes, f$margin, f$meta))
  expect_match(html, "ibpl-ribbon-margin-base")
  expect_match(html, "ibpl-ribbon-margin-focus")
})

test_that("every lane renders one visible name in the gutter", {
  f <- ribbon_fixture()
  html <- as.character(build_stint_ribbon_svg(f$lanes, f$margin, f$meta))
  expect_identical(lengths(regmatches(html, gregexpr("ibpl-ribbon-name", html))), 3L)
  expect_match(html, "A Cohen", fixed = TRUE)
})

test_that("the two teams are identified on the chart", {
  f <- ribbon_fixture()
  f$meta$own_team <- "Hapoel TA"; f$meta$opp_team <- "Maccabi"
  html <- as.character(build_stint_ribbon_svg(f$lanes, f$margin, f$meta))
  expect_match(html, "Hapoel TA", fixed = TRUE)
  expect_match(html, "Maccabi", fixed = TRUE)
})

test_that("the zero-margin baseline is drawn and labelled", {
  f <- ribbon_fixture()
  html <- as.character(build_stint_ribbon_svg(f$lanes, f$margin, f$meta))
  expect_match(html, "ibpl-ribbon-zero", fixed = TRUE)
  expect_match(html, ">tied<", fixed = TRUE)
})

# ---- Round-interval scale gridlines (M7, added 2026-09-06) ----------------

test_that("build_stint_ribbon_svg emits one scale gridline and one label per tick, correctly signed", {
  f <- ribbon_fixture()
  html <- as.character(build_stint_ribbon_svg(f$lanes, f$margin, f$meta))
  scale_info <- ribbon_margin_scale(f$margin)
  expect_gt(length(scale_info$ticks), 0)

  # Anchored on the opening `<line class="ibpl-ribbon-scale"` (with its
  # closing quote) so this does not also count "ibpl-ribbon-scale-label".
  expect_identical(
    lengths(regmatches(html, gregexpr('<line class="ibpl-ribbon-scale"', html, fixed = TRUE))),
    length(scale_info$ticks))
  expect_identical(
    lengths(regmatches(html, gregexpr('class="ibpl-ribbon-scale-label"', html, fixed = TRUE))),
    length(scale_info$ticks))

  # Mutation check: format the label without the "+" flag (e.g. plain
  # `sprintf("%d", v)`) -- the positive-tick lookups below then fail to find
  # ">2<"/ ">4<" (they only find "-2"/"-4" style negatives), confirming this
  # actually checks the sign, not just that some number is present.
  for (v in scale_info$ticks) {
    label <- sprintf("%+d", as.integer(round(v)))
    expect_match(html, paste0(">", label, "<"), fixed = TRUE)
  }
})

test_that("scale gridlines never fall outside the band and never leak NaN", {
  f <- ribbon_fixture()
  html <- as.character(build_stint_ribbon_svg(f$lanes, f$margin, f$meta))
  expect_false(grepl("NaN", html, fixed = TRUE))

  ys <- as.numeric(regmatches(html, gregexpr(
    '(?<=<line class="ibpl-ribbon-scale" x1="220" x2="1070" y1=")[0-9.]+', html, perl = TRUE))[[1]])
  expect_gt(length(ys), 0)
  expect_true(all(ys >= 0))
})

test_that("an all-zero or empty margin renders no scale gridlines and does not crash", {
  f <- ribbon_fixture()
  f$margin <- data.frame(elapsed = c(0, 600, 1200), margin = c(0, 0, 0))
  html <- as.character(build_stint_ribbon_svg(f$lanes, f$margin, f$meta))
  # "ibpl-ribbon-scale" is a substring of "ibpl-ribbon-scale-label", so this
  # single check rules out both the lines and the labels.
  expect_false(grepl("ibpl-ribbon-scale", html, fixed = TRUE))
  expect_false(grepl("NaN", html, fixed = TRUE))

  f$margin <- data.frame(elapsed = numeric(0), margin = numeric(0))
  html2 <- as.character(build_stint_ribbon_svg(f$lanes, f$margin, f$meta))
  expect_false(grepl("ibpl-ribbon-scale", html2, fixed = TRUE))
  expect_false(grepl("NaN", html2, fixed = TRUE))
})

test_that("a scale gridline's y agrees with where the curve maps the same margin value", {
  # The alignment guarantee itself: choose a margin value that lands EXACTLY
  # on a tick (10, with max_abs also 10 -> interval 5 -> ticks -10,-5,5,10),
  # then compare the curve's own y at that point against the gridlines'
  # actual y attributes in the built SVG -- not a recomputed formula, so a
  # bug that changes the mapping in only one of the two call sites is
  # caught here.
  f <- ribbon_fixture()
  f$margin <- data.frame(elapsed = c(0, 600), margin = c(0, 10))
  html <- as.character(build_stint_ribbon_svg(f$lanes, f$margin, f$meta))

  path_tag <- regmatches(html, regexpr(
    '<path class="ibpl-ribbon-margin-base" d="[^"]+"', html))
  expect_true(nzchar(path_tag))
  d <- sub('.*d="([^"]+)".*', "\\1", path_tag)
  tokens <- strsplit(d, " ")[[1]]
  # "M x0 y0 H x1 V y1" -- margin=10 at elapsed=600 is the point right after
  # the first "V".
  v_idx <- which(tokens == "V")
  expect_gt(length(v_idx), 0)
  curve_y <- as.numeric(tokens[v_idx[1] + 1])

  grid_ys <- as.numeric(regmatches(html, gregexpr(
    '(?<=<line class="ibpl-ribbon-scale" x1="220" x2="1070" y1=")[0-9.]+', html, perl = TRUE))[[1]])
  expect_true(any(abs(grid_ys - curve_y) < 0.05))
})

test_that("the chart carries genuine breathing room top and bottom, not just a tight fit", {
  # Follow-up to M7 (2026-09-06): the own-team label and the period labels
  # used to sit within a couple of units of the SVG's top/bottom edges.
  # RIBBON_PAD_TOP/RIBBON_PAD_BOTTOM are added at the couple of places
  # build_stint_ribbon_svg() anchors its y=0 origin (and at the viewBox
  # height), so every drawn element -- lanes, labels, the margin band, the
  # zero baseline, the scale gridlines, period lines/labels -- should have
  # shifted down together, and the viewBox should have grown to match.
  #
  # Deliberately compares against a hardcoded floor (5), not against the
  # live RIBBON_PAD_TOP/RIBBON_PAD_BOTTOM constants: comparing "min(ys) >=
  # RIBBON_PAD_TOP" would be tautological, since RIBBON_PAD_TOP IS the
  # topmost coordinate by construction -- mutating the constant to 0 would
  # move both sides of that comparison together and it would keep passing.
  # Mutation check: set RIBBON_PAD_TOP <- 0 in helpers.R -- the top-most
  # drawn y collapses to 0 and the first assertion below fails. Set
  # RIBBON_PAD_BOTTOM <- 0 instead -- the gap between the bottom-most drawn
  # y and the viewBox height collapses to 0 and the second fails.
  f <- ribbon_fixture()
  html <- as.character(build_stint_ribbon_svg(f$lanes, f$margin, f$meta))

  vb <- regmatches(html, regexpr('viewBox="0 0 [0-9]+ [0-9.]+"', html, perl = TRUE))
  expect_true(nzchar(vb))
  vb_height <- as.numeric(sub('.*viewBox="0 0 [0-9]+ ([0-9.]+)".*', "\\1", vb))

  # Every y-ish coordinate actually drawn: y="..", y1="..", y2="..". \\b
  # keeps this off "opacity=" and similar (no word boundary before that y).
  y_attrs <- regmatches(html, gregexpr('\\by[12]?="[0-9.]+"', html, perl = TRUE))[[1]]
  expect_gt(length(y_attrs), 0)
  ys <- as.numeric(sub('.*="([0-9.]+)"', "\\1", y_attrs))

  # Nothing is clipped at the bottom: the viewBox is taller than the
  # bottom-most drawn coordinate.
  expect_gt(vb_height, max(ys))

  expect_gt(min(ys), 5)
  expect_gt(vb_height - max(ys), 5)
})

test_that("the margin band is inset from the lane blocks by RIBBON_BAND_GAP, not just touching them", {
  # Follow-up to the 2026-09-06 final review: the last own-team lane label
  # used to sit almost on the band's top gridline, and the band's bottom
  # edge almost touched the first opponent label (both gaps were only
  # RIBBON_LANE_GAP * 2 = 6). RIBBON_BAND_GAP now replaces that at the two
  # anchors (margin_top, opp_top) everything else is computed from.
  #
  # Read the gap from the BUILT SVG's own geometry, not by recomputing the
  # margin_top/opp_top formula -- a bug that changes the formula but not the
  # constant (or vice versa) must still be caught here.
  #
  # Mutation check: set RIBBON_BAND_GAP <- 0 in helpers.R -- both gaps below
  # collapse to 0 (top) / RIBBON_HEADER (bottom) and both assertions fail.
  f <- ribbon_fixture()
  html <- as.character(build_stint_ribbon_svg(f$lanes, f$margin, f$meta))

  # Own-side lane rects: `<g class="ibpl-ribbon-lane is-own" ...><title>..
  # </title><rect x=".." y=".." width=".." height=".." rx="2"></rect></g>`.
  # Capture each block, then pull y/height per block -- gives the bottom
  # (y + height) of every own lane, of which the max is the last one.
  own_blocks <- regmatches(html, gregexpr(
    '(?s)<g class="ibpl-ribbon-lane is-own".*?rx="2"></rect>', html, perl = TRUE))[[1]]
  expect_gt(length(own_blocks), 0)
  own_y <- as.numeric(sub('.*<rect[^>]*\\by="([0-9.]+)".*', "\\1", own_blocks))
  own_h <- as.numeric(sub('.*<rect[^>]*\\bheight="([0-9.]+)".*', "\\1", own_blocks))
  own_bottom <- max(own_y + own_h)

  opp_blocks <- regmatches(html, gregexpr(
    '(?s)<g class="ibpl-ribbon-lane is-opp".*?rx="2"></rect>', html, perl = TRUE))[[1]]
  expect_gt(length(opp_blocks), 0)
  opp_y <- as.numeric(sub('.*<rect[^>]*\\by="([0-9.]+)".*', "\\1", opp_blocks))
  opp_top_lane <- min(opp_y)

  # The margin band's own extent, read from the clipPath rects (the only
  # place margin_top/RIBBON_MARGIN_HEIGHT are drawn as literal geometry).
  # These rects have no `rx` attribute, unlike the lane rects above, so the
  # pattern -- requiring `></rect>` immediately after `height="..."` -- only
  # matches the clip rects.
  clip_rects <- regmatches(html, gregexpr(
    '<rect x="[0-9.]+" y="([0-9.]+)" width="[0-9.]+" height="([0-9.]+)"></rect>',
    html))[[1]]
  expect_gt(length(clip_rects), 0)
  band_top <- unique(as.numeric(sub('.*y="([0-9.]+)".*', "\\1", clip_rects)))
  band_height <- unique(as.numeric(sub('.*height="([0-9.]+)".*', "\\1", clip_rects)))
  expect_length(band_top, 1)
  expect_length(band_height, 1)
  band_bottom <- band_top + band_height

  gap_above <- band_top - own_bottom
  gap_below <- opp_top_lane - band_bottom

  # Deliberately a hardcoded floor (22), not `RIBBON_BAND_GAP` itself:
  # comparing against the live constant is tautological the same way the
  # breathing-room test above warns about, since mutating BAND_GAP moves
  # both the drawn gap AND the threshold together and the assertion would
  # keep passing. 22 sits strictly between the un-inset gap this replaces
  # (RIBBON_LANE_GAP * 2 = 6) and the design value, so it fails for
  # BAND_GAP <- 0 while passing as shipped.
  expect_gt(gap_above, 22)
  expect_gt(gap_below, 22)

  # The two gaps must be EQUAL (2026-09-06). They were not: the opponent's
  # team label was given a RIBBON_HEADER row *below* the band on top of the
  # gap, so the chart carried 24 units of space above the band and 44 below
  # and read as lopsided. The label now sits inside the gap instead -- it
  # never filled that space anyway, being left-anchored in the gutter while
  # the band starts at RIBBON_GUTTER.
  #
  # Mutation check: restore `+ RIBBON_HEADER` on opp_top and this fails
  # (44 vs 28) while both expect_gt()s above still pass -- which is exactly
  # why the floors alone were not enough to hold this.
  expect_equal(gap_below, gap_above)
})

test_that("the period markers are drawn above the lanes as well as below them", {
  # 2026-09-06: a game with deep rotations makes the chart taller than the
  # modal, and with the Q1-Q4 row only at the bottom the reader had to scroll
  # to find out which quarter a stint sits in. The top row reuses the
  # own-team label's header row, so it must cost no height either.
  #
  # Mutation check: drop period_label_row(RIBBON_PAD_TOP + 10) from the
  # period_labels list -- the count halves and the "above the lanes"
  # assertion fails.
  f <- ribbon_fixture()
  html <- as.character(build_stint_ribbon_svg(f$lanes, f$margin, f$meta))

  # No backslash escapes: R reads them as control characters in a quoted
  # literal, and a never-matching pattern here would pass vacuously.
  y_of <- function(tags) {
    m <- regmatches(tags, regexpr('y="[0-9.]+"', tags))
    as.numeric(gsub('[^0-9.]', '', m))
  }
  labels <- regmatches(html, gregexpr(
    '<text class="ibpl-ribbon-period-label"[^>]*>Q[0-9]<', html))[[1]]
  expect_identical(length(labels), 8L)   # Q1-Q4, twice

  label_ys <- y_of(labels)
  lane_rects <- regmatches(html, gregexpr(
    '(?s)<g class="ibpl-ribbon-lane[^"]*".*?rx="2"></rect>', html, perl = TRUE))[[1]]
  lane_tops <- y_of(lane_rects)

  # One row above every lane, one row below every lane.
  expect_lt(min(label_ys), min(lane_tops))
  expect_gt(max(label_ys), max(lane_tops))

  # Both rows carry the full set, so neither is a partial decoration.
  expect_identical(sum(label_ys == min(label_ys)), 4L)
  expect_identical(sum(label_ys == max(label_ys)), 4L)
})

test_that("the opponent team label clears a scale label sitting on the band's edge", {
  # The opponent's team label lives INSIDE the band gap, so the gap has to be
  # deep enough to hold it clear of the band's lowest scale label. That label
  # lands exactly on the band's bottom edge whenever the game's max margin is
  # a multiple of the tick interval -- common, not a corner case (20, 25, 40
  # all do it). Measured in a browser at RIBBON_BAND_GAP = 24, the two text
  # boxes overlapped by 1.9px for a long team name; 28 clears.
  #
  # max_abs = 20 puts ribbon_margin_scale() on the interval-5 rung with
  # n_per_side = 4, so -20 IS a tick and maps to the band's bottom edge.
  f <- ribbon_fixture()
  f$margin <- data.frame(elapsed = c(0, 600, 1200), margin = c(0, 20, -20))
  scale_info <- ribbon_margin_scale(f$margin)
  expect_true(-scale_info$max_abs %in% scale_info$ticks)  # premise, not decoration

  html <- as.character(build_stint_ribbon_svg(f$lanes, f$margin, f$meta))
  # No backslash escapes here on purpose: the y attribute is the first
  # `y="..."` in each tag, so a plain match plus a strip is enough --
  # and R reads '\b' in a single-quoted literal as a backspace, not as a
  # word boundary, which silently turned this into a never-matching
  # pattern and an NA comparison the first time round.
  y_of <- function(tags) {
    m <- regmatches(tags, regexpr('y="[0-9.]+"', tags))
    as.numeric(gsub('[^0-9.]', '', m))
  }
  scale_labels <- regmatches(html, gregexpr(
    '<text class="ibpl-ribbon-scale-label"[^>]*>', html))[[1]]
  expect_gt(length(scale_labels), 0)
  lowest_scale_y <- max(y_of(scale_labels))

  # Anchor on content: lanes sort "opp" before "own", so "the second team
  # label in the document" is not reliably the opponent's.
  opp_label <- regmatches(html, regexpr(
    '<text class="ibpl-ribbon-team"[^>]*>Team B<', html))[[1]]
  expect_true(nzchar(opp_label))
  opp_label_y <- y_of(opp_label)

  # One full label height (12px font) between the two baselines. At
  # BAND_GAP 28 the separation is 15; mutating it back to 24 gives 11 and
  # this fails, which is the mutation the equality test above cannot see.
  expect_gt(opp_label_y - lowest_scale_y, 12)
})

test_that("period boundaries are labelled, not merely drawn", {
  f <- ribbon_fixture()
  html <- as.character(build_stint_ribbon_svg(f$lanes, f$margin, f$meta))
  expect_match(html, ">Q1<", fixed = TRUE)
  expect_match(html, ">Q4<", fixed = TRUE)
  f$meta$n_periods <- 5L
  ot <- as.character(build_stint_ribbon_svg(f$lanes, f$margin, f$meta))
  expect_match(ot, ">OT1<", fixed = TRUE)
})

test_that("each lane carries an explicit aria-label, not just a title", {
  f <- ribbon_fixture()
  html <- as.character(build_stint_ribbon_svg(f$lanes, f$margin, f$meta))
  # The gutter label for the same player ALSO carries an aria-label (it is a
  # hover/keyboard target of its own, doubling as a 20-lane index), so the
  # substring now appears twice -- once on the <g> lane, once on the <text>
  # label. Anchored to the enclosing tag so a regression that drops either
  # one specifically (not just the whole mechanism) is still caught.
  expect_identical(
    lengths(regmatches(html, gregexpr('<g[^>]*aria-label="A Cohen', html, perl = TRUE))),
    1L)
  expect_identical(
    lengths(regmatches(html, gregexpr('<text[^>]*aria-label="A Cohen', html, perl = TRUE))),
    1L)
})

test_that("a lane's gutter label carries the same data-clip as its lane", {
  f <- ribbon_fixture()
  html <- as.character(build_stint_ribbon_svg(f$lanes, f$margin, f$meta))
  # Isolate the <g ...> opening tag for the "A Cohen" lane (lookahead makes
  # the aria-label check order-independent of the data-clip attribute), pull
  # its data-clip value, then assert a <text class="ibpl-ribbon-name"> exists
  # with that EXACT same value -- not just that some data-clip exists
  # somewhere (every lane has one; that alone would pass even if the label's
  # value pointed at a different lane).
  lane_tag <- regmatches(html, regexpr(
    '<g(?=[^>]*aria-label="A Cohen)[^>]*data-clip="[^"]+"[^>]*>', html, perl = TRUE))
  expect_true(nzchar(lane_tag))
  clip_id <- sub('.*data-clip="([^"]+)".*', "\\1", lane_tag)
  expect_match(html,
               sprintf('<text class="ibpl-ribbon-name"[^>]*data-clip="%s"', clip_id),
               perl = TRUE)
})

test_that("each lane carries a title for tooltip and screen readers", {
  f <- ribbon_fixture()
  html <- as.character(build_stint_ribbon_svg(f$lanes, f$margin, f$meta))
  expect_match(html, "<title>A Cohen")
})

test_that("build_stint_ribbon_svg returns NULL when there are no lanes", {
  f <- ribbon_fixture()
  expect_null(build_stint_ribbon_svg(f$lanes[0, , drop = FALSE], f$margin, f$meta))
})

test_that("clip ids are namespaced so two ribbons on a page cannot collide", {
  f <- ribbon_fixture()
  a <- as.character(build_stint_ribbon_svg(f$lanes, f$margin, f$meta, id_prefix = "r1"))
  b <- as.character(build_stint_ribbon_svg(f$lanes, f$margin, f$meta, id_prefix = "r2"))
  expect_match(a, 'id="r1-on-own-1"')
  expect_match(b, 'id="r2-on-own-1"')
})

test_that("clip ids stay valid when the key is a EuroLeague player name", {
  id <- ribbon_clip_id("r1", "own", "BIRCH, KHEM")
  expect_false(grepl("[ ,]", id))
  expect_identical(id, "r1-on-own-BIRCH-KHEM")
})

test_that("a named-key lane still resolves to a clipPath that exists", {
  f <- ribbon_fixture()
  # 4 rows in the fixture (player "1" is split into two touching stints); the
  # first two share a name since they are the same real player.
  f$lanes$player_key <- c("BIRCH, KHEM", "BIRCH, KHEM", "HALL, DEVON", "SLEVA, DUSTIN")
  html <- as.character(build_stint_ribbon_svg(f$lanes, f$margin, f$meta))
  clips <- regmatches(html, gregexpr('data-clip="[^"]+"', html))[[1]]
  clips <- sub('data-clip="', "", sub('"$', "", clips))
  ids <- regmatches(html, gregexpr('id="[^"]+"', html))[[1]]
  ids <- sub('id="', "", sub('"$', "", ids))
  # Same vacuous-pass trap as above: assert clips were actually found.
  expect_gt(length(clips), 0)
  expect_true(all(clips %in% ids))
})

test_that("app.css styles every class the SVG builder emits", {
  css <- paste(readLines(testthat::test_path("..", "..", "www", "app.css"),
                         warn = FALSE), collapse = "\n")
  for (cls in c("ibpl-ribbon", "ibpl-ribbon-lane", "ibpl-ribbon-margin-base",
                "ibpl-ribbon-margin-focus", "ibpl-ribbon-period",
                "ibpl-ribbon-name", "ibpl-ribbon-team", "ibpl-ribbon-zero",
                "ibpl-ribbon-period-label", "ibpl-ribbon-scale",
                "ibpl-ribbon-scale-label")) {
    # fixed=TRUE substring matching is vacuously satisfied when `cls` is only
    # a PREFIX of a different, longer class that is separately styled --
    # e.g. "ibpl-ribbon-zero" is a substring of "ibpl-ribbon-zero-label",
    # which app.css styles in its own rule. Deleting the standalone
    # `.ibpl-ribbon-zero { ... }` rule (a real regression: the tied-game
    # baseline stops being dashed) left this assertion green because
    # "-label" is still in the file (confirmed by mutation, 2026-09-06). A
    # trailing negative lookahead requires the class name not be immediately
    # followed by a hyphen or word character, so it only matches the
    # standalone class or a non-hyphenated compound selector (".cls.other",
    # ".cls rect", ".cls,").
    expect_true(grepl(paste0(cls, "(?![-\\w])"), css, perl = TRUE),
                info = paste("missing ribbon style for", cls))
  }
})

test_that("app.css styles the active gutter label, not just the active lane", {
  css <- paste(readLines(testthat::test_path("..", "..", "www", "app.css"),
                         warn = FALSE), collapse = "\n")
  # Anchored with \\b so a superstring class (e.g. a future
  # ".ibpl-ribbon-name.is-active-foo") cannot satisfy this the way fixed=TRUE
  # substring matching would.
  expect_match(css, "\\.ibpl-ribbon-name\\.is-active\\b")
})

test_that("the team label no longer shares a baseline with lane 1", {
  f <- ribbon_fixture()
  html <- as.character(build_stint_ribbon_svg(f$lanes, f$margin, f$meta))
  # Assert the GEOMETRIC relationship, not a hardcoded pixel: the own-team
  # label must clear lane 1's label by at least a full header row. A fixed
  # `y == 10` assertion would keep passing even if a future change shrank
  # the header back down to nothing.
  #
  # Anchor each extraction on content, not on "the first tag of this class"
  # -- merge_adjacent_stints() sorts by side ("opp" < "own" alphabetically),
  # so the first <text class="ibpl-ribbon-name"> in the document is actually
  # the OPPONENT's lane, not lane 1. ribbon_fixture()'s own team is "Team A"
  # and its lane-1 player (is_starter, first row) is "A Cohen".
  extract_y <- function(tag_pattern) {
    tag <- regmatches(html, regexpr(tag_pattern, html, perl = TRUE))
    expect_true(nzchar(tag))
    as.numeric(sub('.*\\by="([0-9.]+)".*', "\\1", tag))
  }
  team_y <- extract_y('<text class="ibpl-ribbon-team"[^>]*>Team A<')
  lane1_y <- extract_y('<text class="ibpl-ribbon-name"[^>]*aria-label="A Cohen[^>]*>')
  expect_true(lane1_y - team_y >= RIBBON_HEADER)
})

test_that("app.js resolves hover targets by data-clip, not only the lane class", {
  js <- paste(readLines(testthat::test_path("..", "..", "www", "app.js"),
                        warn = FALSE), collapse = "\n")
  # laneFrom() must match a gutter label too: a <text data-clip="..."> has no
  # .ibpl-ribbon-lane class of its own, so a hover on the label would
  # otherwise resolve to nothing.
  lane_from <- regmatches(js, regexpr(
    "function laneFrom\\(target\\) \\{(.|\n)*?\\n  \\}", js, perl = TRUE))
  expect_true(nzchar(lane_from))
  expect_match(lane_from, '.closest(".ibpl-ribbon-lane, [data-clip]")', fixed = TRUE)

  # setFocus()'s mates selector must widen the same way, or the label lights
  # up as a hover TARGET (laneFrom finds it) without lighting up as a hover
  # RESULT (its own mates query never selects it back).
  set_focus <- regmatches(js, regexpr(
    "function setFocus\\(svg, lane\\) \\{(.|\n)*?\\n  \\}", js, perl = TRUE))
  expect_true(nzchar(set_focus))
  expect_match(set_focus, "svg.querySelectorAll('[data-clip=\"' + lane.dataset.clip + '\"]')",
               fixed = TRUE)
})

test_that("app.js swaps clip-path rather than recomputing geometry", {
  js <- paste(readLines(testthat::test_path("..", "..", "www", "app.js"),
                        warn = FALSE), collapse = "\n")
  expect_match(js, "ibpl-ribbon-margin-focus", fixed = TRUE)
  expect_match(js, "clip-path", fixed = TRUE)
  expect_match(js, "ibpl-ribbon-lane", fixed = TRUE)
  expect_match(js, "window.ibplSendShinyEvent = sendShinyEvent", fixed = TRUE)
})

test_that("ribbon_link_cell carries the ids and team names the modal needs", {
  html <- ribbon_link_cell(115L, 7L, "12 Mar", own_team = "Hapoel TA",
                           opp_team = "Maccabi")
  expect_match(html, 'data-game-id="115"')
  expect_match(html, 'data-team-id="7"')
  expect_match(html, 'data-own-team="Hapoel TA"')
  expect_match(html, 'data-opp-team="Maccabi"')
  expect_match(html, "12 Mar", fixed = TRUE)
  expect_match(html, "ribbon-link", fixed = TRUE)
})

test_that("ribbon_link_cell escapes labels and data attributes", {
  html <- ribbon_link_cell(1L, 1L, "<script>alert(1)</script>",
                           own_team = 'A "quoted" team')
  expect_false(grepl("<script>", html, fixed = TRUE))
  expect_match(html, 'data-own-team="A &quot;quoted&quot; team"', fixed = TRUE)
})

test_that("add_ribbon_link_column builds links from the source frame", {
  df <- data.frame(game_id = c(115L, 116L), team_id = c(7L, 10L),
                   game_date = c("12 Mar", "14 Mar"), stringsAsFactors = FALSE)
  out <- add_ribbon_link_column(df)
  expect_match(out$game_date[1], 'data-game-id="115"')
  expect_match(out$game_date[2], 'data-team-id="10"')
  expect_match(out$game_date[1], "12 Mar", fixed = TRUE)
})

test_that("add_ribbon_link_column fails loudly on a frame missing the ids", {
  disp <- data.frame(gn = 1L, game_date = "12 Mar", stringsAsFactors = FALSE)
  expect_error(add_ribbon_link_column(disp), "game_id")
})

test_that("a game with no score data gets no ribbon link", {
  df <- data.frame(
    game_id = c(139L, 200L), team_id = c(6L, 6L),
    game_date = c("2026-01-10", "2026-01-17"),
    team_name = c("A", "A"), opp_team_name = c("B", "C"),
    has_scores = c(FALSE, TRUE), stringsAsFactors = FALSE
  )
  out <- add_ribbon_link_column(df)
  expect_false(grepl("<a", out$game_date[1], fixed = TRUE))
  expect_identical(out$game_date[1], "2026-01-10")
  expect_true(grepl('class="ribbon-link"', out$game_date[2], fixed = TRUE))
})

test_that("the link column is unchanged when no has_scores column is supplied", {
  df <- data.frame(game_id = 200L, team_id = 6L, game_date = "2026-01-17",
                   team_name = "A", opp_team_name = "C", stringsAsFactors = FALSE)
  out <- add_ribbon_link_column(df)
  expect_true(grepl('class="ribbon-link"', out$game_date[1], fixed = TRUE))
})

test_that("attach_has_scores keys on (game_id, team_id), not game_id alone", {
  # Non-vacuous: a game_id-only join would gate BOTH rows off here, since
  # both share game_id 139. Keying on the pair leaves team 7's row untouched.
  df <- data.frame(game_id = c(139L, 139L), team_id = c(6L, 7L),
                   stringsAsFactors = FALSE)
  scoreless <- data.frame(game_id = 139L, team_id = 6L)
  out <- attach_has_scores(df, scoreless)
  expect_identical(out$has_scores, c(FALSE, TRUE))
})

test_that("attach_has_scores flags the scoreless row and leaves the other linked", {
  df <- data.frame(game_id = c(139L, 200L), team_id = c(6L, 6L),
                   stringsAsFactors = FALSE)
  scoreless <- data.frame(game_id = 139L, team_id = 6L)
  out <- attach_has_scores(df, scoreless)
  expect_identical(out$has_scores, c(FALSE, TRUE))
})

test_that("attach_has_scores treats a NULL or empty scoreless set as nothing to gate", {
  # This is the shape Tab 11 passes today (EuroLeague has no known scoreless
  # games), and the shape a stale/empty cache would return for Tab 4 -- either
  # way every row must stay linked, not silently lose its ribbon.
  df <- data.frame(game_id = c(139L, 200L), team_id = c(6L, 6L),
                   stringsAsFactors = FALSE)
  out_null <- attach_has_scores(df, NULL)
  out_empty <- attach_has_scores(df, data.frame(game_id = integer(0), team_id = integer(0)))
  expect_true(all(out_null$has_scores))
  expect_true(all(out_empty$has_scores))
})

test_that("both Tab 4 modes build the link before select drops the ids", {
  src <- readLines(testthat::test_path("..", "..", "R", "server_tab4.R"), warn = FALSE)
  add_lines <- grep("add_ribbon_link_column", src)
  sel_lines <- grep("disp <- df %>% select", src, fixed = TRUE)
  expect_length(sel_lines, 2)
  expect_length(add_lines, 2)
  for (sl in sel_lines) expect_true(any(add_lines < sl & add_lines > sl - 12))
})

test_that("both Tab 4 modes attach has_scores immediately before building the link", {
  # Regression guard for Ruling 2: a gate wired into only one of the two Tab 4
  # view modes leaves a live path that still links a scoreless game.
  src <- readLines(testthat::test_path("..", "..", "R", "server_tab4.R"), warn = FALSE)
  attach_lines <- grep("attach_has_scores(df, fetch_scoreless_games(gl_data_version()))",
                       src, fixed = TRUE)
  add_lines <- grep("df <- add_ribbon_link_column(df)", src, fixed = TRUE)
  expect_length(attach_lines, 2)
  expect_length(add_lines, 2)
  for (al in add_lines) expect_true(any(attach_lines == al - 1))
})

test_that("Tab 11 passes an empty scoreless set instead of a second query", {
  # Ruling 6: EuroLeague's ribbon_margin_v is asserted NULL-free elsewhere
  # (see "euroleague.ribbon_margin_v has no NULL margin" in
  # test-stint-ribbon-readers.R), so this tab must not grow its own scan
  # against euroleague's schema for a condition it does not have.
  src <- readLines(testthat::test_path("..", "..", "R", "server_tab11_euro_gamelogs.R"),
                   warn = FALSE)
  expect_true(any(grepl("attach_has_scores(df, NULL)", src, fixed = TRUE)))
  expect_false(any(grepl("fetch_scoreless_games", src, fixed = TRUE)))
})

test_that("fetch_scoreless_games carries no season dimension in its cache key", {
  # Ruling 6: this deviates from the four canonical lookups' one-key-per-
  # season convention on purpose -- the underlying scan's cost does not fall
  # when filtered by season, so a per-season key would mean N scans for one
  # 8-row answer. Assert the deviation stays a single, non-`gy`-scoped key.
  src <- readLines(testthat::test_path("..", "..", "R", "global.R"), warn = FALSE)
  fn_line <- grep("^fetch_scoreless_games <- function", src)
  expect_length(fn_line, 1)
  expect_false(grepl("gy", src[fn_line], fixed = TRUE))
  key_line <- grep('key = sprintf\\("scoreless_games', src, value = TRUE)
  expect_length(key_line, 1)
  expect_false(grepl("gy", key_line, fixed = TRUE))
})

test_that("prewarm_for_year also warms fetch_scoreless_games", {
  # So the 2.56s / 21,240-buffer scan never lands on a user request.
  src <- paste(readLines(testthat::test_path("..", "..", "app.R"), warn = FALSE),
              collapse = "\n")
  idx_pf <- regexpr("prewarm_for_year <- function", src)
  idx_hub <- regexpr("hub_fetch_team_ff\\(gy_int, ver\\)", src)
  idx_scoreless <- regexpr("fetch_scoreless_games\\(ver\\)", src)
  expect_true(idx_pf > 0 && idx_hub > 0 && idx_scoreless > 0)
  expect_true(idx_scoreless > idx_pf && idx_scoreless > idx_hub)
})

test_that("app.js exposes the queued ribbon click handler", {
  js <- paste(readLines(testthat::test_path("..", "..", "www", "app.js"),
                        warn = FALSE), collapse = "\n")
  expect_match(js, "handleRibbonLinkClick", fixed = TRUE)
  expect_match(js, "gl_ribbon_click", fixed = TRUE)
  expect_match(js, "window.ibplSendShinyEvent", fixed = TRUE)
})

test_that("ribbon_link_cell's data-input-id follows the input_id argument", {
  # The league lives in the argument, not in the function name -- one helper
  # serves both leagues via input_id. Per CLAUDE.md, no parallel euro_
  # implementation of logic that already exists. Note the literal id:
  # eurogl_ribbon_click (matching this tab's eurogl_* input prefix), not the
  # brief's euro_gl_ribbon_click.
  israeli <- ribbon_link_cell(1L, 2L, "x")
  euro <- ribbon_link_cell(1L, 2L, "x", input_id = "eurogl_ribbon_click")
  expect_match(israeli, 'data-input-id="gl_ribbon_click"', fixed = TRUE)
  expect_match(euro, 'data-input-id="eurogl_ribbon_click"', fixed = TRUE)
})

test_that("Tab 11 builds the ribbon link on the source frame before the display subset", {
  # Tab 11 has a single renderDT with an `ff` flag that branches column sets
  # for Summary vs Four Factors, and one shared `disp <- df[...]` subset line
  # -- unlike Tab 4's two separate `disp <- df %>% select(...)` blocks, one
  # add_ribbon_link_column() call here covers both view modes. Assert >= 1,
  # not the 2 Tab 4 needs; do not add a second call to satisfy a stale count.
  src <- readLines(testthat::test_path("..", "..", "R", "server_tab11_euro_gamelogs.R"),
                   warn = FALSE)
  expect_gte(length(grep("add_ribbon_link_column", src, fixed = TRUE)), 1)
  expect_true(any(grepl("eurogl_ribbon_click", src, fixed = TRUE)))
})

test_that("EuroLeague game logs reuse the shared ribbon builder, not a parallel euro_ clone", {
  # CLAUDE.md hard rule: EuroLeague tabs REUSE the Israeli implementation and
  # must never grow a parallel euro_ version of logic that already exists.
  # This is the guard against a future edit silently reintroducing
  # euro_build_stint_ribbon / euro_fetch_stint_ribbon -- do not delete this
  # test as "redundant" with the wiring assertions above.
  # Task 2 moved the observer into the shared ribbon_modal_server()
  # (mod_ribbon_modal.R), so fetch_stint_ribbon/build_stint_ribbon_svg no
  # longer appear inline in either tab file -- assert both delegate to the
  # shared function, and that the shared function is where the calls live.
  il <- paste(readLines(testthat::test_path("..", "..", "R", "server_tab4.R"),
                        warn = FALSE), collapse = "\n")
  eu <- paste(readLines(testthat::test_path("..", "..", "R", "server_tab11_euro_gamelogs.R"),
                        warn = FALSE), collapse = "\n")
  mod <- paste(readLines(testthat::test_path("..", "..", "R", "mod_ribbon_modal.R"),
                        warn = FALSE), collapse = "\n")
  expect_match(il, "ribbon_modal_server(", fixed = TRUE)
  expect_match(eu, "ribbon_modal_server(", fixed = TRUE)
  expect_match(mod, "fetch_stint_ribbon", fixed = TRUE)
  expect_match(mod, "build_stint_ribbon_svg", fixed = TRUE)
  expect_false(grepl("euro_build_stint_ribbon", eu, fixed = TRUE))
  expect_false(grepl("euro_fetch_stint_ribbon", eu, fixed = TRUE))
})

test_that("neither game-log tab defines its own ribbon modal observer", {
  # Guards against a re-clone of the extracted observer (Task 2): the
  # existing "reuse the shared ribbon builder" test above only asserts
  # fetch_stint_ribbon/build_stint_ribbon_svg live in mod_ribbon_modal.R --
  # it would not catch someone pasting the old 46-line observeEvent block
  # back into a tab file (that block also called those two functions).
  # Reverting either call site to the inline block restores `showModal(` in
  # that file and fails the second assertion here; dropping the call
  # entirely fails the first.
  for (f in c("server_tab4.R", "server_tab11_euro_gamelogs.R")) {
    src <- paste(readLines(testthat::test_path("..", "..", "R", f), warn = FALSE),
                collapse = "\n")
    expect_true(grepl("ribbon_modal_server(", src, fixed = TRUE), info = f)
    expect_false(grepl("showModal(", src, fixed = TRUE), info = f)
  }
})

test_that("the ribbon modal observer is rate limited", {
  src <- paste(readLines(testthat::test_path("..", "..", "R", "mod_ribbon_modal.R"),
                        warn = FALSE), collapse = "\n")
  expect_true(grepl("guard_heavy_request(", src, fixed = TRUE))
  expect_lt(regexpr("guard_heavy_request(", src, fixed = TRUE)[1],
            regexpr("fetch_stint_ribbon(", src, fixed = TRUE)[1])
})

# ---- Mutation guards added after the 2026-09-05 final review (I4) ----------
# Each of the six tests below asserts BOTH sides of a cross-file contract, so
# renaming either side alone -- not just deleting one -- fails the suite.

test_that("app.js's is-active token matches the class app.css actually styles", {
  js <- paste(readLines(testthat::test_path("..", "..", "www", "app.js"),
                        warn = FALSE), collapse = "\n")
  css <- paste(readLines(testthat::test_path("..", "..", "www", "app.css"),
                         warn = FALSE), collapse = "\n")
  # JS side: setFocus() toggles the literal class via classList.
  fn <- regmatches(js, regexpr(
    "function setFocus\\(svg, lane\\) \\{(.|\n)*?\\n  \\}", js, perl = TRUE))
  expect_true(nzchar(fn))
  # All three literal occurrences must survive TOGETHER: the selector that
  # finds the currently-active lane(s), the removal before reassigning, and
  # the add that actually applies the emphasis. A single count assertion
  # means renaming any ONE of the three (not just deleting the whole
  # mechanism) still fails this test -- matching only "add" or only "remove"
  # would have let exactly that kind of partial rename through.
  #
  # fixed=TRUE substring counting has its own hole: "is-active" is also a
  # SUBSTRING of "is-active-lane", so renaming the token to that (or any
  # other superstring) left all three occurrences "matching" and this
  # assertion still green even though the CSS selector below no longer
  # applies (confirmed by mutation, 2026-09-06). A trailing negative
  # lookahead requires the token not be immediately followed by a hyphen or
  # word character, so a superstring rename actually breaks the count.
  expect_identical(
    lengths(regmatches(fn, gregexpr("is-active(?![-\\w])", fn, perl = TRUE))),
    3L)
  # CSS side: the selector that actually renders the emphasis.
  expect_match(css, "\\.ibpl-ribbon-lane\\.is-active\\b")
})

test_that("handleRibbonLinkClick routes by the link's own data-input-id, not a hardcoded one", {
  js <- paste(readLines(testthat::test_path("..", "..", "www", "app.js"),
                        warn = FALSE), collapse = "\n")
  fn <- regmatches(js, regexpr(
    "window\\.handleRibbonLinkClick = function\\(linkEl\\) \\{(.|\n)*?\\n  \\};", js, perl = TRUE))
  expect_true(nzchar(fn))
  # If this hardcoded "gl_ribbon_click" instead of reading dataset.inputId,
  # every Tab 11 (EuroLeague) click would route into Tab 4's Israeli observer.
  # Two separate expect_match() calls (one for `dataset\.inputId`, one for
  # the quoted literal) would still both pass if the `||` operands were
  # swapped to `"gl_ribbon_click" || linkEl.dataset.inputId` -- the hardcoded
  # literal is truthy and always wins, so the fallback is dead code, but both
  # substrings are still present. Assert the operands in their required
  # order, as one expression, instead.
  expect_match(fn, 'linkEl\\.dataset\\.inputId\\s*\\|\\|\\s*"gl_ribbon_click"')
})

test_that("ribbon_link_cell's onclick target is a function app.js actually defines", {
  html <- ribbon_link_cell(1L, 1L, "x")
  target <- regmatches(html, regexpr('onclick="window\\.[A-Za-z0-9_]+\\(', html))
  expect_true(nzchar(target))
  fn_name <- sub('^onclick="window\\.', "", sub("\\($", "", target))
  js <- paste(readLines(testthat::test_path("..", "..", "www", "app.js"),
                        warn = FALSE), collapse = "\n")
  # A renamed onclick target with no matching JS definition is a dead link:
  # the anchor is clickable but nothing happens.
  expect_match(js, paste0("window\\.", fn_name, "\\s*="))
})

test_that("Tab 4 and Tab 11 game-log tables escape everything except the ribbon-link date column", {
  # Dropping this at any of the three call sites renders the ribbon-link
  # anchor as visible HTML text instead of a clickable link.
  tab4 <- paste(readLines(testthat::test_path("..", "..", "R", "server_tab4.R"),
                          warn = FALSE), collapse = "\n")
  tab11 <- paste(readLines(testthat::test_path("..", "..", "R", "server_tab11_euro_gamelogs.R"),
                           warn = FALSE), collapse = "\n")
  escape_pattern <- 'escape\\s*=\\s*dt_escape_except\\(disp,\\s*"game_date"\\)'
  expect_identical(lengths(regmatches(tab4, gregexpr(escape_pattern, tab4))), 2L)
  expect_match(tab11, escape_pattern)
})

test_that("Tab 4 and Tab 11 ribbon output ids and SVG id_prefix never collide", {
  # Task 2 moved the observer into the shared ribbon_modal_server(); the
  # output id is now built as output[[paste0(prefix, "_ribbon_svg")]] inside
  # mod_ribbon_modal.R, not as an inline `output$..._ribbon_svg <- renderUI`
  # literal in either tab file, so the non-collision guarantee now lives in
  # each call site's `prefix` / `svg_id_prefix` arguments. Read the call line
  # directly (not the pasted whole-file blob) so a nested paren inside
  # `function() ...` in the Israeli call can't confuse a bounded regex.
  src4 <- readLines(testthat::test_path("..", "..", "R", "server_tab4.R"), warn = FALSE)
  src11 <- readLines(testthat::test_path("..", "..", "R", "server_tab11_euro_gamelogs.R"),
                     warn = FALSE)
  call4 <- grep("ribbon_modal_server(", src4, fixed = TRUE, value = TRUE)
  call11 <- grep("ribbon_modal_server(", src11, fixed = TRUE, value = TRUE)
  expect_length(call4, 1)
  expect_length(call11, 1)

  prefix4 <- regmatches(call4, regexpr('"[a-z]+"', call4))
  prefix11 <- regmatches(call11, regexpr('"[a-z]+"', call11))
  expect_true(nzchar(prefix4))
  expect_true(nzchar(prefix11))
  expect_false(identical(prefix4, prefix11))

  # svg_id_prefix defaults to prefix when the call omits it (Israeli does).
  # Normalize to just the quoted value in both branches -- comparing the
  # explicit branch's full "svg_id_prefix = "x"" match against the default
  # branch's bare "x" would make expect_false() below pass on formatting
  # alone, independent of whether the underlying values actually collide.
  extract_svg_prefix <- function(call_line, default_val) {
    if (grepl("svg_id_prefix", call_line, fixed = TRUE)) {
      labeled <- regmatches(call_line, regexpr('svg_id_prefix\\s*=\\s*"[a-z]+"', call_line))
      regmatches(labeled, regexpr('"[a-z]+"', labeled))
    } else {
      default_val
    }
  }
  svg4 <- extract_svg_prefix(call4, prefix4)
  svg11 <- extract_svg_prefix(call11, prefix11)
  expect_true(nzchar(svg4))
  expect_true(nzchar(svg11))
  expect_false(identical(svg4, svg11))
})

# ---------------- ribbon_score_as_of ----------------
# The step-series lookup behind every per-stint number. It must read the RAW
# reader series, never ribbon_complete_margin()'s padded output.

steps_fixture <- function() {
  data.frame(
    elapsed   = c(30, 30, 95, 240, 240),
    order_key = c(1, 2, 3, 4, 5),
    value     = c(2, 4, 7, 9, 12),
    stringsAsFactors = FALSE
  )
}

test_that("ribbon_score_as_of is 0 before the first row", {
  expect_equal(ribbon_score_as_of(steps_fixture(), 0), 0)
  expect_equal(ribbon_score_as_of(steps_fixture(), 29), 0)
})

test_that("ribbon_score_as_of takes the LAST row at a tied elapsed second", {
  # Two events share second 30; the state after that second is 4, not 2.
  expect_equal(ribbon_score_as_of(steps_fixture(), 30), 4)
  expect_equal(ribbon_score_as_of(steps_fixture(), 240), 12)
})

test_that("ribbon_score_as_of carries the last value forward between rows", {
  expect_equal(ribbon_score_as_of(steps_fixture(), 94), 4)
  expect_equal(ribbon_score_as_of(steps_fixture(), 95), 7)
  expect_equal(ribbon_score_as_of(steps_fixture(), 239), 7)
})

test_that("ribbon_score_as_of holds the final value after the last row", {
  expect_equal(ribbon_score_as_of(steps_fixture(), 2400), 12)
})

test_that("ribbon_score_as_of is vectorised over t", {
  expect_equal(ribbon_score_as_of(steps_fixture(), c(0, 30, 94, 2400)),
               c(0, 4, 4, 12))
})

test_that("ribbon_score_as_of sorts an unordered series before reading it", {
  s <- steps_fixture()[c(5, 1, 3, 2, 4), , drop = FALSE]
  expect_equal(ribbon_score_as_of(s, c(30, 95, 240)), c(4, 7, 12))
})

test_that("ribbon_score_as_of returns zeros for an empty or NULL series", {
  expect_equal(ribbon_score_as_of(NULL, c(1, 2)), c(0, 0))
  empty <- data.frame(elapsed = numeric(0), order_key = numeric(0),
                      value = numeric(0))
  expect_equal(ribbon_score_as_of(empty, c(1, 2)), c(0, 0))
})

# ---------------- ribbon_stint_points ----------------

pts_steps <- function(elapsed, own, opp) {
  data.frame(elapsed = elapsed, order_key = seq_along(elapsed),
             own = own, margin = own - opp, stringsAsFactors = FALSE)
}

pts_stint <- function(start_elapsed, end_elapsed) {
  n <- length(start_elapsed)
  data.frame(side = rep("own", n), player_key = rep("1", n),
             player_label = rep("Player 1", n), is_starter = rep(TRUE, n),
             start_elapsed = start_elapsed, end_elapsed = end_elapsed,
             stringsAsFactors = FALSE)
}

test_that("ribbon_stint_points takes net differences across the window", {
  steps <- pts_steps(c(10, 20, 30, 40), own = c(2, 2, 5, 7), opp = c(0, 3, 3, 3))
  out <- ribbon_stint_points(pts_stint(10, 40), steps)
  expect_equal(out$pf, 5)   # own 2 -> 7
  expect_equal(out$pa, 3)   # opp 0 -> 3
  expect_equal(out$pm, 2)
})

test_that("ribbon_stint_points nets out a credit-then-rescind inside the window", {
  # The PBP credits 2 at t=20 and rescinds them at t=30. Summing positive
  # increments would report pf = 5; the net difference reports 3.
  steps <- pts_steps(c(10, 20, 30, 40), own = c(0, 2, 0, 3), opp = c(0, 0, 0, 0))
  out <- ribbon_stint_points(pts_stint(10, 40), steps)
  expect_equal(out$pf, 3)
  expect_equal(out$pa, 0)
  expect_equal(out$pm, 3)
})

test_that("ribbon_stint_points puts a score on the boundary second in the OUTGOING stint", {
  # A basket recorded at exactly t=30, where one stint ends and the next
  # begins. It belongs to the stint that ended, and forms the next one's
  # baseline, so the two stints still telescope to the game total.
  steps <- pts_steps(c(10, 30, 50), own = c(0, 2, 5), opp = c(0, 0, 0))
  stints <- rbind(pts_stint(0, 30), pts_stint(30, 60))
  out <- ribbon_stint_points(stints, steps)
  expect_equal(out$pf, c(2, 3))
  expect_equal(sum(out$pf), 5)
})

test_that("ribbon_stint_points pm always equals the margin delta", {
  # This is the property that keeps a bar's printed number equal to the
  # rise of the curve drawn above it. It must hold independently of `own`.
  steps <- pts_steps(c(10, 25, 45), own = c(3, 3, 8), opp = c(0, 6, 6))
  out <- ribbon_stint_points(pts_stint(10, 45), steps)
  expect_equal(out$pm, out$pf - out$pa)
  expect_equal(out$pm,
               ribbon_score_as_of(
                 data.frame(elapsed = steps$elapsed, order_key = steps$order_key,
                            value = steps$margin), 45) -
               ribbon_score_as_of(
                 data.frame(elapsed = steps$elapsed, order_key = steps$order_key,
                            value = steps$margin), 10))
})

test_that("ribbon_stint_points computes pm without an own column", {
  steps <- data.frame(elapsed = c(10, 30), order_key = c(1, 2), margin = c(1, 6))
  out <- ribbon_stint_points(pts_stint(0, 40), steps)
  expect_equal(out$pm, 6)
  expect_true(is.na(out$pf))
  expect_true(is.na(out$pa))
})

test_that("ribbon_stint_points returns typed empty columns for no stints", {
  empty <- pts_stint(numeric(0), numeric(0))
  out <- ribbon_stint_points(empty, pts_steps(10, 1, 0))
  expect_equal(nrow(out), 0)
  expect_true(all(c("pf", "pa", "pm") %in% names(out)))
})

test_that("an empty step series yields NA, never a fabricated zero", {
  # "Unknown" and "the stint was level" are different facts and the chart
  # renders them differently: NA prints nothing, 0 prints "0". Returning 0
  # here would put a false level-stint number on every bar whenever the
  # step series is missing.
  empty_steps <- data.frame(elapsed = numeric(0), order_key = numeric(0),
                            margin = numeric(0))
  out <- ribbon_stint_points(pts_stint(0, 600), empty_steps)
  expect_true(is.na(out$pm))
  expect_true(is.na(out$pf))
  expect_true(is.na(out$pa))
})

# ---------------- ribbon_stint_overlaps ----------------
# A merged bar spans a median of 4 different fives (measured 2026-09-06 over
# 1,470 real stints), so "the five at the start" is stale for most of most
# bars. The strip lists everyone who shared the floor, with their spans.

ov_lanes <- function() {
  rbind(
    lane_row("own", "1", 0, 600),    # the hovered player
    lane_row("own", "2", 0, 600),    # on the whole stint
    lane_row("own", "3", 300, 900),  # joins partway, stays past the end
    lane_row("own", "4", 0, 200),    # leaves partway
    lane_row("own", "5", 100, 150),  # entirely inside
    lane_row("own", "6", 700, 900),  # no overlap at all
    lane_row("opp", "7", 0, 600)     # other side, never counted
  )
}

test_that("ribbon_stint_overlaps clips each teammate to the stint window", {
  out <- ribbon_stint_overlaps(ov_lanes(), "own", "1", 0, 600)
  expect_equal(out$start_elapsed[out$player_key == "3"], 300)
  expect_equal(out$end_elapsed[out$player_key == "3"], 600)
  expect_equal(out$end_elapsed[out$player_key == "4"], 200)
  expect_equal(out$start_elapsed[out$player_key == "5"], 100)
  expect_equal(out$end_elapsed[out$player_key == "5"], 150)
})

test_that("ribbon_stint_overlaps excludes the player, the other side and non-overlaps", {
  out <- ribbon_stint_overlaps(ov_lanes(), "own", "1", 0, 600)
  expect_false("1" %in% out$player_key)
  expect_false("6" %in% out$player_key)
  expect_false("7" %in% out$player_key)
})

test_that("ribbon_stint_overlaps orders by shared time, longest first", {
  out <- ribbon_stint_overlaps(ov_lanes(), "own", "1", 0, 600)
  expect_equal(out$player_key, c("2", "3", "4", "5"))
  expect_equal(out$shared, c(600, 300, 200, 50))
})

test_that("ribbon_stint_overlaps keeps both spans when a teammate returns", {
  # Teammate 2 subs out and back in while player 1 stays on the floor.
  lanes <- rbind(
    lane_row("own", "1", 0, 600),
    lane_row("own", "2", 0, 100),
    lane_row("own", "2", 400, 600)
  )
  out <- ribbon_stint_overlaps(lanes, "own", "1", 0, 600)
  expect_equal(nrow(out), 2)
  expect_equal(out$start_elapsed, c(0, 400))
  expect_true(all(out$shared == 300))   # 100 + 200, on both rows
})

test_that("ribbon_stint_overlaps drops a zero-length touch at the boundary", {
  # Teammate leaves exactly when this stint starts: they never shared the floor.
  lanes <- rbind(lane_row("own", "1", 300, 600), lane_row("own", "2", 0, 300))
  out <- ribbon_stint_overlaps(lanes, "own", "1", 300, 600)
  expect_equal(nrow(out), 0)
})

test_that("ribbon_stint_overlaps returns zero typed rows when nothing overlaps", {
  out <- ribbon_stint_overlaps(lane_row("own", "1", 0, 600), "own", "1", 0, 600)
  expect_equal(nrow(out), 0)
  expect_true(all(c("player_key", "player_label", "start_elapsed",
                    "end_elapsed", "shared") %in% names(out)))
})

test_that("ribbon_stint_overlaps sorts input that does not arrive in order", {
  # The ov_lanes() fixture happens to list its teammates in descending
  # shared time already, so it would pass even with the order() call
  # removed. This fixture lists the SHORTEST overlap first and the longest
  # last, so it fails unless the sort actually runs.
  lanes <- rbind(
    lane_row("own", "1", 0, 600),
    lane_row("own", "9", 500, 600),   # shared 100, listed first
    lane_row("own", "8", 0, 600),     # shared 600, listed last
    lane_row("own", "7", 200, 600)    # shared 400
  )
  out <- ribbon_stint_overlaps(lanes, "own", "1", 0, 600)
  expect_equal(out$player_key, c("8", "7", "9"))
  expect_equal(out$shared, c(600, 400, 100))
})

# ---------------- ribbon_player_totals ----------------

test_that("ribbon_player_totals sums floor time and +/- per player per side", {
  lanes <- rbind(
    lane_row("own", "1", 0, 300),
    lane_row("own", "1", 600, 900),
    lane_row("own", "2", 0, 600),
    lane_row("opp", "1", 0, 120)
  )
  lanes$pm <- c(4, -1, 3, 7)
  out <- ribbon_player_totals(lanes)

  own1 <- out[out$side == "own" & out$player_key == "1", ]
  expect_equal(own1$secs, 600)
  expect_equal(own1$pm, 3)

  # The same player_key on the other side is a different person's lane.
  opp1 <- out[out$side == "opp" & out$player_key == "1", ]
  expect_equal(opp1$secs, 120)
  expect_equal(opp1$pm, 7)
})

test_that("build_stint_ribbon_svg still renders with steps = NULL", {
  lanes <- ribbon_mark_starters(rbind(lane_row("own", "1", 0, 600),
                                      lane_row("opp", "2", 0, 600)))
  margin <- ribbon_complete_margin(
    data.frame(elapsed = c(100, 500), margin = c(2, -3), order_key = c(1, 2)),
    2400)
  svg <- as.character(build_stint_ribbon_svg(lanes, margin, list(n_periods = 4L)))
  expect_true(grepl("ibpl-ribbon", svg, fixed = TRUE))
})

# ---------------- gutter columns ----------------

test_that("ribbon_minutes_label formats floor time as M:SS", {
  expect_equal(ribbon_minutes_label(0), "0:00")
  expect_equal(ribbon_minutes_label(59), "0:59")
  expect_equal(ribbon_minutes_label(600), "10:00")
  expect_equal(ribbon_minutes_label(1692), "28:12")
})

test_that("ribbon_pm_label signs every nonzero value and prints a bare zero", {
  expect_equal(ribbon_pm_label(0), "0")
  expect_equal(ribbon_pm_label(6), "+6")
  expect_equal(ribbon_pm_label(-4), "-4")
  expect_equal(ribbon_pm_label(12), "+12")
})

test_that("the geometry change preserves the plot area exactly", {
  # RIBBON_GUTTER and RIBBON_WIDTH move together so every measured bar width
  # is unchanged: 1070 - 220 == 1000 - 150.
  expect_equal(RIBBON_WIDTH - RIBBON_GUTTER, 850)
})

test_that("the gutter renders a MIN and a +/- column per player", {
  lanes <- ribbon_mark_starters(rbind(
    lane_row("own", "1", 0, 600, player_label = "Alice Adams"),
    lane_row("own", "1", 900, 1200, player_label = "Alice Adams")
  ))
  steps <- data.frame(elapsed = c(300, 1000), order_key = c(1, 2),
                      margin = c(4, 9))
  svg <- as.character(build_stint_ribbon_svg(
    lanes, ribbon_complete_margin(
      data.frame(elapsed = c(300, 1000), margin = c(4, 9), order_key = c(1, 2)),
      2400),
    list(n_periods = 4L), steps = steps))

  expect_true(grepl("ibpl-ribbon-min", svg, fixed = TRUE))
  expect_true(grepl("ibpl-ribbon-pm", svg, fixed = TRUE))
  expect_true(grepl(">15:00<", svg, fixed = TRUE))   # 600 + 300 seconds
  expect_true(grepl(">+9<", svg, fixed = TRUE))      # margin 0 -> 4 -> 9
})

test_that("the gutter header labels both new columns", {
  lanes <- ribbon_mark_starters(lane_row("own", "1", 0, 600))
  svg <- as.character(build_stint_ribbon_svg(
    lanes, ribbon_complete_margin(
      data.frame(elapsed = 300, margin = 2, order_key = 1), 2400),
    list(n_periods = 4L),
    steps = data.frame(elapsed = 300, order_key = 1, margin = 2)))
  expect_true(grepl(">MIN<", svg, fixed = TRUE))
  expect_true(grepl(">+/-<", svg, fixed = TRUE))
})

test_that("the lane aria-label reports the player's GAME total, not one stint", {
  # Before this task it reported the first stint's duration, which for a
  # player with several stints was simply the wrong number.
  lanes <- ribbon_mark_starters(rbind(
    lane_row("own", "1", 0, 600, player_label = "Alice Adams"),
    lane_row("own", "1", 900, 1200, player_label = "Alice Adams")
  ))
  svg <- as.character(build_stint_ribbon_svg(
    lanes, ribbon_complete_margin(
      data.frame(elapsed = 300, margin = 2, order_key = 1), 2400),
    list(n_periods = 4L),
    steps = data.frame(elapsed = 300, order_key = 1, margin = 2)))
  # Scope to the gutter <text> label. The per-stint <g> aria-labels
  # legitimately report their own stint's duration (10:00 for the first
  # stint here), so an unscoped grepl over the whole SVG conflates two
  # different labels that are both correct.
  name_tags <- regmatches(svg, gregexpr('<text[^>]*ibpl-ribbon-name[^>]*>', svg))[[1]]
  expect_true(any(grepl('aria-label="Alice Adams, 15:00 on the floor', name_tags,
                        fixed = TRUE)))
  expect_false(any(grepl('aria-label="Alice Adams, 10:00 on the floor', name_tags,
                         fixed = TRUE)))
})

# ---------------- on-bar numbers ----------------
# Measured 2026-09-06 over 15 games: 72 bars a game, median width 105px,
# 90.3% at least 20px. So about 65 of 72 bars carry a number and ~7 do not.

test_that("ribbon_number_fits uses the mono advance width", {
  # nchar * 0.6 * 9 + 6
  expect_equal(ribbon_number_fits("0", 11.4), TRUE)
  expect_equal(ribbon_number_fits("0", 11.3), FALSE)
  expect_equal(ribbon_number_fits("+6", 16.8), TRUE)
  expect_equal(ribbon_number_fits("+12", 22.2), TRUE)
  expect_equal(ribbon_number_fits("+12", 20), FALSE)
})

test_that("ribbon_number_fits is vectorised", {
  expect_equal(ribbon_number_fits(c("0", "+12"), c(50, 5)), c(TRUE, FALSE))
})

test_that("a wide bar carries its +/- and a narrow one carries nothing", {
  # 850 units span 2400s, so 600s -> 212px (wide) and 30s -> 10.6px (narrow).
  lanes <- ribbon_mark_starters(rbind(
    lane_row("own", "1", 0, 600),
    lane_row("own", "1", 700, 730)
  ))
  steps <- data.frame(elapsed = c(300, 720), order_key = c(1, 2),
                      margin = c(6, 8))
  svg <- as.character(build_stint_ribbon_svg(
    lanes, ribbon_complete_margin(
      data.frame(elapsed = c(300, 720), margin = c(6, 8), order_key = c(1, 2)),
      2400),
    list(n_periods = 4L), steps = steps))

  expect_true(grepl("ibpl-ribbon-num", svg, fixed = TRUE))
  expect_equal(lengths(regmatches(svg, gregexpr("ibpl-ribbon-num", svg,
                                                fixed = TRUE))), 1)
})

test_that("a level stint prints a zero rather than nothing", {
  lanes <- ribbon_mark_starters(lane_row("own", "1", 0, 600))
  steps <- data.frame(elapsed = 1200, order_key = 1, margin = 5)
  svg <- as.character(build_stint_ribbon_svg(
    lanes, ribbon_complete_margin(
      data.frame(elapsed = 1200, margin = 5, order_key = 1), 2400),
    list(n_periods = 4L), steps = steps))
  # Nothing scored inside 0-600, so the stint is level and must say so.
  # Scope to the on-bar <text class="ibpl-ribbon-num"> node. An unscoped
  # grepl(">0</text>") is also satisfied by the gutter's ibpl-ribbon-pm
  # total (a Task 5 node, always present, and often "0" itself), so it
  # would still pass even if this task's on-bar number were deleted.
  num_tags <- regmatches(svg, gregexpr(
    '<text[^>]*ibpl-ribbon-num[^>]*>[^<]*</text>', svg))[[1]]
  expect_true(length(num_tags) > 0)
  expect_true(any(grepl(">0</text>", num_tags, fixed = TRUE)))
})

test_that("no number is drawn when steps are absent", {
  lanes <- ribbon_mark_starters(lane_row("own", "1", 0, 600))
  svg <- as.character(build_stint_ribbon_svg(
    lanes, ribbon_complete_margin(
      data.frame(elapsed = 300, margin = 2, order_key = 1), 2400),
    list(n_periods = 4L)))
  expect_false(grepl("ibpl-ribbon-num", svg, fixed = TRUE))
})


# ---------------- hover payload ----------------

test_that("each lane carries the numbers and the teammate spans the strip needs", {
  lanes <- ribbon_mark_starters(rbind(
    lane_row("own", "1", 0, 600, player_label = "Alice Adams"),
    lane_row("own", "2", 300, 900, player_label = "Bea Bell")
  ))
  steps <- data.frame(elapsed = c(200, 400), order_key = c(1, 2),
                      margin = c(3, 6), own = c(5, 10))
  svg <- as.character(build_stint_ribbon_svg(
    lanes, ribbon_complete_margin(
      data.frame(elapsed = c(200, 400), margin = c(3, 6), order_key = c(1, 2)),
      2400),
    list(n_periods = 4L), steps = steps))

  expect_true(grepl('data-start="0"', svg, fixed = TRUE))
  expect_true(grepl('data-end="600"', svg, fixed = TRUE))
  expect_true(grepl('data-player="Alice Adams"', svg, fixed = TRUE))
  expect_true(grepl("data-pf=", svg, fixed = TRUE))
  expect_true(grepl("data-pa=", svg, fixed = TRUE))
  # Alice overlapped Bea for 300-600, and the entry carries BOTH axes.
  expect_true(grepl("Bea Bell", svg, fixed = TRUE))
})

test_that("a teammate entry names a window and a swing, never a bare name", {
  lanes <- ribbon_mark_starters(rbind(
    lane_row("own", "1", 0, 600, player_label = "Alice Adams"),
    lane_row("own", "2", 300, 900, player_label = "Bea Bell")
  ))
  steps <- data.frame(elapsed = c(200, 400), order_key = c(1, 2),
                      margin = c(3, 6), own = c(5, 10))
  svg <- as.character(build_stint_ribbon_svg(
    lanes, ribbon_complete_margin(
      data.frame(elapsed = c(200, 400), margin = c(3, 6), order_key = c(1, 2)),
      2400),
    list(n_periods = 4L), steps = steps))
  with_attr <- regmatches(svg, regexpr('data-with="[^"]*"', svg))
  expect_true(nzchar(with_attr))
  # every entry has a clock range and a signed number
  expect_true(grepl("5:00", with_attr, fixed = TRUE))
  expect_true(grepl("+", with_attr, fixed = TRUE))
})

test_that("ribbon_detail_strip renders an aria-hidden container", {
  strip <- as.character(ribbon_detail_strip())
  expect_true(grepl("ibpl-ribbon-detail", strip, fixed = TRUE))
  expect_true(grepl('aria-hidden="true"', strip, fixed = TRUE))
})

test_that("the lane aria-label carries the teammates too, not just the numbers", {
  # The strip is aria-hidden and the band and lit overlaps are purely
  # visual, so this label is the ONLY path to the lineup for a
  # screen-reader user. Dropping the teammates here would leave that user
  # with no equivalent at all.
  lanes <- ribbon_mark_starters(rbind(
    lane_row("own", "1", 0, 600, player_label = "Alice Adams"),
    lane_row("own", "2", 300, 900, player_label = "Bea Bell")
  ))
  steps <- data.frame(elapsed = c(200, 400), order_key = c(1, 2),
                      margin = c(3, 6), own = c(5, 10))
  svg <- as.character(build_stint_ribbon_svg(
    lanes, ribbon_complete_margin(
      data.frame(elapsed = c(200, 400), margin = c(3, 6), order_key = c(1, 2)),
      2400),
    list(n_periods = 4L), steps = steps))

  aria <- regmatches(svg, gregexpr('aria-label="[^"]*"', svg))[[1]]
  alice <- aria[grepl("Alice Adams", aria, fixed = TRUE)]
  expect_true(length(alice) > 0)
  expect_true(any(grepl("on with", alice, fixed = TRUE)))
  expect_true(any(grepl("Bea Bell", alice, fixed = TRUE)))
})
