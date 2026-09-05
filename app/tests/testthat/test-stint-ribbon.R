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
  # The boundary case named but never written: a gap of exactly one second
  # between two stints for the same player must stay two bars, not one.
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
  m <- data.frame(elapsed = c(0, 2500, -10), margin = c(0, 9, 1),
                  order_key = c(1, 2, 3))
  out <- ribbon_complete_margin(m, total_seconds = 2400)
  expect_true(all(out$elapsed >= 0 & out$elapsed <= 2400))
})

test_that("ribbon_complete_margin returns a usable series from no data", {
  m <- data.frame(elapsed = numeric(0), margin = numeric(0), order_key = numeric(0))
  out <- ribbon_complete_margin(m, total_seconds = 2400)
  expect_identical(out$elapsed, c(0, 2400))
  expect_identical(out$margin, c(0, 0))
})
