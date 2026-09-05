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
    lane_row("own", "1", 0, 1200, is_starter = TRUE, player_label = "A Cohen"),
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
  expect_true(all(clips %in% ids))
})

test_that("the viewBox width comes from nominal period length", {
  f <- ribbon_fixture()
  html <- as.character(build_stint_ribbon_svg(f$lanes, f$margin, f$meta))
  expect_match(html, 'viewBox="0 0 1000 ')
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
  expect_identical(lengths(regmatches(html, gregexpr('aria-label="A Cohen', html))), 1L)
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
  f$lanes$player_key <- c("BIRCH, KHEM", "HALL, DEVON", "SLEVA, DUSTIN")
  html <- as.character(build_stint_ribbon_svg(f$lanes, f$margin, f$meta))
  clips <- regmatches(html, gregexpr('data-clip="[^"]+"', html))[[1]]
  clips <- sub('data-clip="', "", sub('"$', "", clips))
  ids <- regmatches(html, gregexpr('id="[^"]+"', html))[[1]]
  ids <- sub('id="', "", sub('"$', "", ids))
  expect_true(all(clips %in% ids))
})

test_that("app.css styles every class the SVG builder emits", {
  css <- paste(readLines(testthat::test_path("..", "..", "www", "app.css"),
                         warn = FALSE), collapse = "\n")
  for (cls in c("ibpl-ribbon", "ibpl-ribbon-lane", "ibpl-ribbon-margin-base",
                "ibpl-ribbon-margin-focus", "ibpl-ribbon-period",
                "ibpl-ribbon-name", "ibpl-ribbon-team", "ibpl-ribbon-zero",
                "ibpl-ribbon-period-label")) {
    expect_match(css, cls, fixed = TRUE, info = paste("missing ribbon style for", cls))
  }
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

test_that("both Tab 4 modes build the link before select drops the ids", {
  src <- readLines(testthat::test_path("..", "..", "R", "server_tab4.R"), warn = FALSE)
  add_lines <- grep("add_ribbon_link_column", src)
  sel_lines <- grep("disp <- df %>% select", src, fixed = TRUE)
  expect_length(sel_lines, 2)
  expect_length(add_lines, 2)
  for (sl in sel_lines) expect_true(any(add_lines < sl & add_lines > sl - 12))
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
  il <- paste(readLines(testthat::test_path("..", "..", "R", "server_tab4.R"),
                        warn = FALSE), collapse = "\n")
  eu <- paste(readLines(testthat::test_path("..", "..", "R", "server_tab11_euro_gamelogs.R"),
                        warn = FALSE), collapse = "\n")
  expect_match(il, "fetch_stint_ribbon", fixed = TRUE)
  expect_match(eu, "fetch_stint_ribbon", fixed = TRUE)
  expect_match(eu, "build_stint_ribbon_svg", fixed = TRUE)
  expect_false(grepl("euro_build_stint_ribbon", eu, fixed = TRUE))
  expect_false(grepl("euro_fetch_stint_ribbon", eu, fixed = TRUE))
})
