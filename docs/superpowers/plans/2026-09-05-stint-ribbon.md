# Stint Ribbon Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Per-game player lanes over the game clock with the score margin behind them, opened from a game-log row in either league, where hovering a lane re-clips the margin curve to that player's floor time.

**Architecture:** One SQL query per ribbon open returns a single row with `lanes` and `margin` as `jsonb`. Two thin league readers normalise into one canonical frame; pure transforms in `helpers.R` merge adjacent stints and compute geometry; a pure builder emits an inline SVG tag tree. Hover is one `clip-path` attribute swap in `app.js`. No plotting library, no new R package.

**Tech Stack:** R 4.4.2, Shiny/bslib, DBI/RPostgres against PostgreSQL (Supabase, schema `basketball_test` and `euroleague`), htmltools SVG, testthat, vanilla JS.

**Spec:** `docs/superpowers/specs/2026-09-05-stint-ribbon-design.md`

**Branch:** `shiny/stint-ribbon` (already created; the spec's three commits are on it)

## Global Constraints

- **One query per ribbon open.** The pooler round-trip floor is 238 ms and the budget is 500 ms; a second trip costs more than the entire rest of the feature. Never split lanes and margin into two queries.
- **Never put a `type_lineup` predicate in the Israeli query.** Filtering to `offense` loses 1.46% of floor time. No predicate is needed at all.
- **X-axis extent comes from nominal period structure** (600 s quarters, 300 s OT), never from `max(elapsed)`.
- **Drop zero-length segments** (`segment_seconds > 0`) before laying out lanes.
- **Merge adjacent runs per player, not per lineup hash.**
- **No new R package dependencies.** `app/R/global.R` loads shiny, DBI, dplyr, pool, RPostgres, DT, bslib, htmltools — that is the whole toolbox.
- `helpers.R` has **no side effects at source time**: no `library()`, no DB pool, no cache objects, no env reads. Impure infrastructure lives in `global.R`.
- **Never copy a helper implementation into `helper-server-mocks.R`** — the mocks `source()` the real `helpers.R` and stub only impure pieces.
- Parameterized SQL only (`$1`, `$2`); never `sprintf()`/`paste0()` for user values.
- 2-space indent, snake_case. Base apply (`lapply`/`vapply`/`Filter`), **not** purrr.
- **Set `IBPL_CACHE_UI=false`** in the environment while editing `app/www/app.css` or `app/www/app.js`, or an edit needs an app restart rather than a browser reload.
- **Launch with Run App / `runApp('app')`, never select-all + Ctrl+Enter** — the latter builds a broken BS3-style navbar and caches it for the life of the process. Health check: the served page carries 11 `class="nav-link"` occurrences.
- After any `DROP` touching the new views, **re-run `scripts/apply_db_security.R` with `CONFIRM_DB_SECURITY_APPLY=1`** — DROP wipes `app_readonly` grants.
- The EuroLeague read layer is enumerated in **two files that must stay in sync**: `sql/security/enable_readonly_rls.sql:91` and `sql/security/audit_app_access.sql:101`.

**Test command** (run from the repo root):

```bash
RSCRIPT="/c/Program Files/R/R-4.4.2/bin/Rscript.exe"
cd app && "$RSCRIPT" -e "testthat::test_dir('tests/testthat', filter='stint-ribbon')"
```

---

## File Structure

| File | Responsibility |
|---|---|
| `app/R/helpers.R` (modify) | Pure transforms + the SVG builder. Everything testable lives here. |
| `app/tests/testthat/test-stint-ribbon.R` (create) | All unit tests for the transforms and the builder. |
| `app/tests/testthat/test-stint-ribbon-readers.R` (create) | Reader tests against a stubbed `db_get_query`. |
| `sql/euroleague/ribbon_views.sql` (create) | The two EuroLeague read-layer views. |
| `sql/security/enable_readonly_rls.sql` (modify) | Register the two views in the curated grant list. |
| `sql/security/audit_app_access.sql` (modify) | Same two names, kept in sync. |
| `app/R/global.R` (modify) | `fetch_stint_ribbon()` — impure: DB + cache. |
| `app/www/app.css` (modify) | Ribbon styling and theme tokens. |
| `app/www/app.js` (modify) | Hover/tap handler that swaps `clip-path`. |
| `app/R/server_tab4.R` (modify) | Israeli link cell, click observer, modal. |
| `app/R/server_tab11_euro_gamelogs.R` (modify) | EuroLeague wiring, same helpers. |

---

### Task 1: Pure geometry and stint-merging transforms

**Files:**
- Modify: `app/R/helpers.R` (append at end)
- Test: `app/tests/testthat/test-stint-ribbon.R` (create)

**Interfaces:**
- Consumes: nothing.
- Produces:
  - `merge_adjacent_stints(lanes)` → data.frame with the same columns, contiguous runs collapsed. Input/output columns: `side` (chr, "own"/"opp"), `player_key` (chr), `player_label` (chr), `is_starter` (lgl), `start_elapsed` (num), `end_elapsed` (num).
  - `ribbon_period_bounds(n_periods, regulation = 4L, regulation_seconds = 600, ot_seconds = 300)` → numeric vector of cumulative period end times; the last element is the nominal game length.
  - `ribbon_lane_index(lanes)` → input plus integer `lane_index`, restarting at 1 per `side`.
  - `ribbon_geometry(lanes, total_seconds, width = 1000, lane_height = 14, lane_gap = 3)` → input plus numeric `x`, `w`, `y`, `h`.

- [ ] **Step 1: Write the failing tests**

Create `app/tests/testthat/test-stint-ribbon.R`:

```r
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

test_that("ribbon_geometry scales elapsed seconds into the viewBox", {
  lanes <- lane_row("own", "7", 0, 1200)
  lanes$lane_index <- 1L
  out <- ribbon_geometry(lanes, total_seconds = 2400, width = 1000)
  expect_identical(out$x, 0)
  expect_identical(out$w, 500)
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
```

- [ ] **Step 2: Run the tests to verify they fail**

```bash
cd app && "$RSCRIPT" -e "testthat::test_dir('tests/testthat', filter='stint-ribbon')"
```

Expected: FAIL, `could not find function "merge_adjacent_stints"`.

- [ ] **Step 3: Implement the transforms**

Append to `app/R/helpers.R`:

```r
# ---------------- Stint ribbon: pure transforms ----------------
# The ribbon draws one bar per continuous stretch a player spent on the floor.
# The readers hand these functions a row per (segment, player); everything
# below is geometry and has no idea which league or table it came from.

# Collapse consecutive segments in which the same player stayed on the floor.
# Merging is per PLAYER, not per lineup hash: a player who survives a
# substitution around them keeps one continuous bar rather than abutting
# rectangles with a visible seam.
merge_adjacent_stints <- function(lanes) {
  if (is.null(lanes) || !nrow(lanes)) return(lanes)

  lanes <- lanes[order(lanes$side, lanes$player_key, lanes$start_elapsed), , drop = FALSE]
  key <- paste(lanes$side, lanes$player_key, sep = "\r")
  prev_key <- c("", key[-length(key)])
  prev_end <- c(NA_real_, lanes$end_elapsed[-nrow(lanes)])

  # A new bar starts when the player changes, or when this interval does not
  # begin exactly where the previous one ended.
  new_run <- key != prev_key | is.na(prev_end) | lanes$start_elapsed > prev_end
  run_id <- cumsum(new_run)

  merged <- lapply(split(seq_len(nrow(lanes)), run_id), function(i) {
    row <- lanes[i[1], , drop = FALSE]
    row$end_elapsed <- max(lanes$end_elapsed[i])
    row
  })

  out <- do.call(rbind, merged)
  rownames(out) <- NULL
  out
}

# Cumulative period end times from the NOMINAL clock, never from observed
# data. Israeli games end ragged (2344, 2351, one at 961), so sizing the axis
# from max(elapsed) would draw different games at different scales and drift
# the period gridlines.
ribbon_period_bounds <- function(n_periods, regulation = 4L,
                                 regulation_seconds = 600, ot_seconds = 300) {
  n <- suppressWarnings(as.integer(n_periods))
  if (length(n) != 1 || is.na(n) || n < regulation) n <- as.integer(regulation)
  lengths <- c(rep(regulation_seconds, regulation),
               rep(ot_seconds, n - regulation))
  cumsum(lengths)
}

# Lane order within each side: starters first, then most floor time.
ribbon_lane_index <- function(lanes) {
  if (is.null(lanes) || !nrow(lanes)) return(lanes)

  order_df <- lanes %>%
    mutate(.dur = end_elapsed - start_elapsed) %>%
    group_by(side, player_key) %>%
    summarise(floor_time = sum(.dur), is_starter = any(is_starter), .groups = "drop") %>%
    arrange(side, desc(is_starter), desc(floor_time), player_key) %>%
    group_by(side) %>%
    mutate(lane_index = as.integer(row_number())) %>%
    ungroup() %>%
    select(side, player_key, lane_index)

  lanes %>% left_join(order_df, by = c("side", "player_key"))
}

# Map elapsed seconds to the 1000-unit viewBox and lane index to a y offset.
ribbon_geometry <- function(lanes, total_seconds, width = 1000,
                            lane_height = 14, lane_gap = 3) {
  if (is.null(lanes) || !nrow(lanes)) return(lanes)
  stopifnot(is.numeric(total_seconds), length(total_seconds) == 1, total_seconds > 0)

  scale <- width / total_seconds
  lanes$x <- lanes$start_elapsed * scale
  # A one-second stint would otherwise be a sub-pixel sliver that still
  # occupies a lane slot; give it a hairline so it is visible and hoverable.
  lanes$w <- pmax((lanes$end_elapsed - lanes$start_elapsed) * scale, 0.75)
  lanes$y <- (lanes$lane_index - 1L) * (lane_height + lane_gap)
  lanes$h <- lane_height
  lanes
}
```

- [ ] **Step 4: Run the tests to verify they pass**

```bash
cd app && "$RSCRIPT" -e "testthat::test_dir('tests/testthat', filter='stint-ribbon')"
```

Expected: PASS, no failures.

- [ ] **Step 5: Commit**

```bash
git add app/R/helpers.R app/tests/testthat/test-stint-ribbon.R
git commit -m "feat: stint ribbon geometry and per-player stint merging"
```

---

### Task 2: SVG builder

**Files:**
- Modify: `app/R/helpers.R` (append after Task 1's block)
- Test: `app/tests/testthat/test-stint-ribbon.R` (append)

**Interfaces:**
- Consumes: `ribbon_period_bounds()`, `ribbon_lane_index()`, `ribbon_geometry()` from Task 1.
- Produces:
  - `ribbon_margin_path(margin, total_seconds, width, top, height)` → SVG path `d` string, stepped.
  - `ribbon_clip_id(id_prefix, side, player_key)` → chr, the DOM id shared by the builder and `app.js`.
  - `build_stint_ribbon_svg(lanes, margin, meta, id_prefix = "ribbon")` → an `htmltools` tag, or `NULL` when `lanes` is empty. `meta` is a list with `game_label` (chr, used as the SVG `aria-label`) and `n_periods` (int). No other `meta` field is read.

- [ ] **Step 1: Write the failing tests**

Append to `app/tests/testthat/test-stint-ribbon.R`:

```r
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
  # H before V is what makes it a step: hold the old value, then jump.
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
  # Unmatched euro players key on the name, e.g. "BIRCH, KHEM". A space or
  # comma in a DOM id makes url(#...) fail silently and hover dies for exactly
  # those players.
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
```

- [ ] **Step 2: Run the tests to verify they fail**

```bash
cd app && "$RSCRIPT" -e "testthat::test_dir('tests/testthat', filter='stint-ribbon')"
```

Expected: FAIL, `could not find function "ribbon_margin_path"`.

- [ ] **Step 3: Implement the builder**

Append to `app/R/helpers.R`:

```r
# ---------------- Stint ribbon: SVG builder ----------------
# The app has no plotting library. This emits an htmltools tag tree directly,
# which keeps the output testable as structure rather than as pixels.

RIBBON_WIDTH <- 1000
RIBBON_LANE_HEIGHT <- 14
RIBBON_LANE_GAP <- 3
RIBBON_MARGIN_HEIGHT <- 90

# EuroLeague player keys fall back to the player NAME when the roster join
# misses ("BIRCH, KHEM"), and that key becomes a DOM id. Spaces and commas make
# url(#...) fail silently, so hover would die for exactly those players.
ribbon_clip_id <- function(id_prefix, side, player_key) {
  slug <- gsub("[^A-Za-z0-9_-]+", "-", as.character(player_key))
  slug <- gsub("(^-+)|(-+$)", "", slug)
  paste0(id_prefix, "-on-", side, "-", slug)
}

# A stepped path: the score holds its value until the next scoring event, then
# jumps. Drawing straight segments between events would imply the margin drifted
# continuously, which it did not.
ribbon_margin_path <- function(margin, total_seconds, width, top, height) {
  if (is.null(margin) || !nrow(margin)) return("")

  margin <- margin[order(margin$elapsed), , drop = FALSE]
  max_abs <- suppressWarnings(max(abs(margin$margin), na.rm = TRUE))
  if (!is.finite(max_abs) || max_abs <= 0) max_abs <- 1

  x <- margin$elapsed * (width / total_seconds)
  y <- top + height / 2 - (margin$margin / max_abs) * (height / 2)

  parts <- sprintf("M %.2f %.2f", x[1], y[1])
  if (length(x) > 1) {
    parts <- c(parts, sprintf("H %.2f V %.2f", x[-1], y[-1]))
  }
  paste(parts, collapse = " ")
}

build_stint_ribbon_svg <- function(lanes, margin, meta, id_prefix = "ribbon") {
  if (is.null(lanes) || !nrow(lanes)) return(NULL)

  bounds <- ribbon_period_bounds(meta$n_periods)
  total_seconds <- bounds[length(bounds)]

  lanes <- merge_adjacent_stints(lanes)
  lanes <- ribbon_lane_index(lanes)
  lanes <- ribbon_geometry(lanes, total_seconds, width = RIBBON_WIDTH,
                           lane_height = RIBBON_LANE_HEIGHT,
                           lane_gap = RIBBON_LANE_GAP)

  own <- lanes[lanes$side == "own", , drop = FALSE]
  opp <- lanes[lanes$side == "opp", , drop = FALSE]
  own_h <- max(own$y + own$h, 0)
  margin_top <- own_h + RIBBON_LANE_GAP * 2
  opp_top <- margin_top + RIBBON_MARGIN_HEIGHT + RIBBON_LANE_GAP * 2
  total_h <- opp_top + max(opp$y + opp$h, 0)

  # Own lanes sit above the curve, opponent lanes mirrored below it.
  lanes$abs_y <- ifelse(lanes$side == "own", lanes$y, opp_top + lanes$y)
  lanes$clip <- ribbon_clip_id(id_prefix, lanes$side, lanes$player_key)

  path_d <- ribbon_margin_path(margin, total_seconds, RIBBON_WIDTH,
                               margin_top, RIBBON_MARGIN_HEIGHT)

  # One clipPath per player: hovering swaps a single clip-path attribute on the
  # focus curve, so no geometry is computed in the browser.
  clip_keys <- unique(lanes$clip)
  clip_paths <- lapply(clip_keys, function(cid) {
    rows <- lanes[lanes$clip == cid, , drop = FALSE]
    tags$clipPath(
      id = cid,
      lapply(seq_len(nrow(rows)), function(i) {
        tags$rect(x = rows$x[i], y = margin_top,
                  width = rows$w[i], height = RIBBON_MARGIN_HEIGHT)
      })
    )
  })

  period_lines <- lapply(bounds[-length(bounds)], function(b) {
    bx <- b * (RIBBON_WIDTH / total_seconds)
    tags$line(class = "ibpl-ribbon-period",
              x1 = bx, x2 = bx, y1 = 0, y2 = total_h)
  })

  lane_rects <- lapply(seq_len(nrow(lanes)), function(i) {
    secs <- lanes$end_elapsed[i] - lanes$start_elapsed[i]
    tags$g(
      class = paste("ibpl-ribbon-lane", paste0("is-", lanes$side[i])),
      `data-clip` = lanes$clip[i],
      tabindex = "0",
      tags$title(sprintf("%s — %.0f:%02.0f on the floor",
                         lanes$player_label[i], secs %/% 60, secs %% 60)),
      tags$rect(x = lanes$x[i], y = lanes$abs_y[i],
                width = lanes$w[i], height = lanes$h[i], rx = 2)
    )
  })

  tags$svg(
    xmlns = "http://www.w3.org/2000/svg",
    viewBox = sprintf("0 0 %d %.0f", RIBBON_WIDTH, total_h),
    class = "ibpl-ribbon",
    role = "img",
    `aria-label` = meta$game_label,
    tags$defs(clip_paths),
    period_lines,
    tags$path(class = "ibpl-ribbon-margin-base", d = path_d),
    tags$path(class = "ibpl-ribbon-margin-focus", d = path_d),
    lane_rects
  )
}
```

- [ ] **Step 4: Run the tests to verify they pass**

```bash
cd app && "$RSCRIPT" -e "testthat::test_dir('tests/testthat', filter='stint-ribbon')"
```

Expected: PASS, no failures.

- [ ] **Step 5: Commit**

```bash
git add app/R/helpers.R app/tests/testthat/test-stint-ribbon.R
git commit -m "feat: stint ribbon SVG builder with per-player clip paths"
```

---

### Task 3: Ribbon styling and the hover handler

**Files:**
- Modify: `app/www/app.css` (append)
- Modify: `app/www/app.js` (append a new IIFE at end of file)
- Test: manual, plus the CSS/JS presence assertions below

**Interfaces:**
- Consumes: the class names and `data-clip` attribute emitted by `build_stint_ribbon_svg()` in Task 2 — `.ibpl-ribbon`, `.ibpl-ribbon-lane`, `.ibpl-ribbon-margin-base`, `.ibpl-ribbon-margin-focus`, `.ibpl-ribbon-period`.
- Produces: no R interface. The contract is the DOM one above.

**Before starting:** export `IBPL_CACHE_UI=false` in the shell that runs the app, or edits to these two files will not appear on a browser reload.

- [ ] **Step 1: Write the failing test**

Append to `app/tests/testthat/test-stint-ribbon.R`:

```r
test_that("app.css styles every class the SVG builder emits", {
  css <- paste(readLines(testthat::test_path("..", "..", "www", "app.css"),
                         warn = FALSE), collapse = "\n")
  for (cls in c("ibpl-ribbon", "ibpl-ribbon-lane", "ibpl-ribbon-margin-base",
                "ibpl-ribbon-margin-focus", "ibpl-ribbon-period")) {
    expect_match(css, cls, fixed = TRUE,
                 info = paste("missing ribbon style for", cls))
  }
})

test_that("app.js swaps clip-path rather than recomputing geometry", {
  js <- paste(readLines(testthat::test_path("..", "..", "www", "app.js"),
                        warn = FALSE), collapse = "\n")
  expect_match(js, "ibpl-ribbon-margin-focus", fixed = TRUE)
  expect_match(js, "clip-path", fixed = TRUE)
  # The hover target is the lane group, keyed by the builder's data-clip.
  expect_match(js, "ibpl-ribbon-lane", fixed = TRUE)
})
```

- [ ] **Step 2: Run to verify it fails**

```bash
cd app && "$RSCRIPT" -e "testthat::test_dir('tests/testthat', filter='stint-ribbon')"
```

Expected: FAIL, "missing ribbon style for ibpl-ribbon".

- [ ] **Step 3: Add the styles**

Append to `app/www/app.css`:

```css
/* ---------------- Stint ribbon ---------------- */
.ibpl-ribbon {
  width: 100%;
  height: auto;
  display: block;
}

.ibpl-ribbon-period {
  stroke: var(--ibpl-border, #3a3a3a);
  stroke-width: 1;
  opacity: 0.45;
}

.ibpl-ribbon-lane rect {
  fill: var(--ibpl-accent, #e8a435);
  opacity: 0.85;
  transition: opacity 120ms ease;
}

.ibpl-ribbon-lane.is-opp rect {
  fill: var(--ibpl-text-muted, #8a8a8a);
}

/* At rest the chart is a rotation map: every lane reads equally. */
.ibpl-ribbon.is-focused .ibpl-ribbon-lane rect { opacity: 0.3; }
.ibpl-ribbon.is-focused .ibpl-ribbon-lane.is-active rect { opacity: 1; }

.ibpl-ribbon-lane:focus { outline: none; }
.ibpl-ribbon-lane:focus rect {
  stroke: var(--ibpl-accent, #e8a435);
  stroke-width: 1.5;
}

.ibpl-ribbon-margin-base,
.ibpl-ribbon-margin-focus {
  fill: none;
  stroke-width: 1.75;
}

.ibpl-ribbon-margin-base {
  stroke: var(--ibpl-text-muted, #8a8a8a);
  opacity: 0.5;
}

/* Hidden until a lane is hovered, then revealed only where that player was on. */
.ibpl-ribbon-margin-focus {
  stroke: var(--ibpl-accent, #e8a435);
  opacity: 0;
}

.ibpl-ribbon.is-focused .ibpl-ribbon-margin-focus { opacity: 1; }
```

- [ ] **Step 4: Add the hover handler**

Append to `app/www/app.js`:

```js
// ---------------- Stint ribbon hover ----------------
// Hovering a lane reveals the margin curve only where that player was on the
// floor. The clip rectangles are rendered server-side, one clipPath per
// player, so this is a single attribute write -- no geometry in the browser.
(function() {
  function setFocus(svg, lane) {
    var focus = svg.querySelector(".ibpl-ribbon-margin-focus");
    if (!focus) return;

    var active = svg.querySelectorAll(".ibpl-ribbon-lane.is-active");
    for (var i = 0; i < active.length; i++) active[i].classList.remove("is-active");

    if (lane && lane.dataset.clip) {
      focus.setAttribute("clip-path", "url(#" + lane.dataset.clip + ")");
      lane.classList.add("is-active");
      svg.classList.add("is-focused");
    } else {
      focus.removeAttribute("clip-path");
      svg.classList.remove("is-focused");
    }
  }

  function laneFrom(target) {
    return target && target.closest ? target.closest(".ibpl-ribbon-lane") : null;
  }

  document.addEventListener("mouseover", function(e) {
    var lane = laneFrom(e.target);
    if (!lane) return;
    var svg = lane.closest(".ibpl-ribbon");
    if (svg) setFocus(svg, lane);
  });

  document.addEventListener("mouseout", function(e) {
    var lane = laneFrom(e.target);
    if (!lane) return;
    var svg = lane.closest(".ibpl-ribbon");
    // Ignore moves between the <g> and its own <rect>/<title> children.
    if (svg && !laneFrom(e.relatedTarget)) setFocus(svg, null);
  });

  // Keyboard parity: lanes are focusable, so focus drives the same split.
  document.addEventListener("focusin", function(e) {
    var lane = laneFrom(e.target);
    if (!lane) return;
    var svg = lane.closest(".ibpl-ribbon");
    if (svg) setFocus(svg, lane);
  });

  // Touch has no hover: tap toggles the same state.
  document.addEventListener("click", function(e) {
    var lane = laneFrom(e.target);
    if (!lane) return;
    var svg = lane.closest(".ibpl-ribbon");
    if (!svg) return;
    setFocus(svg, lane.classList.contains("is-active") ? null : lane);
  });
})();
```

- [ ] **Step 5: Run the tests to verify they pass**

```bash
cd app && "$RSCRIPT" -e "testthat::test_dir('tests/testthat', filter='stint-ribbon')"
```

Expected: PASS, no failures.

- [ ] **Step 6: Commit**

```bash
git add app/www/app.css app/www/app.js app/tests/testthat/test-stint-ribbon.R
git commit -m "feat: ribbon styling and clip-path hover handler"
```

---

### Task 4: EuroLeague read-layer views and grants

**Files:**
- Create: `sql/euroleague/ribbon_views.sql`
- Modify: `sql/security/enable_readonly_rls.sql:91` (the `euro_app_relations` array)
- Modify: `sql/security/audit_app_access.sql:101` (the `euro_app_relations` VALUES list)
- Test: `app/tests/testthat/test-stint-ribbon.R` (append)

**Interfaces:**
- Consumes: nothing from earlier tasks.
- Produces: `euroleague.ribbon_segments_v` with columns `game_id, team_id, segment_id, own_lineup, opp_lineup, own_starters, opp_starters, start_elapsed_seconds, end_elapsed_seconds`; and `euroleague.ribbon_margin_v` with `game_id, period, elapsed_seconds, points_a, points_b, home_team_id`. Task 5's EuroLeague reader depends on exactly these names.

**Why views rather than table grants:** `app_readonly` is denied on `euroleague.actions` (211 MB of raw play-by-play, 40 columns including provider ids and parser traces) and on `matchup_segments_actions`, by design — the euro schema uses a curated read layer, unlike the Israeli blanket grant. Exposing two narrow views keeps that boundary and puts the clock derivation next to its data.

- [ ] **Step 1: Write the failing test**

Append to `app/tests/testthat/test-stint-ribbon.R`:

```r
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
                    "euro_app_relations constant text\\[\\] := ARRAY\\[.*?\\];")
  audit <- extract("audit_app_access.sql",
                   "euro_app_relations\\(relation_name\\) AS \\(.*?\\),")
  expect_identical(grants, audit)
})
```

- [ ] **Step 2: Run to verify it fails**

```bash
cd app && "$RSCRIPT" -e "testthat::test_dir('tests/testthat', filter='stint-ribbon')"
```

Expected: FAIL, no match for "ribbon_segments_v".

- [ ] **Step 3: Create the views**

Create `sql/euroleague/ribbon_views.sql`:

```sql
-- Narrow read layer for the stint ribbon.
--
-- app_readonly is denied on euroleague.actions and
-- euroleague.matchup_segments_actions on purpose: the euro schema keeps raw
-- provider evidence closed and grants only an enumerated read layer. These two
-- views expose exactly what the ribbon needs and nothing else.
--
-- Editing either view means DROP + CREATE, which wipes the app_readonly grant.
-- Re-run scripts/apply_db_security.R with CONFIRM_DB_SECURITY_APPLY=1 after.

CREATE OR REPLACE VIEW euroleague.ribbon_segments_v AS
SELECT
  game_id,
  team_id,
  segment_id,
  own_lineup,
  opp_lineup,
  own_starters,
  opp_starters,
  start_elapsed_seconds,
  end_elapsed_seconds
FROM euroleague.matchup_segments_actions
-- 45% of rows are zero-length (two substitutions at the same clock). Dropping
-- them here is load-bearing: they would occupy lane slots while rendering as
-- invisible slivers.
WHERE segment_seconds > 0;

-- The provider records a per-period countdown (marker_time) plus a period
-- number; the ribbon needs elapsed seconds. Periods 1-4 run 10:00 and period
-- 5+ runs 05:00, verified against observed game lengths of exactly
-- 2400 / 2700 / 3000 / 3300 seconds.
CREATE OR REPLACE VIEW euroleague.ribbon_margin_v AS
WITH home AS (
  SELECT game_id, MIN(team_id) AS home_team_id
  FROM euroleague.actions
  WHERE is_home_team AND team_id IS NOT NULL
  GROUP BY game_id
)
SELECT
  a.game_id,
  a.period,
  (
    (CASE WHEN a.period <= 4 THEN (a.period - 1) * 600
          ELSE 2400 + (a.period - 5) * 300 END)
    + ((CASE WHEN a.period <= 4 THEN 600 ELSE 300 END)
       - (split_part(a.marker_time, ':', 1)::int * 60
          + split_part(a.marker_time, ':', 2)::int))
  )::numeric AS elapsed_seconds,
  a.points_a,
  a.points_b,
  h.home_team_id
FROM euroleague.actions a
LEFT JOIN home h ON h.game_id = a.game_id
WHERE a.marker_time IS NOT NULL
  AND (a.points_a IS NOT NULL OR a.points_b IS NOT NULL);
```

- [ ] **Step 4: Apply the views**

```bash
RSCRIPT="/c/Program Files/R/R-4.4.2/bin/Rscript.exe"
"$RSCRIPT" -e "readRenviron('etl/.Renviron'); library(DBI); library(RPostgres); con <- dbConnect(RPostgres::Postgres(), host=Sys.getenv('PG_HOST'), port=5432L, dbname=Sys.getenv('PG_DB'), user=Sys.getenv('PG_USER'), password=Sys.getenv('PG_PASS'), sslmode=Sys.getenv('PG_SSLMODE'), connect_timeout=15L); dbExecute(con, paste(readLines('sql/euroleague/ribbon_views.sql'), collapse='\n')); dbDisconnect(con)"
```

Note port **5432** (direct), not 6543 — DDL goes to the direct port per `CLAUDE.md`.

- [ ] **Step 5: Register both views in the two enumerations**

In `sql/security/enable_readonly_rls.sql`, add to the `euro_app_relations` array after `'sub_lineups_stats_mv'`:

```sql
    'sub_lineups_stats_mv',
    'ribbon_segments_v',
    'ribbon_margin_v'
  ];
```

In `sql/security/audit_app_access.sql`, add to the `euro_app_relations` VALUES list after `('sub_lineups_stats_mv')`:

```sql
    ('sub_lineups_stats_mv'),
    ('ribbon_segments_v'),
    ('ribbon_margin_v')
),
```

- [ ] **Step 6: Apply the grants**

```bash
CONFIRM_DB_SECURITY_APPLY=1 "$RSCRIPT" scripts/apply_db_security.R
```

- [ ] **Step 7: Verify the grant landed and the query stays in budget**

The `home` CTE aggregates over `euroleague.actions`; confirm the `game_id`
predicate still pushes down rather than scanning the whole table. Write to a
temp file (long `Rscript -e` segfaults on this box):

```bash
cat > /tmp/ribbon_verify.R <<'EOF'
readRenviron("app/.Renviron")
library(DBI); library(RPostgres)
con <- dbConnect(RPostgres::Postgres(), host=Sys.getenv("PG_HOST"),
  port=as.integer(Sys.getenv("PG_PORT")), dbname=Sys.getenv("PG_DB"),
  user=Sys.getenv("PG_USER"), password=Sys.getenv("PG_PASS"),
  sslmode=Sys.getenv("PG_SSLMODE"), connect_timeout=15L, bigint="numeric")
g <- dbGetQuery(con, "select game_id from euroleague.ribbon_segments_v limit 1")$game_id[1]
ms <- replicate(7, {
  t0 <- Sys.time()
  dbGetQuery(con, "select * from euroleague.ribbon_margin_v where game_id = $1",
             params = list(g))
  as.numeric(difftime(Sys.time(), t0, units = "secs")) * 1000
})
cat("app_readonly can read both views; margin median ms:", median(ms), "\n")
stopifnot(median(ms) < 500)
dbDisconnect(con)
EOF
"$RSCRIPT" /tmp/ribbon_verify.R
```

Expected: prints a median well under 500 ms and exits 0. If it exceeds budget, the `home` CTE is not pushing the predicate down — replace it with a lateral subquery keyed on `a.game_id` and re-measure.

- [ ] **Step 8: Run the tests**

```bash
cd app && "$RSCRIPT" -e "testthat::test_dir('tests/testthat', filter='stint-ribbon')"
```

Expected: PASS, no failures.

- [ ] **Step 9: Commit**

```bash
git add sql/euroleague/ribbon_views.sql sql/security/enable_readonly_rls.sql sql/security/audit_app_access.sql app/tests/testthat/test-stint-ribbon.R
git commit -m "feat: euroleague ribbon read-layer views and grants"
```

---

### Task 5: The two readers behind one canonical frame

**Files:**
- Modify: `app/R/global.R` (append after `cached_season_df`, around line 265)
- Test: `app/tests/testthat/test-stint-ribbon-readers.R` (create)

**Interfaces:**
- Consumes: `db_get_query()` and `cached_season_df()` from `global.R`; `merge_adjacent_stints()` from Task 1; the two views from Task 4.
- Produces: `fetch_stint_ribbon(pool, league, game_id, team_id, data_version = NULL)` → `list(lanes = <data.frame>, margin = <data.frame>, meta = <list>, health = <chr or NULL>)`.
  - `lanes` columns: `side`, `player_key`, `player_label`, `is_starter`, `start_elapsed`, `end_elapsed`.
  - `margin` columns: `elapsed`, `margin` (signed so positive means the clicked team leads).
  - `meta`: `n_periods` (int) only. The calling observer adds `game_label` before passing `meta` to the builder, because the reader has no access to team names without a second query.
  - Also produces `ribbon_normalise_lanes(raw, own_team_id)` (pure, in `helpers.R`) — maps a reader's raw rows onto the canonical frame.

- [ ] **Step 1: Write the failing tests**

Create `app/tests/testthat/test-stint-ribbon-readers.R`:

```r
# The readers are the only league-aware code in the feature. These tests stub
# the database and assert the canonical frame, so a schema difference between
# the two leagues can never reach the renderer.

test_that("ribbon_normalise_lanes labels the clicked team own and the other opp", {
  raw <- data.frame(
    team_id = c(7L, 7L, 10L),
    player_id = c(1L, 2L, 3L),
    player_label = c("A", "B", "C"),
    is_starter = c(TRUE, FALSE, TRUE),
    start_elapsed = c(0, 0, 0),
    end_elapsed = c(100, 100, 100),
    stringsAsFactors = FALSE
  )
  out <- ribbon_normalise_lanes(raw, own_team_id = 7L)
  expect_identical(out$side, c("own", "own", "opp"))
  expect_identical(out$player_key, c("1", "2", "3"))
})

test_that("ribbon_normalise_lanes returns the canonical columns and nothing else", {
  raw <- data.frame(team_id = 7L, player_id = 1L, player_label = "A",
                    is_starter = TRUE, start_elapsed = 0, end_elapsed = 10,
                    extra_junk = "drop me", stringsAsFactors = FALSE)
  out <- ribbon_normalise_lanes(raw, own_team_id = 7L)
  expect_identical(
    sort(names(out)),
    sort(c("side", "player_key", "player_label", "is_starter",
           "start_elapsed", "end_elapsed"))
  )
})

test_that("ribbon_health_message reports unmatched and odd-sized rosters", {
  expect_null(ribbon_health_message(0, 0))
  expect_match(ribbon_health_message(3, 0), "3")
  expect_match(ribbon_health_message(0, 2), "2")
  expect_match(ribbon_health_message(1, 1), "lineup")
})

test_that("ribbon_sign_margin flips the sign when the clicked team is away", {
  m <- data.frame(elapsed = c(0, 60), points_a = c(0, 10), points_b = c(0, 4))
  home <- ribbon_sign_margin(m, own_team_id = 5L, home_team_id = 5L)
  away <- ribbon_sign_margin(m, own_team_id = 9L, home_team_id = 5L)
  expect_identical(home$margin, c(0, 6))
  expect_identical(away$margin, c(0, -6))
})

test_that("the Israeli ribbon SQL carries no type_lineup predicate", {
  # Filtering to offense loses 1.46% of floor time -- the same defect as the
  # unattributed floor time fixed 2026-09-05. This is a source assertion so it
  # runs without a database and fails loudly if someone "optimises" the query.
  src <- paste(readLines(testthat::test_path("..", "..", "R", "global.R"),
                         warn = FALSE), collapse = "\n")
  sql <- regmatches(src, regexpr('RIBBON_SQL_ISRAEL <- "(.|\n)*?"\n', src, perl = TRUE))
  expect_true(nzchar(sql))
  expect_false(grepl("type_lineup\\s*=", sql))
  expect_false(grepl("type_lineup\\s+IS\\s+NOT\\s+NULL", sql, ignore.case = TRUE))
  expect_false(grepl("GROUP BY[^)]*type_lineup", sql, ignore.case = TRUE))
})

test_that("the ribbon segment count matches the type_lineup-absent grouping", {
  skip_on_cran()
  skip_if_not(nzchar(Sys.getenv("PG_HOST")), "no database configured")

  con <- DBI::dbConnect(RPostgres::Postgres(),
    host = Sys.getenv("PG_HOST"), port = as.integer(Sys.getenv("PG_PORT")),
    dbname = Sys.getenv("PG_DB"), user = Sys.getenv("PG_USER"),
    password = Sys.getenv("PG_PASS"), sslmode = Sys.getenv("PG_SSLMODE"),
    connect_timeout = 15L, bigint = "numeric")
  on.exit(DBI::dbDisconnect(con), add = TRUE)

  counts <- DBI::dbGetQuery(con, "
    SELECT
      (SELECT COUNT(*) FROM (
         SELECT team_id, segment_id, lineup_hash
         FROM basketball_test.df_pts_poss_lineups_longer_mv
         WHERE game_id = 115
         GROUP BY team_id, segment_id, lineup_hash
         HAVING MAX(segment_seconds) > 0) a)          AS all_perspectives,
      (SELECT COUNT(*) FROM (
         SELECT team_id, segment_id, lineup_hash
         FROM basketball_test.df_pts_poss_lineups_longer_mv
         WHERE game_id = 115 AND type_lineup = 'offense'
         GROUP BY team_id, segment_id, lineup_hash
         HAVING MAX(segment_seconds) > 0) b)          AS offense_only")

  # Offense-only must be strictly fewer. If these ever match, the NULL and
  # defense perspectives have stopped carrying segments of their own and this
  # guard has lost its meaning -- investigate before relaxing it.
  expect_gt(counts$all_perspectives[1], counts$offense_only[1])
})
```

- [ ] **Step 2: Run to verify it fails**

```bash
cd app && "$RSCRIPT" -e "testthat::test_dir('tests/testthat', filter='stint-ribbon')"
```

Expected: FAIL, `could not find function "ribbon_normalise_lanes"`.

- [ ] **Step 3: Add the pure normalisers to `helpers.R`**

Append to `app/R/helpers.R`:

```r
# ---------------- Stint ribbon: reader normalisers ----------------
# Pure so both league readers share one definition of the canonical frame.

RIBBON_LANE_COLS <- c("side", "player_key", "player_label", "is_starter",
                      "start_elapsed", "end_elapsed")

ribbon_normalise_lanes <- function(raw, own_team_id) {
  if (is.null(raw) || !nrow(raw)) {
    return(data.frame(side = character(0), player_key = character(0),
                      player_label = character(0), is_starter = logical(0),
                      start_elapsed = numeric(0), end_elapsed = numeric(0),
                      stringsAsFactors = FALSE))
  }
  data.frame(
    side = ifelse(as.integer(raw$team_id) == as.integer(own_team_id), "own", "opp"),
    player_key = as.character(raw$player_id),
    player_label = as.character(raw$player_label),
    is_starter = as.logical(raw$is_starter) %in% TRUE,
    start_elapsed = as.numeric(raw$start_elapsed),
    end_elapsed = as.numeric(raw$end_elapsed),
    stringsAsFactors = FALSE
  )
}

# Positive margin always means the clicked team is ahead.
ribbon_sign_margin <- function(m, own_team_id, home_team_id) {
  if (is.null(m) || !nrow(m)) {
    return(data.frame(elapsed = numeric(0), margin = numeric(0)))
  }
  diff <- as.numeric(m$points_a) - as.numeric(m$points_b)
  own_is_home <- !is.na(home_team_id) &&
    as.integer(own_team_id) == as.integer(home_team_id)
  data.frame(
    elapsed = as.numeric(m$elapsed),
    margin = if (own_is_home) diff else -diff
  )
}

# Israeli lineup hashes are not always five players: 4-, 6- and 7-player hashes
# exist, and some resolve to no roster row at all. Say so rather than drawing a
# lane set that is quietly wrong.
ribbon_health_message <- function(unmatched_hashes, odd_sized_hashes) {
  unmatched <- as.integer(unmatched_hashes %||% 0)
  odd <- as.integer(odd_sized_hashes %||% 0)
  if (is.na(unmatched)) unmatched <- 0L
  if (is.na(odd)) odd <- 0L
  if (unmatched == 0L && odd == 0L) return(NULL)

  parts <- character(0)
  if (unmatched > 0L) {
    parts <- c(parts, sprintf("%d lineup(s) have no roster record", unmatched))
  }
  if (odd > 0L) {
    parts <- c(parts, sprintf("%d lineup(s) do not hold exactly five players", odd))
  }
  paste0("Lineup data is incomplete for this game: ",
         paste(parts, collapse = "; "), ". Lanes may be missing or wrong.")
}
```

- [ ] **Step 4: Add the readers to `global.R`**

Append to `app/R/global.R` after `cached_season_df()`:

```r
# ---------------- Stint ribbon readers ----------------
# ONE query per ribbon open. The pooler round-trip floor is 238ms against a
# 500ms budget, and server-side execution is 0.5-21.6ms, so a second query
# costs more than everything else in the feature combined. Lanes and margin
# therefore come back as two jsonb columns of a single row.

RIBBON_SQL_ISRAEL <- "
WITH gy AS (
  SELECT game_year FROM basketball_test.final_schedule_mv
  WHERE game_id = $1 LIMIT 1
),
segs AS (
  -- type_lineup is deliberately absent from the grouping AND the filter.
  -- Filtering to 'offense' loses 1.46% of floor time; the NULL rows are
  -- substitutions and timeouts and cost 0.14%, nearly all zero-length.
  SELECT team_id, segment_id, lineup_hash,
         MIN(segment_start_elapsed_seconds) AS start_elapsed,
         MAX(segment_end_elapsed_seconds)   AS end_elapsed
  FROM basketball_test.df_pts_poss_lineups_longer_mv
  WHERE game_id = $1
  GROUP BY team_id, segment_id, lineup_hash
  HAVING MAX(segment_seconds) > 0
),
roster AS (
  SELECT lineup_hash, team_id, game_year, COUNT(*) AS n_players
  FROM basketball_test.lineups_lookup_on
  GROUP BY lineup_hash, team_id, game_year
),
lanes AS (
  SELECT s.team_id,
         s.start_elapsed,
         s.end_elapsed,
         l.player_id,
         COALESCE(NULLIF(TRIM(COALESCE(r.firstname, '') || ' ' ||
                              COALESCE(r.lastname, '')), ''),
                  'Player ' || l.player_id) AS player_label,
         COALESCE(r.starter, FALSE) AS is_starter
  FROM segs s
  JOIN basketball_test.lineups_lookup_on l
    ON l.lineup_hash = s.lineup_hash
   AND l.team_id     = s.team_id
   AND l.game_year   = (SELECT game_year FROM gy)
  LEFT JOIN basketball_test.full_rosters r
    ON r.game_id   = $1
   AND r.team_id   = s.team_id
   AND r.player_id = l.player_id
),
marg AS (
  SELECT DISTINCT event_elapsed_seconds AS elapsed,
         (own_team_score - opp_team_score) AS margin
  FROM basketball_test.df_pts_poss_lineups_longer_mv
  WHERE game_id = $1 AND team_id = $2
),
health AS (
  SELECT
    COUNT(*) FILTER (WHERE ro.lineup_hash IS NULL)                       AS unmatched_hashes,
    COUNT(*) FILTER (WHERE ro.n_players IS NOT NULL AND ro.n_players <> 5) AS odd_sized_hashes
  FROM segs s
  LEFT JOIN roster ro
    ON ro.lineup_hash = s.lineup_hash
   AND ro.team_id     = s.team_id
   AND ro.game_year   = (SELECT game_year FROM gy)
)
SELECT
  (SELECT jsonb_agg(to_jsonb(lanes)) FROM lanes)                AS lanes,
  (SELECT jsonb_agg(to_jsonb(marg) ORDER BY elapsed) FROM marg) AS margin,
  (SELECT MAX(quarter) FROM basketball_test.df_pts_poss_lineups_longer_mv
    WHERE game_id = $1)                                         AS n_periods,
  (SELECT unmatched_hashes FROM health)                         AS unmatched_hashes,
  (SELECT odd_sized_hashes FROM health)                         AS odd_sized_hashes
"

RIBBON_SQL_EURO <- "
WITH segs AS (
  SELECT team_id, segment_id, own_lineup,
         start_elapsed_seconds AS start_elapsed,
         end_elapsed_seconds   AS end_elapsed
  FROM euroleague.ribbon_segments_v
  WHERE game_id = $1
),
lanes AS (
  SELECT s.team_id,
         s.start_elapsed,
         s.end_elapsed,
         COALESCE(r.player_id, 0)          AS player_id,
         p.player_name                     AS player_label,
         COALESCE(r.is_starter, FALSE)     AS is_starter
  FROM segs s
  CROSS JOIN LATERAL unnest(s.own_lineup) AS p(player_name)
  LEFT JOIN euroleague.full_rosters r
    ON r.game_id            = $1
   AND r.team_id            = s.team_id
   AND r.source_player_name = p.player_name
),
marg AS (
  SELECT DISTINCT elapsed_seconds AS elapsed, points_a, points_b, home_team_id
  FROM euroleague.ribbon_margin_v
  WHERE game_id = $1
)
SELECT
  (SELECT jsonb_agg(to_jsonb(lanes)) FROM lanes)                AS lanes,
  (SELECT jsonb_agg(to_jsonb(marg) ORDER BY elapsed) FROM marg) AS margin,
  (SELECT MAX(period) FROM euroleague.ribbon_margin_v WHERE game_id = $1) AS n_periods,
  0 AS unmatched_hashes,
  0 AS odd_sized_hashes
"

# EuroLeague lineups are player names, so player_id is 0 whenever the roster
# join misses. Key lanes on the label in that case, or two unmatched players
# would share one lane.
ribbon_euro_player_key <- function(lanes_df) {
  key <- as.character(lanes_df$player_id)
  key[is.na(key) | key == "0"] <- as.character(lanes_df$player_label[is.na(key) | key == "0"])
  key
}

fetch_stint_ribbon <- function(pool, league, game_id, team_id, data_version = NULL) {
  league <- match.arg(league, c("israel", "euroleague"))
  game_id <- as.integer(game_id)
  team_id <- as.integer(team_id)

  cached_season_df(list("stint_ribbon", league, game_id, team_id, data_version), function() {
    sql <- if (identical(league, "israel")) RIBBON_SQL_ISRAEL else RIBBON_SQL_EURO
    params <- if (identical(league, "israel")) list(game_id, team_id) else list(game_id)

    row <- db_get_query(pool, sql, params = params)
    if (is.null(row) || !nrow(row)) return(NULL)

    lanes_raw <- if (is.na(row$lanes[1])) NULL else
      jsonlite::fromJSON(row$lanes[1], simplifyDataFrame = TRUE)
    marg_raw <- if (is.na(row$margin[1])) NULL else
      jsonlite::fromJSON(row$margin[1], simplifyDataFrame = TRUE)

    if (is.null(lanes_raw) || !NROW(lanes_raw)) return(NULL)

    if (identical(league, "euroleague")) {
      lanes_raw$player_id <- ribbon_euro_player_key(lanes_raw)
      margin <- ribbon_sign_margin(marg_raw, team_id,
                                   if (NROW(marg_raw)) marg_raw$home_team_id[1] else NA)
    } else {
      margin <- data.frame(
        elapsed = as.numeric(marg_raw$elapsed %||% numeric(0)),
        margin = as.numeric(marg_raw$margin %||% numeric(0))
      )
    }

    lanes <- ribbon_normalise_lanes(lanes_raw, team_id)

    list(
      lanes = lanes,
      margin = margin[order(margin$elapsed), , drop = FALSE],
      meta = list(n_periods = as.integer(row$n_periods[1] %||% 4L)),
      health = ribbon_health_message(row$unmatched_hashes[1], row$odd_sized_hashes[1])
    )
  })
}
```

Note: `jsonlite` is already a transitive dependency of shiny and is loaded in the session; it is referenced with `::` so no new `library()` call is added.

- [ ] **Step 5: Run the tests to verify they pass**

```bash
cd app && "$RSCRIPT" -e "testthat::test_dir('tests/testthat', filter='stint-ribbon')"
```

Expected: PASS, no failures.

- [ ] **Step 6: Verify both readers against the live database and the budget**

```bash
cat > /tmp/ribbon_readers.R <<'EOF'
setwd("app")
readRenviron(".Renviron")
library(DBI); library(RPostgres); library(dplyr); library(htmltools)
source("R/helpers.R")
pool <- dbConnect(RPostgres::Postgres(), host=Sys.getenv("PG_HOST"),
  port=as.integer(Sys.getenv("PG_PORT")), dbname=Sys.getenv("PG_DB"),
  user=Sys.getenv("PG_USER"), password=Sys.getenv("PG_PASS"),
  sslmode=Sys.getenv("PG_SSLMODE"), connect_timeout=15L, bigint="numeric")
src <- readLines("R/global.R")
eval(parse(text = paste(src[grep("^RIBBON_SQL_ISRAEL", src)[1]:length(src)], collapse="\n")))
db_get_query <- function(conn, statement, params = NULL)
  if (is.null(params)) DBI::dbGetQuery(conn, statement) else DBI::dbGetQuery(conn, statement, params = params)
cached_season_df <- function(key_parts, fn) fn()

for (arm in list(list("israel", 115L, 7L), list("euroleague", 25L, 25L))) {
  ms <- replicate(7, {
    t0 <- Sys.time()
    r <- fetch_stint_ribbon(pool, arm[[1]], arm[[2]], arm[[3]])
    as.numeric(difftime(Sys.time(), t0, units="secs")) * 1000
  })
  r <- fetch_stint_ribbon(pool, arm[[1]], arm[[2]], arm[[3]])
  cat(arm[[1]], "lanes:", nrow(r$lanes), "margin:", nrow(r$margin),
      "sides:", paste(sort(unique(r$lanes$side)), collapse="/"),
      "median ms:", round(median(ms), 1), "\n")
  stopifnot(median(ms) < 500, nrow(r$lanes) > 0,
            identical(sort(unique(r$lanes$side)), c("opp", "own")))
}
dbDisconnect(pool)
EOF
"$RSCRIPT" /tmp/ribbon_readers.R
```

Expected: both leagues print lanes > 0, `sides: opp/own`, and a median under 500 ms.

- [ ] **Step 7: Commit**

```bash
git add app/R/helpers.R app/R/global.R app/tests/testthat/test-stint-ribbon-readers.R
git commit -m "feat: single-round-trip stint ribbon readers for both leagues"
```

---

### Task 6: Wire the ribbon into Tab 4 (Israeli game logs)

**Files:**
- Modify: `app/R/server_tab4.R` (the Summary `DT::datatable` block around line 640-690, and a new observer near the other `observeEvent` calls around line 300)
- Modify: `app/www/app.js` (add `window.handleRibbonLinkClick`, next to `window.handleLineupLinkClick` around line 104)
- Test: `app/tests/testthat/test-stint-ribbon.R` (append)

**Interfaces:**
- Consumes: `fetch_stint_ribbon()` from Task 5, `build_stint_ribbon_svg()` from Task 2, the CSS/JS contract from Task 3.
- Produces: `input$gl_ribbon_click` carrying `list(game_id, team_id, ts)`; `ribbon_link_cell(game_id, team_id, label)` in `helpers.R`.

- [ ] **Step 1: Write the failing test**

Append to `app/tests/testthat/test-stint-ribbon.R`:

```r
test_that("ribbon_link_cell carries both ids the observer needs", {
  html <- ribbon_link_cell(115L, 7L, "12 Mar")
  expect_match(html, 'data-game-id="115"')
  expect_match(html, 'data-team-id="7"')
  expect_match(html, "12 Mar", fixed = TRUE)
  expect_match(html, "ribbon-link", fixed = TRUE)
})

test_that("ribbon_link_cell escapes its label", {
  html <- ribbon_link_cell(1L, 1L, "<script>alert(1)</script>")
  expect_false(grepl("<script>", html, fixed = TRUE))
})

test_that("app.js exposes the ribbon click handler", {
  js <- paste(readLines(testthat::test_path("..", "..", "www", "app.js"),
                        warn = FALSE), collapse = "\n")
  expect_match(js, "handleRibbonLinkClick", fixed = TRUE)
  expect_match(js, "gl_ribbon_click", fixed = TRUE)
})
```

- [ ] **Step 2: Run to verify it fails**

```bash
cd app && "$RSCRIPT" -e "testthat::test_dir('tests/testthat', filter='stint-ribbon')"
```

Expected: FAIL, `could not find function "ribbon_link_cell"`.

- [ ] **Step 3: Add the link cell helper**

Append to `app/R/helpers.R`:

```r
# A game-log cell that opens the stint ribbon. Both ids travel on the anchor so
# the click handler needs no table lookup.
ribbon_link_cell <- function(game_id, team_id, label) {
  sprintf(
    '<a href="#" class="ribbon-link" data-game-id="%d" data-team-id="%d" onclick="window.handleRibbonLinkClick(this); return false;">%s</a>',
    as.integer(game_id), as.integer(team_id), htmltools::htmlEscape(label)
  )
}
```

- [ ] **Step 4: Add the JS handler**

In `app/www/app.js`, immediately after the `window.handleLineupLinkClick` definition (around line 112), add:

```js
  window.handleRibbonLinkClick = function(linkEl) {
    if (!linkEl || !window.Shiny || typeof window.Shiny.setInputValue !== "function") return;
    var gameId = parseInt(linkEl.dataset.gameId, 10);
    var teamId = parseInt(linkEl.dataset.teamId, 10);
    if (Number.isNaN(gameId) || Number.isNaN(teamId)) return;
    window.Shiny.setInputValue("gl_ribbon_click", {
      game_id: gameId,
      team_id: teamId,
      ts: Date.now()
    }, { priority: "event" });
  };
```

- [ ] **Step 5: Render the link in the Tab 4 Summary table**

In `app/R/server_tab4.R`, in the Summary branch just before `sketch <- gamelog_summary_header(has_shots = has_shots)`, replace the displayed date with a ribbon link. Add:

```r
      # The date cell opens the stint ribbon for that team-game.
      if (all(c("game_id", "team_id", "game_date") %in% names(disp))) {
        disp$game_date <- mapply(
          ribbon_link_cell,
          disp$game_id, disp$team_id, as.character(disp$game_date),
          USE.NAMES = FALSE
        )
      }
```

Then change the `escape` argument of the Summary `DT::datatable(...)` call from
`escape = dt_escape_except(disp)` to:

```r
                          escape = dt_escape_except(disp, "game_date"),
```

- [ ] **Step 6: Add the click observer and modal**

In `app/R/server_tab4.R`, alongside the other observers (after the `observeEvent(input$gl_view_mode, ...)` block around line 327), add:

```r
  observeEvent(input$gl_ribbon_click, {
    click <- input$gl_ribbon_click
    req(click$game_id, click$team_id)

    ribbon <- fetch_stint_ribbon(
      pg_pool, "israel", click$game_id, click$team_id,
      data_version = shared_data_version(shared)
    )

    if (is.null(ribbon) || !nrow(ribbon$lanes)) {
      showModal(modalDialog(title = "No lineup data",
                            "This game has no segment data to draw.",
                            easyClose = TRUE))
      return()
    }

    meta <- ribbon$meta
    meta$game_label <- sprintf("Game %s", click$game_id)

    output$gl_ribbon_svg <- renderUI({
      tagList(
        if (!is.null(ribbon$health)) {
          div(class = "alert alert-warning py-2 px-3 mb-2", ribbon$health)
        },
        build_stint_ribbon_svg(ribbon$lanes, ribbon$margin, meta,
                               id_prefix = paste0("gl", click$game_id))
      )
    })

    showModal(modalDialog(
      title = meta$game_label,
      uiOutput("gl_ribbon_svg"),
      size = "xl",
      easyClose = TRUE
    ))
  })
```

- [ ] **Step 7: Run the tests**

```bash
cd app && "$RSCRIPT" -e "testthat::test_dir('tests/testthat', filter='stint-ribbon')"
```

Expected: PASS, no failures.

- [ ] **Step 8: Verify in the running app**

```bash
IBPL_CACHE_UI=false "$RSCRIPT" -e "shiny::runApp('app')"
```

Launch with `runApp()`, never select-all + Ctrl+Enter. In the browser: open **Game Logs**, click a date cell, confirm the modal shows lanes above and below a margin curve, and that hovering a lane dims the other lanes while the amber curve appears only over that player's stints. Check the console is free of errors.

- [ ] **Step 9: Commit**

```bash
git add app/R/helpers.R app/R/server_tab4.R app/www/app.js app/tests/testthat/test-stint-ribbon.R
git commit -m "feat: open the stint ribbon from Israeli game logs"
```

---

### Task 7: Wire the ribbon into Tab 11 (EuroLeague game logs)

**Files:**
- Modify: `app/R/server_tab11_euro_gamelogs.R`
- Modify: `app/www/app.js` (generalise the handler's input id)
- Test: `app/tests/testthat/test-stint-ribbon.R` (append)

**Interfaces:**
- Consumes: everything from Tasks 1-6.
- Produces: `input$euro_gl_ribbon_click`, same payload shape as `gl_ribbon_click`.

**Note:** per `CLAUDE.md`, do not write a parallel `euro_` implementation. The only difference is the input id, the league argument, and the tab file — every helper is reused unchanged.

- [ ] **Step 1: Write the failing test**

Append to `app/tests/testthat/test-stint-ribbon.R`:

```r
test_that("the ribbon link cell takes the input id as an argument, not a league", {
  # One handler serves both tabs; the league lives in the argument, not the name.
  israeli <- ribbon_link_cell(1L, 2L, "x", input_id = "gl_ribbon_click")
  euro <- ribbon_link_cell(1L, 2L, "x", input_id = "euro_gl_ribbon_click")
  expect_match(israeli, "gl_ribbon_click", fixed = TRUE)
  expect_match(euro, "euro_gl_ribbon_click", fixed = TRUE)
})

test_that("both game-log tabs wire a ribbon click observer", {
  il <- paste(readLines(testthat::test_path("..", "..", "R", "server_tab4.R"),
                        warn = FALSE), collapse = "\n")
  eu <- paste(readLines(testthat::test_path("..", "..", "R", "server_tab11_euro_gamelogs.R"),
                        warn = FALSE), collapse = "\n")
  expect_match(il, "fetch_stint_ribbon", fixed = TRUE)
  expect_match(eu, "fetch_stint_ribbon", fixed = TRUE)
  # The EuroLeague tab must call the shared builder, not a euro_ clone.
  expect_match(eu, "build_stint_ribbon_svg", fixed = TRUE)
  expect_false(grepl("euro_build_stint_ribbon", eu, fixed = TRUE))
})
```

- [ ] **Step 2: Run to verify it fails**

```bash
cd app && "$RSCRIPT" -e "testthat::test_dir('tests/testthat', filter='stint-ribbon')"
```

Expected: FAIL, `unused argument (input_id = ...)`.

- [ ] **Step 3: Parameterise the link cell and the JS handler**

In `app/R/helpers.R`, replace `ribbon_link_cell` with:

```r
ribbon_link_cell <- function(game_id, team_id, label, input_id = "gl_ribbon_click") {
  sprintf(
    '<a href="#" class="ribbon-link" data-game-id="%d" data-team-id="%d" data-input-id="%s" onclick="window.handleRibbonLinkClick(this); return false;">%s</a>',
    as.integer(game_id), as.integer(team_id),
    htmltools::htmlEscape(input_id), htmltools::htmlEscape(label)
  )
}
```

In `app/www/app.js`, change the handler body's `setInputValue` target:

```js
    window.Shiny.setInputValue(linkEl.dataset.inputId || "gl_ribbon_click", {
      game_id: gameId,
      team_id: teamId,
      ts: Date.now()
    }, { priority: "event" });
```

- [ ] **Step 4: Wire Tab 11**

In `app/R/server_tab11_euro_gamelogs.R`, render the link in the game-log table exactly as Task 6 did, passing the EuroLeague input id:

```r
      if (all(c("game_id", "team_id", "game_date") %in% names(disp))) {
        disp$game_date <- mapply(
          ribbon_link_cell,
          disp$game_id, disp$team_id, as.character(disp$game_date),
          MoreArgs = list(input_id = "euro_gl_ribbon_click"),
          USE.NAMES = FALSE
        )
      }
```

Set that table's `escape` argument to `dt_escape_except(disp, "game_date")`, then add the observer:

```r
  observeEvent(input$euro_gl_ribbon_click, {
    click <- input$euro_gl_ribbon_click
    req(click$game_id, click$team_id)

    ribbon <- fetch_stint_ribbon(
      pg_pool, "euroleague", click$game_id, click$team_id,
      data_version = shared_data_version(shared)
    )

    if (is.null(ribbon) || !nrow(ribbon$lanes)) {
      showModal(modalDialog(title = "No lineup data",
                            "This game has no segment data to draw.",
                            easyClose = TRUE))
      return()
    }

    meta <- ribbon$meta
    meta$game_label <- sprintf("Game %s", click$game_id)

    output$euro_gl_ribbon_svg <- renderUI({
      tagList(
        if (!is.null(ribbon$health)) {
          div(class = "alert alert-warning py-2 px-3 mb-2", ribbon$health)
        },
        build_stint_ribbon_svg(ribbon$lanes, ribbon$margin, meta,
                               id_prefix = paste0("eugl", click$game_id))
      )
    })

    showModal(modalDialog(
      title = meta$game_label,
      uiOutput("euro_gl_ribbon_svg"),
      size = "xl",
      easyClose = TRUE
    ))
  })
```

- [ ] **Step 5: Update Task 6's call site**

`server_tab4.R` still calls `ribbon_link_cell` without `input_id`; the default keeps it working. Confirm no change is needed by re-running the Israeli test.

- [ ] **Step 6: Run the full suite**

```bash
cd app && "$RSCRIPT" -e "testthat::test_dir('tests/testthat', reporter = 'summary')"
```

Expected: no new failures versus the pre-change baseline. Record the baseline first if you have not: `git stash && "$RSCRIPT" -e "testthat::test_dir('tests/testthat', reporter='summary')" && git stash pop`.

- [ ] **Step 7: Verify both tabs in the running app**

```bash
IBPL_CACHE_UI=false "$RSCRIPT" -e "shiny::runApp('app')"
```

Open **Game Logs** and the **EuroLeague** game-log tab, click a date in each, and confirm both ribbons render and hover behaves identically. Navbar health check: the served page carries 11 `class="nav-link"` occurrences.

- [ ] **Step 8: Commit**

```bash
git add app/R/helpers.R app/R/server_tab11_euro_gamelogs.R app/www/app.js app/tests/testthat/test-stint-ribbon.R
git commit -m "feat: open the stint ribbon from EuroLeague game logs"
```

---

## Verification Checklist

Before considering the feature done:

- [ ] `cd app && "$RSCRIPT" -e "testthat::test_dir('tests/testthat', reporter='summary')"` shows no new failures against the pre-change baseline.
- [ ] Both readers return `sides: opp/own` and a median under 500 ms (Task 5, Step 6).
- [ ] `CONFIRM_DB_SECURITY_APPLY=1 "$RSCRIPT" scripts/apply_db_security.R` has been run after the views were created, and the audit query returns zero rows.
- [ ] Neither game-log query contains a `type_lineup` predicate.
- [ ] An overtime game draws a wider axis than a regulation game (pick one from the EuroLeague set with max elapsed 2700).
- [ ] A game with incomplete Israeli lineup data shows the health warning rather than silently wrong lanes.
- [ ] The app was launched with `runApp()`, and the served page carries 11 `class="nav-link"` occurrences.
