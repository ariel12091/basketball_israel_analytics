# Stint Ribbon — Richer Stint Data Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add per-stint +/-, per-player game totals, points for/against, and on-chart lineup context to the stint ribbon, without adding a second database round trip.

**Architecture:** Everything is derived in R from data the single existing query already returns, plus exactly one new SQL column per league (`own_team_score`). Per-stint numbers are printed on the bar faces; per-player totals occupy two new gutter columns; points for/against and the lineup live in a hover-driven detail strip plus an on-chart band that lights overlapping bars. All computation is pure functions in `app/R/helpers.R`, tested without a database.

**Tech Stack:** R 4.4.2, Shiny, htmltools inline SVG, testthat, PostgreSQL (Supabase), vanilla JS/CSS in `app/www/`.

**Spec:** `docs/superpowers/specs/2026-09-06-stint-ribbon-richer-data-design.md`

**Branch:** `shiny/stint-ribbon` (the base ribbon feature is complete and unmerged on this branch).

## Review remarks — all three accepted and applied

- **Accessibility:** The detail strip is `aria-hidden`, but the proposed lane
  `aria-label` omitted the teammate (`with`) context. Include the overlap text
  in the accessible label, or provide an equivalent keyboard/screen-reader path.
  → **Applied in Task 9.** The overlap computation moved above the label so the
  label can carry it, and a test now asserts a lane's `aria-label` names its
  teammates. The remark understated it: the old comment claimed the label
  "carries what the strip shows" while omitting them, so the code was
  self-contradictory. With the band and the lit overlaps purely visual and the
  strip `aria-hidden`, that label is the *only* path to the lineup for a
  screen-reader user.
- **Gutter hover:** `setDetail()` must ignore gutter labels, which have no
  `data-start`/`data-player`; otherwise hovering a name can render
  `undefined · undefined · undefined` in the detail strip. Apply the same guard
  used for `setBand()` and `setOverlaps()`.
  → **Applied in Task 10.** The guard is now a named `isStint()` predicate that
  all three functions call, rather than prose telling the implementer to add it
  to two of them — which is how `setDetail()` got missed in the first place. The
  browser checklist gained an explicit "must never show `undefined`" step.
- **League coverage:** The reconciliation tests in Task 11 queried only the
  Israeli reader, despite the plan's "both leagues or neither" constraint. Add
  equivalent EuroLeague coverage after migration 054.
  → **Applied in Task 11.** All three reconciliation tests now loop over a
  `RIBBON_LEAGUES` table, and the SQL-constant loader is parameterised over the
  league. Task 11 already runs after Task 8, and its step now states that
  migration 054 must be applied first — a missing `own_team_score` is the
  correct failure, not a reason to drop the EuroLeague case.

## Global Constraints

- **One round trip per ribbon open.** Pooler latency is 238 ms against a 500 ms budget. No task may add a second query.
- **Both leagues or neither.** Israeli Tab 4 and EuroLeague Tab 11 share `build_stint_ribbon_svg()`.
- **Never key anything on a player NAME.** Use `player_key` (the player id as character). Both leagues carry same-name / different-id players on one team.
- **No plotting library.** Server-built inline SVG only.
- **Base apply, not purrr.** `lapply` / `vapply` / `Filter`. No `map()`.
- **2-space indent, snake_case.**
- **`app/R/helpers.R` has mixed line endings.** Whole file: 2,670 CRLF + 867 LF. **The entire ribbon section (lines 3056–3537) is pure LF, zero CRLF** — verified 2026-09-06. Every edit in this plan is inside that region or appended at EOF. Use the byte-safe procedure in "Editing helpers.R" below and verify the CR count after every edit.
- **Never sum score increments.** The PBP credits and later rescinds baskets. Use net differences across a window.
- **Test command** (verified working, 213 pass / 0 fail at baseline):
  ```bash
  RSCRIPT="/c/Program Files/R/R-4.4.2/bin/Rscript.exe"
  "$RSCRIPT" -e "setwd('app'); testthat::test_file('tests/testthat/test-stint-ribbon.R')"
  ```

## Editing `helpers.R` safely

The Edit tool and `sed -i` both rewrite the whole file and normalise its line endings — a single-token change once produced a 524-line diffstat. Two safe procedures:

**Appending new functions (preferred for new code):**
```bash
cat > /tmp/new_fn.R <<'EOF'
<the new code, LF line endings>
EOF
cat /tmp/new_fn.R >> app/R/helpers.R
```

**Replacing an exact span inside the LF-only ribbon region:**
```bash
"$RSCRIPT" -e '
p <- "app/R/helpers.R"
b <- readBin(p, "raw", file.size(p))
s <- rawToChar(b); Encoding(s) <- "UTF-8"
old <- "EXACT OLD TEXT"
new <- "EXACT NEW TEXT"
stopifnot(length(gregexpr(old, s, fixed = TRUE)[[1]]) == 1)
writeBin(charToRaw(sub(old, new, s, fixed = TRUE)), p)
'
```

**Verification after EVERY helpers.R edit — both must hold:**
```bash
test "$(tr -cd '\r' < app/R/helpers.R | wc -c)" -eq 2670 && echo "CR OK"
git diff --stat app/R/helpers.R
```
If the CR count moved or the diffstat shows hundreds of changed lines, `git checkout app/R/helpers.R` and redo with the byte procedure.

---

### Task 1: `ribbon_score_as_of()`

The step-series lookup every later computation is built on: the value of a running-score series at an arbitrary time.

**Files:**
- Modify: `app/R/helpers.R` (append at EOF)
- Test: `app/tests/testthat/test-stint-ribbon.R` (append at EOF)

**Interfaces:**
- Consumes: nothing.
- Produces: `ribbon_score_as_of(series, t)` where `series` is a data frame with numeric columns `elapsed`, `order_key`, `value`, and `t` is a numeric vector. Returns a numeric vector the same length as `t`.

- [ ] **Step 1: Write the failing tests**

Append to `app/tests/testthat/test-stint-ribbon.R`:

```r
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
```

- [ ] **Step 2: Run the tests to verify they fail**

```bash
RSCRIPT="/c/Program Files/R/R-4.4.2/bin/Rscript.exe"
"$RSCRIPT" -e "setwd('app'); testthat::test_file('tests/testthat/test-stint-ribbon.R')"
```
Expected: FAIL with `could not find function "ribbon_score_as_of"`.

- [ ] **Step 3: Append the implementation**

```bash
cat > /tmp/ribbon_as_of.R <<'EOF'


# ---------------- Stint ribbon: per-stint numbers ----------------
# Value of a running-score step series at time t: the last row with
# elapsed <= t, and 0 before the first row. Vectorised over t.
#
# `series` MUST be the reader's RAW step frame, never
# ribbon_complete_margin()'s output. Completion collapses each second to one
# row and pads both ends to close the drawn path; an as-of lookup must read
# the recorded events, not the padding.
#
# Ties on `elapsed` are resolved by `order_key` and the LAST row wins, which
# is what findInterval() returns for a non-decreasing vector with duplicates.
ribbon_score_as_of <- function(series, t) {
  t <- as.numeric(t)
  if (is.null(series) || !NROW(series)) return(rep(0, length(t)))

  ord <- order(as.numeric(series$elapsed), as.numeric(series$order_key))
  el <- as.numeric(series$elapsed)[ord]
  val <- as.numeric(series$value)[ord]

  i <- findInterval(t, el)
  out <- val[pmax(i, 1L)]
  out[i < 1L] <- 0
  as.numeric(out)
}
EOF
cat /tmp/ribbon_as_of.R >> app/R/helpers.R
```

- [ ] **Step 4: Verify the file was not rewritten**

```bash
test "$(tr -cd '\r' < app/R/helpers.R | wc -c)" -eq 2670 && echo "CR OK"
git diff --stat app/R/helpers.R
```
Expected: `CR OK`, and the diffstat shows insertions only (no deletions).

- [ ] **Step 5: Run the tests to verify they pass**

```bash
"$RSCRIPT" -e "setwd('app'); testthat::test_file('tests/testthat/test-stint-ribbon.R')"
```
Expected: PASS, 0 failures.

- [ ] **Step 6: Commit**

```bash
git add app/R/helpers.R app/tests/testthat/test-stint-ribbon.R
git commit -m "shiny: add ribbon_score_as_of(), the ribbon's step-series lookup

Reads the value of a running-score series at an arbitrary time, taking the
last row at a tied second. Every per-stint number in the richer-data spec is
built on this.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01NS6bqx4bZQ7H8gaPRKBPbr"
```

---

### Task 2: `ribbon_stint_points()`

Per-stint points for, points against, and +/-, as net differences across the stint window.

**Files:**
- Modify: `app/R/helpers.R` (append at EOF)
- Test: `app/tests/testthat/test-stint-ribbon.R` (append at EOF)

**Interfaces:**
- Consumes: `ribbon_score_as_of(series, t)` from Task 1.
- Produces: `ribbon_stint_points(stints, steps)`. `stints` is a lanes data frame with `start_elapsed` / `end_elapsed`. `steps` is a data frame with `elapsed`, `order_key`, `margin`, and optionally `own`. Returns `stints` with three added numeric columns: `pf`, `pa`, `pm`. When `steps` has no `own` column, `pf` and `pa` are `NA_real_` and `pm` is still computed.

- [ ] **Step 1: Write the failing tests**

Append to `app/tests/testthat/test-stint-ribbon.R`:

```r
# ---------------- ribbon_stint_points ----------------

pts_steps <- function(elapsed, own, opp) {
  data.frame(elapsed = elapsed, order_key = seq_along(elapsed),
             own = own, margin = own - opp, stringsAsFactors = FALSE)
}

pts_stint <- function(start_elapsed, end_elapsed) {
  data.frame(side = "own", player_key = "1", player_label = "Player 1",
             is_starter = TRUE, start_elapsed = start_elapsed,
             end_elapsed = end_elapsed, stringsAsFactors = FALSE)
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
```

- [ ] **Step 2: Run the tests to verify they fail**

```bash
"$RSCRIPT" -e "setwd('app'); testthat::test_file('tests/testthat/test-stint-ribbon.R')"
```
Expected: FAIL with `could not find function "ribbon_stint_points"`.

- [ ] **Step 3: Append the implementation**

```bash
cat > /tmp/ribbon_pts.R <<'EOF'


# Points for, points against and +/- for each stint, as NET DIFFERENCES
# across the stint window.
#
# Never sum increments. The PBP credits a basket and later rescinds it
# (measured 2026-09-06: 15 own + 15 opp phantom points across 10 of 40
# Israeli games), so sum(delta[delta > 0]) overstates BOTH sides. A net
# difference cancels a credit-then-rescind that falls inside the window.
#
# `pm` is taken from the margin series directly rather than as pf - pa. The
# two are algebraically equal, but sourcing pm from the margin guarantees
# that a bar's printed number equals the rise of the curve drawn above it
# even if `own` is absent -- the self-consistency the whole chart rests on.
ribbon_stint_points <- function(stints, steps) {
  if (is.null(stints) || !nrow(stints)) {
    stints$pf <- numeric(0)
    stints$pa <- numeric(0)
    stints$pm <- numeric(0)
    return(stints)
  }

  # No series means "unknown", not "level". A fabricated 0 would print a
  # measured-looking zero on every bar; NA prints nothing at all.
  if (is.null(steps) || !NROW(steps)) {
    stints$pf <- rep(NA_real_, nrow(stints))
    stints$pa <- rep(NA_real_, nrow(stints))
    stints$pm <- rep(NA_real_, nrow(stints))
    return(stints)
  }

  as_series <- function(value) {
    data.frame(elapsed = as.numeric(steps$elapsed),
               order_key = as.numeric(steps$order_key),
               value = as.numeric(value))
  }

  mar <- as_series(steps$margin)
  mar_start <- ribbon_score_as_of(mar, stints$start_elapsed)
  mar_end <- ribbon_score_as_of(mar, stints$end_elapsed)
  stints$pm <- mar_end - mar_start

  if (is.null(steps$own)) {
    stints$pf <- rep(NA_real_, nrow(stints))
    stints$pa <- rep(NA_real_, nrow(stints))
    return(stints)
  }

  own <- as_series(steps$own)
  own_start <- ribbon_score_as_of(own, stints$start_elapsed)
  own_end <- ribbon_score_as_of(own, stints$end_elapsed)
  stints$pf <- own_end - own_start
  # The opponent's running score is own - margin, so its net difference is
  # the difference of those two differences.
  stints$pa <- (own_end - mar_end) - (own_start - mar_start)
  stints
}
EOF
cat /tmp/ribbon_pts.R >> app/R/helpers.R
```

- [ ] **Step 4: Verify the file was not rewritten**

```bash
test "$(tr -cd '\r' < app/R/helpers.R | wc -c)" -eq 2670 && echo "CR OK"
git diff --stat app/R/helpers.R
```
Expected: `CR OK`, insertions only.

- [ ] **Step 5: Run the tests to verify they pass**

```bash
"$RSCRIPT" -e "setwd('app'); testthat::test_file('tests/testthat/test-stint-ribbon.R')"
```
Expected: PASS, 0 failures.

- [ ] **Step 6: Commit**

```bash
git add app/R/helpers.R app/tests/testthat/test-stint-ribbon.R
git commit -m "shiny: add ribbon_stint_points() for per-stint pf/pa/+-

Net differences across the stint window, never summed increments -- the PBP
credits then rescinds baskets, which only a net difference cancels. pm comes
from the margin series so a bar's number always equals the curve's rise.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01NS6bqx4bZQ7H8gaPRKBPbr"
```

---

### Task 3: `ribbon_stint_overlaps()`

Which teammates shared the floor during one stint, and for exactly which spans.

**Files:**
- Modify: `app/R/helpers.R` (append at EOF)
- Test: `app/tests/testthat/test-stint-ribbon.R` (append at EOF)

**Interfaces:**
- Consumes: nothing from earlier tasks.
- Produces: `ribbon_stint_overlaps(lanes, side, player_key, start_elapsed, end_elapsed)`. Returns a data frame with columns `player_key`, `player_label`, `start_elapsed`, `end_elapsed`, `shared` (that teammate's TOTAL shared seconds across all their spans in this stint), ordered by `shared` descending then `player_key` then `start_elapsed`. Zero rows when nothing overlaps.

- [ ] **Step 1: Write the failing tests**

Append to `app/tests/testthat/test-stint-ribbon.R`:

```r
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
```

- [ ] **Step 2: Run the tests to verify they fail**

```bash
"$RSCRIPT" -e "setwd('app'); testthat::test_file('tests/testthat/test-stint-ribbon.R')"
```
Expected: FAIL with `could not find function "ribbon_stint_overlaps"`.

- [ ] **Step 3: Append the implementation**

```bash
cat > /tmp/ribbon_ov.R <<'EOF'


# Teammates who shared the floor during one stint, clipped to its window.
#
# Keyed on player_key, NEVER on player_label: both leagues carry same-name /
# different-id players on one team, and a name key would merge two people's
# spans into one.
#
# A teammate can appear more than once -- they subbed out and back in while
# this player stayed on -- so spans are kept separate while `shared` carries
# that teammate's total across all of them, which is what the ordering uses.
ribbon_stint_overlaps <- function(lanes, side, player_key, start_elapsed,
                                  end_elapsed) {
  empty <- data.frame(player_key = character(0), player_label = character(0),
                      start_elapsed = numeric(0), end_elapsed = numeric(0),
                      shared = numeric(0), stringsAsFactors = FALSE)
  if (is.null(lanes) || !nrow(lanes)) return(empty)

  o <- lanes[lanes$side == side & lanes$player_key != player_key, , drop = FALSE]
  if (!nrow(o)) return(empty)

  s <- pmax(as.numeric(o$start_elapsed), start_elapsed)
  e <- pmin(as.numeric(o$end_elapsed), end_elapsed)
  keep <- e > s
  if (!any(keep)) return(empty)

  out <- data.frame(
    player_key = as.character(o$player_key[keep]),
    player_label = as.character(o$player_label[keep]),
    start_elapsed = s[keep],
    end_elapsed = e[keep],
    stringsAsFactors = FALSE
  )
  totals <- tapply(out$end_elapsed - out$start_elapsed, out$player_key, sum)
  out$shared <- as.numeric(totals[out$player_key])

  out <- out[order(-out$shared, out$player_key, out$start_elapsed), , drop = FALSE]
  rownames(out) <- NULL
  out
}
EOF
cat /tmp/ribbon_ov.R >> app/R/helpers.R
```

- [ ] **Step 4: Verify the file was not rewritten**

```bash
test "$(tr -cd '\r' < app/R/helpers.R | wc -c)" -eq 2670 && echo "CR OK"
git diff --stat app/R/helpers.R
```

- [ ] **Step 5: Run the tests to verify they pass**

```bash
"$RSCRIPT" -e "setwd('app'); testthat::test_file('tests/testthat/test-stint-ribbon.R')"
```
Expected: PASS, 0 failures.

- [ ] **Step 6: Commit**

```bash
git add app/R/helpers.R app/tests/testthat/test-stint-ribbon.R
git commit -m "shiny: add ribbon_stint_overlaps() for on-chart lineup context

Clips every teammate's bars to one stint's window, keeping repeat spans
separate and ordering by total shared time. Keyed on player_key, never on a
name -- both leagues carry same-name/different-id players on one team.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01NS6bqx4bZQ7H8gaPRKBPbr"
```

---

### Task 4: Reader plumbing — the raw `steps` frame

The builder currently receives only the *completed* margin, which is padded and collapsed for path drawing. Per-stint numbers need the raw series. This task threads it through without changing any rendered output.

**Files:**
- Modify: `app/R/global.R:358-393` (`fetch_stint_ribbon`)
- Modify: `app/R/helpers.R` (append `ribbon_player_totals`; edit `build_stint_ribbon_svg` signature)
- Test: `app/tests/testthat/test-stint-ribbon-readers.R`, `app/tests/testthat/test-stint-ribbon.R`

**Interfaces:**
- Consumes: `ribbon_stint_points(stints, steps)` from Task 2.
- Produces:
  - `fetch_stint_ribbon()` return list gains `steps` — a data frame with `elapsed`, `order_key`, `margin`, and (from Task 7/8) `own`.
  - `build_stint_ribbon_svg(lanes, margin, meta, id_prefix = "ribbon", steps = NULL)` — new fifth argument, defaulting to `NULL` so existing callers and tests are unaffected.
  - `ribbon_player_totals(lanes)` returns a data frame with `side`, `player_key`, `secs`, `pm`, one row per player per side.

- [ ] **Step 1: Write the failing tests**

Append to `app/tests/testthat/test-stint-ribbon.R`:

```r
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
```

Append to `app/tests/testthat/test-stint-ribbon-readers.R`.

**The tests do NOT source `global.R`** (`helper-server-mocks.R:56` says so explicitly), so `fetch_stint_ribbon` and the `RIBBON_SQL_*` constants are not bound in the test environment. Every existing reader test therefore reads `global.R` as *text*. Follow that convention:

```r
test_that("the reader returns a raw steps frame alongside the drawn margin", {
  # steps is the RAW series: one row per recorded event, order_key intact and
  # no padding. The drawn `margin` stays completed by ribbon_complete_margin().
  # They are different frames on purpose -- as-of lookups must not read the
  # padding. Asserted on the source because tests do not source global.R.
  src <- paste(readLines(testthat::test_path("..", "..", "R", "global.R"),
                         warn = FALSE), collapse = "\n")
  expect_match(src, "steps = steps", fixed = TRUE)
  expect_match(src, "steps <- margin", fixed = TRUE)
})

test_that("completing the margin does not disturb the raw step series", {
  # The pure half of the same contract: completion collapses duplicate
  # seconds and pads both ends, so reading it as-of would answer from
  # padding rather than from recorded events.
  raw <- data.frame(elapsed = c(30, 30, 900), margin = c(2, 4, 9),
                    order_key = c(1, 2, 3))
  completed <- ribbon_complete_margin(raw, 2400)
  expect_equal(nrow(raw), 3)
  expect_true(nrow(completed) != nrow(raw))
  expect_equal(completed$elapsed[1], 0)              # padded start
  expect_equal(completed$elapsed[nrow(completed)], 2400)  # padded end
  expect_null(completed$order_key)                   # dropped by completion
})
```

- [ ] **Step 2: Run the tests to verify they fail**

```bash
"$RSCRIPT" -e "setwd('app'); testthat::test_file('tests/testthat/test-stint-ribbon.R')"
"$RSCRIPT" -e "setwd('app'); testthat::test_file('tests/testthat/test-stint-ribbon-readers.R')"
```
Expected: FAIL — `could not find function "ribbon_player_totals"`, and `ribbon$steps` is NULL.

- [ ] **Step 3: Append `ribbon_player_totals`**

```bash
cat > /tmp/ribbon_totals.R <<'EOF'


# Per-player game totals for the gutter: floor seconds and +/-, one row per
# player per side. `side` is part of the key because the same player_key on
# the other side is a different person's lane.
#
# Requires lanes to carry `pm` (from ribbon_stint_points). Excluded segments
# are not in `lanes` at all, so they are not counted here -- that omission is
# already disclosed by ribbon_health_message().
ribbon_player_totals <- function(lanes) {
  if (is.null(lanes) || !nrow(lanes)) {
    return(data.frame(side = character(0), player_key = character(0),
                      secs = numeric(0), pm = numeric(0),
                      stringsAsFactors = FALSE))
  }
  key <- paste(lanes$side, lanes$player_key, sep = "\r")
  secs <- tapply(lanes$end_elapsed - lanes$start_elapsed, key, sum)
  pm <- tapply(as.numeric(lanes$pm), key, sum)
  parts <- strsplit(names(secs), "\r", fixed = TRUE)

  data.frame(
    side = vapply(parts, `[`, character(1), 1),
    player_key = vapply(parts, `[`, character(1), 2),
    secs = as.numeric(secs),
    pm = as.numeric(pm),
    stringsAsFactors = FALSE
  )
}
EOF
cat /tmp/ribbon_totals.R >> app/R/helpers.R
```

- [ ] **Step 4: Add the `steps` argument to the builder**

```bash
"$RSCRIPT" -e '
p <- "app/R/helpers.R"
b <- readBin(p, "raw", file.size(p)); s <- rawToChar(b); Encoding(s) <- "UTF-8"
old <- "build_stint_ribbon_svg <- function(lanes, margin, meta, id_prefix = \"ribbon\") {\n  if (is.null(lanes) || !nrow(lanes)) return(NULL)\n\n  bounds <- ribbon_period_bounds(meta$n_periods)\n  total_seconds <- bounds[length(bounds)]\n\n  lanes <- merge_adjacent_stints(lanes)\n  lanes <- ribbon_lane_index(lanes)"
new <- "build_stint_ribbon_svg <- function(lanes, margin, meta, id_prefix = \"ribbon\",\n                                   steps = NULL) {\n  if (is.null(lanes) || !nrow(lanes)) return(NULL)\n\n  bounds <- ribbon_period_bounds(meta$n_periods)\n  total_seconds <- bounds[length(bounds)]\n\n  lanes <- merge_adjacent_stints(lanes)\n  # Per-stint numbers come from the RAW step series, not the completed margin\n  # the curve is drawn from. With steps = NULL every number is NA and the\n  # chart renders exactly as it did before this feature -- see\n  # ribbon_stint_points(), which returns NA rather than a fabricated 0.\n  lanes <- ribbon_stint_points(lanes, steps)\n  lanes <- ribbon_lane_index(lanes)"
stopifnot(length(gregexpr(old, s, fixed = TRUE)[[1]]) == 1)
writeBin(charToRaw(sub(old, new, s, fixed = TRUE)), p)
cat("replaced\n")
'
```

- [ ] **Step 5: Return `steps` from the reader**

In `app/R/global.R`, inside `fetch_stint_ribbon`, after the `margin <- data.frame(...)` block and before `n_periods <- ...`, add:

```r
    # The RAW step series, kept separate from the completed margin below.
    # ribbon_complete_margin() collapses each second to one row and pads both
    # ends to close the drawn path; per-stint as-of lookups must read the
    # recorded events instead. `own` is added by the league SQL in a later
    # task and is absent until then.
    steps <- margin
    if (!is.null(marg_raw$own)) steps$own <- as.numeric(marg_raw$own)
```

and change the returned list to include it:

```r
    list(
      lanes = lanes,
      margin = margin,
      steps = steps,
      meta = list(n_periods = n_periods),
      health = ribbon_health_message(row$excluded_segments[1])
    )
```

- [ ] **Step 6: Verify helpers.R was not rewritten**

```bash
test "$(tr -cd '\r' < app/R/helpers.R | wc -c)" -eq 2670 && echo "CR OK"
git diff --stat app/R/helpers.R app/R/global.R
```

- [ ] **Step 7: Run the ribbon suite to verify it passes**

```bash
"$RSCRIPT" -e "setwd('app'); testthat::test_file('tests/testthat/test-stint-ribbon.R')"
"$RSCRIPT" -e "setwd('app'); testthat::test_file('tests/testthat/test-stint-ribbon-readers.R')"
```
Expected: PASS, 0 failures. The rendered SVG is unchanged at this point.

- [ ] **Step 8: Commit**

```bash
git add app/R/helpers.R app/R/global.R app/tests/testthat/test-stint-ribbon.R app/tests/testthat/test-stint-ribbon-readers.R
git commit -m "shiny: thread the raw step series through to the ribbon builder

The builder only had the completed margin, which is padded and collapsed for
path drawing. Per-stint numbers need the recorded events, so the reader now
returns both. No rendered output changes yet.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01NS6bqx4bZQ7H8gaPRKBPbr"
```

---

### Task 5: Gutter columns — MIN and +/-

**Files:**
- Modify: `app/R/helpers.R` (constants at 3062 / 3209-3211; `build_stint_ribbon_svg` lane-label block)
- Modify: `app/www/app.css` (ribbon block, ~line 1481)
- Test: `app/tests/testthat/test-stint-ribbon.R`

**Interfaces:**
- Consumes: `ribbon_player_totals(lanes)` from Task 4.
- Produces: constants `RIBBON_NAME_X`, `RIBBON_MIN_X`, `RIBBON_PM_X`; helper `ribbon_minutes_label(secs)` returning `"M:SS"`; helper `ribbon_pm_label(pm)` returning `"0"` or a signed integer string.

- [ ] **Step 1: Write the failing tests**

Append to `app/tests/testthat/test-stint-ribbon.R`:

```r
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
  expect_true(grepl("Alice Adams, 15:00 on the floor", svg, fixed = TRUE))
  expect_false(grepl("Alice Adams, 10:00 on the floor", svg, fixed = TRUE))
})
```

- [ ] **Step 2: Run the tests to verify they fail**

```bash
"$RSCRIPT" -e "setwd('app'); testthat::test_file('tests/testthat/test-stint-ribbon.R')"
```
Expected: FAIL — `ribbon_minutes_label` not found, and `RIBBON_WIDTH - RIBBON_GUTTER` is 850 already but the gutter class assertions fail.

- [ ] **Step 3: Widen the gutter and add the column anchors**

```bash
"$RSCRIPT" -e '
p <- "app/R/helpers.R"
b <- readBin(p, "raw", file.size(p)); s <- rawToChar(b); Encoding(s) <- "UTF-8"
rep1 <- function(s, old, new) {
  stopifnot(length(gregexpr(old, s, fixed = TRUE)[[1]]) == 1)
  sub(old, new, s, fixed = TRUE)
}
s <- rep1(s,
"# Width reserved on the left for lane labels.\nRIBBON_GUTTER <- 150",
"# Width reserved on the left for lane labels and the two totals columns.\n# RIBBON_GUTTER and RIBBON_WIDTH moved together (150/1000 -> 220/1070) so the\n# plot area stays 850 units and every bar keeps the width it had before the\n# totals columns existed.\nRIBBON_GUTTER <- 220\n\n# Right-hand anchors of the three gutter columns. RIBBON_NAME_X is 142, the\n# same x the name occupied under the 150-unit gutter, so the name column did\n# not move -- the two new columns were added in the space the widening made.\nRIBBON_NAME_X <- RIBBON_GUTTER - 78\nRIBBON_MIN_X <- RIBBON_GUTTER - 35\nRIBBON_PM_X <- RIBBON_GUTTER - 8")
s <- rep1(s, "RIBBON_WIDTH <- 1000", "RIBBON_WIDTH <- 1070")
writeBin(charToRaw(s), p); cat("done\n")
'
```

- [ ] **Step 4: Append the two label formatters**

```bash
cat > /tmp/ribbon_labels.R <<'EOF'


# Floor time as M:SS. floor() rather than round() so a total never reads one
# second longer than the bars it was summed from.
ribbon_minutes_label <- function(secs) {
  secs <- floor(as.numeric(secs))
  sprintf("%d:%02d", secs %/% 60, secs %% 60)
}

# +/- for the gutter and the bar faces. Zero prints bare, so a "0" is
# visibly a measured level stint rather than a sign the renderer gave up.
ribbon_pm_label <- function(pm) {
  pm <- as.integer(round(as.numeric(pm)))
  ifelse(is.na(pm), "", ifelse(pm == 0, "0", sprintf("%+d", pm)))
}
EOF
cat /tmp/ribbon_labels.R >> app/R/helpers.R
```

- [ ] **Step 5: Render the columns**

Replace the `lane_labels` block in `build_stint_ribbon_svg` using the byte-safe procedure. Old text:

```r
  first_row <- !duplicated(paste(lanes$side, lanes$player_key))
  lane_labels <- lapply(which(first_row), function(i) {
    secs <- lanes$end_elapsed[i] - lanes$start_elapsed[i]
    label <- sprintf("%s, %.0f:%02.0f on the floor",
                     lanes$player_label[i], secs %/% 60, secs %% 60)
    tags$text(class = "ibpl-ribbon-name", x = RIBBON_GUTTER - 8,
              y = lanes$abs_y[i] + lanes$h[i] - 3, `text-anchor` = "end",
              `data-clip` = lanes$clip[i], tabindex = "0",
              `aria-label` = label,
              lanes$player_label[i])
  })
```

New text:

```r
  # One gutter row per player: name, floor time, +/-. The totals come from
  # ribbon_player_totals() rather than the first stint's row -- the old
  # aria-label used lanes[i]'s own duration, which for a player with several
  # stints reported one stint instead of their game.
  totals <- ribbon_player_totals(lanes)
  tkey <- paste(totals$side, totals$player_key, sep = "\r")
  first_row <- !duplicated(paste(lanes$side, lanes$player_key))
  lane_labels <- lapply(which(first_row), function(i) {
    ti <- match(paste(lanes$side[i], lanes$player_key[i], sep = "\r"), tkey)
    mins <- ribbon_minutes_label(totals$secs[ti])
    pm <- ribbon_pm_label(totals$pm[ti])
    label <- sprintf("%s, %s on the floor, %s", lanes$player_label[i], mins,
                     if (nzchar(pm)) paste("plus-minus", pm) else "plus-minus unavailable")
    y <- lanes$abs_y[i] + lanes$h[i] - 3
    common <- list(`data-clip` = lanes$clip[i], `text-anchor` = "end")
    list(
      do.call(tags$text, c(common, list(
        class = "ibpl-ribbon-name", x = RIBBON_NAME_X, y = y,
        tabindex = "0", `aria-label` = label, lanes$player_label[i]))),
      do.call(tags$text, c(common, list(
        class = "ibpl-ribbon-min", x = RIBBON_MIN_X, y = y, mins))),
      do.call(tags$text, c(common, list(
        class = "ibpl-ribbon-pm", x = RIBBON_PM_X, y = y, pm)))
    )
  })
```

Then add the header labels. Old text:

```r
  team_labels <- list(
    tags$text(class = "ibpl-ribbon-team", x = 0, y = RIBBON_PAD_TOP + 10, meta$own_team %||% "Own"),
```

New text:

```r
  team_labels <- list(
    tags$text(class = "ibpl-ribbon-col-head", x = RIBBON_MIN_X,
              y = RIBBON_PAD_TOP + 10, `text-anchor` = "end", "MIN"),
    tags$text(class = "ibpl-ribbon-col-head", x = RIBBON_PM_X,
              y = RIBBON_PAD_TOP + 10, `text-anchor` = "end", "+/-"),
    tags$text(class = "ibpl-ribbon-team", x = 0, y = RIBBON_PAD_TOP + 10, meta$own_team %||% "Own"),
```

- [ ] **Step 6: Add the CSS**

Append to the ribbon block in `app/www/app.css`, after the `.ibpl-ribbon-name` rules:

```css
/* Gutter totals columns. Mono so the digits align down the column and so
   their width is computable server-side (see the on-bar numbers below). */
.ibpl-ribbon-min,
.ibpl-ribbon-pm,
.ibpl-ribbon-col-head {
  font-family: var(--ibpl-font-mono);
  font-size: 10px;
  fill: var(--ibpl-text-muted);
}

.ibpl-ribbon-pm { fill: var(--ibpl-text); }

.ibpl-ribbon-col-head {
  fill: var(--ibpl-text-faint);
  font-size: 9px;
  letter-spacing: 0.04em;
}
```

- [ ] **Step 7: Verify helpers.R was not rewritten, then run the tests**

```bash
test "$(tr -cd '\r' < app/R/helpers.R | wc -c)" -eq 2670 && echo "CR OK"
git diff --stat app/R/helpers.R
"$RSCRIPT" -e "setwd('app'); testthat::test_file('tests/testthat/test-stint-ribbon.R')"
```
Expected: `CR OK`, and PASS with 0 failures.

- [ ] **Step 8: Check the columns do not collide, in a browser**

Launch with `IBPL_CACHE_UI=false` (CSS is read at build time), open a game with long player names, and measure the rendered bounding boxes of `.ibpl-ribbon-name`, `.ibpl-ribbon-min` and `.ibpl-ribbon-pm` on the same row.

```bash
IBPL_CACHE_UI=false "$RSCRIPT" -e "shiny::runApp('app')"
```

In the browser console:
```js
var r = document.querySelector('.ibpl-ribbon-name').getBoundingClientRect();
var m = document.querySelector('.ibpl-ribbon-min').getBoundingClientRect();
var p = document.querySelector('.ibpl-ribbon-pm').getBoundingClientRect();
console.log('name->min gap', m.left - r.right, 'min->pm gap', p.left - m.right);
```
Expected: both gaps positive. This is the same failure mode that made `RIBBON_BAND_GAP` 24 → 28 on 2026-09-06, where two boxes that looked fine overlapped by 1.9px. If either gap is negative, widen `RIBBON_GUTTER` (and `RIBBON_WIDTH` by the same amount) and re-measure. Record the measured gaps in the commit message.

- [ ] **Step 9: Commit**

```bash
git add app/R/helpers.R app/www/app.css app/tests/testthat/test-stint-ribbon.R
git commit -m "shiny: give the stint ribbon MIN and +/- gutter columns

RIBBON_GUTTER 150->220 and RIBBON_WIDTH 1000->1070 move together, so the plot
area stays 850 units and every bar keeps its measured width. Also fixes the
lane aria-label, which reported the first stint's duration rather than the
player's game total.

Measured gaps in browser: name->min <X>px, min->pm <Y>px.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01NS6bqx4bZQ7H8gaPRKBPbr"
```

---

### Task 6: Numbers on the bar faces

**Files:**
- Modify: `app/R/helpers.R` (constants; `lane_rects` block in `build_stint_ribbon_svg`)
- Modify: `app/www/app.css`
- Test: `app/tests/testthat/test-stint-ribbon.R`

**Interfaces:**
- Consumes: `ribbon_pm_label(pm)` from Task 5; `lanes$pm` from Task 4.
- Produces: constants `RIBBON_NUM_FONT` (9), `RIBBON_NUM_ADVANCE` (0.6), `RIBBON_NUM_PAD` (6); helper `ribbon_number_fits(text, width)` returning a logical vector.

- [ ] **Step 1: Write the failing tests**

Append to `app/tests/testthat/test-stint-ribbon.R`:

```r
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
  expect_true(grepl(">0</text>", svg, fixed = TRUE))
})

test_that("no number is drawn when steps are absent", {
  lanes <- ribbon_mark_starters(lane_row("own", "1", 0, 600))
  svg <- as.character(build_stint_ribbon_svg(
    lanes, ribbon_complete_margin(
      data.frame(elapsed = 300, margin = 2, order_key = 1), 2400),
    list(n_periods = 4L)))
  expect_false(grepl("ibpl-ribbon-num", svg, fixed = TRUE))
})
```

- [ ] **Step 2: Run the tests to verify they fail**

```bash
"$RSCRIPT" -e "setwd('app'); testthat::test_file('tests/testthat/test-stint-ribbon.R')"
```
Expected: FAIL with `could not find function "ribbon_number_fits"`.

- [ ] **Step 3: Append the fit rule**

```bash
cat > /tmp/ribbon_fit.R <<'EOF'


# On-bar number metrics. The numbers are drawn in JetBrains Mono, whose
# advance is 600/1000 em, so a string's rendered width is computable here
# without measuring a font -- which a server-built SVG cannot do.
RIBBON_NUM_FONT <- 9
RIBBON_NUM_ADVANCE <- 0.6
RIBBON_NUM_PAD <- 6

# Thresholds this yields: "0" 11.4px, "+6" 16.8px, "+12" 22.2px.
ribbon_number_fits <- function(text, width) {
  need <- nchar(text) * RIBBON_NUM_ADVANCE * RIBBON_NUM_FONT + RIBBON_NUM_PAD
  as.numeric(width) >= need
}
EOF
cat /tmp/ribbon_fit.R >> app/R/helpers.R
```

- [ ] **Step 4: Draw the numbers**

Replace the `lane_rects` block. Old text:

```r
      tags$rect(x = lanes$x[i], y = lanes$abs_y[i],
                width = lanes$w[i], height = lanes$h[i], rx = 2)
    )
  })
```

New text:

```r
      tags$rect(x = lanes$x[i], y = lanes$abs_y[i],
                width = lanes$w[i], height = lanes$h[i], rx = 2),
      # Blank means one thing only: too narrow to label. A level stint
      # prints "0", so blank never has to be read as "nothing happened".
      if (!is.na(lanes$pm[i]) && ribbon_number_fits(num[i], lanes$w[i])) {
        tags$text(class = "ibpl-ribbon-num",
                  x = lanes$x[i] + lanes$w[i] / 2,
                  y = lanes$abs_y[i] + lanes$h[i] - 4,
                  `text-anchor` = "middle", num[i])
      }
    )
  })
```

and add the label vector immediately above the `lane_rects <- lapply(` line:

```r
  num <- ribbon_pm_label(lanes$pm)
```

- [ ] **Step 5: Add the CSS**

Append to the ribbon block in `app/www/app.css`:

```css
/* On-bar +/-. Dark ink on both fills: composited over --ibpl-bg at the bars'
   0.85 opacity the own bar is #C88E2F (contrast 6.64:1) and the opp bar is
   #847F78 (4.77:1). Both clear the 4.5:1 small-text threshold. Verify in a
   browser against the COMPOSITED colour, not the raw token. */
.ibpl-ribbon-num {
  font-family: var(--ibpl-font-mono);
  font-size: 9px;
  fill: var(--ibpl-bg);
  pointer-events: none;
  transition: opacity 120ms ease;
}

/* The number must dim with its bar. The existing dimming rule targets
   `rect` only, so without this the numbers would stay bright on bars that
   have been dimmed out of focus. */
.ibpl-ribbon.is-focused .ibpl-ribbon-lane .ibpl-ribbon-num { opacity: 0.3; }
.ibpl-ribbon.is-focused .ibpl-ribbon-lane.is-active .ibpl-ribbon-num { opacity: 1; }
```

- [ ] **Step 6: Verify helpers.R was not rewritten, then run the tests**

```bash
test "$(tr -cd '\r' < app/R/helpers.R | wc -c)" -eq 2670 && echo "CR OK"
"$RSCRIPT" -e "setwd('app'); testthat::test_file('tests/testthat/test-stint-ribbon.R')"
```
Expected: `CR OK`, PASS with 0 failures.

- [ ] **Step 7: Confirm the contrast in a browser**

```bash
IBPL_CACHE_UI=false "$RSCRIPT" -e "shiny::runApp('app')"
```
Open a ribbon and confirm the numbers are legible on both the amber own bars and the muted opponent bars, and that they dim together with their bar on hover. If either reads poorly, adjust the fill and record the measured contrast ratio — do not compare it against what it replaced.

- [ ] **Step 8: Commit**

```bash
git add app/R/helpers.R app/www/app.css app/tests/testthat/test-stint-ribbon.R
git commit -m "shiny: print each stint's +/- on its bar

Numbers are drawn in JetBrains Mono so their width is computable server-side
(nchar * 0.6 * 9 + 6); a bar narrower than that stays bare. Zero prints as
'0', so a blank bar means 'too short to label' and nothing else.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01NS6bqx4bZQ7H8gaPRKBPbr"
```

---

### Task 7: Israeli read layer — `own_team_score`

**Files:**
- Modify: `app/R/global.R:270-322` (`RIBBON_SQL_ISRAEL`)
- Test: `app/tests/testthat/test-stint-ribbon-readers.R`

**Interfaces:**
- Consumes: the `steps` plumbing from Task 4.
- Produces: `fetch_stint_ribbon(..., league = "israel")$steps` gains a numeric `own` column, so `pf` and `pa` stop being `NA`.

- [ ] **Step 1: Write the failing test**

Append to `app/tests/testthat/test-stint-ribbon-readers.R`:

```r
test_that("the Israeli ribbon SQL selects own_team_score in its margin CTE", {
  # own_team_score is already a column on the MV the marg CTE scans, so this
  # costs no new relation, no new grant and no second round trip. pf + pa is
  # not derivable from the margin alone -- this one column is what makes
  # points for/against possible.
  #
  # Read from source: tests do not source global.R, so RIBBON_SQL_ISRAEL is
  # not bound here. Same approach as the euro reader tests above.
  src <- paste(readLines(testthat::test_path("..", "..", "R", "global.R"),
                         warn = FALSE), collapse = "\n")
  sql <- regmatches(src, regexpr('RIBBON_SQL_ISRAEL <- "(.|\n)*?"\n', src,
                                 perl = TRUE))
  expect_true(nzchar(sql))
  expect_match(sql, "own_team_score AS own", fixed = TRUE)
  expect_match(sql, "basketball_test.df_pts_poss_lineups_longer_mv",
               fixed = TRUE)
})
```

- [ ] **Step 2: Run the test to verify it fails**

```bash
"$RSCRIPT" -e "setwd('app'); testthat::test_file('tests/testthat/test-stint-ribbon-readers.R')"
```
Expected: FAIL — `own_team_score AS own` not found.

- [ ] **Step 3: Add the column**

In `app/R/global.R`, in `RIBBON_SQL_ISRAEL`, change the `marg` CTE from:

```sql
marg AS (
  SELECT DISTINCT event_elapsed_seconds AS elapsed,
         (own_team_score - opp_team_score) AS margin,
         id AS order_key
  FROM basketball_test.df_pts_poss_lineups_longer_mv
  WHERE game_id = $1 AND team_id = $2
)
```

to:

```sql
marg AS (
  -- own_team_score rides along so a stint's points FOR and AGAINST can be
  -- taken as net differences across its window. pf - pa is the margin
  -- delta, already here; pf + pa is not derivable from the margin, so this
  -- column is the one number the feature actually needed.
  SELECT DISTINCT event_elapsed_seconds AS elapsed,
         (own_team_score - opp_team_score) AS margin,
         own_team_score AS own,
         id AS order_key
  FROM basketball_test.df_pts_poss_lineups_longer_mv
  WHERE game_id = $1 AND team_id = $2
)
```

- [ ] **Step 4: Run the test to verify it passes**

```bash
"$RSCRIPT" -e "setwd('app'); testthat::test_file('tests/testthat/test-stint-ribbon-readers.R')"
"$RSCRIPT" -e "setwd('app'); testthat::test_file('tests/testthat/test-stint-ribbon.R')"
```
Expected: PASS, 0 failures in both.

- [ ] **Step 5: Confirm against the live database**

```bash
IBPL_CACHE_UI=false "$RSCRIPT" -e "shiny::runApp('app')"
```
Open an Israeli game log, click a date to open the ribbon, and confirm the chart renders with numbers on the bars and totals in the gutter. Time the modal open in the browser Network tab — it must stay at one request and near the ~270 ms measured for this query.

- [ ] **Step 6: Commit**

```bash
git add app/R/global.R app/tests/testthat/test-stint-ribbon-readers.R
git commit -m "shiny: carry own_team_score in the Israeli ribbon query

Already a column on the MV the marg CTE scans, so no new relation, no grant
change and no second round trip. pf - pa was already available as the margin
delta; pf + pa needed exactly this one column.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01NS6bqx4bZQ7H8gaPRKBPbr"
```

---

### Task 8: EuroLeague read layer — migration 054

**Files:**
- Create: `euroleague/sql/054_ribbon_margin_own_score.sql`
- Modify: `app/R/global.R:324-356` (`RIBBON_SQL_EURO`)
- Test: `app/tests/testthat/test-stint-ribbon-readers.R`

**Interfaces:**
- Consumes: the `steps` plumbing from Task 4.
- Produces: `euroleague.ribbon_margin_v` gains a trailing `own_team_score` column; `fetch_stint_ribbon(..., league = "euroleague")$steps` gains `own`.

- [ ] **Step 1: Write the failing test**

Append to `app/tests/testthat/test-stint-ribbon-readers.R`:

```r
test_that("the EuroLeague ribbon SQL selects own_team_score in its margin CTE", {
  # Source-read for the same reason as the Israeli test: global.R is not
  # sourced by the suite. This regex is the one already used by the
  # "euro reader never pairs lineup names with ids positionally" test above.
  src <- paste(readLines(testthat::test_path("..", "..", "R", "global.R"),
                         warn = FALSE), collapse = "\n")
  sql <- regmatches(src, regexpr('RIBBON_SQL_EURO <- "(.|\n)*?"\n', src,
                                 perl = TRUE))
  expect_true(nzchar(sql))
  expect_match(sql, "own_team_score AS own", fixed = TRUE)
})

test_that("migration 054 appends the column with CREATE OR REPLACE, not a drop", {
  # CREATE OR REPLACE VIEW preserves the app_readonly grant; DROP + CREATE
  # wipes it. The column must be appended at the END of the select list,
  # which is the only shape CREATE OR REPLACE allows.
  sql <- paste(readLines(testthat::test_path(
    "..", "..", "..", "euroleague", "sql",
    "054_ribbon_margin_own_score.sql"), warn = FALSE), collapse = "\n")
  expect_match(sql, "CREATE OR REPLACE VIEW euroleague.ribbon_margin_v",
               fixed = TRUE)
  expect_false(grepl("DROP VIEW", sql, fixed = TRUE))
  # own_team_score must come after source_event_order in the select list.
  expect_true(regexpr("source_event_order", sql, fixed = TRUE) <
              regexpr("own_team_score", sql, fixed = TRUE))
})
```

- [ ] **Step 2: Run the test to verify it fails**

```bash
"$RSCRIPT" -e "setwd('app'); testthat::test_file('tests/testthat/test-stint-ribbon-readers.R')"
```
Expected: FAIL — the migration file does not exist.

- [ ] **Step 3: Write the migration**

Create `euroleague/sql/054_ribbon_margin_own_score.sql`:

```sql
-- EUROLEAGUE SHADOW SCHEMA -- migration 054: own_team_score on the ribbon
-- margin view.
--
-- The stint ribbon prints each stint's points FOR and AGAINST, taken as net
-- differences across the stint window. pf - pa is the margin delta, which
-- ribbon_margin_v already exposes; pf + pa is NOT derivable from a margin,
-- so exactly one more column is required.
--
-- euroleague.action_team_context_actions -- which this view already reads --
-- carries own_team_score and opp_team_score and merely subtracts them, so
-- the column is free: no new relation, no new grant, no extra round trip.
--
-- This is CREATE OR REPLACE VIEW with the new column APPENDED at the end of
-- the select list, which is the one shape Postgres allows without dropping
-- the view. That matters: DROP + CREATE would wipe app_readonly's SELECT
-- grant (see the note at the head of 053), and the grant audit would then
-- report the ribbon as unreadable. Verify the grant after applying rather
-- than assuming it survived.

BEGIN;
SET LOCAL search_path TO euroleague, public;

CREATE OR REPLACE VIEW euroleague.ribbon_margin_v AS
SELECT
  game_id,
  team_id,
  period,
  event_elapsed_seconds AS elapsed_seconds,
  (own_team_score - opp_team_score) AS margin,
  source_event_order,
  own_team_score
FROM euroleague.action_team_context_actions
WHERE points > 0;

COMMIT;

-- ---------------------------------------------------------------------------
-- VERIFY after applying (read-only, safe to run any time):
--
--   -- 1. the column is there
--   SELECT own_team_score FROM euroleague.ribbon_margin_v LIMIT 1;
--
--   -- 2. the grant SURVIVED the replace -- expect true
--   SELECT has_table_privilege('app_readonly',
--            'euroleague.ribbon_margin_v', 'SELECT');
--
--   -- 3. the raw sources are still closed -- expect false for each
--   SELECT has_table_privilege('app_readonly',
--            'euroleague.action_team_context_actions', 'SELECT');
--
-- If (2) comes back false, re-run scripts/apply_db_security.R with
-- CONFIRM_DB_SECURITY_APPLY=1. If it comes back true, do NOT re-run it --
-- nothing was lost and there is nothing to restore.
-- ---------------------------------------------------------------------------
```

- [ ] **Step 4: Add the column to the reader**

In `app/R/global.R`, in `RIBBON_SQL_EURO`, change the `marg` CTE's SELECT from:

```sql
  SELECT DISTINCT elapsed_seconds AS elapsed, margin,
         source_event_order AS order_key
```

to:

```sql
  SELECT DISTINCT elapsed_seconds AS elapsed, margin,
         own_team_score AS own,
         source_event_order AS order_key
```

- [ ] **Step 5: Run the tests to verify they pass**

```bash
"$RSCRIPT" -e "setwd('app'); testthat::test_file('tests/testthat/test-stint-ribbon-readers.R')"
"$RSCRIPT" -e "setwd('app'); testthat::test_file('tests/testthat/test-stint-ribbon.R')"
```
Expected: PASS, 0 failures.

- [ ] **Step 6: Apply the migration and verify the grant**

Apply `euroleague/sql/054_ribbon_margin_own_score.sql` against the live database using the same route migration 053 used (`scripts/apply_053_stint_ribbon_read_layer.py` is the reference — read it and follow its connection pattern; DDL goes over port 5432, not the 6543 pooler).

Then run the three verification queries in the migration's footer. Expected: column present, `has_table_privilege(... 'ribbon_margin_v', 'SELECT')` **true**, and the raw source still **false**.

- [ ] **Step 7: Confirm end-to-end in the app**

```bash
IBPL_CACHE_UI=false "$RSCRIPT" -e "shiny::runApp('app')"
```
Open a EuroLeague game log (Tab 11), click a date, and confirm the ribbon renders with numbers and totals, still in one request.

- [ ] **Step 8: Commit**

```bash
git add euroleague/sql/054_ribbon_margin_own_score.sql app/R/global.R app/tests/testthat/test-stint-ribbon-readers.R
git commit -m "euroleague: append own_team_score to ribbon_margin_v (migration 054)

CREATE OR REPLACE with the column appended at the end of the select list, so
the view is never dropped and app_readonly's grant survives -- verified with
has_table_privilege after applying, not assumed. The source view already read
both score columns and merely subtracted them, so the column is free.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01NS6bqx4bZQ7H8gaPRKBPbr"
```

---

### Task 9: Lane data attributes and the detail strip container

Everything the hover needs, emitted server-side so the JS in Task 10 does no data work beyond interval arithmetic.

**Files:**
- Modify: `app/R/helpers.R` (`lane_rects` block; new `ribbon_detail_strip()`)
- Modify: `app/R/server_tab4.R:358-373`
- Modify: `app/R/server_tab11_euro_gamelogs.R:59-74`
- Test: `app/tests/testthat/test-stint-ribbon.R`

**Interfaces:**
- Consumes: `ribbon_stint_overlaps()` (Task 3), `ribbon_minutes_label()` / `ribbon_pm_label()` (Task 5).
- Produces: each lane `<g>` carries `data-start`, `data-end`, `data-player`, `data-pm`, `data-pf`, `data-pa`, `data-window`, `data-with`. `ribbon_detail_strip()` returns the empty strip container `<div class="ibpl-ribbon-detail">`.

- [ ] **Step 1: Write the failing tests**

Append to `app/tests/testthat/test-stint-ribbon.R`:

```r
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
```

- [ ] **Step 2: Run the tests to verify they fail**

```bash
"$RSCRIPT" -e "setwd('app'); testthat::test_file('tests/testthat/test-stint-ribbon.R')"
```
Expected: FAIL — `could not find function "ribbon_detail_strip"`.

- [ ] **Step 3: Append the strip container and the teammate formatter**

```bash
cat > /tmp/ribbon_strip.R <<'EOF'


# The hover detail strip. Rendered empty; app.js fills it from the hovered
# lane's data-* attributes and leaves the last one in place on exit, so the
# reader can look away from the chart to read it.
#
# aria-hidden because the same facts are already on each lane's aria-label.
# An aria-live region here would announce on every hover, which is noise.
ribbon_detail_strip <- function() {
  tags$div(
    class = "ibpl-ribbon-detail", `aria-hidden` = "true",
    tags$span(class = "ibpl-ribbon-detail-rest",
              "Hover or tab to a stint to see who was on the floor.")
  )
}

# One teammate entry for the strip. Carries BOTH axes -- the shared window
# and the margin swing across it -- so each entry maps onto a span the reader
# can see lit in the chart. A bare name and duration was rejected in design
# for having no connection to either axis.
ribbon_overlap_label <- function(overlaps, steps) {
  if (is.null(overlaps) || !nrow(overlaps)) return("")
  spans <- ribbon_stint_points(overlaps, steps)
  paste(sprintf("%s %s-%s %s",
                spans$player_label,
                ribbon_minutes_label(spans$start_elapsed),
                ribbon_minutes_label(spans$end_elapsed),
                ribbon_pm_label(spans$pm)),
        collapse = " · ")
}
EOF
cat /tmp/ribbon_strip.R >> app/R/helpers.R
```

- [ ] **Step 4: Emit the data attributes**

Replace the lane `<g>` opening in `build_stint_ribbon_svg`. Old text:

```r
    tags$g(
      class = paste("ibpl-ribbon-lane", paste0("is-", lanes$side[i])),
      `data-clip` = lanes$clip[i],
      tabindex = "0",
      role = "listitem",
      `aria-label` = label,
      tags$title(label),
```

New text (`ov`, `with_txt` and `window_txt` are already in scope — they are computed at the top of the `lapply` by the second replacement below):

```r
    tags$g(
      class = paste("ibpl-ribbon-lane", paste0("is-", lanes$side[i])),
      `data-clip` = lanes$clip[i],
      `data-start` = lanes$start_elapsed[i],
      `data-end` = lanes$end_elapsed[i],
      `data-player` = lanes$player_label[i],
      `data-window` = window_txt,
      `data-pm` = num[i],
      `data-pf` = if (is.na(lanes$pf[i])) "" else as.character(lanes$pf[i]),
      `data-pa` = if (is.na(lanes$pa[i])) "" else as.character(lanes$pa[i]),
      `data-with` = with_txt,
      tabindex = "0",
      role = "listitem",
      `aria-label` = label,
      tags$title(label),
```

Also extend the per-stint `label` used just above it so the accessible name carries the same facts:

Old text:
```r
  lane_rects <- lapply(seq_len(nrow(lanes)), function(i) {
    secs <- lanes$end_elapsed[i] - lanes$start_elapsed[i]
    label <- sprintf("%s, %.0f:%02.0f on the floor",
                     lanes$player_label[i], secs %/% 60, secs %% 60)
```

New text:
```r
  lane_rects <- lapply(seq_len(nrow(lanes)), function(i) {
    secs <- lanes$end_elapsed[i] - lanes$start_elapsed[i]
    ov <- ribbon_stint_overlaps(lanes, lanes$side[i], lanes$player_key[i],
                                lanes$start_elapsed[i], lanes$end_elapsed[i])
    with_txt <- ribbon_overlap_label(ov, steps)
    window_txt <- sprintf("%s-%s", ribbon_minutes_label(lanes$start_elapsed[i]),
                          ribbon_minutes_label(lanes$end_elapsed[i]))
    # The accessible name carries EVERYTHING the strip shows, teammates
    # included, because the strip is aria-hidden -- see
    # ribbon_detail_strip(). If the teammates were left out here there
    # would be no path to them at all for a screen-reader user: the band
    # and the lit overlaps are purely visual, so this label is their only
    # equivalent.
    label <- sprintf("%s, %s on the floor, plus-minus %s%s%s",
                     lanes$player_label[i], ribbon_minutes_label(secs), num[i],
                     if (is.na(lanes$pf[i])) "" else
                       sprintf(", %d points for and %d against",
                               lanes$pf[i], lanes$pa[i]),
                     if (nzchar(with_txt)) paste(", on with", with_txt) else "")
```

- [ ] **Step 5: Render the strip in both tabs**

In `app/R/server_tab4.R`, inside the `renderUI` for `gl_ribbon_svg`, add `ribbon_detail_strip()` after the `build_stint_ribbon_svg(...)` call and pass `steps`:

```r
        build_stint_ribbon_svg(ribbon$lanes, ribbon$margin, meta,
                               id_prefix = paste0("gl", click$game_id),
                               steps = ribbon$steps),
        ribbon_detail_strip()
```

Make the identical change in `app/R/server_tab11_euro_gamelogs.R` for `eurogl_ribbon_svg`, using its own `id_prefix`.

- [ ] **Step 6: Verify helpers.R was not rewritten, then run the tests**

```bash
test "$(tr -cd '\r' < app/R/helpers.R | wc -c)" -eq 2670 && echo "CR OK"
"$RSCRIPT" -e "setwd('app'); testthat::test_file('tests/testthat/test-stint-ribbon.R')"
```
Expected: `CR OK`, PASS with 0 failures.

- [ ] **Step 7: Commit**

```bash
git add app/R/helpers.R app/R/server_tab4.R app/R/server_tab11_euro_gamelogs.R app/tests/testthat/test-stint-ribbon.R
git commit -m "shiny: emit the ribbon hover payload and the detail strip container

Each lane now carries its window, +/-, points for/against and its teammate
spans as data-* attributes, so the hover script does interval arithmetic and
nothing else. Teammate entries carry both a window and a swing.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01NS6bqx4bZQ7H8gaPRKBPbr"
```

---

### Task 10: The band, the lit overlaps, and the strip

**Files:**
- Modify: `app/www/app.js:418-489` (the stint ribbon hover IIFE)
- Modify: `app/www/app.css` (ribbon block)
- Test: manual, in a browser (this task is DOM behaviour; the R suite must still pass unchanged)

**Interfaces:**
- Consumes: the `data-start` / `data-end` / `data-player` / `data-window` / `data-pm` / `data-pf` / `data-pa` / `data-with` attributes from Task 9, and `.ibpl-ribbon-detail` from Task 9.
- Produces: no R-visible interface.

- [ ] **Step 1: Extend `setFocus` in `app/www/app.js`**

Inside the existing stint-ribbon IIFE, add these helpers above `setFocus` and call them from it. The band is one `<rect>` inserted once per SVG and repositioned; the overlap highlights are recomputed per hover.

```js
  // laneFrom() deliberately also matches the gutter <text> labels, which
  // carry data-clip so a name works as an index into the lanes. Those
  // elements have NO data-start/data-end/data-player, so every function
  // below that reads a stint's attributes must reject them -- otherwise
  // hovering a name renders "undefined · undefined · undefined" in the
  // strip and bands a NaN-wide window.
  function isStint(lane) {
    return !!(lane && lane.dataset && lane.dataset.start !== undefined);
  }

  function bandFor(svg) {
    var band = svg.querySelector(".ibpl-ribbon-band");
    if (!band) {
      band = document.createElementNS("http://www.w3.org/2000/svg", "rect");
      band.setAttribute("class", "ibpl-ribbon-band");
      // First child so it paints behind every lane, curve and gridline.
      svg.insertBefore(band, svg.firstChild);
    }
    return band;
  }

  // The lane <g> elements carry their own x/width, so the band's geometry
  // comes from the hovered lane's rect rather than from a seconds->px
  // conversion the script would have to keep in step with the R constants.
  function setBand(svg, lane) {
    var band = bandFor(svg);
    if (!isStint(lane)) { band.setAttribute("width", "0"); return; }
    var r = lane.querySelector("rect");
    if (!r) { band.setAttribute("width", "0"); return; }
    var vb = svg.viewBox.baseVal;
    band.setAttribute("x", r.getAttribute("x"));
    band.setAttribute("width", r.getAttribute("width"));
    band.setAttribute("y", vb.y);
    band.setAttribute("height", vb.height);
  }

  function setOverlaps(svg, lane) {
    var lit = svg.querySelectorAll(".ibpl-ribbon-lane.is-overlap");
    for (var i = 0; i < lit.length; i++) lit[i].classList.remove("is-overlap");
    if (!isStint(lane)) return;

    var s = Number(lane.dataset.start);
    var e = Number(lane.dataset.end);
    var side = lane.classList.contains("is-own") ? "is-own" : "is-opp";
    var lanes = svg.querySelectorAll(".ibpl-ribbon-lane." + side);
    for (var k = 0; k < lanes.length; k++) {
      var o = lanes[k];
      if (o === lane) continue;
      // A shared instant is not shared floor time: require a real overlap.
      if (Math.min(Number(o.dataset.end), e) > Math.max(Number(o.dataset.start), s)) {
        o.classList.add("is-overlap");
      }
    }
  }

  function setDetail(svg, lane) {
    var host = svg.parentNode && svg.parentNode.querySelector(".ibpl-ribbon-detail");
    // Keep the last contents in place on exit (!lane) AND when the pointer
    // is on a gutter name (!isStint) -- a name is a whole-game index, not a
    // stint, and has none of the attributes read below.
    if (!host || !isStint(lane)) return;
    var d = lane.dataset;
    var score = (d.pf !== "" && d.pa !== "") ? "  " + d.pf + "-" + d.pa : "";
    var head = d.player + "  ·  " + d.window + "  ·  " + d.pm + score;
    host.innerHTML = "";
    var h = document.createElement("div");
    h.className = "ibpl-ribbon-detail-head";
    h.textContent = head;
    host.appendChild(h);
    if (d.with) {
      var w = document.createElement("div");
      w.className = "ibpl-ribbon-detail-with";
      w.textContent = "with  " + d.with;
      host.appendChild(w);
    }
  }
```

Then call all three from inside `setFocus`, immediately after the existing `svg.classList.add("is-focused")` / `remove("is-focused")` branches:

```js
    setBand(svg, lane);
    setOverlaps(svg, lane);
    setDetail(svg, lane);
```

All three read stint attributes, so all three go through `isStint()`. Hovering a gutter name still clips the curve and lights that player's bars exactly as it does today, and now draws no band and leaves the strip alone.

- [ ] **Step 2: Add the CSS**

Append to the ribbon block in `app/www/app.css`:

```css
/* Hover band: the vertical slice of the chart the hovered stint occupies.
   Painted behind everything, so the lanes and the curve stay readable. */
.ibpl-ribbon-band {
  fill: var(--ibpl-accent);
  opacity: 0.08;
  pointer-events: none;
}

/* A teammate's bar inside the band. Brighter than the dimmed-out lanes but
   below the hovered lane itself, so the three states stay distinguishable:
   hovered (1.0) > overlapping (0.65) > not on the floor (0.3). */
.ibpl-ribbon.is-focused .ibpl-ribbon-lane.is-overlap rect { opacity: 0.65; }
.ibpl-ribbon.is-focused .ibpl-ribbon-lane.is-overlap .ibpl-ribbon-num { opacity: 0.65; }

.ibpl-ribbon-detail {
  margin-top: 10px;
  padding: 8px 10px;
  border-top: 1px solid var(--ibpl-border);
  font-family: var(--ibpl-font-mono);
  font-size: 11px;
  line-height: 1.6;
  color: var(--ibpl-text);
  min-height: 3.2em;   /* reserve both lines so the chart never jumps */
}

.ibpl-ribbon-detail-with,
.ibpl-ribbon-detail-rest { color: var(--ibpl-text-muted); }
```

- [ ] **Step 3: Verify the R suite is untouched**

```bash
"$RSCRIPT" -e "setwd('app'); testthat::test_file('tests/testthat/test-stint-ribbon.R')"
"$RSCRIPT" -e "setwd('app'); testthat::test_file('tests/testthat/test-stint-ribbon-readers.R')"
```
Expected: PASS, 0 failures — this task changes no R code.

- [ ] **Step 4: Test the interaction in a browser**

```bash
IBPL_CACHE_UI=false "$RSCRIPT" -e "shiny::runApp('app')"
```

Check each of these on an Israeli game and again on a EuroLeague game:

1. Hovering a bar draws a band across the full chart height at that stint's window.
2. Teammates' bars inside the band are visibly brighter than bars outside it, and dimmer than the hovered bar.
3. The strip fills with the player, window, +/-, for-against, and the teammate list — and every teammate entry shows a clock range and a signed number.
4. Moving off the chart leaves the last strip contents in place.
5. **Tab** moves focus between lanes and produces the same band, highlights and strip as hover — with no pointer involved.
6. Hovering a gutter *name* still clips the curve and lights that player's bars, draws no band, and leaves the strip showing the last stint — **it must never show `undefined`**. Check the strip text explicitly here; this is the one case where `laneFrom()` returns an element with none of the stint attributes.
7. The strip does not change height between a stint with teammates and one without (the `min-height` holds).

- [ ] **Step 5: Commit**

```bash
git add app/www/app.js app/www/app.css
git commit -m "shiny: answer the ribbon's lineup question on the chart

Hovering or focusing a stint bands its window across the whole chart and
lights the teammates' bars inside it, so the lineup is read off the time
axis. The strip carries what the picture cannot: each teammate's window and
margin swing. Keyboard focus drives all of it identically to hover.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01NS6bqx4bZQ7H8gaPRKBPbr"
```

---

### Task 11: Database reconciliation tests

Turn the two measurements the design rests on into assertions.

**Files:**
- Modify: `app/tests/testthat/test-stint-ribbon-readers.R`
- Test: itself

**Interfaces:**
- Consumes: everything above.
- Produces: no runtime interface.

- [ ] **Step 1: Write the tests**

Append to `app/tests/testthat/test-stint-ribbon-readers.R`.

The file has **no** `skip_if_no_db()` helper and **no** pool fixture — its DB tests use a two-line env guard and a direct `DBI::dbConnect` (see `test-stint-ribbon-readers.R:117-124`). And because the suite does not source `global.R`, `fetch_stint_ribbon` is unavailable: extract and evaluate the SQL constant from source instead, which is the same trick `helper-server-mocks.R:31-36` uses for the colour ramp.

```r
# Shared by the reconciliation tests below: bind RIBBON_SQL_ISRAEL or
# RIBBON_SQL_EURO by evaluating just that assignment out of global.R, which
# the suite does not source. Reading the real constant (rather than pasting a
# copy here) is the point -- a copy would keep passing after the query changed.
ribbon_sql_for <- function(league) {
  const <- if (identical(league, "israel")) "RIBBON_SQL_ISRAEL" else "RIBBON_SQL_EURO"
  src <- paste(readLines(testthat::test_path("..", "..", "R", "global.R"),
                         warn = FALSE), collapse = "\n")
  assign_txt <- regmatches(src, regexpr(paste0(const, ' <- "(.|\n)*?"\n'),
                                        src, perl = TRUE))
  stopifnot(nzchar(assign_txt))
  eval(parse(text = assign_txt))
}

# Every reconciliation below runs against BOTH leagues. The feature's standing
# constraint is "both leagues or neither", and the spec's evidence covers both
# (301/301 Israeli and 323/323 EuroLeague player-games). A test that checked
# only one would let the other drift silently -- which is exactly how the two
# leagues' tab code diverged three ways before.
RIBBON_LEAGUES <- list(
  israel = list(
    games = "SELECT DISTINCT ON (game_id) game_id, team_id
             FROM basketball_test.final_schedule_mv
             WHERE game_year = 2026 ORDER BY game_id LIMIT %d",
    minutes = "SELECT player_id::text AS player_key, SUM(minutes) AS mv_min
               FROM basketball_test.player_four_factors_by_game
               WHERE game_id = $1 AND team_id = $2
                 AND is_on_key = 1 AND type_lineup = 'offense'
               GROUP BY player_id"),
  euroleague = list(
    games = "SELECT DISTINCT ON (game_id) game_id, team_id
             FROM euroleague.final_schedule ORDER BY game_id DESC LIMIT %d",
    minutes = "SELECT player_id::text AS player_key, SUM(minutes) AS mv_min
               FROM euroleague.player_four_factors_by_game
               WHERE game_id = $1 AND team_id = $2
                 AND is_on_key = 1 AND type_lineup = 'offense'
               GROUP BY player_id")
)

ribbon_db_con <- function() {
  DBI::dbConnect(RPostgres::Postgres(),
    host = Sys.getenv("PG_HOST"), port = as.integer(Sys.getenv("PG_PORT")),
    dbname = Sys.getenv("PG_DB"), user = Sys.getenv("PG_USER"),
    password = Sys.getenv("PG_PASS"), sslmode = Sys.getenv("PG_SSLMODE"),
    connect_timeout = 15L, bigint = "numeric")
}

# Run the real query and rebuild what the builder would draw.
ribbon_fixture <- function(con, league, game_id, team_id) {
  row <- DBI::dbGetQuery(con, ribbon_sql_for(league),
                         params = list(game_id, team_id))
  lanes_raw <- jsonlite::fromJSON(row$lanes[1], simplifyDataFrame = TRUE)
  marg_raw <- jsonlite::fromJSON(row$margin[1], simplifyDataFrame = TRUE)
  steps <- data.frame(elapsed = as.numeric(marg_raw$elapsed),
                      order_key = as.numeric(marg_raw$order_key),
                      margin = as.numeric(marg_raw$margin),
                      own = as.numeric(marg_raw$own))
  lanes <- merge_adjacent_stints(ribbon_normalise_lanes(lanes_raw, team_id))
  list(lanes = ribbon_stint_points(lanes, steps), steps = steps)
}

test_that("ribbon floor time equals the app's published per-game minutes", {
  # Measured 2026-09-06 before any of this was built: 301/301 Israeli
  # player-games agreed to 7e-15 and 323/323 EuroLeague ones to 0.002 min
  # (the MV stores 3 decimals). This asserts it, so a change to either
  # minutes path is caught here rather than by a reader noticing two
  # different totals for one player in one app.
  skip_if_not(nzchar(Sys.getenv("RUN_DB_TESTS")), "RUN_DB_TESTS not enabled")
  skip_if_not(nzchar(Sys.getenv("PG_HOST")), "no database configured")
  con <- ribbon_db_con()
  on.exit(DBI::dbDisconnect(con), add = TRUE)

  for (league in names(RIBBON_LEAGUES)) {
    cfg <- RIBBON_LEAGUES[[league]]
    games <- DBI::dbGetQuery(con, sprintf(cfg$games, 5L))
    expect_gt(nrow(games), 0)

    for (i in seq_len(nrow(games))) {
      fx <- ribbon_fixture(con, league, games$game_id[i], games$team_id[i])
      tot <- ribbon_player_totals(fx$lanes)
      tot <- tot[tot$side == "own", , drop = FALSE]

      mv <- DBI::dbGetQuery(con, cfg$minutes,
                            params = list(games$game_id[i], games$team_id[i]))

      both <- merge(tot, mv, by = "player_key")
      # info = so a failure names the league and game rather than just a row.
      expect_gt(nrow(both), 0)
      expect_true(all(abs(both$secs / 60 - both$mv_min) < 0.01),
                  info = sprintf("%s game %s", league, games$game_id[i]))
    }
  }
})

test_that("each bar's +/- equals the margin curve's rise across that bar", {
  # The self-consistency the whole chart rests on: a reader can check a
  # printed number against the curve drawn above it by eye, so it must never
  # be possible for the two to disagree.
  skip_if_not(nzchar(Sys.getenv("RUN_DB_TESTS")), "RUN_DB_TESTS not enabled")
  skip_if_not(nzchar(Sys.getenv("PG_HOST")), "no database configured")
  con <- ribbon_db_con()
  on.exit(DBI::dbDisconnect(con), add = TRUE)

  for (league in names(RIBBON_LEAGUES)) {
    games <- DBI::dbGetQuery(con, sprintf(RIBBON_LEAGUES[[league]]$games, 3L))
    for (i in seq_len(nrow(games))) {
      fx <- ribbon_fixture(con, league, games$game_id[i], games$team_id[i])
      mar <- data.frame(elapsed = fx$steps$elapsed,
                        order_key = fx$steps$order_key,
                        value = fx$steps$margin)
      rise <- ribbon_score_as_of(mar, fx$lanes$end_elapsed) -
              ribbon_score_as_of(mar, fx$lanes$start_elapsed)
      expect_equal(fx$lanes$pm, rise)

      # And pf - pa must reproduce it, which ties the printed number to the
      # for/against pair the strip shows.
      expect_equal(fx$lanes$pm, fx$lanes$pf - fx$lanes$pa)
    }
  }
})

test_that("a player's bars sum to their gutter total", {
  skip_if_not(nzchar(Sys.getenv("RUN_DB_TESTS")), "RUN_DB_TESTS not enabled")
  skip_if_not(nzchar(Sys.getenv("PG_HOST")), "no database configured")
  con <- ribbon_db_con()
  on.exit(DBI::dbDisconnect(con), add = TRUE)

  for (league in names(RIBBON_LEAGUES)) {
    games <- DBI::dbGetQuery(con, sprintf(RIBBON_LEAGUES[[league]]$games, 1L))
    fx <- ribbon_fixture(con, league, games$game_id[1], games$team_id[1])
    tot <- ribbon_player_totals(fx$lanes)

    key <- paste(fx$lanes$side, fx$lanes$player_key, sep = "\r")
    by_hand <- tapply(fx$lanes$pm, key, sum)
    tkey <- paste(tot$side, tot$player_key, sep = "\r")
    expect_equal(as.numeric(by_hand[tkey]), tot$pm)
  }
})
```

- [ ] **Step 2: Run them with the database enabled**

**Migration 054 (Task 8) must already be applied to the live database**, or the EuroLeague half of these tests fails on a missing `own_team_score` column. That is the correct failure — do not skip the EuroLeague league to get green.

```bash
RUN_DB_TESTS=1 "$RSCRIPT" -e "setwd('app'); testthat::test_file('tests/testthat/test-stint-ribbon-readers.R')"
```
Expected: PASS with **zero SKIPs**. A skip here means the guard short-circuited and nothing was actually verified — treat a skip as a failure and fix the guard.

- [ ] **Step 3: Run the full ribbon suite both ways**

```bash
"$RSCRIPT" -e "setwd('app'); testthat::test_file('tests/testthat/test-stint-ribbon.R')"
RUN_DB_TESTS=1 "$RSCRIPT" -e "setwd('app'); testthat::test_dir('tests/testthat', filter = 'stint|ribbon|db-security')"
```
Expected: PASS, 0 failures.

- [ ] **Step 4: Run the whole suite and compare against the baseline**

```bash
"$RSCRIPT" -e "setwd('app'); testthat::test_dir('tests/testthat', reporter = 'summary')" 2>&1 | tail -20
```
Expected: the only failure is the pre-existing `test-companion-query-counts.R:293` (`object 'minutes' not found`), which was already failing before this work. Any other failure is ours.

- [ ] **Step 5: Commit**

```bash
git add app/tests/testthat/test-stint-ribbon-readers.R
git commit -m "test: assert the ribbon's minutes and self-consistency against the db

Turns the two measurements the design rests on into tests: ribbon floor time
equals the published per-game minutes, and every bar's printed +/- equals the
curve's rise across it. Zero SKIPs is the evidence -- a skip means nothing
was checked.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01NS6bqx4bZQ7H8gaPRKBPbr"
```

---

## Spec coverage

| Spec section | Task |
|---|---|
| §1 item 1 — per-stint +/- on bar faces | 6 |
| §1 item 2 — gutter minutes and +/- | 5 |
| §1 item 3 — points for/against | 2, 7, 8 (computation, Israel, EuroLeague) |
| §1 item 4 — who was on together | 3, 9, 10 |
| §4 — one new column, no new query | 7, 8 |
| §5 — `ribbon_score_as_of`, `ribbon_stint_points`, `ribbon_stint_overlaps`, the three traps | 1, 2, 3, 4 |
| §6 — geometry, header, mono fit rule, contrast | 5, 6 |
| §7 — band, lit overlaps, strip, accessibility | 9, 10 |
| §8 — unit, reconciliation, structural tests | 1-6, 9, 11 |
| §9 — line endings, one round trip, both leagues | Global Constraints; verified per task |
