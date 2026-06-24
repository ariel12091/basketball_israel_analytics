# Tab 5 Identity Display Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** In Tab 5 (Player Stats), show a multi-team player's name consistently (canonical identity name) in the table, the player dropdown, and the selected chips, and group each multi-team player's per-team rows together followed by their TOTAL.

**Architecture:** Display-only changes in `app/R/server_tab5_traditional.R`. Reuse the existing identity lookup (`load_ts_identity_lookup` → `resolved_player_identity_v`, columns `team_id, player_id, identity_id, display_name`). Three independent edits: (R1) override per-team `Player` for multi-team identities inside `add_ts_multi_team_totals`; (R2) a new pure `ts_group_display_order` helper applied before rendering, with the DataTable initial sort set to empty so it renders in that order; (R3) a `lookup` parameter on `ts_player_choices` that remaps the displayed name. No data/stat semantics change.

**Tech Stack:** R, Shiny, DT (DataTables), testthat.

**Spec:** `docs/superpowers/specs/2026-06-24-tab5-identity-display-design.md`

## Global Constraints

- All edits are in ONE file: `app/R/server_tab5_traditional.R`. Tests in `app/tests/testthat/test-tab5-multi-team-totals.R`.
- These are **display-only** changes. Do NOT alter stat aggregation, the identity merge, or selection/filter keys (keys stay `team_id:player_id`).
- The functions changed are top-level (sourced by `app/tests/testthat/helper-server-mocks.R`), so they are unit-testable directly.
- Keys/labels contract: dropdown keys remain `"<team_id>:<player_id>"`; labels remain `"<name> (<team_name>)"`. Only `<name>` is normalized.
- **Line endings (repo pitfall):** the HEAD blobs of both edited files are pure LF, but the working tree is CRLF. After editing a file, BEFORE staging run `tr -d '\r' < f > f.tmp && mv f.tmp f` then stage with `git -c core.autocrlf=false add <file>`. Verify each commit's diff is only the logical change (`git show --stat HEAD`), not a whole-file flip.
- Test runner (run from repo root): `RSCRIPT="/c/Program Files/R/R-4.4.2/bin/Rscript.exe"` then
  `"$RSCRIPT" -e "setwd('app'); library(testthat); test_dir('tests/testthat', filter='tab5-multi-team-totals', reporter='summary')"`

---

### Task 1: R1 — canonical per-team name in `add_ts_multi_team_totals`

**Files:**
- Modify: `app/R/server_tab5_traditional.R` (function `add_ts_multi_team_totals`, ~lines 243-281)
- Test: `app/tests/testthat/test-tab5-multi-team-totals.R`

**Interfaces:**
- Consumes: existing `add_ts_multi_team_totals(df, lookup, min_teams = 2L)`, `build_ts_total_row`.
- Produces: same signature; additionally, for any `df` row whose `.identity_id` is a multi-team identity and which is not itself a TOTAL row, `Player` is set to the canonical `lookup$display_name`. TOTAL rows already use the canonical name.

- [ ] **Step 1: Write the failing test**

Add to `app/tests/testthat/test-tab5-multi-team-totals.R`:

```r
test_that("add_ts_multi_team_totals normalizes per-team names to the canonical name", {
  df <- data.frame(
    team_id = c(14L, 6L, 9L),
    player_id = c(1143L, 1982L, 555L),
    Player = c("DJ BURNS", "D.J. BURNS", "SOLO GUY"),
    team_name = c("Rishon", "Bnei H", "Ness Z"),
    gp = c(27, 17, 20),
    poss_on_floor = c(1448, 692, 400), minutes = c(900, 500, 600),
    pts = c(341, 166, 200), reb = c(178, 71, 50), oreb = c(52, 24, 10),
    dreb = c(126, 47, 40), ast = c(61, 38, 30), stl = c(25, 13, 5),
    blk = c(8, 6, 2), tov = c(43, 29, 10),
    fgm = c(124, 72, 80), fga = c(242, 132, 150),
    `3pm` = c(19, 1, 10), `3pa` = c(64, 1, 30), ftm = c(74, 21, 20), fta = c(98, 32, 25),
    fg_pct = c(51, 54, 53), tp_pct = c(30, 100, 33), ft_pct = c(75, 66, 80),
    efg = c(55, 55, 56), ts = c(58, 60, 57), usg_pct = c(20, 23, 19),
    check.names = FALSE, stringsAsFactors = FALSE
  )
  # 1143 + 1982 are one identity (canonical "DJ BURNS"); 555 is single-team.
  lookup <- data.frame(
    team_id = c(14L, 6L, 9L), player_id = c(1143L, 1982L, 555L),
    identity_id = c("idBurns", "idBurns", "idSolo"),
    display_name = c("DJ BURNS", "DJ BURNS", "SOLO GUY"),
    stringsAsFactors = FALSE
  )

  out <- add_ts_multi_team_totals(df, lookup)

  burns <- out[out$.identity_id == "idBurns", , drop = FALSE]
  # Both per-team rows AND the TOTAL row all read the canonical "DJ BURNS".
  expect_true(all(burns$Player == "DJ BURNS"))
  expect_false(any(grepl("D\\.J\\.", burns$Player)))
  # Single-team player is untouched.
  solo <- out[out$.identity_id == "idSolo" & !out$is_multi_team_total, , drop = FALSE]
  expect_equal(solo$Player, "SOLO GUY")
})
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `"$RSCRIPT" -e "setwd('app'); library(testthat); test_dir('tests/testthat', filter='tab5-multi-team-totals', reporter='summary')"`
Expected: FAIL — the "D.J. BURNS" per-team row keeps its original name (`all(burns$Player == "DJ BURNS")` is FALSE).

- [ ] **Step 3: Implement the name override**

In `add_ts_multi_team_totals`, locate the block that computes `multi_ids` and returns early when empty:

```r
  team_counts <- tapply(resolved$team_id, resolved$.identity_id,
                        function(t) length(unique(t[!is.na(t)])))
  multi_ids <- names(team_counts[team_counts >= min_teams])
  if (!length(multi_ids)) return(df)
```

Immediately AFTER that block (before the `totals <- lapply(...)` line), insert:

```r
  # Per-team rows of a multi-team identity display the canonical identity name,
  # so the same person reads consistently across teams (and matches the TOTAL).
  if (!is.null(dispmap) && "Player" %in% names(df)) {
    is_total_row <- vapply(df$is_multi_team_total, isTRUE, logical(1))
    member <- !is.na(df$.identity_id) & df$.identity_id %in% multi_ids & !is_total_row
    if (any(member)) {
      canon <- unname(dispmap[df$.identity_id[member]])
      ok <- !is.na(canon) & nzchar(canon)
      idx <- which(member)[ok]
      df$Player[idx] <- canon[ok]
    }
  }
```

(`dispmap` is already built earlier in the function as `identity_id -> display_name`. The TOTAL rows are appended afterward via `build_ts_total_row(..., nm)` which already uses `dispmap[id]`, so they are unaffected by this and remain canonical.)

- [ ] **Step 4: Run the test to verify it passes**

Run: `"$RSCRIPT" -e "setwd('app'); library(testthat); test_dir('tests/testthat', filter='tab5-multi-team-totals', reporter='summary')"`
Expected: PASS (all tests in the file, including the three pre-existing ones).

- [ ] **Step 5: Commit**

```bash
cd /c/Users/ariel/documents/on_off_israel_pbp
for f in app/R/server_tab5_traditional.R app/tests/testthat/test-tab5-multi-team-totals.R; do tr -d '\r' < "$f" > "$f.tmp" && mv "$f.tmp" "$f"; done
git -c core.autocrlf=false add app/R/server_tab5_traditional.R app/tests/testthat/test-tab5-multi-team-totals.R
git commit -m "Tab 5: show canonical name on multi-team per-team rows

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
git show --stat HEAD | head -8   # confirm only the logical change, not an EOL flip
```

---

### Task 2: R2 — group ordering (`ts_group_display_order` + render wiring)

**Files:**
- Modify: `app/R/server_tab5_traditional.R` (add top-level `ts_group_display_order`; wire into the table render block ~lines 1002-1096)
- Test: `app/tests/testthat/test-tab5-multi-team-totals.R`

**Interfaces:**
- Consumes: a data frame with at least `pts`; optionally `.identity_id` and `is_multi_team_total`.
- Produces: `ts_group_display_order(df) -> df` (same columns, rows reordered). Each `.identity_id` group is contiguous; within a group, non-TOTAL rows come first (PTS desc) then the TOTAL row; groups are ordered by the group's max PTS (= the TOTAL's PTS for multi-team groups) descending. The render block calls it and sets the DataTable initial `order` to empty so DT renders in this order.

- [ ] **Step 1: Write the failing test**

Add to `app/tests/testthat/test-tab5-multi-team-totals.R`:

```r
test_that("ts_group_display_order groups team rows with their TOTAL by combined PTS", {
  df <- data.frame(
    team_id = c(1L, 2L, NA_integer_, 5L, 6L, 7L),
    player_id = c(11L, 11L, NA_integer_, 51L, 61L, 71L),
    Player = c("A", "A", "A", "Hi", "Mid", "Lo"),
    team_name = c("T1", "T2", "TOTAL", "T5", "T6", "T7"),
    pts = c(100, 40, 140, 130, 120, 30),
    is_multi_team_total = c(FALSE, FALSE, TRUE, FALSE, FALSE, FALSE),
    .identity_id = c("idA", "idA", "idA", "idHi", "idMid", "idLo"),
    check.names = FALSE, stringsAsFactors = FALSE
  )

  out <- ts_group_display_order(df)

  # idA's combined PTS (140) ranks the whole group first; rows contiguous; TOTAL last.
  expect_equal(out$team_name[1:3], c("T1", "T2", "TOTAL"))
  expect_equal(out$pts[1:3], c(100, 40, 140))
  # Singles follow by PTS desc.
  expect_equal(out$pts[4:6], c(130, 120, 30))
  # The 40-PTS team row is grouped at the top, NOT placed beside the 30-PTS single.
  expect_lt(which(out$team_name == "T2"), which(out$team_name == "T7"))
})

test_that("ts_group_display_order is a no-op without pts / on empty input", {
  expect_equal(nrow(ts_group_display_order(data.frame())), 0L)
  d <- data.frame(a = 1:3)
  expect_identical(ts_group_display_order(d), d)
})
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `"$RSCRIPT" -e "setwd('app'); library(testthat); test_dir('tests/testthat', filter='tab5-multi-team-totals', reporter='summary')"`
Expected: FAIL with "could not find function \"ts_group_display_order\"".

- [ ] **Step 3: Add the `ts_group_display_order` helper**

Insert this new function in `app/R/server_tab5_traditional.R` immediately AFTER the `ts_drop_totals` function (just before `ts_no_data_message`, ~line 289):

```r
# Order rows for display so each player's per-team rows sit together followed by
# its multi-team TOTAL row. The whole group is positioned at the group's combined
# PTS (the TOTAL's PTS = sum of its parts = the group max), so single-team players
# and multi-team groups interleave by PTS desc and the table still reads PTS-first.
# Rows with no resolved identity never group. Pure; safe on missing columns.
ts_group_display_order <- function(df) {
  if (is.null(df) || !nrow(df) || !("pts" %in% names(df))) return(df)
  n <- nrow(df)
  ident <- if (".identity_id" %in% names(df)) as.character(df$.identity_id) else rep(NA_character_, n)
  is_total <- if ("is_multi_team_total" %in% names(df)) {
    vapply(df$is_multi_team_total, isTRUE, logical(1))
  } else {
    rep(FALSE, n)
  }
  # Group key: the identity when present, else a unique per-row token (so
  # unresolved rows never collapse together).
  g <- ifelse(is.na(ident) | !nzchar(ident), paste0(".row", seq_len(n)), ident)
  pts <- suppressWarnings(as.numeric(df$pts))
  pts[is.na(pts)] <- -Inf
  grp_pts <- stats::ave(pts, g, FUN = function(p) max(p, na.rm = TRUE))
  ord <- order(-grp_pts, g, is_total, -pts)
  df[ord, , drop = FALSE]
}
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `"$RSCRIPT" -e "setwd('app'); library(testthat); test_dir('tests/testthat', filter='tab5-multi-team-totals', reporter='summary')"`
Expected: PASS.

- [ ] **Step 5: Wire it into the render block**

In the `output$ts_table <- DT::renderDT(...)` render body:

(a) Just after the line that defaults `is_multi_team_total` (~line 1002):

```r
    if (!("is_multi_team_total" %in% names(df))) df$is_multi_team_total <- FALSE
```

add:

```r
    df <- ts_group_display_order(df)
```

(b) Remove the now-unused initial-order column computation (~lines 1064-1065):

```r
    order_col <- which(grepl("^PTS", names(disp)))
    if (!length(order_col)) order_col <- 6L
```

(c) Change the DataTable `order` option (~line 1091) from:

```r
        order = list(list(order_col - 1L, "desc")),
```

to:

```r
        order = list(),
```

(DT then renders in the R-provided `ts_group_display_order` order; `ordering` stays enabled so clicking a column header re-sorts normally.)

- [ ] **Step 6: Verify no regressions and confirm render still builds**

Run the test file (must stay green):
`"$RSCRIPT" -e "setwd('app'); library(testthat); test_dir('tests/testthat', filter='tab5-multi-team-totals', reporter='summary')"`
Expected: PASS.

Then sanity-check the app source loads without error:
`"$RSCRIPT" -e "setwd('app'); source('R/server_tab5_traditional.R'); cat('loaded; order_col removed:', !any(grepl('order_col', readLines('R/server_tab5_traditional.R'))), '\n')"`
Expected: `loaded; order_col removed: TRUE`.

- [ ] **Step 7: Commit**

```bash
cd /c/Users/ariel/documents/on_off_israel_pbp
for f in app/R/server_tab5_traditional.R app/tests/testthat/test-tab5-multi-team-totals.R; do tr -d '\r' < "$f" > "$f.tmp" && mv "$f.tmp" "$f"; done
git -c core.autocrlf=false add app/R/server_tab5_traditional.R app/tests/testthat/test-tab5-multi-team-totals.R
git commit -m "Tab 5: group a player's team rows with their TOTAL by combined PTS

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
git show --stat HEAD | head -8
```

---

### Task 3: R3 — canonical names in the player dropdown and selected chips

**Files:**
- Modify: `app/R/server_tab5_traditional.R` (function `ts_player_choices` ~lines 134-148; `refresh_ts_player_choices` ~lines 371-375; chip builder ~line 1172)
- Test: `app/tests/testthat/test-tab5-multi-team-totals.R`

**Interfaces:**
- Consumes: existing `ts_player_choices(players_df, teams_df = NULL, team_ids = NULL)`; the identity lookup data frame `(team_id, player_id, identity_id, display_name)` from `load_ts_identity_lookup`.
- Produces: `ts_player_choices(players_df, teams_df = NULL, team_ids = NULL, lookup = NULL)`. When `lookup` has `display_name`, the displayed name for a `(team, player)` is replaced by `lookup$display_name`; keys (`team_id:player_id`) and label format (`"<name> (<team>)"`) are unchanged.

- [ ] **Step 1: Write the failing test**

Add to `app/tests/testthat/test-tab5-multi-team-totals.R`:

```r
test_that("ts_player_choices uses the canonical identity name across a player's teams", {
  players <- data.frame(
    team_id = c(14L, 6L, 9L),
    player_id = c(1143L, 1982L, 555L),
    player_name = c("DJ BURNS", "D.J. BURNS", "SOLO GUY"),
    team_name = c("Rishon", "Bnei H", "Ness Z"),
    stringsAsFactors = FALSE
  )
  lookup <- data.frame(
    team_id = c(14L, 6L), player_id = c(1143L, 1982L),
    identity_id = c("id1", "id1"), display_name = c("DJ BURNS", "DJ BURNS"),
    stringsAsFactors = FALSE
  )

  ch <- ts_player_choices(players, lookup = lookup)

  # Keys remain one entry per (team, player).
  expect_true(all(c("14:1143", "6:1982", "9:555") %in% unname(ch)))
  # Both DJ Burns entries read the canonical name; no "D.J." spelling remains.
  expect_true("DJ BURNS (Rishon)" %in% names(ch))
  expect_true("DJ BURNS (Bnei H)" %in% names(ch))
  expect_false(any(grepl("D\\.J\\.", names(ch))))
  # Unmatched player keeps its original name.
  expect_true("SOLO GUY (Ness Z)" %in% names(ch))
})

test_that("ts_player_choices without a lookup is unchanged", {
  players <- data.frame(
    team_id = 6L, player_id = 1982L,
    player_name = "D.J. BURNS", team_name = "Bnei H",
    stringsAsFactors = FALSE
  )
  ch <- ts_player_choices(players)
  expect_equal(unname(ch), "6:1982")
  expect_equal(names(ch), "D.J. BURNS (Bnei H)")
})
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `"$RSCRIPT" -e "setwd('app'); library(testthat); test_dir('tests/testthat', filter='tab5-multi-team-totals', reporter='summary')"`
Expected: FAIL — `ts_player_choices` does not accept `lookup`, or names still contain "D.J.".

- [ ] **Step 3: Add the `lookup` parameter to `ts_player_choices`**

Replace the whole `ts_player_choices` function (~lines 134-148) with:

```r
ts_player_choices <- function(players_df, teams_df = NULL, team_ids = NULL, lookup = NULL) {
  players <- normalize_ts_players(players_df, teams_df)
  if (!nrow(players)) return(stats::setNames(character(0), character(0)))
  if (!is.null(team_ids) && length(team_ids)) {
    players <- players[players$team_id %in% as.integer(team_ids), , drop = FALSE]
  }
  if (!nrow(players)) return(stats::setNames(character(0), character(0)))

  # Use the canonical identity display name when the lookup resolves a
  # (team, player), so a player who appears under different provider ids/teams
  # reads consistently. Keys stay "<team_id>:<player_id>".
  if (!is.null(lookup) && nrow(lookup) &&
      all(c("team_id", "player_id", "display_name") %in% names(lookup))) {
    dispmap <- stats::setNames(as.character(lookup$display_name),
                               paste0(lookup$team_id, ":", lookup$player_id))
    canon <- unname(dispmap[players$key])
    ok <- !is.na(canon) & nzchar(canon)
    players$player_name[ok] <- canon[ok]
  }

  labels <- ifelse(
    nzchar(players$team_name),
    sprintf("%s (%s)", players$player_name, players$team_name),
    players$player_name
  )
  stats::setNames(players$key, labels)
}
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `"$RSCRIPT" -e "setwd('app'); library(testthat); test_dir('tests/testthat', filter='tab5-multi-team-totals', reporter='summary')"`
Expected: PASS.

- [ ] **Step 5: Wire the lookup into the dropdown refresh and chip builder**

(a) Replace `refresh_ts_player_choices` (~lines 371-375) with:

```r
  refresh_ts_player_choices <- function() {
    gy_int <- suppressWarnings(as.integer(input$game_year))
    lk <- if (length(gy_int) && is.finite(gy_int)) load_ts_identity_lookup(gy_int) else NULL
    choices <- ts_player_choices(ts_ref$players, ts_ref$teams, selected_team_ids_now(), lookup = lk)
    selected <- intersect(input$ts_players %||% character(0), unname(choices))
    updateSelectizeInput(session, "ts_players", choices = choices, selected = selected, server = TRUE)
  }
```

(b) In the chip builder block, change (~line 1172):

```r
      choice_map <- ts_player_choices(ts_ref$players, ts_ref$teams)
```

to:

```r
      gy_int <- suppressWarnings(as.integer(input$game_year))
      lk <- if (length(gy_int) && is.finite(gy_int)) load_ts_identity_lookup(gy_int) else NULL
      choice_map <- ts_player_choices(ts_ref$players, ts_ref$teams, lookup = lk)
```

(`load_ts_identity_lookup` is defined later in the same server function scope; both call sites run at event time, after the body has defined it, so the forward reference resolves.)

- [ ] **Step 6: Verify no regressions and that the source loads**

Run: `"$RSCRIPT" -e "setwd('app'); library(testthat); test_dir('tests/testthat', filter='tab5-multi-team-totals', reporter='summary')"`
Expected: PASS.

Run: `"$RSCRIPT" -e "setwd('app'); source('R/server_tab5_traditional.R'); cat('loaded OK\n')"`
Expected: `loaded OK`.

- [ ] **Step 7: Commit**

```bash
cd /c/Users/ariel/documents/on_off_israel_pbp
for f in app/R/server_tab5_traditional.R app/tests/testthat/test-tab5-multi-team-totals.R; do tr -d '\r' < "$f" > "$f.tmp" && mv "$f.tmp" "$f"; done
git -c core.autocrlf=false add app/R/server_tab5_traditional.R app/tests/testthat/test-tab5-multi-team-totals.R
git commit -m "Tab 5: canonical player name in dropdown and selected chips

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
git show --stat HEAD | head -8
```

---

## Final verification (after all tasks)

- [ ] Run the full Tab 5 test file once more; expect all tests green:
  `"$RSCRIPT" -e "setwd('app'); library(testthat); test_dir('tests/testthat', filter='tab5-multi-team-totals', reporter='summary')"`
- [ ] (Optional, recommended) Launch the app and confirm visually for 2026: a multi-team player (e.g. DJ Burns) shows the same name on both team rows + TOTAL, the three rows are contiguous (team, team, TOTAL), and the dropdown/chips show the canonical name.
  `"$RSCRIPT" -e "shiny::runApp('app')"`
