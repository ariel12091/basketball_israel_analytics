# Team Hub on Home — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Home lands on a team dashboard (identity card, key players, best/worst lineups, Compare-driven storylines) with a team always selected — remembered via localStorage, else the net-rating leader.

**Architecture:** Pure logic goes in `app/R/helpers.R` (tested via testthat, no DB). A new `app/R/mod_team_hub.R` holds the hub UI builder + a plain server function `server_team_hub(input, output, session, shared)` (NOT a namespaced Shiny module — matches the `server_tab*` convention; it must update top-level inputs like `main_tabs`). Data comes only from existing MVs/SQL functions via `cached_season_df()` cross-session caching. Deep links reuse the existing pending-state reactives; `pending_compare_preset` is extended to carry a team.

**Tech Stack:** R 4.4.2, Shiny (bslib/BS5), DBI/RPostgres (no dbplyr), PostgreSQL (Supabase, schema `basketball_test`), testthat.

**Spec:** `docs/superpowers/specs/2026-07-28-team-hub-home-design.md`

**Approved deviations from spec** (implementation realities, confirmed against code):
1. Key players show `"Net RTG Diff"` directly — it is already ±pts per 100 possessions. The `est. ±X pts` annotation applies only to four-factor deltas, so it is dropped here.
2. The "Last 10 vs season" storyline deep-links to Tab 3 (Team Ratings), not Compare — Compare has no last-N preset. The other storylines link to Compare preloaded.
3. Spec's `mod_team_hub.R` filename is kept, but the contents follow the plain `server_tab*` pattern (no `NS()`), because the hub must drive top-level inputs (`main_tabs`, `home_team`, `teams`).

## Global Constraints

- 2-space indent, snake_case. Parameterized SQL only (`$1, $2` placeholders) — never `sprintf()`/`paste0()` for user values.
- Schema is `basketball_test`. All DB access via `db_get_query(pg_pool, ...)`.
- `app/www/app.js` is LF-stored — edit with exact-string edits only, then verify `git diff --stat` shows a small line count (not a whole-file rewrite). Same caution for `app/www/app.css`.
- Never copy a pure helper implementation into `app/tests/testthat/helper-server-mocks.R` — real helpers live in `app/R/helpers.R` (the mocks file sources it).
- All hub client inputs (`hub_remembered_team`, `hub_story_click`) are untrusted — validate against server-side lists, fail closed.
- `uiOutput`/`renderUI` is fine here (output-only HTML blocks); the known pitfall applies to *inputs* rendered via renderUI — do not put inputs inside the hub renders.
- Test run command (from repo root, Git Bash):
  `RSCRIPT="/c/Program Files/R/R-4.4.2/bin/Rscript.exe"; "$RSCRIPT" -e "testthat::test_dir('app/tests/testthat', stop_on_failure = TRUE)"`
- Branch: `shiny/team-hub-home` off `main`. Commit after every task.

---

### Task 1: Pure hub helpers + tests

**Files:**
- Modify: `app/R/helpers.R` (append at end)
- Test: `app/tests/testthat/test-team-hub-helpers.R` (create)

**Interfaces:**
- Consumes: `%||%` (already defined in helpers.R), dplyr (loaded app-wide; use `dplyr::coalesce` explicitly so tests don't rely on attach order).
- Produces (used by Tasks 2–3):
  - `hub_default_team(remembered_id, teams_df, ratings_df)` → character team_id or `""`
  - `hub_identity_data(ratings_df, ff_df, team_id)` → `list(row, n_teams, ff)` or NULL
  - `hub_ff_mini(ff_df, team_id)` → data.frame(label, value, rank, n) or NULL
  - `hub_key_players(onoff_df, team_id, min_on_poss = 100, top_n = 5)` → data.frame or NULL
  - `hub_top_scorer(ts_df, team_id, min_gp = 3)` → 1-row data.frame or NULL
  - `hub_best_worst_lineups(lineups_df)` → `list(best, worst)` or NULL
  - `hub_ordinal(n)` → "1st"/"2nd"/"3rd"/"4th"…
  - `hub_storyline_specs()` → list of specs `(id, preset, min_poss, sentence(a, b))`
  - `hub_storyline_lines(specs, fetch_pair)` → list of `list(id, preset, text)`

- [ ] **Step 1: Create branch**

```bash
git checkout -b shiny/team-hub-home
```

- [ ] **Step 2: Write the failing tests**

Create `app/tests/testthat/test-team-hub-helpers.R`:

```r
# Pure team-hub helpers (R/helpers.R) — no DB, no Shiny session.

teams_df <- data.frame(team_id = c(10L, 20L, 30L),
                       team_name = c("Alpha", "Beta", "Gamma"))

ratings_df <- data.frame(
  team_id = c(10L, 20L, 30L),
  team_name = c("Alpha", "Beta", "Gamma"),
  off_ppp = c(110, 105, 100), def_ppp = c(100, 104, 108),
  net_rtg = c(10, 1, -8),
  games_played = c(10L, 10L, 10L), wins = c(8L, 5L, 2L), losses = c(2L, 5L, 8L),
  off_poss = c(800L, 810L, 790L), def_poss = c(805L, 800L, 795L),
  rank_net_rtg = c(1L, 2L, 3L), rank_off_ppp = c(1L, 2L, 3L), rank_def_ppp = c(1L, 2L, 3L)
)

ff_df <- data.frame(
  team_id = c(10L, 20L, 30L),
  off_efg = c(55, 52, 49), off_tov = c(12, 14, 16),
  off_oreb = c(30, 28, 26), off_ftr = c(25, 22, 20)
)

test_that("hub_default_team prefers a valid remembered id", {
  expect_equal(hub_default_team("20", teams_df, ratings_df), "20")
})

test_that("hub_default_team falls back to net-rating leader", {
  expect_equal(hub_default_team("", teams_df, ratings_df), "10")
  expect_equal(hub_default_team("999", teams_df, ratings_df), "10")
  expect_equal(hub_default_team(NULL, teams_df, ratings_df), "10")
})

test_that("hub_default_team falls back to first team without ratings", {
  expect_equal(hub_default_team("", teams_df, NULL), "10")
  expect_equal(hub_default_team("", teams_df, ratings_df[0, ]), "10")
})

test_that("hub_default_team returns empty string with no teams", {
  expect_equal(hub_default_team("20", teams_df[0, ], ratings_df), "")
})

test_that("hub_identity_data returns team row, league size and ff row", {
  info <- hub_identity_data(ratings_df, ff_df, "20")
  expect_equal(info$row$team_name, "Beta")
  expect_equal(info$n_teams, 3L)
  expect_equal(info$ff$off_efg, 52)
})

test_that("hub_identity_data is NULL for unknown team or empty data", {
  expect_null(hub_identity_data(ratings_df, ff_df, "999"))
  expect_null(hub_identity_data(NULL, ff_df, "10"))
  expect_null(hub_identity_data(ratings_df[0, ], ff_df, "10"))
})

test_that("hub_ff_mini ranks offense factors with TOV inverted", {
  mini <- hub_ff_mini(ff_df, "10")
  expect_equal(nrow(mini), 4L)
  expect_true(all(mini$rank == 1L))  # Alpha best at everything incl. lowest TOV
  mini30 <- hub_ff_mini(ff_df, "30")
  expect_true(all(mini30$rank == 3L))
})

test_that("hub_key_players filters team + min poss, sorts by diff, caps at top_n", {
  onoff_df <- data.frame(
    team_id = c(10L, 10L, 10L, 20L),
    `First Name` = c("A", "B", "C", "D"),
    `Last Name` = c("One", "Two", "Three", "Four"),
    `ON Poss` = c(500, 150, 50, 400),
    `Net RTG Diff` = c(5, 12, 99, 3),
    check.names = FALSE
  )
  out <- hub_key_players(onoff_df, "10", min_on_poss = 100, top_n = 5)
  expect_equal(out[["Last Name"]], c("Two", "One"))  # C excluded (50 poss), sorted desc
  out1 <- hub_key_players(onoff_df, "10", min_on_poss = 100, top_n = 1)
  expect_equal(nrow(out1), 1L)
  expect_null(hub_key_players(onoff_df, "30"))
})

test_that("hub_top_scorer picks highest ppg with min games", {
  ts_df <- data.frame(
    team_id = c(10L, 10L, 10L), player_id = 1:3,
    player_name = c("Low GP", "Scorer", "Role"),
    pts = c(60, 200, 100), gp = c(2L, 10L, 10L)
  )
  out <- hub_top_scorer(ts_df, "10", min_gp = 3)
  expect_equal(out$player_name, "Scorer")
  expect_equal(out$ppg, 20)
  expect_null(hub_top_scorer(ts_df, "20"))
})

test_that("hub_best_worst_lineups computes total_poss and extremes", {
  lu <- data.frame(
    player_names_str = c("L1", "L2", "L3"),
    net_rtg = c(12.5, -8.0, 3.0),
    off_poss = c(100L, 120L, 90L), def_poss = c(110L, 100L, 95L)
  )
  bw <- hub_best_worst_lineups(lu)
  expect_equal(bw$best$player_names_str, "L1")
  expect_equal(bw$worst$player_names_str, "L2")
  expect_equal(bw$best$total_poss, 210)
  expect_null(hub_best_worst_lineups(lu[0, ]))
  expect_null(hub_best_worst_lineups(NULL))
})

test_that("hub_ordinal formats English ordinals", {
  expect_equal(hub_ordinal(1), "1st")
  expect_equal(hub_ordinal(2), "2nd")
  expect_equal(hub_ordinal(3), "3rd")
  expect_equal(hub_ordinal(4), "4th")
  expect_equal(hub_ordinal(11), "11th")
  expect_equal(hub_ordinal(12), "12th")
  expect_equal(hub_ordinal(13), "13th")
  expect_equal(hub_ordinal(22), "22nd")
})

test_that("hub_storyline_specs returns the three v1 specs", {
  specs <- hub_storyline_specs()
  expect_equal(vapply(specs, `[[`, "", "id"),
               c("starters_bench", "clutch", "last10"))
  expect_equal(specs[[1]]$preset, "starters_bench")
  expect_equal(specs[[2]]$preset, "clutch")
  expect_equal(specs[[3]]$preset, "")
})

test_that("hub_storyline_lines qualifies on both sides' possessions and skips failures", {
  row_ok <- data.frame(net_rtg = 8, off_poss = 100L, def_poss = 100L)
  row_thin <- data.frame(net_rtg = 8, off_poss = 20L, def_poss = 20L)
  specs <- list(
    list(id = "a", preset = "p", min_poss = 100,
         sentence = function(a, b) sprintf("diff %+.1f", a$net_rtg - b$net_rtg)),
    list(id = "b", preset = "", min_poss = 100,
         sentence = function(a, b) stop("boom"))
  )
  fetch_pair <- function(id) {
    if (id == "a") list(a = row_ok, b = row_ok) else list(a = row_ok, b = row_ok)
  }
  out <- hub_storyline_lines(specs, fetch_pair)
  expect_equal(length(out), 1L)          # spec "b" errored -> skipped
  expect_equal(out[[1]]$id, "a")
  expect_equal(out[[1]]$text, "diff +0.0")

  fetch_thin <- function(id) list(a = row_ok, b = row_thin)
  expect_equal(length(hub_storyline_lines(specs[1], fetch_thin)), 0L)  # under min_poss
  fetch_null <- function(id) NULL
  expect_equal(length(hub_storyline_lines(specs[1], fetch_null)), 0L)
})

test_that("v1 storyline sentences read correctly in both directions", {
  specs <- hub_storyline_specs()
  a <- data.frame(net_rtg = 6.0, off_poss = 200L, def_poss = 200L)
  b <- data.frame(net_rtg = 1.9, off_poss = 200L, def_poss = 200L)
  sb <- specs[[1]]$sentence(a, b)
  expect_match(sb, "Starter-heavy")
  expect_match(sb, "4.1", fixed = TRUE)
  sb_rev <- specs[[1]]$sentence(b, a)
  expect_match(sb_rev, "Bench-heavy")
  cl <- specs[[2]]$sentence(a, b)
  expect_match(cl, "Clutch")
  l10 <- specs[[3]]$sentence(a, b)
  expect_match(l10, "Last 10")
})
```

- [ ] **Step 3: Run tests to verify they fail**

```bash
RSCRIPT="/c/Program Files/R/R-4.4.2/bin/Rscript.exe"
"$RSCRIPT" -e "testthat::test_dir('app/tests/testthat', filter = 'team-hub', stop_on_failure = TRUE)"
```
Expected: FAIL with "could not find function \"hub_default_team\"" (and siblings).

- [ ] **Step 4: Implement the helpers**

Append to `app/R/helpers.R`:

```r
# ---------------- Team hub (Tab 0) pure helpers ----------------

# Resolve the hub's default team: remembered id if it exists this season,
# else the season's net-rating leader, else the first team, else "".
hub_default_team <- function(remembered_id, teams_df, ratings_df) {
  rid <- trimws(as.character(remembered_id %||% ""))
  ids <- as.character(teams_df$team_id %||% character(0))
  if (!length(ids)) return("")
  if (length(rid) == 1 && nzchar(rid) && rid %in% ids) return(rid)
  if (!is.null(ratings_df) && nrow(ratings_df) > 0 && "rank_net_rtg" %in% names(ratings_df)) {
    leader <- as.character(ratings_df$team_id[which.min(ratings_df$rank_net_rtg)])
    if (length(leader) == 1 && leader %in% ids) return(leader)
  }
  ids[[1]]
}

hub_identity_data <- function(ratings_df, ff_df, team_id) {
  tid <- suppressWarnings(as.integer(team_id))
  if (is.null(ratings_df) || !nrow(ratings_df) || !is.finite(tid)) return(NULL)
  row <- ratings_df[as.integer(ratings_df$team_id) == tid, , drop = FALSE]
  if (!nrow(row)) return(NULL)
  ff_row <- NULL
  if (!is.null(ff_df) && nrow(ff_df) && "team_id" %in% names(ff_df)) {
    fr <- ff_df[as.integer(ff_df$team_id) == tid, , drop = FALSE]
    if (nrow(fr)) ff_row <- fr[1, , drop = FALSE]
  }
  list(row = row[1, , drop = FALSE], n_teams = nrow(ratings_df), ff = ff_row)
}

# Offense four-factor mini-row with league ranks (TOV% rank inverted: low = good).
hub_ff_mini <- function(ff_df, team_id) {
  tid <- suppressWarnings(as.integer(team_id))
  need <- c("team_id", "off_efg", "off_tov", "off_oreb", "off_ftr")
  if (is.null(ff_df) || !nrow(ff_df) || !all(need %in% names(ff_df)) || !is.finite(tid)) return(NULL)
  idx <- which(as.integer(ff_df$team_id) == tid)
  if (!length(idx)) return(NULL)
  cols <- c(off_efg = "eFG%", off_tov = "TOV%", off_oreb = "OREB%", off_ftr = "FTR")
  rows <- lapply(names(cols), function(col) {
    x <- as.numeric(ff_df[[col]])
    r <- rank(if (identical(col, "off_tov")) x else -x, ties.method = "min")
    data.frame(label = cols[[col]], value = x[idx[[1]]], rank = as.integer(r[idx[[1]]]),
               n = sum(is.finite(x)), stringsAsFactors = FALSE)
  })
  do.call(rbind, rows)
}

hub_key_players <- function(onoff_df, team_id, min_on_poss = 100, top_n = 5) {
  need <- c("team_id", "ON Poss", "Net RTG Diff", "First Name", "Last Name")
  if (is.null(onoff_df) || !nrow(onoff_df) || !all(need %in% names(onoff_df))) return(NULL)
  tid <- suppressWarnings(as.integer(team_id))
  if (!is.finite(tid)) return(NULL)
  keep <- as.integer(onoff_df$team_id) == tid &
    dplyr::coalesce(as.numeric(onoff_df[["ON Poss"]]), 0) >= min_on_poss
  df <- onoff_df[which(keep), , drop = FALSE]
  if (!nrow(df)) return(NULL)
  df <- df[order(-as.numeric(df[["Net RTG Diff"]])), , drop = FALSE]
  utils::head(df, top_n)
}

hub_top_scorer <- function(ts_df, team_id, min_gp = 3) {
  need <- c("team_id", "pts", "gp")
  if (is.null(ts_df) || !nrow(ts_df) || !all(need %in% names(ts_df))) return(NULL)
  tid <- suppressWarnings(as.integer(team_id))
  if (!is.finite(tid)) return(NULL)
  keep <- as.integer(ts_df$team_id) == tid &
    dplyr::coalesce(as.numeric(ts_df$gp), 0) >= min_gp
  df <- ts_df[which(keep), , drop = FALSE]
  if (!nrow(df)) return(NULL)
  df$ppg <- as.numeric(df$pts) / pmax(as.numeric(df$gp), 1)
  df[which.max(df$ppg), , drop = FALSE]
}

hub_best_worst_lineups <- function(lineups_df) {
  need <- c("player_names_str", "net_rtg", "off_poss", "def_poss")
  if (is.null(lineups_df) || !nrow(lineups_df) || !all(need %in% names(lineups_df))) return(NULL)
  df <- lineups_df
  df$total_poss <- dplyr::coalesce(as.numeric(df$off_poss), 0) +
    dplyr::coalesce(as.numeric(df$def_poss), 0)
  df <- df[is.finite(as.numeric(df$net_rtg)), , drop = FALSE]
  if (!nrow(df)) return(NULL)
  list(best = df[which.max(as.numeric(df$net_rtg)), , drop = FALSE],
       worst = df[which.min(as.numeric(df$net_rtg)), , drop = FALSE])
}

hub_ordinal <- function(n) {
  n <- as.integer(n)
  suffix <- ifelse(n %% 100 %in% 11:13, "th",
    c("st", "nd", "rd", rep("th", 7))[pmax(pmin(n %% 10, 4), 1) + (n %% 10 == 0) * 3])
  paste0(n, suffix)
}

# Storyline spec list. Each entry: id, Compare preset id ("" = no Compare
# preset; the line deep-links to Tab 3 instead), min sample size per side
# (total possessions), and a sentence builder over two result rows (each with
# net_rtg, off_poss, def_poss).
hub_storyline_specs <- function() {
  list(
    list(
      id = "starters_bench", preset = "starters_bench", min_poss = 100,
      sentence = function(a, b) {
        d <- as.numeric(a$net_rtg) - as.numeric(b$net_rtg)
        who <- if (d >= 0) "Starter-heavy lineups (3+ starters) outscore bench-heavy ones"
               else "Bench-heavy lineups (2 or fewer starters) outscore starter-heavy ones"
        sprintf("%s by %.1f pts per 100", who, abs(d))
      }
    ),
    list(
      id = "clutch", preset = "clutch", min_poss = 100,
      sentence = function(a, b) {
        d <- as.numeric(a$net_rtg) - as.numeric(b$net_rtg)
        sprintf("Clutch net rating %+.1f — %.1f pts per 100 %s than overall",
                as.numeric(a$net_rtg), abs(d), if (d >= 0) "better" else "worse")
      }
    ),
    list(
      id = "last10", preset = "", min_poss = 100,
      sentence = function(a, b) {
        sprintf("Last 10 games: net rating %+.1f vs %+.1f on the season",
                as.numeric(a$net_rtg), as.numeric(b$net_rtg))
      }
    )
  )
}

# Render qualified storylines. fetch_pair(id) returns list(a=row, b=row) or
# NULL; rows carry net_rtg/off_poss/def_poss. Lines that error, miss data, or
# fall under min_poss are skipped entirely (never shown grayed).
hub_storyline_lines <- function(specs, fetch_pair) {
  out <- list()
  for (sp in specs) {
    pair <- tryCatch(fetch_pair(sp$id), error = function(e) NULL)
    if (is.null(pair) || is.null(pair$a) || is.null(pair$b) ||
        !nrow(pair$a) || !nrow(pair$b)) next
    tot <- function(r) dplyr::coalesce(as.numeric(r$off_poss), 0) +
      dplyr::coalesce(as.numeric(r$def_poss), 0)
    if (tot(pair$a) < sp$min_poss || tot(pair$b) < sp$min_poss) next
    txt <- tryCatch(sp$sentence(pair$a, pair$b), error = function(e) NULL)
    if (is.null(txt) || !nzchar(txt)) next
    out[[length(out) + 1L]] <- list(id = sp$id, preset = sp$preset, text = txt)
  }
  out
}
```

- [ ] **Step 5: Run tests to verify they pass**

```bash
"$RSCRIPT" -e "testthat::test_dir('app/tests/testthat', filter = 'team-hub', stop_on_failure = TRUE)"
```
Expected: PASS (all).

- [ ] **Step 6: Run the full suite (regression check)**

```bash
"$RSCRIPT" -e "testthat::test_dir('app/tests/testthat', stop_on_failure = TRUE)"
```
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add app/R/helpers.R app/tests/testthat/test-team-hub-helpers.R
git commit -m "feat(shiny): pure helpers for Home team hub"
```

---

### Task 2: Hub module — UI, data reactives, identity/players/lineups blocks

**Files:**
- Create: `app/R/mod_team_hub.R`
- Modify: `app/app.R` (source list at top; server call after `server_tab7_compare`; `prewarm_for_year` at ~line 354)
- Modify: `app/R/ui_tab0_home.R` (insert hub container after the team selectize, before Row 1)
- Modify: `app/www/app.css` (append hub styles)

**Interfaces:**
- Consumes (Task 1): `hub_identity_data`, `hub_ff_mini`, `hub_key_players`, `hub_top_scorer`, `hub_best_worst_lineups`, `hub_ordinal`.
- Consumes (existing): `cached_season_df`, `db_get_query`, `pg_pool`, `shared_data_version`, `shared$selected_game_year`, `shared$season_date_bounds`, `shared$teams_for_year_df`, `guard_heavy_request`, `normalize_ts_result_cols` (file-scope fn in `server_tab5_traditional.R`, sourced before this module runs).
- Produces: `team_hub_ui()` (UI builder), `server_team_hub(input, output, session, shared)`, file-scope fetchers `hub_fetch_team_ratings(gy, ver)` / `hub_fetch_team_ff(gy, ver)` (also called by prewarm). Outputs: `output$hub_identity`, `output$hub_players`, `output$hub_lineups`, `output$hub_storylines` (storylines filled in Task 3).

**Cache-key discipline:** the onoff and traditional pulls reuse Tab 1 / Tab 5's exact key parts and query so the cache is shared, per the one-key-per-dataset rule:
- `list("onoff_default_mv", gy, ver)` with Tab 1's exact SQL (`server_tab1.R:541-546`).
- `list("player_traditional_stats_mv", gy, ver)` with Tab 5's exact query fun incl. `normalize_ts_result_cols` (`server_tab5_traditional.R:835-851`).

- [ ] **Step 1: Create `app/R/mod_team_hub.R`**

```r
# mod_team_hub.R — Home team hub: identity card, key players, best/worst
# lineups, storylines. Plain server-function pattern (not a namespaced module)
# because it drives top-level inputs (main_tabs, home_team, teams).

# File-scope fetchers so prewarm_for_year (app.R) can warm the same cache keys.
hub_fetch_team_ratings <- function(gy, ver) {
  cached_season_df(
    list("team_ppp_ratings_mv", as.integer(gy), ver),
    function() tryCatch(
      db_get_query(pg_pool,
        "SELECT game_year, team_id, team_name, off_ppp, def_ppp, net_rtg,
                games_played, wins, losses, off_poss, def_poss,
                rank_net_rtg, rank_off_ppp, rank_def_ppp
           FROM basketball_test.team_ppp_ratings_mv
          WHERE game_year = $1::int4
          ORDER BY rank_net_rtg",
        params = list(as.integer(gy))),
      error = function(e) NULL
    )
  )
}

hub_fetch_team_ff <- function(gy, ver) {
  cached_season_df(
    list("team_four_factors_mv", as.integer(gy), ver),
    function() tryCatch(
      db_get_query(pg_pool,
        "SELECT * FROM basketball_test.team_four_factors_mv WHERE game_year = $1::int4",
        params = list(as.integer(gy))),
      error = function(e) NULL
    )
  )
}

team_hub_ui <- function() {
  div(
    id = "team_hub_section",
    uiOutput("hub_identity"),
    fluidRow(
      style = "align-items: stretch;",
      column(width = 6, uiOutput("hub_players")),
      column(width = 6, uiOutput("hub_lineups"))
    ),
    uiOutput("hub_storylines")
  )
}

server_team_hub <- function(input, output, session, shared) {
  hub_ver <- reactive(shared_data_version(shared))
  hub_gy <- reactive({
    gy <- suppressWarnings(as.integer(shared$selected_game_year()))
    req(is.finite(gy))
    gy
  })
  hub_team_id <- reactive({
    tid <- as.character(input$home_team %||% "")
    req(nzchar(tid))
    tid
  })

  hub_ratings_df <- reactive(hub_fetch_team_ratings(hub_gy(), hub_ver()))
  hub_ff_df <- reactive(hub_fetch_team_ff(hub_gy(), hub_ver()))

  hub_onoff_df <- reactive({
    gy <- hub_gy()
    cached_season_df(
      list("onoff_default_mv", gy, hub_ver()),
      function() tryCatch(
        db_get_query(pg_pool,
          'SELECT * FROM basketball_test.onoff_default_mv WHERE "Year" = $1::int4 ORDER BY "Net RTG Diff" DESC, "Team", "Last Name", "First Name"',
          params = list(gy)),
        error = function(e) NULL
      )
    )
  })

  hub_ts_df <- reactive({
    gy <- hub_gy()
    cached_season_df(
      list("player_traditional_stats_mv", gy, hub_ver()),
      function() {
        raw <- tryCatch(
          db_get_query(pg_pool,
            "SELECT *
             FROM basketball_test.player_traditional_stats_mv
             WHERE game_year = $1",
            params = list(gy)),
          error = function(e) NULL
        )
        if (is.null(raw)) return(NULL)
        normalize_ts_result_cols(raw)
      }
    )
  })

  hub_lineups_df <- reactive({
    gy <- hub_gy()
    tid <- hub_team_id()
    b <- shared$season_date_bounds(as.character(gy))
    allowed <- guard_heavy_request(session, key = "hub_lineups",
                                   max_calls = 20L, window_sec = 60L)
    if (!isTRUE(allowed)) return(NULL)
    cached_season_df(
      list("hub_lineups", tid, gy, hub_ver()),
      function() tryCatch(
        db_get_query(pg_pool,
          paste0(
            "SELECT * FROM basketball_test.fetch_lineups_csv_v2(",
            "$1::int4,$2::text,$3::text,$4::text,$5::bool,$6::date,$7::date,$8::int4,$9::int4,",
            "$10::text,$11::text,$12::text,$13::text,$14::text,$15::int4,$16::text,$17::int4,$18::text,$19::int4,$20::bool,",
            "$21::int4,$22::int4,$23::int4,$24::int4,$25::int4,$26::int4,$27::int4,$28::int4,$29::int4",
            ")"
          ),
          params = list(
            5L, tid, NA_character_, NA_character_, FALSE,
            as.Date(b$start), as.Date(b$end), 100L, gy,
            NA_character_, NA_character_, NA_character_, NA_character_,
            NA_character_, NA_integer_, NA_character_,
            NA_integer_, NA_character_, NA_integer_, FALSE,
            NA_integer_, NA_integer_, NA_integer_,
            NA_integer_, NA_integer_, NA_integer_, NA_integer_, NA_integer_, NA_integer_
          )),
        error = function(e) NULL
      )
    )
  })

  # ---- Identity card ----
  output$hub_identity <- renderUI({
    info <- hub_identity_data(hub_ratings_df(), hub_ff_df(), hub_team_id())
    if (is.null(info)) return(NULL)
    r <- info$row
    n <- info$n_teams
    mini <- hub_ff_mini(hub_ff_df(), hub_team_id())
    stat <- function(label, value, rank) {
      div(class = "hub-stat",
        div(class = "hub-stat-value", value),
        div(class = "hub-stat-label", label),
        div(class = "hub-stat-rank", sprintf("%s of %d", hub_ordinal(rank), n)))
    }
    div(
      class = "card bg-dark border-secondary mb-4 hub-card js-shiny-event",
      `data-input-id` = "hub_go_team", role = "button",
      div(class = "card-body",
        div(class = "d-flex justify-content-between align-items-baseline mb-2",
          tags$h5(class = "card-title mb-0", as.character(r$team_name)),
          tags$span(class = "hub-record",
            sprintf("%d–%d", as.integer(r$wins), as.integer(r$losses)))),
        div(class = "hub-stat-row",
          stat("Off PPP", sprintf("%.1f", as.numeric(r$off_ppp)), r$rank_off_ppp),
          stat("Def PPP", sprintf("%.1f", as.numeric(r$def_ppp)), r$rank_def_ppp),
          stat("Net", sprintf("%+.1f", as.numeric(r$net_rtg)), r$rank_net_rtg)),
        if (!is.null(mini)) div(class = "hub-ff-row",
          lapply(seq_len(nrow(mini)), function(i) tags$span(class = "hub-ff-chip",
            sprintf("%s %.1f (%s)", mini$label[[i]], mini$value[[i]],
                    hub_ordinal(mini$rank[[i]])))))
      )
    )
  })

  # ---- Key players ----
  output$hub_players <- renderUI({
    kp <- hub_key_players(hub_onoff_df(), hub_team_id())
    scorer <- hub_top_scorer(hub_ts_df(), hub_team_id())
    if (is.null(kp) && is.null(scorer)) return(NULL)
    div(
      class = "card bg-dark border-secondary mb-4 h-100 hub-card js-shiny-event",
      `data-input-id` = "hub_go_players", role = "button",
      div(class = "card-body",
        tags$h6(class = "hub-block-title", "Key players (on/off impact)"),
        if (!is.null(kp)) tags$ul(class = "hub-list",
          lapply(seq_len(nrow(kp)), function(i) {
            d <- as.numeric(kp[["Net RTG Diff"]][[i]])
            tags$li(
              tags$span(class = "hub-player-name",
                paste(kp[["First Name"]][[i]], kp[["Last Name"]][[i]])),
              tags$span(class = if (d >= 0) "hub-pos" else "hub-neg",
                sprintf("%+.1f / 100", d)))
          })),
        if (!is.null(scorer)) tags$p(class = "hub-footnote",
          sprintf("Top scorer: %s — %.1f ppg",
                  as.character(scorer$player_name %||% scorer$Player), scorer$ppg))
      )
    )
  })

  # ---- Best/worst lineups ----
  output$hub_lineups <- renderUI({
    bw <- hub_best_worst_lineups(hub_lineups_df())
    if (is.null(bw)) return(NULL)
    lineup_row <- function(label, row, cls) {
      div(class = "hub-lineup",
        tags$span(class = paste("hub-lineup-tag", cls), label),
        tags$span(class = "hub-lineup-players", as.character(row$player_names_str)),
        tags$span(class = cls, sprintf("%+.1f net, %d poss",
          as.numeric(row$net_rtg), as.integer(row$total_poss))))
    }
    div(
      class = "card bg-dark border-secondary mb-4 h-100 hub-card js-shiny-event",
      `data-input-id` = "hub_go_lineups", role = "button",
      div(class = "card-body",
        tags$h6(class = "hub-block-title", "Lineups (min 100 poss)"),
        lineup_row("Best", bw$best, "hub-pos"),
        lineup_row("Worst", bw$worst, "hub-neg")
      )
    )
  })

  # ---- Deep links ----
  observeEvent(input$hub_go_team, {
    updateTabsetPanel(session, "main_tabs", selected = "team_ratings")
  })

  observeEvent(input$hub_go_players, {
    teams_df <- shared$teams_for_year_df()
    team_choices <- stats::setNames(as.character(teams_df$team_id),
                                    as.character(teams_df$team_name))
    tid <- as.character(input$home_team %||% "")
    if (nzchar(tid) && tid %in% unname(team_choices)) {
      updateSelectizeInput(session, "teams", choices = team_choices,
                           selected = tid, server = TRUE)
    }
    updateTabsetPanel(session, "main_tabs", selected = "onoff")
  })

  observeEvent(input$hub_go_lineups, {
    tid <- as.character(input$home_team %||% "")
    if (nzchar(tid)) shared$pending_ld_team(tid)
    updateRadioButtons(session, "ld_num", selected = "5")
    updateTabsetPanel(session, "main_tabs", selected = "lineup_data")
  })
}
```

- [ ] **Step 2: Wire into `app/app.R`**

Add to the source list (after line 9, `mod_lineup_player_filter.R`):

```r
source("R/mod_team_hub.R", local = TRUE)
```

Add the server call after `server_tab7_compare(input, output, session, shared)`:

```r
server_team_hub(input, output, session, shared)
```

Extend `prewarm_for_year` (app.R:354-362) — replace its body's fetch block with:

```r
    fetch_teams_distinct(gy_int)
    fetch_teams_min(gy_int)
    fetch_gn_values(gy_int)
    fetch_players_basic(gy_int)
    ver <- tryCatch(shared_data_version(list(data_version = data_version_cache)),
                    error = function(e) "unknown")
    hub_fetch_team_ratings(gy_int, ver)
    hub_fetch_team_ff(gy_int, ver)
    invisible(NULL)
```

- [ ] **Step 3: Insert hub container in `app/R/ui_tab0_home.R`**

After the team-selector `div` (closes at line 34) and before `# Row 1`, insert:

```r
      team_hub_ui(),
```

- [ ] **Step 4: Append hub styles to `app/www/app.css`**

```css
/* ---------------- Home team hub ---------------- */
.hub-card { cursor: pointer; transition: border-color .15s ease; }
.hub-card:hover { border-color: #e8a435 !important; }
.hub-block-title { color: #8b949e; text-transform: uppercase; letter-spacing: .06em; font-size: .72rem; margin-bottom: 10px; }
.hub-record { font-family: "JetBrains Mono", monospace; color: #e6edf3; font-weight: 600; }
.hub-stat-row { display: flex; gap: 24px; flex-wrap: wrap; margin-bottom: 10px; }
.hub-stat-value { font-family: "JetBrains Mono", monospace; font-size: 1.25rem; font-weight: 600; color: #e6edf3; }
.hub-stat-label { color: #8b949e; font-size: .75rem; }
.hub-stat-rank { color: #e8a435; font-size: .72rem; }
.hub-ff-row { display: flex; gap: 8px; flex-wrap: wrap; }
.hub-ff-chip { background: #21262d; border-radius: 999px; padding: 2px 10px; font-size: .72rem; color: #c9d1d9; }
.hub-list { list-style: none; padding: 0; margin: 0; }
.hub-list li { display: flex; justify-content: space-between; padding: 3px 0; font-size: .88rem; }
.hub-pos { color: #34d399; font-family: "JetBrains Mono", monospace; }
.hub-neg { color: #f87171; font-family: "JetBrains Mono", monospace; }
.hub-footnote { color: #8b949e; font-size: .78rem; margin: 8px 0 0 0; }
.hub-lineup { display: flex; gap: 8px; align-items: baseline; padding: 4px 0; font-size: .82rem; flex-wrap: wrap; }
.hub-lineup-tag { font-size: .7rem; text-transform: uppercase; letter-spacing: .05em; }
.hub-lineup-players { color: #c9d1d9; flex: 1 1 100%; }
.hub-story-line { display: block; padding: 6px 10px; border-left: 2px solid #e8a435; margin-bottom: 8px; color: #c9d1d9; font-size: .88rem; cursor: pointer; }
.hub-story-line:hover { background: #21262d; color: #e6edf3; }
@media (max-width: 767px) { .hub-stat-row { gap: 14px; } }
```

- [ ] **Step 5: Verify the app sources and starts**

```bash
"$RSCRIPT" -e "app <- shiny::shinyAppDir('app'); cat('sourced OK\n')"
```
Expected: `sourced OK` with no error. Then run the full test suite:

```bash
"$RSCRIPT" -e "testthat::test_dir('app/tests/testthat', stop_on_failure = TRUE)"
```
Expected: PASS. Check `git diff --stat app/www/app.css` shows only the appended lines.

- [ ] **Step 6: Commit**

```bash
git add app/R/mod_team_hub.R app/app.R app/R/ui_tab0_home.R app/www/app.css
git commit -m "feat(shiny): Home team hub — identity, key players, lineups blocks"
```

---

### Task 3: Storyline block — league-cached dynamic pulls, render, deep links

**Files:**
- Modify: `app/R/mod_team_hub.R` (add dyn fetcher, storyline render + observer inside `server_team_hub`)

**Interfaces:**
- Consumes (Task 1): `hub_storyline_specs()`, `hub_storyline_lines()`.
- Consumes (existing): `get_team_ratings_dynamic` 23-param SQL call (same shape as `server_tab7_compare.R:616-620`). Variant→param mapping mirrors Compare's side-param builder (`server_tab7_compare.R:256-283`): starters gte 3 → `off_min=3, off_max=5`; lte 2 → `off_min=0, off_max=2`; clutch → `max_margin=5, max_time_remaining=300, ot_margin_filter=FALSE`.
- Produces: `output$hub_storylines`; `input$hub_story_click` observer; extended `shared$pending_compare_preset` payload `list(preset = <chr>, team_id = <chr>)` (consumed in Task 4).

- [ ] **Step 1: Add the league-wide dynamic fetcher inside `server_team_hub`** (after `hub_lineups_df`)

```r
  # League-wide get_team_ratings_dynamic pulls, one per storyline variant per
  # season per ETL cycle — every team's hub shares the same cached result.
  HUB_DYN_VARIANTS <- list(
    starters_hi = list(off_min = 3L, off_max = 5L),
    starters_lo = list(off_min = 0L, off_max = 2L),
    clutch      = list(max_margin = 5L, max_time = 300L),
    last10      = list(last_n = 10L)
  )

  hub_dyn_df <- function(variant) {
    gy <- hub_gy()
    v <- HUB_DYN_VARIANTS[[variant]]
    if (is.null(v)) return(NULL)
    allowed <- guard_heavy_request(session, key = "hub_storylines",
                                   max_calls = 20L, window_sec = 60L)
    if (!isTRUE(allowed)) return(NULL)
    cached_season_df(
      list("hub_team_dyn", variant, gy, hub_ver()),
      function() tryCatch(
        db_get_query(pg_pool,
          paste0(
            "SELECT * FROM basketball_test.get_team_ratings_dynamic(",
            "$1::int4,$2::date,$3::date,$4::text,$5::text,$6::text,$7::text,$8::text,$9::int4,$10::text,",
            "$11::int4,$12::text,$13::int4,$14::bool,$15::int4,$16::int4,$17::int4,",
            "$18::int4,$19::int4,$20::int4,$21::int4,$22::int4,$23::int4",
            ")"
          ),
          params = list(
            gy, NA, NA,
            NA_character_, NA_character_, NA_character_, NA_character_,
            NA_character_, NA_integer_, NA_character_,
            v$max_margin %||% NA_integer_, NA_character_,
            v$max_time %||% NA_integer_, FALSE,
            NA_integer_, NA_integer_, v$last_n %||% NA_integer_,
            NA_integer_, NA_integer_,
            v$off_min %||% NA_integer_, v$off_max %||% NA_integer_,
            NA_integer_, NA_integer_
          )),
        error = function(e) NULL
      )
    )
  }

  hub_team_row <- function(df) {
    tid <- suppressWarnings(as.integer(hub_team_id()))
    if (is.null(df) || !nrow(df) || !is.finite(tid)) return(NULL)
    row <- df[as.integer(df$team_id) == tid, , drop = FALSE]
    if (!nrow(row)) return(NULL)
    row[1, , drop = FALSE]
  }
```

- [ ] **Step 2: Add the storyline render + click observer** (after the deep-link observers)

```r
  output$hub_storylines <- renderUI({
    overall <- hub_team_row(hub_ratings_df())
    fetch_pair <- function(id) {
      switch(id,
        starters_bench = list(a = hub_team_row(hub_dyn_df("starters_hi")),
                              b = hub_team_row(hub_dyn_df("starters_lo"))),
        clutch = list(a = hub_team_row(hub_dyn_df("clutch")), b = overall),
        last10 = list(a = hub_team_row(hub_dyn_df("last10")), b = overall),
        NULL
      )
    }
    lines <- hub_storyline_lines(hub_storyline_specs(), fetch_pair)
    if (!length(lines)) return(NULL)
    div(
      class = "card bg-dark border-secondary mb-4 hub-card-static",
      div(class = "card-body",
        tags$h6(class = "hub-block-title", "Storylines"),
        lapply(lines, function(ln) {
          tags$span(
            class = "hub-story-line js-shiny-event",
            `data-input-id` = "hub_story_click",
            `data-shiny-value` = ln$id,
            ln$text
          )
        })
      )
    )
  })

  observeEvent(input$hub_story_click, {
    sid <- as.character(input$hub_story_click %||% "")
    specs <- hub_storyline_specs()
    sp <- NULL
    for (s in specs) if (identical(s$id, sid)) sp <- s
    if (is.null(sp)) return()  # fail closed on unknown client value
    if (nzchar(sp$preset)) {
      shared$pending_compare_preset(list(
        preset = sp$preset,
        team_id = as.character(input$home_team %||% "")
      ))
      updateTabsetPanel(session, "main_tabs", selected = "compare")
    } else {
      updateTabsetPanel(session, "main_tabs", selected = "team_ratings")
    }
  })
```

- [ ] **Step 3: Verify sourcing + suite**

```bash
"$RSCRIPT" -e "app <- shiny::shinyAppDir('app'); cat('sourced OK\n')"
"$RSCRIPT" -e "testthat::test_dir('app/tests/testthat', stop_on_failure = TRUE)"
```
Expected: `sourced OK`, tests PASS.

- [ ] **Step 4: Commit**

```bash
git add app/R/mod_team_hub.R
git commit -m "feat(shiny): hub storylines from cached league-wide dynamic pulls"
```

---

### Task 4: Compare accepts `list(preset, team_id)` pending payload

**Files:**
- Modify: `app/R/server_tab7_compare.R:1615-1624` (pending-preset block inside the tab-init observer)

**Interfaces:**
- Consumes: `shared$pending_compare_preset()` now either a string (legacy, from `go_compare`) or `list(preset = <chr>, team_id = <chr>)` (from Task 3).
- Produces: on list payload with a team, both compare sides get `cmp_a_teams`/`cmp_b_teams` selected to that team after the preset applies.

- [ ] **Step 1: Replace the pending block**

Old (server_tab7_compare.R:1615-1624):

```r
    # Apply pending preset from home tab
    pending <- shared$pending_compare_preset()
    if (!is.null(pending) && nzchar(pending)) {
      shared$pending_compare_preset(NULL)
      reset_compare_side_filters("a", reset_clutch_sliders = FALSE)
      reset_compare_side_filters("b", reset_clutch_sliders = FALSE)
      apply_compare_preset(pending)
      cmp_suppress_preset_echo(pending)
      updateSelectInput(session, "cmp_preset", selected = pending)
    }
```

New:

```r
    # Apply pending preset from home tab. Payload is either a preset id string
    # or list(preset = <id>, team_id = <id>) from the team hub, which also
    # pins both sides to that team.
    pending <- shared$pending_compare_preset()
    if (!is.null(pending)) {
      shared$pending_compare_preset(NULL)
      preset_id <- if (is.list(pending)) as.character(pending$preset %||% "") else as.character(pending)
      preset_team <- if (is.list(pending)) as.character(pending$team_id %||% "") else ""
      if (nzchar(preset_id)) {
        reset_compare_side_filters("a", reset_clutch_sliders = FALSE)
        reset_compare_side_filters("b", reset_clutch_sliders = FALSE)
        apply_compare_preset(preset_id)
        cmp_suppress_preset_echo(preset_id)
        updateSelectInput(session, "cmp_preset", selected = preset_id)
        if (nzchar(preset_team)) {
          updateSelectizeInput(session, "cmp_a_teams", selected = preset_team)
          updateSelectizeInput(session, "cmp_b_teams", selected = preset_team)
        }
      }
    }
```

- [ ] **Step 2: Verify sourcing + suite**

```bash
"$RSCRIPT" -e "app <- shiny::shinyAppDir('app'); cat('sourced OK\n')"
"$RSCRIPT" -e "testthat::test_dir('app/tests/testthat', stop_on_failure = TRUE)"
```
Expected: `sourced OK`, tests PASS (existing tab7 tests must not break — the legacy string path is preserved).

- [ ] **Step 3: Commit**

```bash
git add app/R/server_tab7_compare.R
git commit -m "feat(shiny): compare pending preset carries hub team"
```

---

### Task 5: Default team — localStorage remember + auto-select

**Files:**
- Modify: `app/www/app.js` (localStorage report + store handler, near the `safeLocal*` helpers at ~line 379)
- Modify: `app/app.R` (REMOVE the `home_team` choices observer at lines 531-536 — it moves into the hub module)
- Modify: `app/R/mod_team_hub.R` (default-selection observer + store-on-change observer)

**Interfaces:**
- Consumes (Task 1): `hub_default_team()`; (Task 2): `hub_fetch_team_ratings` via `hub_ratings_df()`.
- Produces: `input$hub_remembered_team` (set once by JS at connect, string team_id or `""`); custom message `ibpl-store-hub-team` `{teamId}` JS-side.

- [ ] **Step 1: Add JS (in `app/www/app.js`, immediately after the `safeLocalRemove` function ~line 389)**

```js
  var hubTeamKey = "ibplHubTeam";

  document.addEventListener("shiny:connected", function() {
    if (!window.Shiny || typeof window.Shiny.setInputValue !== "function") return;
    window.Shiny.setInputValue("hub_remembered_team", safeLocalGet(hubTeamKey) || "");
  });

  if (window.Shiny && typeof window.Shiny.addCustomMessageHandler === "function") {
    window.Shiny.addCustomMessageHandler("ibpl-store-hub-team", function(msg) {
      if (msg && msg.teamId) safeLocalSet(hubTeamKey, String(msg.teamId));
    });
  }
```

Note: `addCustomMessageHandler` at top level is safe — this file only runs inside the Shiny page. After editing run `git diff --stat app/www/app.js` and confirm only ~15 lines changed (LF-preservation check).

- [ ] **Step 2: Remove the old `home_team` observer from `app/app.R`**

Delete lines 531-536:

```r
  observe({
    teams <- shared$teams_for_year_df()
    req(nrow(teams) > 0)
    choices <- c("", setNames(as.character(teams$team_id), teams$team_name))
    updateSelectizeInput(session, "home_team", choices = choices, selected = "", server = TRUE)
  }) |> bindEvent(shared$teams_for_year_df(), ignoreNULL = TRUE)
```

- [ ] **Step 3: Add default-selection + persistence observers to `server_team_hub`** (top of the function body, before the reactives is fine; they only use `shared` and `input`)

```r
  # Populate the Home team selector and auto-select the default team:
  # remembered (localStorage) if valid this season, else net-rating leader.
  # Re-fires when the remembered id arrives from the client; never clobbers a
  # user selection that is still valid for the season.
  observe({
    teams <- shared$teams_for_year_df()
    req(!is.null(teams), nrow(teams) > 0)
    choices <- c("", stats::setNames(as.character(teams$team_id), teams$team_name))
    current <- as.character(input$home_team %||% "")
    if (nzchar(current) && current %in% as.character(teams$team_id)) return()
    ratings <- tryCatch(hub_fetch_team_ratings(
      as.integer(shared$selected_game_year()),
      shared_data_version(shared)
    ), error = function(e) NULL)
    default_id <- hub_default_team(input$hub_remembered_team, teams, ratings)
    updateSelectizeInput(session, "home_team", choices = choices,
                         selected = default_id, server = TRUE)
  }) |> bindEvent(shared$teams_for_year_df(), input$hub_remembered_team,
                  ignoreNULL = FALSE)

  # Persist the user's team choice client-side.
  observeEvent(input$home_team, {
    tid <- as.character(input$home_team %||% "")
    if (nzchar(tid)) {
      session$sendCustomMessage("ibpl-store-hub-team", list(teamId = tid))
    }
  }, ignoreInit = TRUE)
```

- [ ] **Step 4: Verify sourcing + suite + line endings**

```bash
"$RSCRIPT" -e "app <- shiny::shinyAppDir('app'); cat('sourced OK\n')"
"$RSCRIPT" -e "testthat::test_dir('app/tests/testthat', stop_on_failure = TRUE)"
git diff --stat app/www/app.js app/app.R
```
Expected: `sourced OK`, tests PASS, app.js diff ~15 lines / app.R diff ~7 lines (no whole-file EOL rewrite).

- [ ] **Step 5: Commit**

```bash
git add app/www/app.js app/app.R app/R/mod_team_hub.R
git commit -m "feat(shiny): default hub team — localStorage remember, leader fallback"
```

---

### Task 6: Live smoke test and finish

**Files:** none (verification only)

- [ ] **Step 1: Run the app locally**

```bash
"$RSCRIPT" -e "shiny::runApp('app', port = 7666)" &
```

- [ ] **Step 2: Manual checks (browser at http://127.0.0.1:7666)**

1. Home loads with a team auto-selected (first visit → net-rating leader) and all four blocks render with data.
2. Switch teams in the dropdown → all blocks update; reload the page → the same team is pre-selected (localStorage).
3. Click the identity card → Tab 3. Click Key players → Tab 1 filtered to the team. Click Lineups → Tab 2 filtered to the team.
4. Click the starters/bench storyline → Compare opens with the starters_bench preset AND both sides' Teams set to the hub team; results render.
5. Click the clutch storyline → Compare with clutch preset + team. Click the last-10 line → Tab 3.
6. Nav cards still work and pre-filter to the selected team.
7. Change season in the navbar → hub re-renders for the new season; if the remembered team doesn't exist that season, the leader is selected.
8. Narrow the window to mobile width → blocks stack, nothing overflows.
9. Browser F12 console: no JS errors.

- [ ] **Step 3: Full suite one last time**

```bash
"$RSCRIPT" -e "testthat::test_dir('app/tests/testthat', stop_on_failure = TRUE)"
```
Expected: PASS.

- [ ] **Step 4: Finish the branch**

Use superpowers:finishing-a-development-branch — merge `shiny/team-hub-home` into `main` per the repo's workflow (merge locally, push, delete branch). Deployment (`rsconnect::deployApp('app')`) remains a separate, user-triggered step.
