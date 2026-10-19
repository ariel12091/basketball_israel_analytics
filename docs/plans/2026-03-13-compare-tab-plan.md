# Compare Tab Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a new "Compare" tab that lets users compare team/lineup/player performance under two different filter conditions side-by-side, sorted by gap.

**Architecture:** New Shiny tab (tab7) following existing modular pattern — `ui_tab7_compare.R` + `server_tab7_compare.R` sourced from `app.R`. No new SQL functions; reuses existing `get_team_ratings_dynamic`, `get_team_four_factors_dynamic`, `fetch_lineups_csv_v2`, `fetch_lineups_four_factors_csv`, and `get_player_traditional_dynamic` — each called twice (Side A, Side B) and joined by entity key.

**Tech Stack:** R/Shiny, bslib BS5, DBI/RPostgres, DT

**Spec:** `docs/plans/2026-03-13-compare-tab-design.md`

---

## File Structure

| File | Action | Responsibility |
|------|--------|----------------|
| `app/R/ui_tab7_compare.R` | Create | Compare tab UI: mode tabs, sidebar with two filter panels, metric chips, summary cards, results table |
| `app/R/server_tab7_compare.R` | Create | Compare tab server: collect Side A/B params, call SQL, join, compute gap, render table |
| `app/app.R` | Modify | Source new files, add tab to navbar, add `pending_compare_preset` to shared, add home card handler |
| `app/R/ui_tab0_home.R` | Modify | Add 5th card: "How do starters compare to the bench?" |
| `app/tests/testthat/test-tab-wiring.R` | Modify | Add tab7 to `TAB_SPECS` |

---

## Chunk 1: Tab skeleton + Teams mode

### Task 1: Create UI skeleton

**Files:**
- Create: `app/R/ui_tab7_compare.R`

- [ ] **Step 1: Create the UI file with full layout**

```r
# ui_tab7_compare.R - Tab 7: Compare

ui_tab7_compare <- tabPanel(
  title = tags$span(tags$i(class = "bi bi-arrow-left-right"), "Compare"),
  value = "compare",
  fluidPage(
    shared_head_tags(),
    sidebarLayout(
      sidebarPanel(
        width = 3,

        # Mode tabs
        radioButtons("cmp_mode", label = NULL,
                     choices = c("Teams", "Lineups", "Players"),
                     selected = "Teams", inline = TRUE),

        # Preset dropdown
        selectInput("cmp_preset", "Quick preset",
                    choices = c("— Custom —" = "",
                                "Starters vs Bench" = "starters_bench",
                                "Clutch vs Non-Clutch" = "clutch",
                                "Home vs Away" = "home_away",
                                "Win vs Loss" = "win_loss"),
                    selected = ""),

        tags$hr(),

        # ── Side A ──
        tags$div(
          class = "mb-2",
          tags$span(class = "badge rounded-pill",
                    style = "background: rgba(123,140,222,.2); color: #7b8cde; border: 1px solid rgba(123,140,222,.4);",
                    "Side A")
        ),

        # Player picker (Players mode only)
        conditionalPanel(
          condition = "input.cmp_mode == 'Players'",
          selectizeInput("cmp_player_a", "Player A", choices = NULL,
                         options = list(placeholder = "Search player..."))
        ),

        # Starters filter (Teams + Lineups only)
        conditionalPanel(
          condition = "input.cmp_mode != 'Players'",
          fluidRow(
            column(6, selectInput("cmp_a_starters_mode", "Starters",
                                  choices = c("ALL" = "", "At least (>=)" = "gte", "At most (<=)" = "lte"), selected = "")),
            column(6, selectInput("cmp_a_starters_val", "Value",
                                  choices = c("—" = "", as.character(0:5)), selected = ""))
          )
        ),

        selectInput("cmp_a_home_away", "Home/Away",
                    choices = c("All" = "", "Home" = "home", "Away" = "away"), selected = ""),
        selectInput("cmp_a_outcome", "Win/Loss",
                    choices = c("All" = "", "Win" = "win", "Loss" = "loss"), selected = ""),
        checkboxInput("cmp_a_clutch", "Clutch", value = FALSE),
        conditionalPanel(
          condition = "input.cmp_a_clutch == true",
          sliderInput("cmp_a_clutch_margin", "Margin", min = 0, max = 10, value = 5, step = 1),
          sliderInput("cmp_a_clutch_minutes", "Minutes left", min = 1, max = 5, value = 5, step = 1)
        ),
        fluidRow(
          column(6, selectInput("cmp_a_cutoff_type", "Before/After",
                                choices = c("—" = "", "Before GN" = "before_gn", "After GN" = "after_gn",
                                            "Before date" = "before_date", "After date" = "after_date"), selected = "")),
          column(6, uiOutput("cmp_a_cutoff_value_ui"))
        ),
        selectizeInput("cmp_a_opponents", "Opponents", choices = NULL, multiple = TRUE,
                       options = list(placeholder = "All opponents")),
        selectizeInput("cmp_a_game_type", "Game type",
                       choices = c("All" = "", "Regular season" = "5", "Playoffs – QF" = "16",
                                   "Playoffs – Finals" = "17", "Playoffs – SF" = "26",
                                   "Play-in" = "33", "Winner Cup" = "34", "State Cup" = "35"),
                       selected = "", multiple = TRUE, options = list(placeholder = "All")),
        fluidRow(
          column(6, selectInput("cmp_a_opp_rank_side", "Opp rank",
                                choices = c("Off" = "", "Top" = "top", "Bottom" = "bottom"), selected = "")),
          column(6, selectInput("cmp_a_opp_rank_n", "N",
                                choices = c("—" = "", as.character(1:12)), selected = ""))
        ),

        # ── vs divider ──
        tags$div(class = "text-center text-muted fw-bold my-2", "— vs —"),

        # ── Side B ──
        tags$div(
          class = "mb-2",
          tags$span(class = "badge rounded-pill",
                    style = "background: rgba(232,164,53,.15); color: #e8a435; border: 1px solid rgba(232,164,53,.35);",
                    "Side B")
        ),

        conditionalPanel(
          condition = "input.cmp_mode == 'Players'",
          selectizeInput("cmp_player_b", "Player B", choices = NULL,
                         options = list(placeholder = "Search player..."))
        ),

        conditionalPanel(
          condition = "input.cmp_mode != 'Players'",
          fluidRow(
            column(6, selectInput("cmp_b_starters_mode", "Starters",
                                  choices = c("ALL" = "", "At least (>=)" = "gte", "At most (<=)" = "lte"), selected = "")),
            column(6, selectInput("cmp_b_starters_val", "Value",
                                  choices = c("—" = "", as.character(0:5)), selected = ""))
          )
        ),

        selectInput("cmp_b_home_away", "Home/Away",
                    choices = c("All" = "", "Home" = "home", "Away" = "away"), selected = ""),
        selectInput("cmp_b_outcome", "Win/Loss",
                    choices = c("All" = "", "Win" = "win", "Loss" = "loss"), selected = ""),
        checkboxInput("cmp_b_clutch", "Clutch", value = FALSE),
        conditionalPanel(
          condition = "input.cmp_b_clutch == true",
          sliderInput("cmp_b_clutch_margin", "Margin", min = 0, max = 10, value = 5, step = 1),
          sliderInput("cmp_b_clutch_minutes", "Minutes left", min = 1, max = 5, value = 5, step = 1)
        ),
        fluidRow(
          column(6, selectInput("cmp_b_cutoff_type", "Before/After",
                                choices = c("—" = "", "Before GN" = "before_gn", "After GN" = "after_gn",
                                            "Before date" = "before_date", "After date" = "after_date"), selected = "")),
          column(6, uiOutput("cmp_b_cutoff_value_ui"))
        ),
        selectizeInput("cmp_b_opponents", "Opponents", choices = NULL, multiple = TRUE,
                       options = list(placeholder = "All opponents")),
        selectizeInput("cmp_b_game_type", "Game type",
                       choices = c("All" = "", "Regular season" = "5", "Playoffs – QF" = "16",
                                   "Playoffs – Finals" = "17", "Playoffs – SF" = "26",
                                   "Play-in" = "33", "Winner Cup" = "34", "State Cup" = "35"),
                       selected = "", multiple = TRUE, options = list(placeholder = "All")),
        fluidRow(
          column(6, selectInput("cmp_b_opp_rank_side", "Opp rank",
                                choices = c("Off" = "", "Top" = "top", "Bottom" = "bottom"), selected = "")),
          column(6, selectInput("cmp_b_opp_rank_n", "N",
                                choices = c("—" = "", as.character(1:12)), selected = ""))
        ),

        tags$hr(),
        actionButton("cmp_go", "Compare \u2192",
                     class = "btn btn-warning w-100 fw-bold"),
        actionButton("cmp_reset", "Reset",
                     class = "btn btn-outline-secondary btn-sm w-100 mt-2")
      ),

      mainPanel(
        width = 9,

        # Metric chips
        div(
          class = "d-flex align-items-center gap-2 mb-3 flex-wrap",
          tags$span(class = "text-muted small text-uppercase", "Metric"),
          uiOutput("cmp_metric_chips_ui")
        ),

        # Rate mode (Players mode only)
        conditionalPanel(
          condition = "input.cmp_mode == 'Players'",
          div(
            class = "mb-3",
            radioButtons("cmp_rate_mode", NULL,
                         choices = c("Per Game", "Per 75 Possessions", "Totals"),
                         selected = "Per Game", inline = TRUE)
          )
        ),

        # Summary cards
        fluidRow(
          column(4, div(class = "card bg-dark border-secondary p-3 mb-3",
            tags$div(class = "small text-uppercase", style = "color: #7b8cde;", "Side A"),
            tags$div(class = "fs-4 fw-bold", style = "color: #4caf7d;", textOutput("cmp_summary_a", inline = TRUE)),
            tags$div(class = "small text-muted", textOutput("cmp_summary_a_label", inline = TRUE))
          )),
          column(4, div(class = "card bg-dark border-secondary p-3 mb-3",
            tags$div(class = "small text-uppercase", style = "color: #e8a435;", "Side B"),
            tags$div(class = "fs-4 fw-bold", style = "color: #e05c5c;", textOutput("cmp_summary_b", inline = TRUE)),
            tags$div(class = "small text-muted", textOutput("cmp_summary_b_label", inline = TRUE))
          )),
          column(4, div(class = "card bg-dark border-secondary p-3 mb-3",
            tags$div(class = "small text-uppercase text-muted", "Avg Gap"),
            tags$div(class = "fs-4 fw-bold", style = "color: #e8a435;", textOutput("cmp_summary_gap", inline = TRUE)),
            tags$div(class = "small text-muted", "league-wide")
          ))
        ),

        # Results table
        DT::dataTableOutput("cmp_table"),

        # Filter chips
        uiOutput("cmp_filter_chips")
      )
    )
  )
)
```

- [ ] **Step 2: Commit**

```bash
git add app/R/ui_tab7_compare.R
git commit -m "feat: add Compare tab UI skeleton"
```

### Task 2: Create server skeleton with Teams mode

**Files:**
- Create: `app/R/server_tab7_compare.R`

- [ ] **Step 1: Create the server file with core infrastructure**

The server needs:
- `collect_side_params(side)` — helper that reads all `cmp_{side}_*` inputs and returns a named list
- `starters_to_sql_params(mode, val)` — converts "gte"/"lte" + value into `num_starters_off`, `num_starters_off_min`, `num_starters_off_max`
- `cutoff_to_date_gn(type, val, season_bounds)` — converts before/after cutoff into `start_d`/`end_d` or `min_gn`/`max_gn`
- Preset observer — fills Side A/B inputs when a preset is selected
- `cmp_go` observer — runs the comparison
- Teams mode: calls `get_team_ratings_dynamic` twice, joins by `team_id`, computes gap
- Metric chip rendering and selection tracking
- Summary card rendering
- DT table rendering with inline dumbbell JS

```r
# server_tab7_compare.R - Tab 7: Compare server logic

server_tab7_compare <- function(input, output, session, shared) {

  cmp_ref <- reactiveValues(
    result_a = NULL,
    result_b = NULL,
    joined = NULL,
    teams = NULL,
    players = NULL
  )
  selected_metric <- reactiveVal("net_rtg")

  # ── Helpers ──

  collect_side_params <- function(side) {
    pfx <- paste0("cmp_", side, "_")
    get_input <- function(name) input[[paste0(pfx, name)]]

    gy <- as.integer(input$game_year)
    b <- shared$season_date_bounds(as.character(gy))

    # Cutoff handling
    cutoff_type <- get_input("cutoff_type") %||% ""
    cutoff_val <- input[[paste0(pfx, "cutoff_value")]]
    start_d <- b$start
    end_d <- b$end
    min_gn <- NA_integer_
    max_gn <- NA_integer_
    last_n <- NA_integer_

    if (nzchar(cutoff_type) && !is.null(cutoff_val) && nzchar(cutoff_val)) {
      if (cutoff_type == "before_gn") max_gn <- as.integer(cutoff_val)
      else if (cutoff_type == "after_gn") min_gn <- as.integer(cutoff_val)
      else if (cutoff_type == "before_date") end_d <- as.Date(cutoff_val)
      else if (cutoff_type == "after_date") start_d <- as.Date(cutoff_val)
    }

    # Starters
    st_mode <- get_input("starters_mode") %||% ""
    st_val <- get_input("starters_val") %||% ""
    num_starters_off <- NA_integer_
    num_starters_off_min <- NA_integer_
    num_starters_off_max <- NA_integer_
    if (nzchar(st_mode) && nzchar(st_val)) {
      v <- as.integer(st_val)
      if (st_mode == "gte") { num_starters_off_min <- v; num_starters_off_max <- 5L }
      else if (st_mode == "lte") { num_starters_off_min <- 0L; num_starters_off_max <- v }
    }

    # Clutch
    clutch_on <- isTRUE(get_input("clutch"))
    max_margin <- if (clutch_on) as.integer(get_input("clutch_margin")) else NA_integer_
    margin_status <- NA_character_
    max_time_remaining <- if (clutch_on) as.integer(get_input("clutch_minutes")) else NA_integer_
    ot_margin_filter <- FALSE

    # Opponents
    opp_sel <- get_input("opponents") %||% character(0)
    td <- cmp_ref$teams
    opp_ids_csv <- NA_character_
    if (length(opp_sel) && !is.null(td)) {
      ids <- td$team_id[td$team_name %in% opp_sel]
      if (length(ids)) opp_ids_csv <- paste(ids, collapse = ",")
    }

    # Game type
    gt <- get_input("game_type") %||% character(0)
    game_type_csv <- if (length(gt) && any(nzchar(gt))) paste(gt[nzchar(gt)], collapse = ",") else NA_character_

    # Home/away, outcome
    home_away <- get_input("home_away") %||% ""
    if (!nzchar(home_away)) home_away <- NA_character_
    outcome <- get_input("outcome") %||% ""
    if (!nzchar(outcome)) outcome <- NA_character_

    # Opp rank
    opp_rank_side <- get_input("opp_rank_side") %||% ""
    if (!nzchar(opp_rank_side)) opp_rank_side <- NA_character_
    opp_rank_n_val <- get_input("opp_rank_n") %||% ""
    opp_rank_n <- if (nzchar(opp_rank_n_val)) as.integer(opp_rank_n_val) else NA_integer_
    opp_rank_metric <- NA_character_  # default net

    list(
      game_year = gy, start_d = start_d, end_d = end_d,
      game_type_csv = game_type_csv, opp_ids_csv = opp_ids_csv,
      home_away = home_away, outcome = outcome,
      opp_rank_side = opp_rank_side, opp_rank_n = opp_rank_n,
      opp_rank_metric = opp_rank_metric,
      max_margin = max_margin, margin_status = margin_status,
      max_time_remaining = max_time_remaining, ot_margin_filter = ot_margin_filter,
      min_gn = min_gn, max_gn = max_gn, last_n_games = last_n,
      num_starters_off = num_starters_off, num_starters_def = NA_integer_,
      num_starters_off_min = num_starters_off_min,
      num_starters_off_max = num_starters_off_max,
      num_starters_def_min = NA_integer_, num_starters_def_max = NA_integer_
    )
  }

  run_team_ratings <- function(p) {
    allowed <- guard_heavy_request(
      session, key = "cmp_team_ratings",
      start_d = p$start_d, end_d = p$end_d,
      min_gn = p$min_gn, max_gn = p$max_gn, last_n = p$last_n_games,
      max_calls = 50L, window_sec = 60L
    )
    if (!isTRUE(allowed)) return(data.frame())
    DBI::dbGetQuery(pg_pool, paste0(
      "SELECT * FROM basketball_test.get_team_ratings_dynamic(",
      "$1::int4,$2::date,$3::date,$4::text,$5::text,$6::text,$7::text,$8::text,$9::int4,$10::text,",
      "$11::int4,$12::text,$13::int4,$14::bool,$15::int4,$16::int4,$17::int4,",
      "$18::int4,$19::int4,$20::int4,$21::int4,$22::int4,$23::int4",
      ")"), params = list(
      p$game_year, as.Date(p$start_d), as.Date(p$end_d),
      p$game_type_csv, p$opp_ids_csv, p$home_away, p$outcome,
      p$opp_rank_side, p$opp_rank_n, p$opp_rank_metric,
      p$max_margin, p$margin_status, p$max_time_remaining, p$ot_margin_filter,
      p$min_gn, p$max_gn, p$last_n_games,
      p$num_starters_off, p$num_starters_def,
      p$num_starters_off_min, p$num_starters_off_max,
      p$num_starters_def_min, p$num_starters_def_max
    ))
  }

  run_team_ff <- function(p) {
    allowed <- guard_heavy_request(
      session, key = "cmp_team_ff",
      start_d = p$start_d, end_d = p$end_d,
      min_gn = p$min_gn, max_gn = p$max_gn, last_n = p$last_n_games,
      max_calls = 50L, window_sec = 60L
    )
    if (!isTRUE(allowed)) return(data.frame())
    DBI::dbGetQuery(pg_pool, paste0(
      "SELECT * FROM basketball_test.get_team_four_factors_dynamic(",
      "$1::int4,$2::date,$3::date,$4::text,$5::text,$6::text,$7::text,$8::text,$9::int4,$10::text,",
      "$11::int4,$12::text,$13::int4,$14::bool,$15::int4,$16::int4,$17::int4,",
      "$18::int4,$19::int4,$20::int4,$21::int4,$22::int4,$23::int4",
      ")"), params = list(
      p$game_year, as.Date(p$start_d), as.Date(p$end_d),
      p$game_type_csv, p$opp_ids_csv, p$home_away, p$outcome,
      p$opp_rank_side, p$opp_rank_n, p$opp_rank_metric,
      p$max_margin, p$margin_status, p$max_time_remaining, p$ot_margin_filter,
      p$min_gn, p$max_gn, p$last_n_games,
      p$num_starters_off, p$num_starters_def,
      p$num_starters_off_min, p$num_starters_off_max,
      p$num_starters_def_min, p$num_starters_def_max
    ))
  }

  # ── Metric chip definitions per mode ──

  TEAM_METRICS <- c(
    "Net Rtg" = "net_rtg", "Offense" = "off_ppp", "Defense" = "def_ppp",
    "TS%" = "off_ts_pct", "TOV%" = "off_tov_pct", "OREB%" = "off_oreb_pct", "FTR" = "off_ftr"
  )

  output$cmp_metric_chips_ui <- renderUI({
    metrics <- if (input$cmp_mode == "Players") {
      c("PPG" = "ppg", "RPG" = "rpg", "APG" = "apg", "SPG" = "spg",
        "FG%" = "fg_pct", "3P%" = "fg3_pct", "FT%" = "ft_pct", "TS%" = "ts_pct")
    } else {
      TEAM_METRICS
    }
    cur <- isolate(selected_metric())
    if (!(cur %in% metrics)) {
      selected_metric(metrics[[1]])
      cur <- metrics[[1]]
    }
    chips <- lapply(seq_along(metrics), function(i) {
      nm <- names(metrics)[i]
      val <- metrics[[i]]
      cls <- if (identical(val, cur)) "btn btn-sm btn-warning" else "btn btn-sm btn-outline-secondary"
      actionButton(
        paste0("cmp_metric_", val), nm,
        class = cls, style = "border-radius: 20px; padding: 2px 12px; font-size: .76rem;"
      )
    })
    do.call(tagList, chips)
  })

  # Observe metric chip clicks
  observe({
    mode <- input$cmp_mode
    metrics <- if (mode == "Players") {
      c("ppg", "rpg", "apg", "spg", "fg_pct", "fg3_pct", "ft_pct", "ts_pct")
    } else {
      unname(TEAM_METRICS)
    }
    lapply(metrics, function(m) {
      observeEvent(input[[paste0("cmp_metric_", m)]], {
        selected_metric(m)
      }, ignoreInit = TRUE)
    })
  })

  # ── Tab init: load ref data ──

  observeEvent(list(input$main_tabs, input$game_year), ignoreInit = TRUE, {
    if (!identical(input$main_tabs, "compare")) return(NULL)
    gy_int <- as.integer(input$game_year)

    teams_df <- cached_ref_query(
      key = sprintf("cmp_teams_%d", gy_int),
      query_fun = function() DBI::dbGetQuery(pg_pool, sprintf(
        "SELECT DISTINCT team_id, team_name FROM basketball_test.full_rosters WHERE game_year = %d ORDER BY team_name", gy_int))
    )
    cmp_ref$teams <- teams_df
    updateSelectizeInput(session, "cmp_a_opponents", choices = teams_df$team_name, selected = character(0), server = TRUE)
    updateSelectizeInput(session, "cmp_b_opponents", choices = teams_df$team_name, selected = character(0), server = TRUE)

    players_df <- cached_ref_query(
      key = sprintf("cmp_players_%d", gy_int),
      query_fun = function() DBI::dbGetQuery(pg_pool, sprintf(
        "SELECT team_id, player_id, MIN(btrim(firstname)||' '||btrim(lastname)) AS name FROM basketball_test.full_rosters WHERE game_year = %d GROUP BY team_id, player_id ORDER BY MIN(btrim(firstname)||' '||btrim(lastname))", gy_int))
    )
    cmp_ref$players <- players_df
    player_choices <- setNames(players_df$player_id, players_df$name)
    updateSelectizeInput(session, "cmp_player_a", choices = player_choices, selected = character(0), server = TRUE)
    updateSelectizeInput(session, "cmp_player_b", choices = player_choices, selected = character(0), server = TRUE)

    # Apply pending preset from home tab
    pending <- shared$pending_compare_preset()
    if (!is.null(pending) && nzchar(pending)) {
      shared$pending_compare_preset(NULL)
      updateSelectInput(session, "cmp_preset", selected = pending)
    }
  })

  # ── Preset handler ──

  observeEvent(input$cmp_preset, {
    preset <- input$cmp_preset
    if (is.null(preset) || !nzchar(preset)) return()

    if (preset == "starters_bench") {
      updateSelectInput(session, "cmp_a_starters_mode", selected = "gte")
      updateSelectInput(session, "cmp_a_starters_val", selected = "3")
      updateSelectInput(session, "cmp_b_starters_mode", selected = "lte")
      updateSelectInput(session, "cmp_b_starters_val", selected = "2")
      # Clear other filters on both sides
      for (s in c("a", "b")) {
        updateSelectInput(session, paste0("cmp_", s, "_home_away"), selected = "")
        updateSelectInput(session, paste0("cmp_", s, "_outcome"), selected = "")
        updateCheckboxInput(session, paste0("cmp_", s, "_clutch"), value = FALSE)
        updateSelectInput(session, paste0("cmp_", s, "_cutoff_type"), selected = "")
        updateSelectizeInput(session, paste0("cmp_", s, "_opponents"), selected = character(0))
        updateSelectizeInput(session, paste0("cmp_", s, "_game_type"), selected = character(0))
        updateSelectInput(session, paste0("cmp_", s, "_opp_rank_side"), selected = "")
        updateSelectInput(session, paste0("cmp_", s, "_opp_rank_n"), selected = "")
      }
    } else if (preset == "clutch") {
      # Side A: clutch on
      updateCheckboxInput(session, "cmp_a_clutch", value = TRUE)
      updateSliderInput(session, "cmp_a_clutch_margin", value = 5)
      updateSliderInput(session, "cmp_a_clutch_minutes", value = 2)
      # Side B: clutch off (defaults)
      updateCheckboxInput(session, "cmp_b_clutch", value = FALSE)
      # Clear starters + other filters
      for (s in c("a", "b")) {
        updateSelectInput(session, paste0("cmp_", s, "_starters_mode"), selected = "")
        updateSelectInput(session, paste0("cmp_", s, "_starters_val"), selected = "")
        updateSelectInput(session, paste0("cmp_", s, "_home_away"), selected = "")
        updateSelectInput(session, paste0("cmp_", s, "_outcome"), selected = "")
        updateSelectInput(session, paste0("cmp_", s, "_cutoff_type"), selected = "")
        updateSelectizeInput(session, paste0("cmp_", s, "_opponents"), selected = character(0))
        updateSelectizeInput(session, paste0("cmp_", s, "_game_type"), selected = character(0))
        updateSelectInput(session, paste0("cmp_", s, "_opp_rank_side"), selected = "")
        updateSelectInput(session, paste0("cmp_", s, "_opp_rank_n"), selected = "")
      }
    } else if (preset == "home_away") {
      updateSelectInput(session, "cmp_a_home_away", selected = "home")
      updateSelectInput(session, "cmp_b_home_away", selected = "away")
      for (s in c("a", "b")) {
        updateSelectInput(session, paste0("cmp_", s, "_starters_mode"), selected = "")
        updateSelectInput(session, paste0("cmp_", s, "_starters_val"), selected = "")
        updateSelectInput(session, paste0("cmp_", s, "_outcome"), selected = "")
        updateCheckboxInput(session, paste0("cmp_", s, "_clutch"), value = FALSE)
        updateSelectInput(session, paste0("cmp_", s, "_cutoff_type"), selected = "")
        updateSelectizeInput(session, paste0("cmp_", s, "_opponents"), selected = character(0))
        updateSelectizeInput(session, paste0("cmp_", s, "_game_type"), selected = character(0))
        updateSelectInput(session, paste0("cmp_", s, "_opp_rank_side"), selected = "")
        updateSelectInput(session, paste0("cmp_", s, "_opp_rank_n"), selected = "")
      }
    } else if (preset == "win_loss") {
      updateSelectInput(session, "cmp_a_outcome", selected = "win")
      updateSelectInput(session, "cmp_b_outcome", selected = "loss")
      for (s in c("a", "b")) {
        updateSelectInput(session, paste0("cmp_", s, "_starters_mode"), selected = "")
        updateSelectInput(session, paste0("cmp_", s, "_starters_val"), selected = "")
        updateSelectInput(session, paste0("cmp_", s, "_home_away"), selected = "")
        updateCheckboxInput(session, paste0("cmp_", s, "_clutch"), value = FALSE)
        updateSelectInput(session, paste0("cmp_", s, "_cutoff_type"), selected = "")
        updateSelectizeInput(session, paste0("cmp_", s, "_opponents"), selected = character(0))
        updateSelectizeInput(session, paste0("cmp_", s, "_game_type"), selected = character(0))
        updateSelectInput(session, paste0("cmp_", s, "_opp_rank_side"), selected = "")
        updateSelectInput(session, paste0("cmp_", s, "_opp_rank_n"), selected = "")
      }
    }
  }, ignoreInit = TRUE)

  # ── Before/After cutoff dynamic UI ──

  output$cmp_a_cutoff_value_ui <- renderUI({
    ct <- input$cmp_a_cutoff_type %||% ""
    if (!nzchar(ct)) return(NULL)
    if (grepl("gn", ct)) {
      selectizeInput("cmp_a_cutoff_value", "GN", choices = c("", as.character(1:40)),
                     selected = "", options = list(placeholder = "#"))
    } else {
      b <- shared$season_date_bounds(input$game_year %||% DEFAULT_GAME_YEAR)
      dateInput("cmp_a_cutoff_value", "Date", value = b$start, min = b$start, max = b$end)
    }
  })

  output$cmp_b_cutoff_value_ui <- renderUI({
    ct <- input$cmp_b_cutoff_type %||% ""
    if (!nzchar(ct)) return(NULL)
    if (grepl("gn", ct)) {
      selectizeInput("cmp_b_cutoff_value", "GN", choices = c("", as.character(1:40)),
                     selected = "", options = list(placeholder = "#"))
    } else {
      b <- shared$season_date_bounds(input$game_year %||% DEFAULT_GAME_YEAR)
      dateInput("cmp_b_cutoff_value", "Date", value = b$start, min = b$start, max = b$end)
    }
  })

  # ── Compare button: Teams mode ──

  observeEvent(input$cmp_go, {
    req(input$cmp_mode)
    mode <- input$cmp_mode

    if (mode == "Teams") {
      pa <- collect_side_params("a")
      pb <- collect_side_params("b")

      metric <- selected_metric()
      is_ff <- metric %in% c("off_ts_pct", "off_tov_pct", "off_oreb_pct", "off_ftr")

      if (is_ff) {
        res_a <- run_team_ff(pa)
        res_b <- run_team_ff(pb)
      } else {
        res_a <- run_team_ratings(pa)
        res_b <- run_team_ratings(pb)
      }

      if (!nrow(res_a) || !nrow(res_b)) {
        cmp_ref$joined <- data.frame()
        return()
      }

      # Select metric column + join
      pick_cols <- function(df, suffix) {
        base <- c("team_id", "team_name")
        poss_col <- if ("off_poss" %in% names(df)) "off_poss" else if ("total_poss" %in% names(df)) "total_poss" else NULL
        metric_col <- if (metric %in% names(df)) metric else NULL
        cols <- c(base, metric_col, poss_col)
        cols <- cols[cols %in% names(df)]
        out <- df[, cols, drop = FALSE]
        # Rename metric + poss cols
        if (!is.null(metric_col)) names(out)[names(out) == metric_col] <- paste0("metric_", suffix)
        if (!is.null(poss_col)) names(out)[names(out) == poss_col] <- paste0("poss_", suffix)
        out
      }

      df_a <- pick_cols(res_a, "a")
      df_b <- pick_cols(res_b, "b")
      joined <- merge(df_a, df_b, by = c("team_id", "team_name"), suffixes = c("", ".b"))
      joined$metric_a <- as.numeric(joined$metric_a)
      joined$metric_b <- as.numeric(joined$metric_b)
      joined$gap <- joined$metric_a - joined$metric_b
      joined <- joined[order(-abs(joined$gap)), ]
      joined$rank <- seq_len(nrow(joined))
      cmp_ref$joined <- joined

    } else if (mode == "Lineups") {
      # Placeholder — Task 3
      showNotification("Lineups mode coming soon", type = "message")
    } else if (mode == "Players") {
      # Placeholder — Task 4
      showNotification("Players mode coming soon", type = "message")
    }
  })

  # ── Summary cards ──

  output$cmp_summary_a <- renderText({
    df <- cmp_ref$joined
    if (is.null(df) || !nrow(df)) return("—")
    sprintf("%+.1f", mean(df$metric_a, na.rm = TRUE))
  })
  output$cmp_summary_a_label <- renderText({
    metric_label()
  })
  output$cmp_summary_b <- renderText({
    df <- cmp_ref$joined
    if (is.null(df) || !nrow(df)) return("—")
    sprintf("%+.1f", mean(df$metric_b, na.rm = TRUE))
  })
  output$cmp_summary_b_label <- renderText({
    metric_label()
  })
  output$cmp_summary_gap <- renderText({
    df <- cmp_ref$joined
    if (is.null(df) || !nrow(df)) return("—")
    sprintf("%.1f", mean(abs(df$gap), na.rm = TRUE))
  })

  metric_label <- reactive({
    m <- selected_metric()
    nms <- c(TEAM_METRICS,
             c("PPG" = "ppg", "RPG" = "rpg", "APG" = "apg", "SPG" = "spg",
               "FG%" = "fg_pct", "3P%" = "fg3_pct", "FT%" = "ft_pct", "TS%" = "ts_pct"))
    names(nms)[match(m, nms)] %||% m
  })

  # ── Results table ──

  output$cmp_table <- DT::renderDataTable({
    df <- cmp_ref$joined
    req(df, nrow(df) > 0)

    show_df <- data.frame(
      `#` = df$rank,
      Team = df$team_name,
      `Side A` = sprintf("%+.1f", df$metric_a),
      `Poss A` = if ("poss_a" %in% names(df)) as.integer(df$poss_a) else NA_integer_,
      `Side B` = sprintf("%+.1f", df$metric_b),
      `Poss B` = if ("poss_b" %in% names(df)) as.integer(df$poss_b) else NA_integer_,
      Gap = sprintf("%+.1f", df$gap),
      check.names = FALSE, stringsAsFactors = FALSE
    )

    DT::datatable(
      show_df,
      options = list(
        dom = "t", paging = FALSE, ordering = TRUE,
        order = list(list(6, "desc")),
        columnDefs = list(
          list(className = "dt-right", targets = 2:6),
          list(className = "dt-left", targets = 1)
        )
      ),
      rownames = FALSE, selection = "none",
      class = "compact stripe nowrap"
    )
  }, server = FALSE)

  # ── Reset ──

  observeEvent(input$cmp_reset, {
    updateSelectInput(session, "cmp_preset", selected = "")
    for (s in c("a", "b")) {
      updateSelectInput(session, paste0("cmp_", s, "_starters_mode"), selected = "")
      updateSelectInput(session, paste0("cmp_", s, "_starters_val"), selected = "")
      updateSelectInput(session, paste0("cmp_", s, "_home_away"), selected = "")
      updateSelectInput(session, paste0("cmp_", s, "_outcome"), selected = "")
      updateCheckboxInput(session, paste0("cmp_", s, "_clutch"), value = FALSE)
      updateSelectInput(session, paste0("cmp_", s, "_cutoff_type"), selected = "")
      updateSelectizeInput(session, paste0("cmp_", s, "_opponents"), selected = character(0))
      updateSelectizeInput(session, paste0("cmp_", s, "_game_type"), selected = character(0))
      updateSelectInput(session, paste0("cmp_", s, "_opp_rank_side"), selected = "")
      updateSelectInput(session, paste0("cmp_", s, "_opp_rank_n"), selected = "")
    }
    cmp_ref$joined <- NULL
  })

  # ── Filter chips (minimal) ──

  output$cmp_filter_chips <- renderUI(NULL)
}
```

- [ ] **Step 2: Commit**

```bash
git add app/R/server_tab7_compare.R
git commit -m "feat: add Compare tab server with Teams mode"
```

### Task 3: Wire tab into app.R

**Files:**
- Modify: `app/app.R`

- [ ] **Step 1: Add source lines** (after line 16 — `source("R/server_tab5_traditional.R", ...)`)

```r
source("R/ui_tab7_compare.R", local = TRUE)
source("R/server_tab7_compare.R", local = TRUE)
```

- [ ] **Step 2: Add tab to navbar** (after `ui_tab5_traditional` in the `navbarPage` call)

```r
  ui_tab5_traditional,
  ui_tab7_compare
```

- [ ] **Step 3: Add `pending_compare_preset` to shared list**

```r
    pending_ld_team = reactiveVal(NULL),
    pending_gl_team = reactiveVal(NULL),
    pending_compare_preset = reactiveVal(NULL)
```

- [ ] **Step 4: Add server module call** (after `server_tab5_traditional(...)`)

```r
  server_tab7_compare(input, output, session, shared)
```

- [ ] **Step 5: Commit**

```bash
git add app/app.R
git commit -m "feat: wire Compare tab into app.R"
```

### Task 4: Manual smoke test — Teams mode

- [ ] **Step 1: Run the app**

```bash
RSCRIPT="/c/Program Files/R/R-4.4.2/bin/Rscript.exe"
"$RSCRIPT" -e "shiny::runApp('app')"
```

- [ ] **Step 2: Verify**
- Navigate to Compare tab
- Select "Starters vs Bench" preset → verify Side A/B filters fill in
- Click "Compare →" → verify table appears with teams sorted by gap
- Switch metric chip → verify table re-renders
- Click Reset → verify all filters clear

- [ ] **Step 3: Fix any issues found, commit**

---

## Chunk 2: Lineups + Players modes

### Task 5: Implement Lineups mode

**Files:**
- Modify: `app/R/server_tab7_compare.R`

- [ ] **Step 1: Add lineup SQL runner** (same pattern as `run_team_ratings`)

Add `run_lineups_summary(p)` and `run_lineups_ff(p)` functions that call `fetch_lineups_csv_v2` and `fetch_lineups_four_factors_csv` with the same params pattern from `server_tab2.R:run_fetch_lineups_20`.

Key differences from Teams mode:
- Join key: `sub_lineup_hash` (not `team_id`)
- Extra params: `num` (lineup size, default 5), `team_csv` (NA = all), `player_csv` (NA), `player_off_csv` (NA), `exact` (FALSE), `min_poss` (0)
- Lineup display: `player_names` column from the result

- [ ] **Step 2: Add Lineups branch in `cmp_go` observer**

Replace the `showNotification("Lineups mode coming soon")` with logic that:
1. Calls `run_lineups_summary(pa)` and `run_lineups_summary(pb)` (or FF variant based on selected metric)
2. Joins by `sub_lineup_hash`
3. Computes gap, sorts, stores in `cmp_ref$joined`

- [ ] **Step 3: Adjust table rendering** to show lineup player names instead of team name when in Lineups mode

- [ ] **Step 4: Commit**

```bash
git commit -am "feat: add Lineups mode to Compare tab"
```

### Task 6: Implement Players mode

**Files:**
- Modify: `app/R/server_tab7_compare.R`

- [ ] **Step 1: Add player SQL runner**

Add `run_player_traditional(p, player_id)` that calls `get_player_traditional_dynamic` with the player's team_id as `team_ids_csv`. Look up team from `cmp_ref$players`.

- [ ] **Step 2: Add Players branch in `cmp_go` observer**

Replace `showNotification("Players mode coming soon")` with logic:
1. Read `input$cmp_player_a` and `input$cmp_player_b`
2. Call `run_player_traditional(pa, player_a_id)` and `run_player_traditional(pb, player_b_id)`
3. If same player: 1 row with metric_a vs metric_b
4. If different players: 2 rows, head-to-head
5. Apply rate mode (`input$cmp_rate_mode`) — per game / per 75 poss / totals conversion

- [ ] **Step 3: Adjust table rendering** for Players mode (different columns: Player name instead of Team, player stats columns)

- [ ] **Step 4: Commit**

```bash
git commit -am "feat: add Players mode to Compare tab"
```

### Task 7: Manual smoke test — all modes

- [ ] **Step 1: Test Lineups mode** — preset "Home vs Away", verify lineups appear sorted by gap
- [ ] **Step 2: Test Players mode** — select two different players, verify head-to-head table
- [ ] **Step 3: Test same player, two conditions** — verify single row with gap
- [ ] **Step 4: Test rate mode toggle** in Players mode (per game, per 75 poss, totals)
- [ ] **Step 5: Fix issues, commit**

---

## Chunk 3: Home tab card + polish

### Task 8: Add 5th card to Home tab

**Files:**
- Modify: `app/R/ui_tab0_home.R`
- Modify: `app/app.R`

- [ ] **Step 1: Add the card** to `ui_tab0_home.R` after the second `fluidRow` (existing 4 cards)

```r
      # 5th card - full width
      fluidRow(
        column(
          width = 12,
          div(
            class = "card bg-dark border-secondary mb-4",
            style = "cursor: default;",
            div(
              class = "card-body d-flex align-items-center gap-3",
              tags$i(class = "bi bi-arrow-left-right", style = "font-size: 2rem; color: #e8a435;"),
              div(
                tags$h5("How do starters compare to the bench?", class = "card-title mb-1"),
                tags$small(class = "text-muted", "Compare any two situations side-by-side")
              ),
              div(
                class = "ms-auto",
                actionButton("go_compare", "Go \u2192", class = "btn btn-outline-warning btn-sm")
              )
            )
          )
        )
      )
```

- [ ] **Step 2: Add navigation handler** in `app.R` (alongside existing `go_onoff`, `go_lineups`, etc.)

```r
  observeEvent(input$go_compare, {
    shared$pending_compare_preset("starters_bench")
    updateTabsetPanel(session, "main_tabs", selected = "compare")
  })
```

- [ ] **Step 3: Commit**

```bash
git add app/R/ui_tab0_home.R app/app.R
git commit -m "feat: add Compare card to Home tab"
```

### Task 9: Update test wiring

**Files:**
- Modify: `app/tests/testthat/test-tab-wiring.R`

- [ ] **Step 1: Add tab7 to TAB_SPECS**

```r
  list(name = "tab7", ui = repo_file("R", "ui_tab7_compare.R"), server = repo_file("R", "server_tab7_compare.R"), reset_id = "cmp_reset", chips_id = "cmp_filter_chips", prefix = "cmp")
```

Note: This requires that the Compare tab has a `build_filter_chips` call in the server and `uiOutput("cmp_filter_chips")` in the UI (already present in the UI skeleton). Add a minimal `build_filter_chips` call to the server if not present.

- [ ] **Step 2: Run tests**

```bash
"$RSCRIPT" -e "testthat::test_file('app/tests/testthat/test-tab-wiring.R')"
```

- [ ] **Step 3: Fix any failures, commit**

```bash
git commit -am "test: add Compare tab to tab wiring tests"
```

### Task 10: Final smoke test + merge

- [ ] **Step 1: Full manual test**
- Home tab → "How do starters compare to the bench?" card → verify navigates to Compare tab with "Starters vs Bench" preset active
- Teams mode: all 4 presets
- Lineups mode: switch mode, compare, verify
- Players mode: same player two conditions + two different players
- Season switch: change season, verify ref data reloads
- Reset: verify clears everything

- [ ] **Step 2: Merge to main**

```bash
git checkout main
git merge shiny/compare-tab
git push origin main
git branch -d shiny/compare-tab
```
