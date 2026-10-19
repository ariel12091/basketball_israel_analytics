# IBPL Analytics App Architecture Guide

This guide explains how the Shiny application is assembled, how shared helpers and data flow work, and how each user-facing tab is wired from UI inputs to database queries and rendered outputs.

The guide is written for maintainers who need to change the app safely. It focuses on the current Shiny app under `app/`, not the separate React prototypes under `frontend/` or `frontend-v2/`.

## Table Of Contents

1. [Purpose And Domain](#purpose-and-domain)
2. [Repository Areas Used By The App](#repository-areas-used-by-the-app)
3. [Runtime Entry Point](#runtime-entry-point)
4. [Global Helpers And Shared Infrastructure](#global-helpers-and-shared-infrastructure)
5. [Shared Server State](#shared-server-state)
6. [Common UI Patterns](#common-ui-patterns)
7. [Common Server Patterns](#common-server-patterns)
8. [Database Access Pattern](#database-access-pattern)
9. [Tab 0: Home](#tab-0-home)
10. [Tab 1: On/Off Impact](#tab-1-onoff-impact)
11. [Tab 2: Lineup Data](#tab-2-lineup-data)
12. [Tab 3: Team Ratings](#tab-3-team-ratings)
13. [Tab 4: Game Logs](#tab-4-game-logs)
14. [Tab 5: Player Stats](#tab-5-player-stats)
15. [Tab 7: Compare](#tab-7-compare)
16. [Dormant Tab 6 Files](#dormant-tab-6-files)
17. [Shared Module: Lineup Player Filter](#shared-module-lineup-player-filter)
18. [JavaScript And CSS](#javascript-and-css)
19. [Tests And Contracts](#tests-and-contracts)
20. [Operational Notes](#operational-notes)
21. [Change Checklist](#change-checklist)

## Purpose And Domain

The app is an R/Shiny dashboard for Israel Basketball Premier League analytics. It lets users explore:

- Player on/off impact.
- Player and lineup four-factor profiles.
- Lineup performance and game-by-game details.
- Team ratings and traditional team stats.
- Game logs.
- Traditional player box-score stats.
- Compare splits such as starters vs bench, home vs away, win vs loss, clutch vs non-clutch, and custom filters.

The data path is:

```text
Basketball play-by-play JSON
  -> ETL scripts under etl/
  -> PostgreSQL/Supabase schema basketball_test
  -> Shiny app queries via DBI/RPostgres/pool
  -> DT tables and Shiny UI
```

The app uses season identifiers like:

```r
DEFAULT_GAME_YEAR <- "2026"
DEFAULT_START <- as.Date("2024-10-01")
DEFAULT_END   <- as.Date("2025-07-01")
DEFAULT_2026_START <- as.Date("2025-10-01")
DEFAULT_2026_END   <- as.Date("2026-07-01")

season_date_bounds_for_year <- function(gy = DEFAULT_GAME_YEAR) {
  if (identical(as.character(gy), "2026")) {
    list(start = DEFAULT_2026_START, end = DEFAULT_2026_END)
  } else {
    list(start = DEFAULT_START, end = DEFAULT_END)
  }
}
```

Source: `app/R/global.R`.

## Repository Areas Used By The App

Main runtime files:

```text
app/
  app.R                         Main Shiny entry point
  R/
    global.R                    Libraries, constants, DB pool, helpers
    logger.R                    App logging
    mod_lineup_player_filter.R  Reusable lineup team/player filter module
    ui_tab0_home.R              Home tab
    ui_tab1_onoff.R             Tab 1 UI
    server_tab1.R               Tab 1 server
    ui_tab2_lineup.R            Tab 2 UI
    server_tab2.R               Tab 2 server
    ui_tab3_team.R              Tab 3 UI
    server_tab3.R               Tab 3 server
    ui_tab4_gamelogs.R          Tab 4 UI
    server_tab4.R               Tab 4 server
    ui_tab5_traditional.R       Tab 5 UI
    server_tab5_traditional.R   Tab 5 server
    ui_tab6_team_stats.R        Dormant/unused in app.R
    server_tab6_team_stats.R    Dormant/unused in app.R
    ui_tab7_compare.R           Tab 7 UI
    server_tab7_compare.R       Tab 7 server
  www/
    app.css                     Styling
    app.js                      Client-side interactions
    images/                     Example snippets and screenshots
  tests/testthat/               Unit, contract, parse, and smoke tests
```

Database and ETL areas:

```text
etl/
  etl_full.R                    ETL orchestrator
  ...                           ETL helpers

sql/
  functions/                    PostgreSQL functions used by app queries
  materialized_views/           MV definitions and rebuild helpers
```

Project-level operational docs:

```text
PROJECT.md                     Primary repo/domain reference
AGENTS.md                      Codex operating instructions
CLAUDE.md                      Historical reference; do not edit
```

## Runtime Entry Point

`app/app.R` is the application entry point. It sources every module, builds the navbar UI, creates shared reactives, then calls each tab server function.

### Module Loading

The app sources global helpers first, then UI and server modules:

```r
source("R/global.R", local = TRUE)
source("R/logger.R", local = TRUE)
source("R/mod_lineup_player_filter.R", local = TRUE)
source("R/ui_tab0_home.R", local = TRUE)
source("R/ui_tab1_onoff.R", local = TRUE)
source("R/ui_tab2_lineup.R", local = TRUE)
source("R/ui_tab3_team.R", local = TRUE)
source("R/server_tab1.R", local = TRUE)
source("R/server_tab2.R", local = TRUE)
source("R/server_tab3.R", local = TRUE)
source("R/ui_tab4_gamelogs.R", local = TRUE)
source("R/server_tab4.R", local = TRUE)
source("R/ui_tab5_traditional.R", local = TRUE)
source("R/server_tab5_traditional.R", local = TRUE)
source("R/ui_tab7_compare.R", local = TRUE)
source("R/server_tab7_compare.R", local = TRUE)
```

Source: `app/app.R`.

### Navbar UI

The app is a `navbarPage()` with a global season selector, glossary button, and last-updated label:

```r
ui <- navbarPage(
  id = "main_tabs",
  title = tags$span(tags$i(class = "bi bi-activity"), "IBPL Analytics"),
  theme = bslib::bs_theme(...),
  header = tagList(
    includeCSS("www/app.css"),
    includeScript("www/app.js"),
    tags$div(
      class = "navbar-season-select",
      selectInput("game_year", NULL,
                  choices = c("25-26" = "2026", "24-25" = "2025"),
                  selected = DEFAULT_GAME_YEAR)
    ),
    actionButton("open_glossary", ...),
    textOutput("last_updated", inline = TRUE)
  ),
  ui_tab0_home,
  ui_tab1_onoff,
  ui_tab2_lineup,
  ui_tab3_team,
  ui_tab4_gamelogs,
  ui_tab5_traditional,
  ui_tab7_compare
)
```

The global `game_year` input drives season-specific date bounds, team lists, GN lists, and query params across tabs.

### Server Composition

Inside `server <- function(input, output, session)`, the app:

1. Initializes request guards and idle timeout.
2. Defines shared reactives such as selected season and teams.
3. Prewarms reference caches.
4. Builds a `shared` list passed to every tab server.
5. Calls each tab server function.
6. Wires Home card navigation.

Server module calls:

```r
server_tab1(input, output, session, shared)
server_tab2(input, output, session, shared)
server_tab3(input, output, session, shared)
server_tab4(input, output, session, shared)
server_tab5_traditional(input, output, session, shared)
server_tab7_compare(input, output, session, shared)
```

Source: `app/app.R`.

## Global Helpers And Shared Infrastructure

Most cross-tab helpers live in `app/R/global.R`.

### Libraries

The app uses:

```r
library(shiny)
library(DBI)
library(dplyr)
library(pool)
library(RPostgres)
library(DT)
library(purrr)
library(bslib)
library(htmltools)
```

### Tooltip Definitions

Column and filter tooltips are centralized:

```r
COLUMN_TOOLTIPS <- c(
  "PPP" = "Points per 100 possessions",
  "Off PPP" = "Offensive points per 100 possessions",
  "Def PPP" = "Defensive points per 100 possessions",
  "Net RTG" = "Offensive PPP minus Defensive PPP",
  "TS%" = "True Shooting: pts / (2 x (FGA + FT trips))",
  "OREB%" = "Offense: offensive rebounds / available misses. Defense: opponent offensive rebounds allowed / available misses",
  "TOV%" = "Turnover rate: turnovers / possessions",
  "FTR" = "Free throw rate: FTA / FGA"
)

FILTER_TOOLTIPS <- c(
  "gn" = "Sequential game number within a team's season; player and team views both use the team's GN",
  "last_n" = "Only include the most recent N games in the selected team context",
  "clutch" = "Limit results to close-game situations based on margin, time remaining, and score status"
)

tt <- function(label, key) {
  tip <- FILTER_TOOLTIPS[[key]]
  if (is.null(tip)) return(label)
  tags$span(label, `data-tooltip` = tip)
}
```

Source: `app/R/global.R`.

### Shared Header Tags

Tabs can call `shared_head_tags()` to load viewport, fonts, and Bootstrap Icons:

```r
shared_head_tags <- function() {
  tags$head(
    tags$meta(name = "viewport", content = "width=device-width, initial-scale=1, maximum-scale=1"),
    tags$link(rel = "stylesheet", href = "https://fonts.googleapis.com/css2?..."),
    tags$link(rel = "stylesheet", href = "https://cdn.jsdelivr.net/npm/bootstrap-icons@1.11.3/font/bootstrap-icons.min.css")
  )
}
```

### Explainer Cards

Tabs use `tab_explainer()` for collapsible explanatory content:

```r
tab_explainer <- function(id, title, intro, bullets) {
  body_id <- paste0(id, "_body")
  tags$div(
    class = "explainer-card",
    tags$div(class = "explainer-top", tags$h4(class = "explainer-title", title), ...),
    tags$div(id = body_id, class = "explainer-body collapse",
             tags$p(intro),
             tags$ul(lapply(bullets, tags$li)),
             tags$a(class = "js-shiny-event", `data-input-id` = "open_glossary", "Open glossary"))
  )
}
```

### Filter Chips

The global filter chip system renders active filters as removable chips and wires clear buttons back to input resets.

Typical usage:

```r
output$ld_filter_chips <- renderUI({
  build_filter_chips(
    "ld", input, shared$season_date_bounds,
    reset_btn_id = "ld_reset",
    team_label_map = team_map,
    player_label_map = player_map,
    extra_children = stat_filter_chips_ui("ld", ld_stat_filter_state, ld_stat_filter_cols)
  )
})

setup_chip_clears("ld", session, input, shared,
  game_type_id = "ld_game_type",
  opponents_id = "ld_opponents",
  date_id = "ld_dates",
  gy_input_id = "game_year"
)
```

Sources: `app/R/global.R`, `app/R/server_tab2.R`.

## Shared Server State

The app passes a shared context to tab servers:

```r
shared <- list(
  season_date_bounds = season_date_bounds,
  selected_game_year = selected_game_year,
  teams_for_year_df = teams_for_year_df,
  selected_opp_ids_on = selected_opp_ids_on,
  selected_opp_ids_ld = selected_opp_ids_ld,
  pending_ld_team = reactiveVal(NULL),
  pending_gl_team = reactiveVal(NULL),
  pending_compare_preset = reactiveVal(NULL)
)
```

Source: `app/app.R`.

The shared state solves several cross-tab needs:

- All tabs use one global `game_year`.
- Home can choose a team and navigate to Tab 1, 2, or 4.
- Home can set a pending Compare preset before opening Tab 7.
- Tabs can use a consistent season date range.
- Opponent selection can map team names to IDs in a shared way.

### Home Navigation Shared State

Home card examples:

```r
observeEvent(input$go_lineups, {
  if (!is.null(input$home_team) && input$home_team != "") {
    shared$pending_ld_team(input$home_team)
  }
  updateRadioButtons(session, "ld_num", selected = "5")
  updateTabsetPanel(session, "main_tabs", selected = "lineup_data")
})

observeEvent(input$go_compare, {
  shared$pending_compare_preset("starters_bench")
  updateTabsetPanel(session, "main_tabs", selected = "compare")
})
```

Source: `app/app.R`.

## Common UI Patterns

Most tabs follow this shape:

```r
tabPanel(
  title = tags$span(tags$i(class = "..."), "Tab Name"),
  value = "tab_value",
  fluidPage(
    sidebarLayout(
      sidebarPanel(width = 3, ... filters ...),
      mainPanel(width = 9, ... explainer, chips, DT output ...)
    )
  )
)
```

Common UI elements:

- `radioButtons()` for view mode.
- `dateRangeInput()` for season/window filters.
- `selectizeInput()` for team, player, opponent, GN filters.
- `bslib::accordion()` for collapsible advanced filters.
- `actionButton()` reset buttons.
- `uiOutput("*_filter_chips")` for active filter chips.
- `DTOutput()` / `dataTableOutput()` for interactive tables.
- `tab_explainer()` for usage documentation in-app.

Mobile sidebar filters are hidden behind a Bootstrap collapse button:

```r
tags$button(
  class = "btn btn-outline-secondary d-md-none w-100 mb-2",
  `data-bs-toggle` = "collapse",
  `data-bs-target` = "#ld-filters",
  "Show Filters"
)
```

## Common Server Patterns

### Season-Specific Ref Refresh

Most tabs refresh team/GN choices when the tab is active and `game_year` changes:

```r
observeEvent(list(input$main_tabs, input$game_year), ignoreInit = TRUE, {
  if (!identical(input$main_tabs, "lineup_data")) return(NULL)
  gy_int <- as.integer(input$game_year)
  b <- shared$season_date_bounds(input$game_year %||% DEFAULT_GAME_YEAR)

  updateDateRangeInput(session, "ld_dates",
                       start = b$start, end = b$end,
                       min = b$start, max = b$end)
  ...
})
```

Source: `app/R/server_tab2.R`.

### Reset Pattern

Each tab has a reset observer that:

1. Computes season date bounds.
2. Resets view mode and date ranges.
3. Clears filters.
4. Resets stat chips.
5. Often uses `session$onFlushed()` to release a reset flag.

Tab 2 example:

```r
observeEvent(input$ld_reset, {
  resetting(TRUE)
  b <- shared$season_date_bounds(input$game_year %||% DEFAULT_GAME_YEAR)
  updateRadioButtons(session, "ld_view_mode", selected = "Summary")
  updateRadioButtons(session, "ld_num", selected = LD_DEFAULT_NUM)
  updateDateRangeInput(session, "ld_dates",
                       start = b$start, end = b$end,
                       min = b$start, max = b$end)
  ...
  session$onFlushed(function() resetting(FALSE), once = TRUE)
})
```

### Debounce And Bind Events

Tabs with heavier queries often debounce filter inputs and bind reactive execution to a finite trigger set.

Tab 1 example:

```r
debounced_range <- reactive(input$date_range) %>% debounce(300)
debounced_teams <- reactive(input$teams) %>% debounce(300)

result_df <- reactive({
  ...
}) %>% bindEvent(
  debounced_range(), debounced_teams(), debounced_on_filters(),
  gn_params(), input$min_all_poss, input$min_on_poss,
  input$game_year, input$onoff_view_mode
)
```

Source: `app/R/server_tab1.R`.

### Heavy Request Guard

Long or repeated query paths use `guard_heavy_request()`:

```r
allowed <- guard_heavy_request(
  session, key = "tab2_lineups_summary",
  start_d = start_date, end_d = end_date,
  min_gn = min_gn, max_gn = max_gn, last_n = last_n_games,
  max_calls = 40L, window_sec = 60L
)
if (!isTRUE(allowed)) return(data.frame())
```

Source: `app/R/server_tab2.R`.

## Database Access Pattern

Database access is intentionally direct DBI/RPostgres, not dbplyr lazy tables.

### Pool

`pg_pool` is created in `global.R`:

```r
pg_pool <- dbPool(
  drv      = Postgres(),
  bigint   = "numeric",
  host     = Sys.getenv("PG_HOST"),
  port     = as.integer(Sys.getenv("PG_PORT", "6543")),
  dbname   = Sys.getenv("PG_DB"),
  user     = Sys.getenv("PG_USER"),
  password = Sys.getenv("PG_PASS"),
  sslmode  = Sys.getenv("PG_SSLMODE", "require"),
  options  = sprintf("-c statement_timeout=%d", PG_STATEMENT_TIMEOUT_MS),
  minSize  = 0,
  maxSize  = as.integer(Sys.getenv("POOL_MAX", "3")),
  idleTimeout = 15000
)
onStop(function() poolClose(pg_pool))
```

The app prewarms the connection:

```r
tryCatch(db_get_query(pg_pool, "SELECT 1"), error = function(e) NULL)
```

### Query Helper

All modules call the thin wrapper:

```r
db_get_query <- function(conn_or_pool, statement, params = NULL) {
  if (is.null(params)) {
    DBI::dbGetQuery(conn_or_pool, statement)
  } else {
    DBI::dbGetQuery(conn_or_pool, statement, params = params)
  }
}
```

### Ref Cache

Reference queries use short-lived cache keys:

```r
cached_ref_query <- function(key, query_fun, ttl_sec = REF_CACHE_TTL_SEC) {
  now <- as.numeric(Sys.time())
  if (exists(key, envir = .ref_cache_env, inherits = FALSE)) {
    cached <- get(key, envir = .ref_cache_env, inherits = FALSE)
    if (!is.null(cached$ts) && !is.null(cached$val) && (now - cached$ts) <= ttl_sec) {
      return(cached$val)
    }
  }
  val <- query_fun()
  assign(key, list(ts = now, val = val), envir = .ref_cache_env)
  val
}
```

This is used heavily for teams, players, and GN choices.

### Common SQL Function Mapping

The main Shiny app calls these PostgreSQL functions/views:

```text
Tab 1 On/Off
  onoff_default_mv
  player_advanced_stats_mv
  onoff_compute(...)
  four_factors_compute(...)

Tab 2 Lineups
  fetch_lineups_csv_v2(...)
  fetch_lineups_four_factors_csv(...)

Tab 3 Team Ratings
  team_ppp_ratings_mv
  team_four_factors_mv
  get_team_ratings_dynamic(...)
  get_team_four_factors_dynamic(...)
  get_team_traditional_dynamic(...)

Tab 4 Game Logs
  final_schedule_mv
  mv_lineup_totals_by_day
  lineup_four_factors_by_game

Tab 5 Player Stats
  player_traditional_stats_mv or dynamic traditional function path
  get_player_traditional_dynamic(...)

Tab 7 Compare
  get_team_ratings_dynamic(...)
  get_team_four_factors_dynamic(...)
  fetch_lineups_csv_v2(...)
  fetch_lineups_four_factors_csv(...)
  get_player_traditional_dynamic(...)
  four_factors_compute(...)
  onoff_compute(...)
```

Exact query paths vary by filter state and view mode.

## Tab 0: Home

Files:

- UI: `app/R/ui_tab0_home.R`
- Server wiring: `app/app.R`

Purpose:

- Gives a simple landing page.
- Lets user optionally choose a team.
- Navigates to common workflows by setting pending state and switching `main_tabs`.

UI card excerpt:

```r
tags$button(
  type = "button",
  class = "card bg-dark border-secondary mb-4 h-100 w-100 text-start p-0 home-nav-card js-shiny-event",
  `data-input-id` = "go_compare",
  div(
    class = "card-body d-flex flex-column gap-2",
    tags$i(class = "bi bi-arrow-left-right"),
    tags$h5("How do starters compare to the bench?", class = "card-title mb-1"),
    tags$small(class = "text-muted", "Compare any two situations side-by-side")
  )
)
```

Navigation is handled in `app.R`:

```r
observeEvent(input$go_compare, {
  shared$pending_compare_preset("starters_bench")
  updateTabsetPanel(session, "main_tabs", selected = "compare")
})
```

Important behavior:

- If a team is selected in Home, some target tabs receive it through `shared$pending_ld_team` or `shared$pending_gl_team`.
- Compare receives the `starters_bench` pending preset.
- Home cards use `js-shiny-event` from `www/app.js`.

## Tab 1: On/Off Impact

Files:

- UI: `app/R/ui_tab1_onoff.R`
- Server: `app/R/server_tab1.R`

Purpose:

- Measures how team performance changes when each player is on vs off the floor.
- Supports Summary and Four Factors views.

### Main Inputs

From `ui_tab1_onoff.R`:

```r
radioButtons("onoff_view_mode", label = "Select View:",
             choices = c("Summary", "Four Factors"),
             selected = "Summary", inline = TRUE)

dateRangeInput("date_range", "Game Date Range", ...)
selectizeInput("teams", "Teams", choices = NULL, multiple = TRUE)
selectizeInput("on_game_type", "Game type", choices = ..., multiple = TRUE)
selectizeInput("on_opponents", "Opponents", choices = NULL, multiple = TRUE)
selectInput("on_home_away", "Home/Away", choices = c("All" = "", "Home" = "home", "Away" = "away"))
selectInput("on_outcome", "Outcome", choices = c("All" = "", "Win" = "win", "Loss" = "loss"))
selectizeInput("on_gn_min", tt("From Game Number (GN)", "gn"), choices = NULL)
selectizeInput("on_gn_max", tt("To Game Number (GN)", "gn"), choices = NULL)
selectizeInput("on_last_n", tt("Last N Team Games", "last_n"), choices = NULL)
sliderInput("min_all_poss", tt("Min possessions per side (eligibility):", "min_poss_side"), ...)
sliderInput("min_on_poss", tt("Minimum ON possessions (for ranking):", "min_on_poss"), ...)
```

### Server Flow

High-level:

1. Refreshes teams and GN choices on season change.
2. Debounces date/team/context filters.
3. Resolves team IDs, GN params, starter filters, game context filters.
4. Chooses fast path or dynamic SQL.
5. Builds Summary or Four Factors table.
6. Applies stat filters.
7. Renders `output$onoff_dt`.

Important reactive pattern:

```r
debounced_range <- reactive(input$date_range) %>% debounce(300)
debounced_teams <- reactive(input$teams) %>% debounce(300)
debounced_on_filters <- reactive(list(
  input$on_game_type,
  input$on_opponents,
  input$on_home_away,
  input$on_outcome,
  input$on_opp_rank_side,
  input$on_opp_rank_n,
  input$on_opp_rank_metric
)) %>% debounce(300)
```

GN params are included in bind events so GN changes recompute results:

```r
gn_params <- reactive({
  resolve_gn_last_n_params(input, "on")
})

result_df <- reactive({
  ...
}) %>% bindEvent(
  debounced_range(), debounced_teams(), debounced_on_filters(),
  gn_params(), input$min_all_poss, input$min_on_poss,
  input$game_year, input$onoff_view_mode
)
```

### Fast Path vs Dynamic Path

Tab 1 uses materialized views for default/no-filter cases and dynamic SQL functions when filters require recomputation.

Typical dynamic calls:

```r
db_get_query(pool, paste0(
  "SELECT * FROM basketball_test.onoff_compute(",
  "$1::date,$2::date,$3::text,$4::int4,$5::int4,$6::numeric,$7::text,",
  "$8::text,$9::text,$10::text,$11::text,$12::text,$13::int4,$14::text,",
  "$15::int4,$16::int4,$17::int4,$18::int4,$19::int4,$20::int4,$21::int4,$22::int4,$23::int4",
  ")"
), params = list(...))
```

Four Factors uses `four_factors_compute(...)`.

### Output

`output$onoff_dt` renders a DT table with:

- Player/team identity.
- On/off PPP and net impact.
- Possession counts.
- Shooting splits.
- Four-factor cells in Four Factors mode.
- Tooltips and heat coloring.

Filter chips:

```r
output$on_filter_chips <- renderUI({
  build_filter_chips("on", input, shared$season_date_bounds, ...)
})
```

## Tab 2: Lineup Data

Files:

- UI: `app/R/ui_tab2_lineup.R`
- Server: `app/R/server_tab2.R`
- Shared module: `app/R/mod_lineup_player_filter.R`

Purpose:

- Evaluates lineup combinations by group size 2-5.
- Supports Summary and Four Factors views.
- Supports team/player inclusion/exclusion filters.
- Supports modal drill-down for selected lineups.

### Main Inputs

From `ui_tab2_lineup.R`:

```r
radioButtons("ld_view_mode", label = "View:",
             choices = c("Summary", "Four Factors"),
             selected = "Summary", inline = TRUE)

sliderInput("ld_minposs", tt("Minimum possessions (Off + Def)", "min_poss_lineup"), ...)
radioButtons("ld_num", tt("Group size", "group_size"),
             choices = c("2", "3", "4", "5"),
             selected = LD_DEFAULT_NUM, inline = TRUE)

lineup_player_filter_ui(
  "ld_lineup_filter",
  team_label = "Team",
  team_help = "Pick a team to enable player filtering.",
  team_placeholder = "All teams",
  players_on_placeholder = "Select a team first...",
  players_off_placeholder = "Select a team first..."
)

dateRangeInput("ld_dates", "Date range",
               start = season_date_bounds_for_year(DEFAULT_GAME_YEAR)$start,
               end = season_date_bounds_for_year(DEFAULT_GAME_YEAR)$end)
```

### Season Refresh

On Tab 2 activation or year change, the server refreshes:

- `ld_dates` start/end/min/max.
- Team choices.
- Player reference map.
- GN choices.

Current key excerpt:

```r
observeEvent(list(input$main_tabs, input$game_year), ignoreInit = TRUE, {
  if (!identical(input$main_tabs, "lineup_data")) return(NULL)
  gy_int <- as.integer(input$game_year)
  b <- shared$season_date_bounds(input$game_year %||% DEFAULT_GAME_YEAR)

  updateDateRangeInput(session, "ld_dates",
                       start = b$start, end = b$end,
                       min = b$start, max = b$end)

  teams_ld <- cached_ref_query(key = sprintf("ld_teams_%d", gy_int), ...)
  ld_ref$teams <- teams_ld
  ...
  players_map <- cached_ref_query(key = sprintf("ld_players_%d", gy_int), ...)
  ld_ref$players <- players_map
  ...
  update_gn_last_n_choices(session, "ld", gn_vals)
})
```

This is the path that keeps Tab 2 date range aligned to `24-25` or `25-26`.

### Query Arguments

`build_ld_common_db_args()` collects shared filters:

```r
build_ld_common_db_args <- function() {
  game_type_csv <- csv_if_any(input$ld_game_type)
  opp_ids_csv <- csv_if_any(shared$selected_opp_ids_ld())
  home_away <- blank_to_na_character(input$ld_home_away)
  outcome <- blank_to_na_character(input$ld_outcome)
  gn_params <- resolve_gn_last_n_params(input, "ld")
  start_date <- as.Date(input$ld_dates[1])
  end_date <- as.Date(input$ld_dates[2])
  clutch <- resolve_clutch_params(...)
  starters <- resolve_starters_bounds(...)
  list(...)
}
```

### Query Functions

Summary path:

```r
run_fetch_lineups_20 <- function(pool, num, team_csv, player_csv, player_off_csv,
                                 exact, start_date, end_date, min_poss, game_year, ...) {
  allowed <- guard_heavy_request(session, key = "tab2_lineups_summary", ...)
  if (!isTRUE(allowed)) return(data.frame())
  db_get_query(pool, paste0(
    "SELECT * FROM basketball_test.fetch_lineups_csv_v2(",
    "$1::int4,$2::text,$3::text,$4::text,$5::bool,$6::date,$7::date,$8::int4,$9::int4,",
    "$10::text,$11::text,$12::text,$13::text,$14::text,$15::int4,$16::text,$17::int4,$18::text,$19::int4,$20::bool,",
    "$21::int4,$22::int4,$23::int4,$24::int4,$25::int4,$26::int4,$27::int4,$28::int4,$29::int4",
    ")"
  ), params = list(...))
}
```

Four Factors path:

```r
run_fetch_lineups_ff_20 <- function(...) {
  allowed <- guard_heavy_request(session, key = "tab2_lineups_ff", ...)
  if (!isTRUE(allowed)) return(data.frame())
  db_get_query(pool, paste0(
    "SELECT * FROM basketball_test.fetch_lineups_four_factors_csv(...)"
  ), params = list(...))
}
```

### Ranked Data Reactives

Tab 2 fetches broad ranked datasets first, then filters locally:

```r
ld_ff_ranked_df <- reactive({
  req(identical(input$main_tabs, "lineup_data"))
  gy <- as.integer(input$game_year)
  num <- as.integer(input$ld_num)
  db_args <- build_ld_common_db_args()
  df <- run_fetch_lineups_ff_20(pg_pool, num = num, ..., min_poss = 0L, game_year = gy, ...)
  ...
  rank_thresh <- adaptive_baseline(df$total_poss)
  qualified <- df$total_poss >= rank_thresh
  ...
}) %>% bindEvent(input$ld_num, input$ld_dates, input$game_year, ...)
```

Summary has a parallel `ld_summary_ranked_df`.

### Auto Minimum Possessions

Tab 2 can auto-adjust `ld_minposs` toward a target row count:

```r
auto_minposs_from_df <- function(df, usage_col = "total_poss",
                                 step = 10L, target_rows = AUTO_TARGET_ROWS) {
  vals <- sort(suppressWarnings(as.numeric(df[[usage_col]])), decreasing = TRUE)
  if (length(vals) <= target_rows) return(0L)
  as.integer(ceiling(vals[target_rows] / step) * step)
}
```

### Output

`output$ld_table` renders the main table. A row click can open a modal drill-down:

```r
observeEvent(input$ld_lineup_click, {
  lineup_hashes <- db_get_query(pg_pool, ...)
  sched <- db_get_query(pg_pool, ...)
  lineup_name <- db_get_query(pg_pool, ...)
  ...
  showModal(modalDialog(...))
})
```

The modal has Summary and Four Factors breakdowns by game.

## Tab 3: Team Ratings

Files:

- UI: `app/R/ui_tab3_team.R`
- Server: `app/R/server_tab3.R`

Purpose:

- Ranks teams by offensive, defensive, net, factor, and traditional profiles.
- Supports Summary, Four Factors, and Traditional views.
- Shows rank deltas compared with a previous matchday/baseline where applicable.

### Main Inputs

From `ui_tab3_team.R`:

```r
radioButtons(
  "tr_view_mode",
  label = "View:",
  choices = c("Summary", "Four Factors", "Traditional"),
  selected = "Summary",
  inline = TRUE
)

conditionalPanel(
  condition = "input.tr_view_mode == 'Traditional'",
  bslib::input_switch("tr_trad_defense_mode", label = NULL, value = FALSE),
  selectInput("tr_trad_display_mode",
              "Traditional display mode",
              choices = c("Per Game", "Per 75 Possessions", "Per 40 Minutes"))
)

dateRangeInput("tr_dates", "Date range", start = DEFAULT_START, end = DEFAULT_END)
checkboxInput("tr_clutch_enabled", tt("Clutch", "clutch"), value = FALSE)
game_context_filters_ui("tr")
```

### Server Responsibilities

The Tab 3 server:

1. Refreshes team and GN choices on season/tab activation.
2. Collects common DB args.
3. Chooses Summary, Four Factors, or Traditional path.
4. Uses dynamic SQL functions with guard rails.
5. Computes or fetches previous-period ranks.
6. Renders a formatted DT table with hidden sort columns and rank movement.

Important query wrappers:

```r
run_team_ratings <- function(pool, game_year, start_d, end_d, ...) {
  allowed <- guard_heavy_request(session, key = "tab3_team_ratings", ...)
  if (!isTRUE(allowed)) return(data.frame())
  db_get_query(pool, paste0(
    "SELECT * FROM basketball_test.get_team_ratings_dynamic(",
    "$1::int4,$2::date,$3::date,$4::text,$5::text,$6::text,$7::text,$8::text,$9::int4,$10::text,",
    "$11::int4,$12::text,$13::int4,$14::bool,$15::int4,$16::int4,$17::int4,$18::int4,$19::int4,$20::int4,$21::int4,$22::int4,$23::int4",
    ")"
  ), params = list(...))
}

run_team_four_factors <- function(...) {
  db_get_query(pool, paste0(
    "SELECT * FROM basketball_test.get_team_four_factors_dynamic(...)"
  ), params = list(...))
}
```

### Previous Rank / Delta System

Tab 3 has helper reactives for rank comparison:

```r
tr_delta_enabled <- reactive({ ... })
tr_effective_anchor <- reactive({ ... })
tr_prev_match_end <- reactive({ ... })
tr_prev_data <- reactive({ ... })
tr_prev_ff_data <- reactive({ ... })
```

The table uses hidden numeric columns and `orderData` so formatted HTML cells sort correctly.

### Output

`output$tr_table <- renderDT({ ... })` builds:

- Summary table: season, team, GP/W/L, Off PPP, Def PPP, Net RTG, pace, possessions.
- Four Factors table: offense and defense PPP/eFG/OREB/TOV/FTR.
- Traditional table: team or opponent box-score stats, scaled by selected display mode.

Filter chips:

```r
output$tr_filter_chips <- renderUI({
  build_filter_chips("tr", input, shared$season_date_bounds, ...)
})
```

## Tab 4: Game Logs

Files:

- UI: `app/R/ui_tab4_gamelogs.R`
- Server: `app/R/server_tab4.R`

Purpose:

- Shows game-by-game team performance.
- Combines schedule context, lineup totals, and four-factor game aggregates.

### Main Inputs

From `ui_tab4_gamelogs.R`:

```r
radioButtons("gl_view_mode", label = "View:",
             choices = c("Summary", "Four Factors"),
             selected = "Summary", inline = TRUE)

selectizeInput("gl_team", "Team", choices = NULL, multiple = FALSE)
dateRangeInput("gl_dates", "Date range", start = DEFAULT_START, end = DEFAULT_END)
game_context_filters_ui("gl", include_opp_rank = FALSE)
```

Home can set a pending team:

```r
observeEvent(input$go_gamelogs, {
  if (!is.null(input$home_team) && input$home_team != "") {
    shared$pending_gl_team(input$home_team)
  }
  updateTabsetPanel(session, "main_tabs", selected = "game_logs")
})
```

Source: `app/app.R`.

### Server Flow

Tab 4 server:

1. Refreshes teams/GN choices on activation/year change.
2. Applies pending Home team.
3. Reads `final_schedule_mv`.
4. Reads lineup totals from `mv_lineup_totals_by_day`.
5. Reads four-factor rows from `lineup_four_factors_by_game`.
6. Filters by date, team, game type, home/away, outcome, GN, last N, starters.
7. Produces Summary or Four Factors game log table.

Representative reactive skeleton:

```r
gl_schedule <- reactive({
  db_get_query(pg_pool,
    "SELECT * FROM basketball_test.final_schedule_mv WHERE game_year = $1::int4",
    params = list(as.integer(input$game_year))
  )
}) %>% bindEvent(input$game_year, input$main_tabs)

gl_lineup_totals <- reactive({
  db_get_query(pg_pool, "... mv_lineup_totals_by_day ...", params = list(...))
}) %>% bindEvent(input$game_year, input$main_tabs)

gl_lineup_ff <- reactive({
  db_get_query(pg_pool, "... lineup_four_factors_by_game ...", params = list(...))
}) %>% bindEvent(input$game_year, input$main_tabs)
```

### Output

`output$gl_table <- DT::renderDataTable({ ... })` renders:

- Summary view: GN, game type, date, team, opponent, W/L, score, Off PPP, Def PPP, Net, shot split cells, possessions.
- Four Factors view: single-game PPP/eFG/OREB/TOV/FTR for offense and defense.

Filter chips:

```r
output$gl_filter_chips <- renderUI({
  build_filter_chips("gl", input, shared$season_date_bounds, ...)
})
```

## Tab 5: Player Stats

Files:

- UI: `app/R/ui_tab5_traditional.R`
- Server: `app/R/server_tab5_traditional.R`

Purpose:

- Shows traditional player box-score stats under selected filters.
- Supports Totals, Per Game, Per 60 Possessions, and Per 30 Minutes modes.

### Main Inputs

From `ui_tab5_traditional.R`:

```r
dateRangeInput("ts_dates", "Date range", start = DEFAULT_START, end = DEFAULT_END)
selectizeInput("ts_teams", "Teams", choices = NULL, multiple = TRUE)

selectInput(
  "ts_display_mode",
  "Display mode",
  choices = c("Totals", "Per Game", "Per 60 Possessions", "Per 30 Minutes"),
  selected = "Per Game"
)

sliderInput("ts_min_gp_slider", "Min GP", min = 1, max = 40, value = 1)
numericInput("ts_min_gp", "Min GP (type)", value = 1, min = 1, max = 40)
checkboxInput("ts_show_ineligible", "Show non-eligible players in rate modes", value = FALSE)
checkboxInput("ts_clutch_enabled", tt("Enable clutch filter", "clutch"), value = FALSE)
game_context_filters_ui("ts", opp_rank_blank_label = "-")
downloadButton("ts_download_csv", "Download CSV")
```

The display mode is hidden in the sidebar because the top nav hover menu in `app.js` can expose it.

### Server Flow

Tab 5:

1. Refreshes team/GN choices on activation/year change.
2. Syncs typed Min GP and slider Min GP.
3. Debounces date/team/context filters.
4. Maps selected teams/opponents to IDs.
5. Uses MV fast path when filters are default/simple.
6. Uses dynamic player traditional query for filtered cases.
7. Builds mode-specific display context.
8. Renders `ts_table`.
9. Provides CSV download.

Representative dynamic query wrapper:

```r
run_player_traditional <- function(...) {
  allowed <- guard_heavy_request(session, key = "tab5_traditional", ...)
  if (!isTRUE(allowed)) return(data.frame())
  db_get_query(
    pg_pool,
    "SELECT * FROM basketball_test.get_player_traditional_dynamic(...)",
    params = list(...)
  )
}
```

### Mode Context

The server computes mode context so Totals and rate views use the same source data differently:

```r
ts_mode_context <- reactive({
  df <- result_df()
  ...
}) %>% bindEvent(result_df())

ts_display_context <- reactive({
  mode <- input$ts_display_mode %||% "Per Game"
  ...
}) %>% bindEvent(ts_mode_context(), input$ts_display_mode, input$ts_show_ineligible, ...)
```

### Output

`output$ts_table <- DT::renderDataTable({ ... })` renders:

- Team, player, GP, possessions on floor, minutes.
- Counting stats: PTS, REB, OREB, DREB, AST, STL, BLK, TOV.
- Shooting stats: FGM/FGA/FG%, 3PM/3PA/3P%, FTM/FTA/FT%, eFG%, TS%.
- Heat coloring and eligibility logic.

Download:

```r
output$ts_download_csv <- downloadHandler(
  filename = function() paste0("player_stats_", Sys.Date(), ".csv"),
  content = function(file) write.csv(ts_display_context()$df, file, row.names = FALSE)
)
```

## Tab 7: Compare

Files:

- UI: `app/R/ui_tab7_compare.R`
- Server: `app/R/server_tab7_compare.R`
- Shared JS: `app/www/app.js`

Purpose:

- Compares two situations side-by-side.
- Teams and Lineups modes compare the same entity under Side A and Side B conditions.
- Players mode compares two players under the same selected window/context.

### Main Inputs

Top-level UI:

```r
radioButtons("cmp_mode", label = NULL,
             choices = c("Teams", "Lineups", "Players"),
             selected = "Teams", inline = TRUE)

selectInput("cmp_preset", tt("Quick preset", "quick_preset"),
            choices = c(
              "Custom" = "",
              "Starters vs Bench" = "starters_bench",
              "Vs Starters vs Vs Bench" = "opp_starters_bench",
              "Clutch vs Non-Clutch" = "clutch",
              "Home vs Away" = "home_away",
              "Win vs Loss" = "win_loss",
              "Top vs Bottom (Opp Rank)" = "top_bottom_rank",
              "Date split" = "date_split",
              "GN split" = "gn_split"
            ))

sliderInput("cmp_min_poss", tt("Min possessions", "min_poss_lineup"), ...)
```

Each side has:

- Own starters filter.
- Opponent starters filter.
- Team filter.
- Home/away.
- Win/loss.
- Clutch.
- Opponents.
- Game type.
- Opponent rank filter.

Players mode adds:

- Shared date/GN filters.
- Player A and Player B selectors.
- Optional team selectors if a player appears for multiple teams.

Lineups mode adds:

```r
radioButtons("cmp_lu_num", NULL,
             choices = c("2", "3", "4", "5"), selected = "5",
             inline = TRUE)

lineup_player_filter_ui("cmp_lu_filter", layout = "inline", ...)
```

### Compare Initialization

Recent stabilization in `server_tab7_compare.R` prevents the table from rendering multiple intermediate states on tab entry.

Key state:

```r
cmp_ready <- reactiveVal(FALSE)
cmp_init_token <- reactiveVal(0L)
cmp_suppress_preset_echo <- reactiveVal(NULL)
cmp_teams_ref_loaded_year <- reactiveVal(NA_integer_)
cmp_refs_loaded_year <- reactiveVal(NA_integer_)
cmp_player_refs_loaded_year <- reactiveVal(NA_integer_)
```

Ready release:

```r
release_compare_ready_after_flush <- function(token) {
  if (inherits(session, "MockShinySession")) {
    cmp_ready(TRUE)
    return(invisible(NULL))
  }

  session$onFlushed(function() {
    later::later(function() {
      should_release <- isolate(
        identical(cmp_init_token(), token) &&
          identical(input$main_tabs, "compare")
      )
      if (!isTRUE(should_release)) return(invisible(NULL))
      isolate(cmp_ready(TRUE))
      invisible(NULL)
    }, delay = (CMP_FILTER_DEBOUNCE_MS + 150) / 1000)
  }, once = TRUE)
}
```

Tab init:

```r
observeEvent(list(input$main_tabs, input$game_year), ignoreInit = FALSE, {
  if (!identical(input$main_tabs, "compare")) return(NULL)
  gy_int <- as.integer(input$game_year)
  token <- cmp_init_token() + 1L

  cmp_init_token(token)
  cmp_ready(FALSE)
  cmp_auto_min_bootstrapped(FALSE)

  refresh_compare_ref_inputs(
    gy_int,
    include_players = (input$cmp_mode %in% c("Lineups", "Players"))
  )

  pending <- shared$pending_compare_preset()
  if (!is.null(pending) && nzchar(pending)) {
    shared$pending_compare_preset(NULL)
    reset_compare_side_filters("a", reset_clutch_sliders = FALSE)
    reset_compare_side_filters("b", reset_clutch_sliders = FALSE)
    apply_compare_preset(pending)
    cmp_suppress_preset_echo(pending)
    updateSelectInput(session, "cmp_preset", selected = pending)
  }

  release_compare_ready_after_flush(token)
})
```

The expensive compare reactives guard on readiness:

```r
cmp_joined_base <- reactive({
  req(identical(input$main_tabs, "compare"))
  req(isTRUE(cmp_ready()))
  ...
})
```

### Reference Loading

To speed startup:

- Teams/GN/base refs load on Teams mode.
- Player refs load lazily only for Lineups or Players mode.
- Same-season repeat visits skip re-sending browser input choices.

Excerpt:

```r
ensure_cmp_player_refs_loaded <- function(game_year,
                                          refresh_player_inputs = FALSE,
                                          apply_defaults = FALSE) {
  gy_int <- as.integer(game_year)
  ensure_cmp_teams_ref_loaded(gy_int)
  loaded <- identical(cmp_player_refs_loaded_year(), gy_int) &&
    !is.null(cmp_ref$players) && nrow(cmp_ref$players)
  if (!isTRUE(loaded)) {
    cmp_ref$players <- load_cmp_players_ref(gy_int)
    cmp_player_refs_loaded_year(gy_int)
  }
  if (isTRUE(refresh_player_inputs)) {
    refresh_player_choices("a")
    refresh_player_choices("b")
  }
  if (isTRUE(apply_defaults)) {
    apply_default_players()
  }
}
```

### Side Params

Each side is normalized into SQL-ready params:

```r
collect_side_params <- function(side) {
  pfx <- paste0("cmp_", side, "_")
  get_input <- function(name) input[[paste0(pfx, name)]]
  gy <- as.integer(input$game_year)
  b <- shared$season_date_bounds(as.character(gy))
  ...
  list(
    game_year = gy,
    start_d = start_d,
    end_d = end_d,
    game_type_csv = game_type_csv,
    team_ids_csv = team_ids_csv,
    opp_ids_csv = opp_ids_csv,
    home_away = home_away,
    outcome = outcome,
    max_margin = max_margin,
    min_gn = min_gn,
    max_gn = max_gn,
    num_starters_off_min = num_starters_off_min,
    ...
  )
}
```

### Query Paths

Teams mode:

```r
if (mode == "Teams") {
  is_ff <- metric %in% c("off_efg", "off_tov", "off_oreb", "off_ftr")
  if (is_ff) {
    res_a <- run_team_ff(pa)
    res_b <- run_team_ff(pb)
  } else {
    res_a <- run_team_ratings(pa)
    res_b <- run_team_ratings(pb)
  }
  joined <- dplyr::inner_join(df_a, df_b, by = c("team_id", "team_name"))
}
```

Lineups mode:

```r
if (mode == "Lineups") {
  if (is_ff) {
    res_a <- run_lineups_ff(pa, min_poss = sql_min_poss)
    res_b <- run_lineups_ff(pb, min_poss = sql_min_poss)
  } else {
    res_a <- run_lineups_summary(pa, min_poss = sql_min_poss)
    res_b <- run_lineups_summary(pb, min_poss = sql_min_poss)
  }
  joined <- dplyr::inner_join(df_a, df_b, by = c("sub_lineup_hash", extra))
}
```

Players mode:

```r
cmp_player_raw_live <- reactive({
  req(identical(input$cmp_mode, "Players"))
  req(identical(input$main_tabs, "compare"))
  player_state <- cmp_player_selection_state()
  side_state <- cmp_side_state()
  res_a <- run_player_traditional(pa, player_state$team_csv_a)
  res_b <- run_player_traditional(pb, player_state$team_csv_b)
  ...
})
```

### League Table And Detail View

League table:

```r
output$cmp_table <- DT::renderDataTable({
  df <- cmp_joined()
  req(df, nrow(df) > 0)
  ...
  DT::datatable(
    show_df,
    callback = DT::JS(sprintf("
      table.on('click', 'tbody tr', function() {
        window.handleCompareTableRowClick(table, this, %d);
      });
    ", entity_col_idx)),
    options = list(dom = "t", paging = FALSE, ordering = TRUE, ...)
  )
}, server = FALSE)
```

Row click:

```r
observeEvent(input$cmp_table_row_click, {
  info <- input$cmp_table_row_click
  req(info$entity_name)
  df <- cmp_joined()
  ...
  selected_detail_entity(list(key = row$team_id[1], name = row$team_name[1], mode = "Teams"))
  detail_view_active(TRUE)
})
```

Detail data fetches all metrics for one entity:

```r
cmp_detail_data <- reactive({
  entity <- selected_detail_entity()
  req(entity)
  req(identical(input$main_tabs, "compare"))
  req(isTRUE(cmp_ready()))
  side_state <- cmp_side_state()
  ...
  ratings_a <- run_team_ratings(pa)
  ratings_b <- run_team_ratings(pb)
  ff_a <- run_team_ff(pa)
  ff_b <- run_team_ff(pb)
  ...
})
```

### Client-Side Compare JS

`app/www/app.js` handles:

- Row click relay from DT to Shiny.
- League/detail view toggle.
- Detail grid sorting.

Snippet:

```js
window.handleCompareTableRowClick = function(table, rowEl, entityColIdx) {
  if (!table || !rowEl || !window.Shiny || typeof window.Shiny.setInputValue !== "function") return;
  var data = table.row(rowEl).data();
  if (!data) return;
  window.Shiny.setInputValue("cmp_table_row_click", {
    entity_name: data[entityColIdx],
    rand: Math.random()
  }, { priority: "event" });
};
```

## Dormant Tab 6 Files

Files exist:

- `app/R/ui_tab6_team_stats.R`
- `app/R/server_tab6_team_stats.R`

But `app/app.R` does not source or register Tab 6 in the `navbarPage()`, and does not call `server_tab6_team_stats()`.

That means Tab 6 is currently dormant and not part of the live Shiny app. If re-enabled, `app.R` would need to source both files, include `ui_tab6_team_stats`, and call `server_tab6_team_stats(input, output, session, shared)`.

## Shared Module: Lineup Player Filter

File: `app/R/mod_lineup_player_filter.R`

Used by:

- Tab 2 Lineup Data.
- Tab 7 Compare Lineups mode.

UI module:

```r
lineup_player_filter_ui <- function(id, layout = c("stacked", "inline"), ...) {
  ns <- shiny::NS(id)
  team_input <- selectizeInput(ns("team"), team_label, choices = NULL, multiple = FALSE, ...)
  players_on_input <- selectizeInput(ns("players_on"), players_on_label, choices = NULL, multiple = TRUE, ...)
  players_off_input <- selectizeInput(ns("players_off"), players_off_label, choices = NULL, multiple = TRUE, ...)
  ...
}
```

Server module:

```r
lineup_player_filter_server <- function(id, players_ref) {
  moduleServer(id, function(input, output, session) {
    observeEvent(input$team, {
      team_val <- input$team %||% ""
      players_df <- players_ref()
      ...
      updateSelectizeInput(session, "players_on", choices = choices, selected = character(0))
      updateSelectizeInput(session, "players_off", choices = choices, selected = character(0))
    }, ignoreInit = TRUE)

    list(
      team = reactive(input$team %||% ""),
      players_on = reactive(input$players_on %||% character(0)),
      players_off = reactive(input$players_off %||% character(0)),
      update_team_choices = update_team_choices,
      clear_player_choices = clear_player_choices,
      reset_inputs = reset_inputs
    )
  })
}
```

Important behavior:

- Player selectors stay empty until a team is chosen.
- A player cannot remain simultaneously in On and Off lists.
- Parent server modules call `reset_inputs()` when season/team choices refresh.

## JavaScript And CSS

Files:

- `app/www/app.js`
- `app/www/app.css`

### Event Bridge

Many UI elements are plain buttons with data attributes:

```r
tags$button(
  class = "js-shiny-event",
  `data-input-id` = "cmp_detail_toggle",
  `data-shiny-value` = "detail",
  "Detail"
)
```

`app.js` sends those to Shiny:

```js
function sendShinyEvent(inputId, value) {
  if (!inputId || !window.Shiny || typeof window.Shiny.setInputValue !== "function") return;
  window.Shiny.setInputValue(inputId, value === undefined ? Math.random() : value, { priority: "event" });
}

document.addEventListener("click", function(e) {
  var eventEl = e.target.closest("[data-shiny-event], .js-shiny-event");
  if (eventEl) {
    e.preventDefault();
    var value = Object.prototype.hasOwnProperty.call(eventEl.dataset, "shinyValue")
      ? eventEl.dataset.shinyValue
      : undefined;
    sendShinyEvent(eventEl.dataset.shinyEvent || eventEl.dataset.inputId, value);
    return;
  }
});
```

### Navbar Hover Menu

`app.js` also builds tab hover menus for view modes:

```js
var CFG = [
  { tab: "onoff", inputId: "onoff_view_mode", items: ["Summary", "Four Factors"], def: "Summary" },
  { tab: "lineup_data", inputId: "ld_view_mode", items: ["Summary", "Four Factors"], def: "Summary" },
  { tab: "team_ratings", inputId: "tr_view_mode", items: ["Summary", "Four Factors", "Traditional"], def: "Summary" },
  { tab: "game_logs", inputId: "gl_view_mode", items: ["Summary", "Four Factors"], def: "Summary" },
  { tab: "traditional_stats", inputId: "ts_display_mode", items: ["Totals", "Per Game", "Per 60 Possessions", "Per 30 Minutes"], def: "Per Game", type: "select" },
  { tab: "compare", inputId: "cmp_mode", items: ["Teams", "Lineups", "Players"], def: "Teams" }
];
```

### CSS

`app.css` defines:

- Dark theme table styling.
- Filter chips.
- Explainer cards.
- Shot split bars.
- Compare detail grid.
- Mobile responsive behavior.
- Shiny busy/progress visuals.

Example:

```css
.recalculating {
  opacity: 0.4 !important;
  transition: opacity 0.2s ease;
}

.dataTables_wrapper {
  animation: fadeInTable 0.3s ease-in;
}
```

## Tests And Contracts

Important test files:

```text
app/tests/testthat/test-tab-parse.R
app/tests/testthat/test-tab-wiring.R
app/tests/testthat/test-date-reset-contracts.R
app/tests/testthat/test-server-tabs-smoke.R
app/tests/testthat/test-primary-table-render-smoke.R
app/tests/testthat/test-tab7-compare-server.R
app/tests/testthat/test-tab7-compare-contracts.R
```

### Parse Tests

`test-tab-parse.R` confirms key tab UI/server files parse.

### Wiring Tests

`test-tab-wiring.R` confirms each tab has:

- Reset button in UI.
- Filter chips output in UI.
- Reset observer in server.
- Filter chips builder in server.

### Date Reset / Season Bounds

`test-date-reset-contracts.R` verifies reset observers use concrete season bounds. It also includes a Tab 2 regression test for season-change date refresh:

```r
test_that("tab2 season change updates lineup date range", {
  txt <- paste(readLines(repo_file("R", "server_tab2.R"), warn = FALSE), collapse = "\n")
  expect_true(
    grepl(
      "observeEvent\\(list\\(input\\$main_tabs, input\\$game_year\\)[\\s\\S]*?updateDateRangeInput\\(session,\\s*\"ld_dates\"",
      txt,
      perl = TRUE
    )
  )
})
```

### Compare Tests

`test-tab7-compare-server.R` verifies:

- Team Compare four-factor chips use FF query outputs.
- Lineups Compare FF chips work.
- Team detail view includes shooting accuracy/frequency.
- Players Compare keeps last successful view during debounce.
- Presets mirror or block filters correctly.

### Typical Commands

Run targeted tests from repo root:

```powershell
& 'C:/Program Files/R/R-4.4.2/bin/Rscript.exe' -e "setwd('app'); testthat::test_file('tests/testthat/test-tab-parse.R')"
& 'C:/Program Files/R/R-4.4.2/bin/Rscript.exe' -e "setwd('app'); testthat::test_file('tests/testthat/test-tab-wiring.R')"
& 'C:/Program Files/R/R-4.4.2/bin/Rscript.exe' -e "setwd('app'); testthat::test_file('tests/testthat/test-tab7-compare-server.R')"
```

## Operational Notes

### Running The App Locally

Per `PROJECT.md`, use the configured Rscript path:

```powershell
& 'C:/Program Files/R/R-4.4.2/bin/Rscript.exe' -e "shiny::runApp('app')"
```

### Deploying

```powershell
& 'C:/Program Files/R/R-4.4.2/bin/Rscript.exe' -e "rsconnect::deployApp('app')"
```

### Environment

The app expects `app/.Renviron` with readonly DB credentials:

```text
PG_HOST=<supabase-pooler>
PG_PORT=6543
PG_DB=postgres
PG_USER=<readonly-user>
PG_PASS=<password>
PG_SSLMODE=require
POOL_MAX=3
```

ETL uses `etl/.Renviron` with write credentials.

### Last Updated Label

The navbar shows ETL freshness from DB first, then file fallback:

```r
last_success_db <- function() {
  q <- db_get_query(
    pg_pool,
    "SELECT value FROM basketball_test.app_meta WHERE key = 'etl_full_last_success' LIMIT 1"
  )
  if (nrow(q) && nzchar(q$value[1])) q$value[1] else NA_character_
}
```

Source: `app/app.R`.

## Change Checklist

Use this checklist when modifying a tab:

1. Identify whether the change is UI-only, server-only, SQL-backed, or shared helper behavior.
2. Keep changes scoped to the relevant tab/module.
3. If adding a filter, update:
   - UI input.
   - Reset observer.
   - Filter chips.
   - Query arg builder.
   - `bindEvent()` triggers.
   - Tests/contracts.
4. If adding a season-aware control, use `shared$season_date_bounds(input$game_year %||% DEFAULT_GAME_YEAR)`.
5. If adding a heavy query, wrap with `guard_heavy_request()`.
6. If adding reference data, use `cached_ref_query()`.
7. If adding DataTables columns, verify:
   - Display formatting.
   - Hidden numeric sort columns if needed.
   - Tooltips.
   - Mobile/responsive width.
8. Run at least:

```powershell
& 'C:/Program Files/R/R-4.4.2/bin/Rscript.exe' -e "setwd('app'); testthat::test_file('tests/testthat/test-tab-parse.R')"
& 'C:/Program Files/R/R-4.4.2/bin/Rscript.exe' -e "setwd('app'); testthat::test_file('tests/testthat/test-tab-wiring.R')"
```

For tab-specific changes, run that tab's focused test file too.

## Summary Mental Model

The app is a modular Shiny dashboard:

```text
app.R
  sources global helpers and tab modules
  builds navbar UI
  defines shared state
  calls server_tab* modules
  wires Home navigation

global.R
  constants, DB pool, cache, guards, common UI helpers, filter chips

ui_tab*.R
  declares input controls, explainers, table outputs

server_tab*.R
  refreshes refs
  resolves filters
  calls DB functions/views
  computes ranks/derived metrics
  renders DT tables and chips

www/app.js
  turns custom UI buttons into Shiny events
  supports hover menus, compare row clicks, compare detail toggle

www/app.css
  dark theme, tables, chips, detail layouts
```

The key to safe maintenance is to treat each tab as a bounded module while respecting shared contracts: `game_year`, date bounds, GN/Last-N handling, filter chips, cached refs, and guarded DB queries.
