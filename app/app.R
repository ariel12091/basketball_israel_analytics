# app.R -------------------------------------------------------------
library(shiny)
library(DBI)
library(dplyr)
library(dbplyr)
library(pool)
library(RPostgres)
library(DT)
library(purrr)
library(bslib)
library(htmltools)

# ---------------- Defaults ----------------
DEFAULT_START <- as.Date("2024-10-01")
DEFAULT_END   <- as.Date("2025-07-01")
DEFAULT_GAME_YEAR <- "2026"
DEFAULT_MIN_ALL <- 100L
DEFAULT_MIN_ON  <- 300L
DEFAULT_MIN_NET <- -1e9
LD_DEFAULT_MIN_POSS <- 20L
LD_DEFAULT_NUM      <- "5"

# Players with fewer possessions than this won't get a color/rank bar
RANKING_BASELINE <- 100
RANKING_MIN_PCT  <- 0.25   # at least 25% of rows should be ranked

# Adaptive baseline: use RANKING_BASELINE when enough data qualifies,
# otherwise lower to the 75th-percentile so ~25% still get colored.
adaptive_baseline <- function(poss_vec) {
  n <- sum(!is.na(poss_vec))
  if (n == 0) return(0)
  pct_above <- sum(poss_vec >= RANKING_BASELINE, na.rm = TRUE) / n
  if (pct_above >= RANKING_MIN_PCT) return(RANKING_BASELINE)
  unname(quantile(poss_vec, 1 - RANKING_MIN_PCT, na.rm = TRUE))
}

# ---------------- PostgreSQL pool ----------------
pg_pool <- dbPool(
  drv      = Postgres(),
  host     = Sys.getenv("PG_HOST"),
  port     = as.integer(Sys.getenv("PG_PORT", "6543")),
  dbname   = Sys.getenv("PG_DB"),
  user     = Sys.getenv("PG_USER"),
  password = Sys.getenv("PG_PASS"),
  sslmode  = Sys.getenv("PG_SSLMODE", "require"),
  minSize  = 0,
  maxSize  = as.integer(Sys.getenv("POOL_MAX", "3")),
  idleTimeout = 15000
)
onStop(function() poolClose(pg_pool))

# Lazy tables
full_rosters      <- tbl(pg_pool, in_schema("basketball_test", "full_rosters"))
onoff_mv          <- tbl(pg_pool, in_schema("basketball_test", "onoff_default_mv"))
advanced_stats_mv <- tbl(pg_pool, in_schema("basketball_test", "player_advanced_stats_mv"))
schedule_tbl      <- tbl(pg_pool, in_schema("basketball_test", "schedule"))
team_ratings_mv   <- tbl(pg_pool, in_schema("basketball_test", "team_ppp_ratings_mv"))
team_ff_mv        <- tbl(pg_pool, in_schema("basketball_test", "team_four_factors_mv"))

# ---------------- UI ----------------
ui <- navbarPage(
  id = "main_tabs",
  title = "Player Analytics",
  theme = bslib::bs_theme(version = 5),
  
  # -------- Tab 1: On/Off Impact --------
  tabPanel(
    title = "On/Off Impact",
    value = "onoff",
    fluidPage(
      tags$head(
        tags$link(rel = "stylesheet", href = "https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap"),
        tags$style(HTML("
          /* Global Font */
          body, .container-fluid, .form-control, .btn, table.dataTable {
            font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif !important;
          }

          /* Table Headers */
          table.dataTable thead th {
            text-transform: uppercase;
            font-size: 0.85rem;
            letter-spacing: 0.5px;
            color: #555;
            padding-top: 12px !important;
            padding-bottom: 12px !important;
            border-bottom: 1px solid #ddd !important;
          }
          
          /* Table Body */
          table.dataTable tbody td {
            vertical-align: middle;
            font-size: 0.95rem;
            padding: 8px 10px !important; 
          }

          /* Section Dividers - Thick Border */
          table.dataTable thead th.section-left-border,
          table.dataTable tbody td.section-left-border {
            border-left: 3px solid #e0e0e0 !important;
            padding-left: 25px !important;
          }

          th.group-head { 
            background:#f7efe5 !important; 
            font-weight:800; 
            text-align:center; 
            border-bottom: 1px solid #ddd !important; 
          }
          th.sub-head { background:#fafafa !important; font-weight:700; }

          .accordion-button { padding: 0.5rem 1rem; font-weight: 600; background-color: #f8f9fa; }
          
          /* Visual Range Plot Styles */
          .diff-val { 
            font-size: 1.15em; font-weight: 700; line-height: 1; margin-bottom: 5px; letter-spacing: -0.5px;
          }
          .diff-val.unranked { color: #999; font-weight: 500; }

          .rank-bar-container { 
            position: relative; width: 90px; height: 12px; margin: 0 auto; background: #e9ecef; border-radius: 6px;
          }
          .rank-bar-container.hidden { display: none; }
          .rank-track { display: none; }
          .range-connect {
            position: absolute; top: 50%; height: 4px; background: #adb5bd; z-index: 1; transform: translateY(-50%); border-radius: 2px;
          }
          .dot-off {
            position: absolute; top: 50%; width: 8px; height: 8px; background: #fff; border: 2px solid #6c757d; border-radius: 50%; transform: translate(-50%, -50%); z-index: 2;
          }
          .dot-on {
            position: absolute; top: 50%; width: 10px; height: 10px; background: #212529; border: 1px solid #fff; border-radius: 50%; transform: translate(-50%, -50%); z-index: 3;
          }
          .sub-text { 
            font-size: 0.75em; color: #6c757d; margin-top: 4px; white-space: nowrap; font-family: 'Inter', monospace;
          }
          
          /* View Mode Toggle */
          .view-mode-container .shiny-options-group { display: flex; width: 100%; justify-content: center; gap: 10px; }
          .view-mode-container .radio label { font-weight: 600; background: #fff; padding: 8px 15px; border: 1px solid #dee2e6; border-radius: 6px; cursor: pointer; transition: all 0.2s; }
          .view-mode-container .radio label:hover { background: #f8f9fa; }
          .view-mode-container .radio input[type='radio']:checked + span { color: #0d6efd; }
          
          /* Legend */
          .legend-box {
            display: flex; align-items: center; justify-content: center; gap: 20px;
            background: #f8f9fa; border: 1px solid #e9ecef; border-radius: 8px;
            padding: 10px 20px; margin-bottom: 15px; font-size: 0.85rem; color: #495057;
          }
          .legend-item { display: flex; align-items: center; gap: 6px; }
          .legend-icon-on { width: 10px; height: 10px; background: #212529; border: 1px solid #fff; border-radius: 50%; }
          .legend-icon-off { width: 8px; height: 8px; background: #fff; border: 2px solid #6c757d; border-radius: 50%; }
          .legend-bar { position: relative; width: 60px; height: 6px; background: #e9ecef; border-radius: 3px; }
          .legend-tick { position: absolute; top: -2px; bottom: -2px; width: 1px; background: #999; }
        "))
      ),
      
      titlePanel("Player ON/OFF Impact"),
      
      sidebarLayout(
        sidebarPanel(
          div(
            class = "view-mode-container",
            radioButtons("onoff_view_mode", label = "Select View:",
                         choices = c("Summary", "Four Factors"),
                         selected = "Summary",
                         inline = TRUE)
          ),
          tags$hr(),
          
          actionButton("reset_defaults", "Reset to defaults"),
          tags$hr(),
          
          selectInput(
            "game_year", "Season",
            choices = c("2025-26" = "2026", "2024-25" = "2025"),
            selected = DEFAULT_GAME_YEAR
          ),
          
          uiOutput("date_filter_ui"),
          uiOutput("team_filter_ui"),
          tags$hr(),
          
          bslib::accordion(
            bslib::accordion_panel(
              "Game Filters",
              selectizeInput("on_game_type", "Game type",
                             choices = c("All" = "", "Regular season" = "5", "Playoffs – Quarterfinals" = "16",
                                         "Playoffs – Finals" = "17", "Playoffs – Semifinals" = "26",
                                         "Play-in" = "33", "Winner Cup" = "34"),
                             selected = "", multiple = TRUE, options = list(placeholder = "All game types")),
              selectizeInput("on_opponents", "Opponents", choices = NULL, selected = character(0), multiple = TRUE, options = list(placeholder = "All opponents")),
              selectInput("on_home_away", "Home/Away", choices = c("All" = "", "Home" = "home", "Away" = "away"), selected = ""),
              selectInput("on_outcome", "Outcome", choices = c("All" = "", "Win" = "win", "Loss" = "loss"), selected = "")
            ),
            bslib::accordion_panel(
              "Opponent Strength",
              selectInput("on_opp_rank_side", "Top / Bottom", choices = c("Off" = "", "Top" = "top", "Bottom" = "bottom"), selected = ""),
              selectInput("on_opp_rank_n", "Rank N", choices = c("—" = "", as.character(1:12)), selected = ""),
              selectInput("on_opp_rank_metric", "Metric", choices = c("—" = "", "Offense" = "off", "Defense" = "def", "Net rating" = "net"), selected = "")
            ),
            open = FALSE
          ),
          
          tags$hr(),
          sliderInput("min_all_poss", "Min possessions per side (eligibility):", min = 0, max = 2000, value = DEFAULT_MIN_ALL, step = 10),
          sliderInput("min_on_poss", "Min ON possessions (eligibility):", min = 0, max = 3000, value = DEFAULT_MIN_ON, step = 10),
          tags$hr(),
          downloadButton("download_csv", "Download CSV")
        ),
        
        mainPanel(
          # --- LEGEND (Only visible in Four Factors mode) ---
          conditionalPanel(
            condition = "input.onoff_view_mode == 'Four Factors'",
            div(
              class = "legend-box",
              span(style = "font-weight:700; margin-right:5px;", "Legend:"),
              div(class = "legend-item", div(class = "legend-icon-on"), span("On-Court")),
              div(class = "legend-item", div(class = "legend-icon-off"), span("Off-Court")),
              div(
                class = "legend-item",
                span("0%"),
                div(
                  class = "legend-bar",
                  div(class = "legend-tick", style = "left:0;"),
                  div(class = "legend-tick", style = "left:50%; height:12px; top:-2px; background:#888;"), 
                  div(class = "legend-tick", style = "right:0;")
                ),
                span("100% Rank")
              ),
              span(style="margin-left: 15px; font-size: 0.8em; color: #888;", paste0("(Ranked Players: > ", RANKING_BASELINE, " poss)"))
            )
          ),
          DTOutput("onoff_dt")
        )
      )
    )
  ),
  
  # -------- Tab 2: Lineup Data --------
  tabPanel(
    title = "Lineup Data",
    value = "lineup_data",
    fluidPage(
      sidebarLayout(
        sidebarPanel(
          actionButton("ld_reset", "Reset Lineup Filters"),
          tags$hr(),
          div(
            class = "view-mode-container",
            radioButtons("ld_view_mode", label = "View:",
                         choices = c("Summary", "Four Factors"),
                         selected = "Summary", inline = TRUE)
          ),
          tags$hr(),
          sliderInput("ld_minposs", "Min possessions (sum of Off/Def)", min = 0, max = 2000, value = LD_DEFAULT_MIN_POSS, step = 10),
          tags$hr(),
          selectizeInput("ld_team", "Team", choices = NULL, multiple = FALSE),
          helpText("Pick a team to enable player filtering."),
          selectizeInput("ld_players_on", "Players On (exact/contains)", choices = NULL, multiple = TRUE, options = list(placeholder = "Select a team first…")),
          selectizeInput("ld_players_off", "Players Off (exclude any)", choices = NULL, multiple = TRUE, options = list(placeholder = "Select a team first…")),
          tags$hr(),
          selectInput("game_year_ld", "Season", choices = c("2025-26" = "2026", "2024-25" = "2025"), selected = DEFAULT_GAME_YEAR),
          radioButtons("ld_num", "Group size", choices = c("2", "3", "4", "5"), selected = LD_DEFAULT_NUM, inline = TRUE),
          dateRangeInput("ld_dates", "Date range", start = NA, end = NA),
          tags$hr(),
          bslib::accordion(
            bslib::accordion_panel(
              "Game Filters",
              selectizeInput("ld_game_type", "Game type", choices = c("All" = "", "Regular season" = "5", "Playoffs – Quarterfinals" = "16", "Playoffs – Finals" = "17", "Playoffs – Semifinals" = "26", "Play-in" = "33", "Winner Cup" = "34"), selected = "", multiple = TRUE, options = list(placeholder = "All game types")),
              selectizeInput("ld_opponents", "Opponents", choices = NULL, selected = character(0), multiple = TRUE, options = list(placeholder = "All opponents")),
              selectInput("ld_home_away", "Home/Away", choices = c("All" = "", "Home" = "home", "Away" = "away"), selected = ""),
              selectInput("ld_outcome", "Outcome", choices = c("All" = "", "Win" = "win", "Loss" = "loss"), selected = "")
            ),
            bslib::accordion_panel(
              "Opponent Strength",
              selectInput("ld_opp_rank_side", "Top / Bottom", choices = c("Off" = "", "Top" = "top", "Bottom" = "bottom"), selected = ""),
              selectInput("ld_opp_rank_n", "Rank N", choices = c("—" = "", as.character(1:12)), selected = ""),
              selectInput("ld_opp_rank_metric", "Metric", choices = c("—" = "", "Offense" = "off", "Defense" = "def", "Net rating" = "net"), selected = "")
            ),
            open = FALSE
          )
        ),
        mainPanel(DTOutput("ld_table"))
      )
    )
  ),
  
  # -------- Tab 3: Team Ratings --------
  tabPanel(
    title = "Team Ratings",
    value = "team_ratings",
    fluidPage(
      sidebarLayout(
        sidebarPanel(
          width = 3,
          actionButton("tr_reset", "Reset Filters"),
          tags$hr(),
          div(
            class = "view-mode-container",
            radioButtons("tr_view_mode", label = "View:",
                         choices = c("Summary", "Four Factors"),
                         selected = "Summary", inline = TRUE)
          ),
          tags$hr(),
          selectInput("tr_game_year", "Season", choices = c("2025-26" = "2026", "2024-25" = "2025"), selected = DEFAULT_GAME_YEAR),
          dateRangeInput("tr_dates", "Date range", start = NA, end = NA),
          tags$hr(),
          bslib::accordion(
            bslib::accordion_panel(
              "Game Filters",
              selectizeInput("tr_game_type", "Game type", choices = c("All" = "", "Regular season" = "5", "Playoffs – Quarterfinals" = "16", "Playoffs – Finals" = "17", "Playoffs – Semifinals" = "26", "Play-in" = "33", "Winner Cup" = "34"), selected = "", multiple = TRUE, options = list(placeholder = "All game types")),
              selectizeInput("tr_opponents", "Opponents", choices = NULL, selected = character(0), multiple = TRUE, options = list(placeholder = "All opponents")),
              selectInput("tr_home_away", "Home/Away", choices = c("All" = "", "Home" = "home", "Away" = "away"), selected = ""),
              selectInput("tr_outcome", "Outcome", choices = c("All" = "", "Win" = "win", "Loss" = "loss"), selected = "")
            ),
            bslib::accordion_panel(
              "Opponent Strength",
              selectInput("tr_opp_rank_side", "Top / Bottom", choices = c("Off" = "", "Top" = "top", "Bottom" = "bottom"), selected = ""),
              selectInput("tr_opp_rank_n", "Rank N", choices = c("—" = "", as.character(1:12)), selected = ""),
              selectInput("tr_opp_rank_metric", "Metric", choices = c("—" = "", "Offense" = "off", "Defense" = "def", "Net rating" = "net"), selected = "")
            ),
            open = FALSE
          )
        ),
        mainPanel(width = 9, DTOutput("tr_table"))
      )
    )
  )
)

# ---------------- Server ----------------
server <- function(input, output, session) {
  
  # ---- Season helpers ----
  season_date_bounds <- function(gy) {
    if (identical(gy, "2026")) {
      list(start = as.Date("2025-10-01"), end = as.Date("2026-07-01"))
    } else {
      list(start = DEFAULT_START, end = DEFAULT_END)
    }
  }
  
  selected_game_year <- reactive({
    input$game_year %||% DEFAULT_GAME_YEAR
  })
  
  `%||%` <- function(a, b) if (!is.null(a)) a else b
  
  # ===== Opponents dropdown choices =====
  teams_for_year_df <- reactive({
    gy_int <- as.integer(selected_game_year())
    req(gy_int)
    full_rosters %>%
      filter(game_year == !!gy_int) %>%
      distinct(team_id, team_name) %>%
      arrange(team_name) %>%
      collect()
  })
  
  observeEvent(selected_game_year(), {
    td <- teams_for_year_df()
    updateSelectizeInput(session, "on_opponents", choices = td$team_name, selected = character(0), server = TRUE)
    updateSelectizeInput(session, "ld_opponents", choices = td$team_name, selected = character(0), server = TRUE)
  }, ignoreInit = FALSE)
  
  selected_opp_ids_on <- reactive({
    td <- teams_for_year_df()
    sel <- input$on_opponents
    if (is.null(sel) || !length(sel)) return(NULL)
    td %>% filter(team_name %in% sel) %>% pull(team_id)
  })
  
  selected_opp_ids_ld <- reactive({
    td <- teams_for_year_df()
    sel <- input$ld_opponents
    if (is.null(sel) || !length(sel)) return(NULL)
    td %>% filter(team_name %in% sel) %>% pull(team_id)
  })
  
  # ======== On/Off tab Logic ===================================
  output$date_filter_ui <- renderUI({
    gy <- selected_game_year()
    bounds <- season_date_bounds(gy)
    dateRangeInput("date_range", "Game Date Range", start = bounds$start, end = bounds$end, min = bounds$start, max = bounds$end, format = "yyyy-mm-dd", weekstart = 0)
  })
  
  teams_for_year <- reactive({
    gy_int <- as.integer(selected_game_year())
    req(gy_int)
    full_rosters %>%
      filter(game_year == !!gy_int) %>%
      distinct(team_id, team_name) %>%
      arrange(team_name) %>%
      collect()
  })
  
  output$team_filter_ui <- renderUI({
    teams <- teams_for_year()
    selectizeInput("teams", "Teams", choices = teams$team_name, multiple = TRUE, options = list(placeholder = "All teams"))
  })
  
  # --- Reset Logic (RESTORED) ---
  observeEvent(input$reset_defaults, {
    updateSelectInput(session, "game_year", selected = DEFAULT_GAME_YEAR)
    updateDateRangeInput(session, "date_range", start = DEFAULT_START, end = DEFAULT_END)
    updateSelectizeInput(session, "on_game_type", selected = "")
    updateSelectizeInput(session, "on_opponents", selected = character(0))
    updateSelectInput(session, "on_home_away", selected = "")
    updateSelectInput(session, "on_outcome", selected = "")
    updateSelectInput(session, "on_opp_rank_side", selected = "")
    updateSelectInput(session, "on_opp_rank_n", selected = "")
    updateSelectInput(session, "on_opp_rank_metric", selected = "")
    updateSliderInput(session, "min_all_poss", value = DEFAULT_MIN_ALL)
    updateSliderInput(session, "min_on_poss", value = DEFAULT_MIN_ON)
    # Clear teams
    updateSelectizeInput(session, "teams", selected = character(0))
  })
  
  debounced_range <- reactive(input$date_range) %>% debounce(300)
  debounced_teams <- reactive(input$teams) %>% debounce(300)
  debounced_on_filters <- reactive(list(
    game_type = input$on_game_type, 
    opp_names = input$on_opponents, 
    home_away = input$on_home_away, 
    outcome = input$on_outcome, 
    rank_side = input$on_opp_rank_side, 
    rank_n = input$on_opp_rank_n, 
    metric = input$on_opp_rank_metric
  )) %>% debounce(300)
  
  selected_team_ids <- reactive({
    teams <- teams_for_year()
    teams_in <- debounced_teams()
    if (is.null(teams_in) || !length(teams_in)) return(NULL)
    teams %>% filter(team_name %in% teams_in) %>% pull(team_id)
  })
  
  # --- UPDATED: Fallback Logic ---
  # We do NOT return true if only team/min_poss changed.
  fallback_needed <- reactive({
    rng <- debounced_range()
    if (is.null(rng)) return(TRUE)
    start_d <- as.Date(rng[1])
    end_d <- as.Date(rng[2])
    gy <- selected_game_year()
    season_bounds <- season_date_bounds(gy)
    
    date_changed <- (start_d != season_bounds$start) || (end_d != season_bounds$end)
    
    f <- debounced_on_filters()
    extra_filters <- (!is.null(f$game_type) && any(nzchar(f$game_type))) || 
      (!is.null(f$opp_names) && length(f$opp_names) > 0) || 
      nzchar(f$home_away %||% "") || 
      nzchar(f$outcome %||% "") || 
      nzchar(f$rank_side %||% "")
    
    date_changed || extra_filters
  })
  
  # --- On/Off Compute Function ---
  run_onoff_compute_14 <- function(pool, start_d, end_d, team_ids, min_all, min_on, min_net, game_year, game_type_csv, opp_ids_csv, home_away, outcome, opp_rank_side, opp_rank_n, opp_rank_metric) {
    team_csv <- if (is.null(team_ids) || !length(team_ids)) NA_character_ else paste(team_ids, collapse = ",")
    DBI::dbGetQuery(pool, paste0("SELECT * FROM basketball_test.onoff_compute(", "$1::date,$2::date,$3::text,$4::int4,$5::int4,$6::numeric,$7::text,", "$8::text,$9::text,$10::text,$11::text,$12::text,$13::int4,$14::text", ")"),
                    params = list(as.Date(start_d), as.Date(end_d), team_csv, as.integer(min_all), as.integer(min_on), as.numeric(min_net), as.character(game_year), game_type_csv, opp_ids_csv, home_away, outcome, opp_rank_side, opp_rank_n, opp_rank_metric))
  }
  
  # --- Four Factors Compute Function ---
  run_four_factors_compute <- function(pool, game_year, start_d, end_d, team_ids,
                                       game_type_csv, opp_ids_csv, home_away, outcome,
                                       opp_rank_side, opp_rank_n, opp_rank_metric) {
    team_csv <- if (is.null(team_ids) || !length(team_ids)) NA_character_ else paste(team_ids, collapse = ",")
    DBI::dbGetQuery(pool,
                    paste0("SELECT * FROM basketball_test.four_factors_compute(",
                           "$1::int4,$2::date,$3::date,$4::text,$5::text,$6::text,",
                           "$7::text,$8::text,$9::text,$10::int4,$11::text",
                           ")"),
                    params = list(as.integer(game_year), start_d, end_d, team_csv,
                                  game_type_csv, opp_ids_csv, home_away, outcome,
                                  opp_rank_side, opp_rank_n, opp_rank_metric))
  }
  
  # --- Live Calculation (Summary) ---
  live_result_df <- reactive({
    req(input$min_all_poss, input$min_on_poss)
    rng <- debounced_range()
    req(rng)
    tids <- selected_team_ids()
    gy <- selected_game_year()
    f <- debounced_on_filters()
    
    game_type_csv <- if (is.null(f$game_type) || !any(nzchar(f$game_type))) NA_character_ else paste(f$game_type[nzchar(f$game_type)], collapse = ",")
    opp_ids_csv <- {
      ids <- selected_opp_ids_on()
      if (is.null(ids)) NA_character_ else paste(ids, collapse = ",")
    }
    home_away <- if (!nzchar(f$home_away %||% "")) NA_character_ else f$home_away
    outcome <- if (!nzchar(f$outcome %||% "")) NA_character_ else f$outcome
    
    run_onoff_compute_14(pg_pool, start_d = as.Date(rng[1]), end_d = as.Date(rng[2]), team_ids = tids, min_all = input$min_all_poss, min_on = input$min_on_poss, min_net = DEFAULT_MIN_NET, game_year = gy, game_type_csv = game_type_csv, opp_ids_csv = opp_ids_csv, home_away = home_away, outcome = outcome, opp_rank_side = if (!nzchar(f$rank_side %||% "")) NA else f$rank_side, opp_rank_n = suppressWarnings(as.integer(if (!nzchar(f$rank_n %||% "")) NA else f$rank_n)), opp_rank_metric = if (!nzchar(f$metric %||% "")) NA else f$metric)
  })
  
  # --- Live Calculation (Four Factors) ---
  live_ff_result_df <- reactive({
    rng <- debounced_range()
    req(rng)
    gy <- selected_game_year()
    f <- debounced_on_filters()
    
    game_type_csv <- if (is.null(f$game_type) || !any(nzchar(f$game_type))) NA_character_ else paste(f$game_type[nzchar(f$game_type)], collapse = ",")
    opp_ids_csv <- {
      ids <- selected_opp_ids_on()
      if (is.null(ids)) NA_character_ else paste(ids, collapse = ",")
    }
    home_away <- if (!nzchar(f$home_away %||% "")) NA_character_ else f$home_away
    outcome <- if (!nzchar(f$outcome %||% "")) NA_character_ else f$outcome
    
    run_four_factors_compute(pg_pool,
                             game_year = gy,
                             start_d = as.Date(rng[1]),
                             end_d = as.Date(rng[2]),
                             team_ids = NULL,
                             game_type_csv = game_type_csv,
                             opp_ids_csv = opp_ids_csv,
                             home_away = home_away,
                             outcome = outcome,
                             opp_rank_side = if (!nzchar(f$rank_side %||% "")) NA else f$rank_side,
                             opp_rank_n = suppressWarnings(as.integer(if (!nzchar(f$rank_n %||% "")) NA else f$rank_n)),
                             opp_rank_metric = if (!nzchar(f$metric %||% "")) NA else f$metric)
  })
  
  # --- MV Fetch (Summary - LOAD FULL DATA) ---
  # Only load raw MV here. Filtering happens later in result_df.
  mv_result_df <- reactive({
    gy <- as.integer(selected_game_year())
    onoff_mv %>%
      filter(`Year` == !!gy) %>%
      arrange(desc(`Net RTG Diff`), `Team`, `Last Name`, `First Name`) %>%
      collect()
  })
  
  # --- MV Fetch (Four Factors - LOAD FULL DATA) ---
  advanced_result_df <- reactive({
    gy <- as.integer(selected_game_year())
    advanced_stats_mv %>%
      filter(game_year == !!gy) %>%
      collect()
  })
  
  # --- Full ranked Four Factors data (ranks computed BEFORE any user filtering) ---
  ff_ranked_df <- reactive({
    if (isTRUE(fallback_needed())) {
      # Dynamic SQL path: use four_factors_compute + onoff_compute for rating diffs
      df_adv <- live_ff_result_df()
      
      # Get RTG diffs for ALL players (no min_poss or team filter)
      # Min-poss and team filtering is applied later in result_df()
      rng <- debounced_range()
      gy <- selected_game_year()
      f <- debounced_on_filters()
      game_type_csv <- if (is.null(f$game_type) || !any(nzchar(f$game_type))) NA_character_ else paste(f$game_type[nzchar(f$game_type)], collapse = ",")
      opp_ids_csv <- {
        ids <- selected_opp_ids_on()
        if (is.null(ids)) NA_character_ else paste(ids, collapse = ",")
      }
      home_away <- if (!nzchar(f$home_away %||% "")) NA_character_ else f$home_away
      outcome <- if (!nzchar(f$outcome %||% "")) NA_character_ else f$outcome
      
      df_sum <- run_onoff_compute_14(pg_pool,
                                     start_d = as.Date(rng[1]), end_d = as.Date(rng[2]),
                                     team_ids = NULL, min_all = 0L, min_on = 0L, min_net = DEFAULT_MIN_NET,
                                     game_year = gy, game_type_csv = game_type_csv, opp_ids_csv = opp_ids_csv,
                                     home_away = home_away, outcome = outcome,
                                     opp_rank_side = if (!nzchar(f$rank_side %||% "")) NA else f$rank_side,
                                     opp_rank_n = suppressWarnings(as.integer(if (!nzchar(f$rank_n %||% "")) NA else f$rank_n)),
                                     opp_rank_metric = if (!nzchar(f$metric %||% "")) NA else f$metric) %>%
        select(player_id, team_id, `Net RTG Diff`, `Off ON Diff`, `Def ON Diff`)
      
      df <- df_adv %>%
        left_join(df_sum, by = c("player_id", "team_id"))
    } else {
      # MV path (existing behavior)
      df_adv <- advanced_result_df()
      
      # Join with Summary Stats to get Ratings (Net/Off/Def Diff)
      if (!"Net RTG Diff" %in% names(df_adv)) {
        df_sum <- mv_result_df() %>%
          select(player_id, team_id, "Year", `Net RTG Diff`, `Off ON Diff`, `Def ON Diff`)
        
        df <- df_adv %>%
          left_join(df_sum, by = c("player_id", "team_id", "game_year" = "Year"))
      } else {
        df <- df_adv
      }
    }
    
    # Derived display columns
    df <- df %>% mutate(
      `Off Rtg Diff` = as.numeric(`Off ON Diff`),
      `Def Rtg Diff` = as.numeric(`Def ON Diff`),
      `Net Diff`     = round(`Net RTG Diff`, 1)
    )
    
    # Calculate ALL ranks on full unfiltered dataset
    # Adaptive baseline: lower threshold when data is sparse (narrow date ranges)
    rank_thresh <- adaptive_baseline(df$off_on_poss)
    
    # Background color ranks (pr_ prefix)
    df <- df %>% mutate(
      pr_net_diff = percent_rank(if_else(off_on_poss >= rank_thresh, coalesce(`Net Diff`, -999), NA_real_)),
      pr_off_rtg  = percent_rank(if_else(off_on_poss >= rank_thresh, coalesce(`Off Rtg Diff`, -999), NA_real_)),
      pr_def_rtg  = percent_rank(if_else(off_on_poss >= rank_thresh, coalesce(`Def Rtg Diff`, 999), NA_real_)),
      
      pr_diff_off_ts   = percent_rank(if_else(off_on_poss >= rank_thresh, off_on_ts - off_off_ts, NA_real_)),
      pr_diff_off_oreb = percent_rank(if_else(off_on_poss >= rank_thresh, off_on_oreb - off_off_oreb, NA_real_)),
      pr_diff_off_ftr  = percent_rank(if_else(off_on_poss >= rank_thresh, off_on_ftr - off_off_ftr, NA_real_)),
      pr_diff_off_tov  = percent_rank(if_else(off_on_poss >= rank_thresh, off_on_tov - off_off_tov, NA_real_)),
      
      pr_diff_def_ts   = percent_rank(if_else(off_on_poss >= rank_thresh, def_on_ts - def_off_ts, NA_real_)),
      pr_diff_def_oreb = percent_rank(if_else(off_on_poss >= rank_thresh, def_on_oreb - def_off_oreb, NA_real_)),
      pr_diff_def_ftr  = percent_rank(if_else(off_on_poss >= rank_thresh, def_on_ftr - def_off_ftr, NA_real_)),
      pr_diff_def_tov  = percent_rank(if_else(off_on_poss >= rank_thresh, def_on_tov - def_off_tov, NA_real_))
    )
    
    # Dot position ranks (_rank suffix) for range bar visuals
    raw_cols <- c("off_on_ts", "off_off_ts", "off_on_oreb", "off_off_oreb",
                  "off_on_tov", "off_off_tov", "off_on_ftr", "off_off_ftr",
                  "def_on_ts", "def_off_ts", "def_on_oreb", "def_off_oreb",
                  "def_on_tov", "def_off_tov", "def_on_ftr", "def_off_ftr")
    for (col in intersect(raw_cols, names(df))) {
      vals <- if_else(df$off_on_poss >= rank_thresh, coalesce(df[[col]], 0), NA_real_)
      df[[paste0(col, "_rank")]] <- percent_rank(vals) * 100
    }
    
    df
  })
  
  # --- Final Switcher ---
  result_df <- reactive({
    mode <- input$onoff_view_mode
    if (identical(mode, "Four Factors")) {
      
      df <- ff_ranked_df()
      
      # Filter LOCALLY (ranks already computed on full data)
      tids <- selected_team_ids()
      if (!is.null(tids) && length(tids) > 0) {
        df <- df %>% filter(team_id %in% !!tids)
      }
      df <- df %>% filter(off_on_poss >= !!input$min_on_poss)
      
      return(df)
      
    } else {
      # Summary Mode
      if (isTRUE(fallback_needed())) {
        return(live_result_df())
      } else {
        # Standard View = Use MV
        df <- mv_result_df()
        
        # --- FILTERING for Summary (Local) ---
        tids_names <- input$teams 
        if (!is.null(tids_names) && length(tids_names) > 0) {
          df <- df %>% filter(Team %in% tids_names)
        }
        
        # Filter Min Poss (Summary MV uses 'ON Poss')
        df <- df %>% filter(`ON Poss` >= !!input$min_on_poss)
        
        return(df)
      }
    }
  }) %>% bindEvent(debounced_range(), debounced_teams(), debounced_on_filters(), input$min_all_poss, input$min_on_poss, input$game_year, input$onoff_view_mode)
  
  # --- Render Table ---
  output$onoff_dt <- renderDT({
    df <- result_df()
    mode <- input$onoff_view_mode
    
    # Standard Name Cleanup
    if (!"Player" %in% names(df) && all(c("First Name", "Last Name") %in% names(df))) {
      df <- df %>% mutate(Player = paste(`First Name`, `Last Name`))
    } else if (!"Player" %in% names(df) && all(c("firstname", "lastname") %in% names(df))) {
      df <- df %>% mutate(Player = paste(firstname, lastname))
    }
    if ("team_name" %in% names(df)) df <- df %>% rename(Team = team_name)
    
    cuts <- seq(0.05, 0.95, by = 0.05)
    cols_grad <- colorRampPalette(c("#d73027", "#fee08b", "#1a9850"))(20)
    cols_rev  <- rev(cols_grad)
    
    if (identical(mode, "Summary")) {
      keep_cols <- c(
        "Team", "Player", 
        "Net RTG Diff", "Off ON Diff", "Def ON Diff", 
        "Off ON PPP", "Def ON PPP", "On Net RTG", 
        "Off OFF PPP", "Def OFF PPP", "Off Net RTG", 
        "ON Poss", "OFF Poss",
        "pr_net", "pr_off_on_d", "pr_def_on_d", "pr_off_on", "pr_def_on_inv", "pr_on_net", "pr_off_off", "pr_def_off_inv", "pr_off_net", "pr_def_on_d_inv"
      )
      df <- df[, intersect(keep_cols, names(df))]
      
      idx_net <- which(names(df) == "Net RTG Diff") - 1
      idx_on  <- which(names(df) == "Off ON PPP") - 1
      idx_off <- which(names(df) == "Off OFF PPP") - 1
      idx_use <- which(names(df) == "ON Poss") - 1
      
      pr_cols <- names(df)[grep("^pr_", names(df))]
      hide_idx <- which(names(df) %in% pr_cols) - 1
      
      sketch_summary <- htmltools::withTags(table(class = 'display', thead(
        tr(
          th(class="group-head", colspan=2, ""),
          th(class="group-head section-left-border", colspan=3, "Net Impact"),
          th(class="group-head section-left-border", colspan=3, "On Court Stats"),
          th(class="group-head section-left-border", colspan=3, "Off Court Stats"),
          th(class="group-head section-left-border", colspan=2, "Usage")
        ),
        tr(
          th(class="sub-head", "Team"), th(class="sub-head", "Player"),
          th(class="sub-head section-left-border", "Net"), th(class="sub-head", "Off"), th(class="sub-head", "Def"),
          th(class="sub-head section-left-border", "Off PPP"), th(class="sub-head", "Def PPP"), th(class="sub-head", "Net Rtg"),
          th(class="sub-head section-left-border", "Off PPP"), th(class="sub-head", "Def PPP"), th(class="sub-head", "Net Rtg"),
          th(class="sub-head section-left-border", "On Poss"), th(class="sub-head", "Off Poss")
        )
      )))
      
      dt <- datatable(df, container = sketch_summary, rownames = FALSE, 
                      options = list(dom = "tip", pageLength = 30, scrollX = TRUE,
                                     order = list(list(which(names(df) == "Net RTG Diff") - 1, "desc")),
                                     columnDefs = list(
                                       list(targets = c(idx_net, idx_on, idx_off, idx_use), className = "section-left-border"),
                                       list(targets = hide_idx, visible = FALSE),
                                       list(targets = "_all", className = "dt-center")
                                     ))) |>
        formatRound(c("Net RTG Diff", "Off ON Diff", "Def ON Diff", "On Net RTG", "Off Net RTG"), 2) |>
        formatRound(c("Off ON PPP", "Def ON PPP", "Off OFF PPP", "Def OFF PPP"), 1) |>
        formatCurrency(c("ON Poss", "OFF Poss"), currency = "", interval = 3, mark = ",", digits = 0)
      
      if("pr_net" %in% names(df)) dt <- formatStyle(dt, "Net RTG Diff", backgroundColor = styleInterval(cuts, cols_grad), valueColumns = "pr_net")
      if("pr_off_on_d" %in% names(df)) dt <- formatStyle(dt, "Off ON Diff", backgroundColor = styleInterval(cuts, cols_grad), valueColumns = "pr_off_on_d")
      if("pr_def_on_d" %in% names(df)) dt <- formatStyle(dt, "Def ON Diff", backgroundColor = styleInterval(cuts, cols_rev), valueColumns = "pr_def_on_d")
      
      if("pr_off_on" %in% names(df)) dt <- formatStyle(dt, "Off ON PPP", backgroundColor = styleInterval(cuts, cols_grad), valueColumns = "pr_off_on")
      if("pr_def_on_inv" %in% names(df)) dt <- formatStyle(dt, "Def ON PPP", backgroundColor = styleInterval(cuts, cols_grad), valueColumns = "pr_def_on_inv")
      if("pr_on_net" %in% names(df)) dt <- formatStyle(dt, "On Net RTG", backgroundColor = styleInterval(cuts, cols_grad), valueColumns = "pr_on_net")
      
      if("pr_off_off" %in% names(df)) dt <- formatStyle(dt, "Off OFF PPP", backgroundColor = styleInterval(cuts, cols_grad), valueColumns = "pr_off_off")
      if("pr_def_off_inv" %in% names(df)) dt <- formatStyle(dt, "Def OFF PPP", backgroundColor = styleInterval(cuts, cols_grad), valueColumns = "pr_def_off_inv")
      if("pr_off_net" %in% names(df)) dt <- formatStyle(dt, "Off Net RTG", backgroundColor = styleInterval(cuts, cols_grad), valueColumns = "pr_off_net")
      
      return(dt)
      
    } else {
      # === MODE 2: FOUR FACTORS ===
      
      metric_map <- list(
        "Off TS% Diff"   = c("off_on_ts", "off_off_ts"),
        "Off OREB% Diff" = c("off_on_oreb", "off_off_oreb"),
        "Off TOV% Diff"  = c("off_on_tov", "off_off_tov"),
        "Off FTR Diff"   = c("off_on_ftr", "off_off_ftr"),
        "Def TS% Diff"   = c("def_on_ts", "def_off_ts"),
        "Def OREB% Diff" = c("def_on_oreb", "def_off_oreb"),
        "Def TOV% Diff"  = c("def_on_tov", "def_off_tov"),
        "Def FTR Diff"   = c("def_on_ftr", "def_off_ftr")
      )
      
      raw_cols_all <- unique(unlist(metric_map))
      
      # Rounding
      df <- df %>% mutate(across(all_of(intersect(raw_cols_all, names(df))), ~ round(as.numeric(.) * 100, 1)))
      df <- df %>% mutate(across(all_of(intersect(names(metric_map), names(df))), ~ round(as.numeric(.), 1)))
      
      # Dot position ranks (_rank columns) already computed in ff_ranked_df()
      
      # Rename poss columns for display
      df <- df %>% rename(`ON Poss` = off_on_poss, `OFF Poss` = off_off_poss)
      
      # 3. SELECT & ORDER COLUMNS
      vis_cols <- c("Team", "Player", "Net Diff", "Off Rtg Diff", "Def Rtg Diff", intersect(names(metric_map), names(df)), "ON Poss", "OFF Poss")
      
      rank_cols <- intersect(c(
        "pr_net_diff", "pr_off_rtg", "pr_def_rtg",
        "pr_diff_off_ts", "pr_diff_off_oreb", "pr_diff_off_tov", "pr_diff_off_ftr",
        "pr_diff_def_ts", "pr_diff_def_oreb", "pr_diff_def_tov", "pr_diff_def_ftr"
      ), names(df))
      
      df_final <- df %>% select(all_of(vis_cols), any_of(rank_cols), ends_with("_rank"), all_of(raw_cols_all))
      
      final_vis_order <- c(
        "Team", "Player", "Net Diff",
        "Off Rtg Diff", "Off TS% Diff", "Off OREB% Diff", "Off TOV% Diff", "Off FTR Diff",
        "Def Rtg Diff", "Def TS% Diff", "Def OREB% Diff", "Def TOV% Diff", "Def FTR Diff",
        "ON Poss", "OFF Poss"
      )
      
      final_vis_order <- intersect(final_vis_order, names(df_final))
      final_col_order <- c(final_vis_order, setdiff(names(df_final), final_vis_order))
      df_final <- df_final %>% select(all_of(final_col_order))
      
      defs <- list()
      
      for (i in seq_along(metric_map)) {
        diff_name <- names(metric_map)[i]
        if (!diff_name %in% names(df_final)) next
        target_idx <- which(names(df_final) == diff_name) - 1L
        
        on_col <- metric_map[[i]][1]
        off_col <- metric_map[[i]][2]
        
        if (on_col %in% names(df_final) && off_col %in% names(df_final)) {
          on_val_idx <- which(names(df_final) == on_col) - 1L
          off_val_idx <- which(names(df_final) == off_col) - 1L
          on_rank_idx <- which(names(df_final) == paste0(on_col, "_rank")) - 1L
          off_rank_idx <- which(names(df_final) == paste0(off_col, "_rank")) - 1L
          
          js_func <- JS(sprintf(
            "function(data, type, row, meta) {
               if (type === 'display') {
                 var diffVal = (data === null) ? '-' : data;
                 var onVal   = row[%d] || '-';
                 var offVal  = row[%d] || '-';
                 var onPct   = row[%d];
                 var offPct  = row[%d];
                 
                 if (onPct === null || onPct === undefined) {
                    return '<div class=\"diff-val unranked\">' + diffVal + '</div>' +
                           '<div class=\"rank-bar-container hidden\"></div>' + 
                           '<div class=\"sub-text\" style=\"opacity:0.5;\">' + onVal + ' | ' + offVal + '</div>';
                 }
                 
                 var rangeLineLeft  = Math.min(onPct, offPct);
                 var rangeLineWidth = Math.abs(onPct - offPct);
                 
                 return '<div class=\"diff-val\">' + diffVal + '</div>' +
                        '<div class=\"rank-bar-container\">' +
                          '<div class=\"rank-track\"></div>' +
                          '<div class=\"range-connect\" style=\"left:' + rangeLineLeft + '%%; width:' + rangeLineWidth + '%%;\"></div>' +
                          '<div class=\"dot-off\" style=\"left:' + offPct + '%%;\" title=\"Off: ' + offVal + '\"></div>' +
                          '<div class=\"dot-on\" style=\"left:' + onPct + '%%;\" title=\"On: ' + onVal + '\"></div>' +
                        '</div>' + 
                        '<div class=\"sub-text\">' +
                          '<span style=\"font-weight:700; color:#222;\">' + onVal + '</span>' + 
                          ' <span style=\"opacity:0.6;\">|</span> ' + 
                          '<span style=\"color:#666;\">' + offVal + '</span>' +
                        '</div>';
               }
               return data;
             }", on_val_idx, off_val_idx, on_rank_idx, off_rank_idx
          ))
          defs[[length(defs) + 1]] <- list(targets = target_idx, render = js_func)
        }
      }
      
      # Hide auxiliary columns
      hide_cols <- c(rank_cols, raw_cols_all, names(df)[grep("_rank$", names(df))])
      hide_idx <- which(names(df_final) %in% hide_cols) - 1L
      if (length(hide_idx)) defs[[length(defs) + 1]] <- list(targets = hide_idx, visible = FALSE)
      
      # --- SEPARATORS (Thick borders for 3 sections) ---
      
      off_rtg_idx <- which(names(df_final) == "Off Rtg Diff") - 1L
      if(length(off_rtg_idx)) defs[[length(defs) + 1]] <- list(targets = off_rtg_idx, className = "section-left-border")
      
      def_rtg_idx <- which(names(df_final) == "Def Rtg Diff") - 1L
      if(length(def_rtg_idx)) defs[[length(defs) + 1]] <- list(targets = def_rtg_idx, className = "section-left-border")
      
      on_poss_idx <- which(names(df_final) == "ON Poss") - 1L
      if(length(on_poss_idx)) defs[[length(defs) + 1]] <- list(targets = on_poss_idx, className = "section-left-border")
      
      # Net Diff Style
      net_diff_idx <- which(names(df_final) == "Net Diff") - 1L
      if(length(net_diff_idx)) {
        defs[[length(defs) + 1]] <- list(targets = net_diff_idx, className = "dt-center",
                                         render = JS("function(data, type, row) {
                                            if(type === 'display') return '<div style=\"font-weight:800; font-size:1.05em;\">' + data + '</div>';
                                            return data;
                                         }"))
      }
      
      defs[[length(defs) + 1]] <- list(targets = "_all", className = "dt-center")
      
      sketch_ff <- htmltools::withTags(table(class = 'display', thead(
        tr(
          th(class = "group-head", colspan = 2, ""),
          th(class = "group-head", "Total"),
          th(class = "group-head section-left-border", colspan = 5, "Offense Impact (On-Off)"),
          th(class = "group-head section-left-border", colspan = 5, "Defense Impact (On-Off)"),
          th(class = "group-head section-left-border", colspan = 2, "Usage")
        ),
        tr(
          th(class = "sub-head", "Team"), th(class = "sub-head", "Player"),
          th(class = "sub-head", "Diff"),
          th(class = "sub-head section-left-border", "Diff"), th(class = "sub-head", "TS%"), th(class = "sub-head", "OREB%"), th(class = "sub-head", "TOV%"), th(class = "sub-head", "FTR"),
          th(class = "sub-head section-left-border", "Diff"), th(class = "sub-head", "TS%"), th(class = "sub-head", "OREB%"), th(class = "sub-head", "TOV%"), th(class = "sub-head", "FTR"),
          th(class = "sub-head section-left-border", "On Poss"), th(class = "sub-head", "Off Poss")
        )
      )))
      
      dt <- datatable(df_final,
                      container = sketch_ff, rownames = FALSE, escape = FALSE,
                      options = list(
                        dom = "t", pageLength = 50, deferRender = TRUE, scrollX = TRUE,
                        order = list(list(2, "desc")),
                        columnDefs = defs
                      )
      )
      
      # --- FORMAT POSS COLUMNS ---
      dt <- formatCurrency(dt, c("ON Poss", "OFF Poss"), currency = "", interval = 3, mark = ",", digits = 0)
      
      # --- COLOR LOGIC ---
      if ("pr_net_diff" %in% names(df_final)) dt <- formatStyle(dt, "Net Diff", backgroundColor = styleInterval(cuts, cols_grad), valueColumns = "pr_net_diff")
      
      # Offense Ratings (High Diff = Good)
      if ("pr_off_rtg" %in% names(df_final)) dt <- formatStyle(dt, "Off Rtg Diff", backgroundColor = styleInterval(cuts, cols_grad), valueColumns = "pr_off_rtg")
      
      # Defense Ratings (High Diff = Bad -> Reverse)
      cols_grad_rev <- rev(cols_grad)
      if ("pr_def_rtg" %in% names(df_final)) dt <- formatStyle(dt, "Def Rtg Diff", backgroundColor = styleInterval(cuts, cols_grad_rev), valueColumns = "pr_def_rtg")
      
      # Offense Factors
      if ("pr_diff_off_ts" %in% names(df_final)) dt <- formatStyle(dt, "Off TS% Diff", backgroundColor = styleInterval(cuts, cols_grad), valueColumns = "pr_diff_off_ts")
      if ("pr_diff_off_oreb" %in% names(df_final)) dt <- formatStyle(dt, "Off OREB% Diff", backgroundColor = styleInterval(cuts, cols_grad), valueColumns = "pr_diff_off_oreb")
      if ("pr_diff_off_ftr" %in% names(df_final)) dt <- formatStyle(dt, "Off FTR Diff", backgroundColor = styleInterval(cuts, cols_grad), valueColumns = "pr_diff_off_ftr")
      if ("pr_diff_off_tov" %in% names(df_final)) dt <- formatStyle(dt, "Off TOV% Diff", backgroundColor = styleInterval(cuts, cols_grad_rev), valueColumns = "pr_diff_off_tov")
      
      # Defense Factors
      if ("pr_diff_def_ts" %in% names(df_final)) dt <- formatStyle(dt, "Def TS% Diff", backgroundColor = styleInterval(cuts, cols_grad_rev), valueColumns = "pr_diff_def_ts")
      if ("pr_diff_def_oreb" %in% names(df_final)) dt <- formatStyle(dt, "Def OREB% Diff", backgroundColor = styleInterval(cuts, cols_grad_rev), valueColumns = "pr_diff_def_oreb")
      if ("pr_diff_def_ftr" %in% names(df_final)) dt <- formatStyle(dt, "Def FTR Diff", backgroundColor = styleInterval(cuts, cols_grad_rev), valueColumns = "pr_diff_def_ftr")
      if ("pr_diff_def_tov" %in% names(df_final)) dt <- formatStyle(dt, "Def TOV% Diff", backgroundColor = styleInterval(cuts, cols_grad), valueColumns = "pr_diff_def_tov")
      
      return(dt)
    }
  }) %>% bindEvent(debounced_range(), debounced_teams(), debounced_on_filters(), input$min_all_poss, input$min_on_poss, input$game_year, input$onoff_view_mode)
  
  # ... (Tab 2 and Tab 3 logic remains unchanged) ...
  ld_ref <- reactiveValues(teams = NULL, players = NULL)
  observeEvent(list(input$main_tabs, input$game_year_ld), ignoreInit = TRUE, {
    if (!identical(input$main_tabs, "lineup_data")) return(NULL)
    gy_int <- as.integer(input$game_year_ld)
    
    teams_ld <- DBI::dbGetQuery(pg_pool, "SELECT DISTINCT team_id, MIN(team_name) AS team_name FROM basketball_test.full_rosters WHERE game_year = $1 GROUP BY team_id ORDER BY MIN(team_name)", params = list(gy_int))
    ld_ref$teams <- teams_ld
    team_values <- c("", as.character(teams_ld$team_id))
    names(team_values) <- c("— All teams —", teams_ld$team_name)
    updateSelectizeInput(session, "ld_team", choices = team_values, selected = "", server = TRUE)
    
    players_map <- DBI::dbGetQuery(pg_pool, "SELECT team_id, player_id, MIN(btrim(firstname)||' '||btrim(lastname)) AS name FROM basketball_test.full_rosters WHERE game_year = $1 GROUP BY team_id, player_id ORDER BY MIN(btrim(firstname)||' '||btrim(lastname))", params = list(gy_int))
    ld_ref$players <- players_map
    
    updateSelectizeInput(session, "ld_players_on", choices = setNames(integer(0), character(0)), selected = character(0), server = TRUE)
    updateSelectizeInput(session, "ld_players_off", choices = setNames(integer(0), character(0)), selected = character(0), server = TRUE)
  })
  
  observeEvent(input$ld_team, {
    req(identical(input$main_tabs, "lineup_data"))
    if (is.null(input$ld_team) || is.na(input$ld_team) || !nzchar(input$ld_team)) {
      updateSelectizeInput(session, "ld_players_on", choices = setNames(integer(0), character(0)), selected = character(0), server = TRUE)
      updateSelectizeInput(session, "ld_players_off", choices = setNames(integer(0), character(0)), selected = character(0), server = TRUE)
      return(invisible(NULL))
    }
    team_id <- as.integer(input$ld_team)
    players <- ld_ref$players %>% filter(team_id == !!team_id)
    choices <- setNames(players$player_id, players$name)
    updateSelectizeInput(session, "ld_players_on", choices = choices, selected = character(0), server = TRUE)
    updateSelectizeInput(session, "ld_players_off", choices = choices, selected = character(0), server = TRUE)
  }, ignoreInit = TRUE)
  
  observeEvent(input$ld_players_on, {
    on_sel <- input$ld_players_on %||% character(0)
    off_sel <- input$ld_players_off %||% character(0)
    inter <- intersect(on_sel, off_sel)
    if (length(inter)) updateSelectizeInput(session, "ld_players_off", selected = setdiff(off_sel, inter), server = TRUE)
  }, ignoreInit = TRUE)
  
  observeEvent(input$ld_players_off, {
    on_sel <- input$ld_players_on %||% character(0)
    off_sel <- input$ld_players_off %||% character(0)
    inter <- intersect(on_sel, off_sel)
    if (length(inter)) updateSelectizeInput(session, "ld_players_on", selected = setdiff(on_sel, inter), server = TRUE)
  }, ignoreInit = TRUE)
  
  observeEvent(input$ld_reset, {
    updateRadioButtons(session, "ld_view_mode", selected = "Summary")
    updateRadioButtons(session, "ld_num", selected = LD_DEFAULT_NUM)
    updateDateRangeInput(session, "ld_dates", start = NA, end = NA)
    if (!is.null(ld_ref$teams)) {
      team_values <- c("", as.character(ld_ref$teams$team_id))
      names(team_values) <- c("— All teams —", ld_ref$teams$team_name)
      updateSelectizeInput(session, "ld_team", choices = team_values, selected = "", server = TRUE)
    } else {
      updateSelectizeInput(session, "ld_team", selected = "", server = TRUE)
    }
    updateSelectizeInput(session, "ld_players_on", choices = setNames(integer(0), character(0)), selected = character(0), server = TRUE)
    updateSelectizeInput(session, "ld_players_off", choices = setNames(integer(0), character(0)), selected = character(0), server = TRUE)
    updateSliderInput(session, "ld_minposs", value = LD_DEFAULT_MIN_POSS)
    updateSelectizeInput(session, "ld_game_type", selected = "")
    updateSelectizeInput(session, "ld_opponents", selected = character(0))
    updateSelectInput(session, "ld_home_away", selected = "")
    updateSelectInput(session, "ld_outcome", selected = "")
    updateSelectInput(session, "ld_opp_rank_side", selected = "")
    updateSelectInput(session, "ld_opp_rank_n", selected = "")
    updateSelectInput(session, "ld_opp_rank_metric", selected = "")
  })
  
  run_fetch_lineups_16 <- function(pool, num, team_csv, player_csv, player_off_csv, exact, start_date, end_date, min_poss, game_year, game_type_csv, opp_ids_csv, home_away, outcome, opp_rank_side, opp_rank_n, opp_rank_metric) {
    DBI::dbGetQuery(pool, paste0("SELECT * FROM basketball_test.fetch_lineups_csv_v2(", "$1::int4,$2::text,$3::text,$4::text,$5::bool,$6::date,$7::date,$8::int4,$9::int4,", "$10::text,$11::text,$12::text,$13::text,$14::text,$15::int4,$16::text", ")"), params = list(as.integer(num), team_csv, player_csv, player_off_csv, as.logical(exact), as.Date(start_date), as.Date(end_date), as.integer(min_poss), as.integer(game_year), game_type_csv, opp_ids_csv, home_away, outcome, opp_rank_side, opp_rank_n, opp_rank_metric))
  }
  
  run_fetch_lineups_ff_16 <- function(pool, num, team_csv, player_csv, player_off_csv, exact, start_date, end_date, min_poss, game_year, game_type_csv, opp_ids_csv, home_away, outcome, opp_rank_side, opp_rank_n, opp_rank_metric) {
    DBI::dbGetQuery(pool, paste0("SELECT * FROM basketball_test.fetch_lineups_four_factors_csv(", "$1::int4,$2::text,$3::text,$4::text,$5::bool,$6::date,$7::date,$8::int4,$9::int4,", "$10::text,$11::text,$12::text,$13::text,$14::text,$15::int4,$16::text", ")"), params = list(as.integer(num), team_csv, player_csv, player_off_csv, as.logical(exact), as.Date(start_date), as.Date(end_date), as.integer(min_poss), as.integer(game_year), game_type_csv, opp_ids_csv, home_away, outcome, opp_rank_side, opp_rank_n, opp_rank_metric))
  }
  
  # --- Full ranked FF data (ranks computed BEFORE any local filtering) ---
  # Only re-fetches when game filters or group size change.
  # Team, players on/off, min poss are applied locally afterward.
  ld_ff_ranked_df <- reactive({
    req(identical(input$main_tabs, "lineup_data"))
    gy <- as.integer(input$game_year_ld)
    num <- as.integer(input$ld_num)
    
    # Extract game filter params (same logic as ld_params)
    game_type_csv <- {
      x <- input$ld_game_type
      if (is.null(x) || !length(x) || !any(nzchar(x))) NA_character_ else paste(x[nzchar(x)], collapse = ",")
    }
    opp_ids_csv <- {
      ids <- selected_opp_ids_ld()
      if (is.null(ids) || !length(ids)) NA_character_ else paste(ids, collapse = ",")
    }
    home_away <- if (!nzchar(input$ld_home_away %||% "")) NA_character_ else input$ld_home_away
    outcome <- if (!nzchar(input$ld_outcome %||% "")) NA_character_ else input$ld_outcome
    rank_side <- if (!nzchar(input$ld_opp_rank_side %||% "")) NA_character_ else input$ld_opp_rank_side
    rank_n <- suppressWarnings(as.integer(if (!nzchar(input$ld_opp_rank_n %||% "")) NA_character_ else input$ld_opp_rank_n))
    metric <- if (!nzchar(input$ld_opp_rank_metric %||% "")) NA_character_ else input$ld_opp_rank_metric
    
    start_date <- if (!is.null(input$ld_dates[1]) && !is.na(input$ld_dates[1])) as.Date(input$ld_dates[1]) else NA
    end_date <- if (!is.null(input$ld_dates[2]) && !is.na(input$ld_dates[2])) as.Date(input$ld_dates[2]) else NA
    
    # Fetch ALL lineups for group size + game filters (no team/player/min_poss)
    df <- run_fetch_lineups_ff_16(pg_pool,
                                  num = num, team_csv = NA_character_, player_csv = NA_character_,
                                  player_off_csv = NA_character_, exact = TRUE,
                                  start_date = start_date, end_date = end_date,
                                  min_poss = 0L, game_year = gy,
                                  game_type_csv = game_type_csv, opp_ids_csv = opp_ids_csv,
                                  home_away = home_away, outcome = outcome,
                                  opp_rank_side = rank_side, opp_rank_n = rank_n, opp_rank_metric = metric)
    
    if (is.null(df) || NROW(df) == 0L) return(df)
    
    df$total_poss <- dplyr::coalesce(df$off_poss, 0L) + dplyr::coalesce(df$def_poss, 0L)
    
    # Compute percentile ranks on the FULL unfiltered dataset.
    # Adaptive baseline: lowers threshold when data is sparse.
    rank_thresh <- adaptive_baseline(df$total_poss)
    qualified <- df$total_poss >= rank_thresh
    
    pr_vec <- function(x, invert = FALSE) {
      vals <- ifelse(qualified, x, NA_real_)
      n <- sum(!is.na(vals))
      if (n <= 1) return(rep(NA_real_, length(vals)))
      r <- rank(vals, na.last = "keep", ties.method = "average")
      p <- (r - 1) / (n - 1)
      if (invert) p <- 1 - p
      as.numeric(p)
    }
    
    if ("off_ppp"  %in% names(df)) df$pr_off_ppp  <- pr_vec(df$off_ppp)
    if ("off_ts"   %in% names(df)) df$pr_off_ts   <- pr_vec(df$off_ts)
    if ("off_oreb" %in% names(df)) df$pr_off_oreb <- pr_vec(df$off_oreb)
    if ("off_tov"  %in% names(df)) df$pr_off_tov  <- pr_vec(df$off_tov, invert = TRUE)
    if ("off_ftr"  %in% names(df)) df$pr_off_ftr  <- pr_vec(df$off_ftr)
    if ("def_ppp"  %in% names(df)) df$pr_def_ppp  <- pr_vec(df$def_ppp, invert = TRUE)
    if ("def_ts"   %in% names(df)) df$pr_def_ts   <- pr_vec(df$def_ts, invert = TRUE)
    if ("def_oreb" %in% names(df)) df$pr_def_oreb <- pr_vec(df$def_oreb, invert = TRUE)
    if ("def_tov"  %in% names(df)) df$pr_def_tov  <- pr_vec(df$def_tov)
    if ("def_ftr"  %in% names(df)) df$pr_def_ftr  <- pr_vec(df$def_ftr, invert = TRUE)
    if ("net_rtg"  %in% names(df)) df$pr_net      <- pr_vec(df$net_rtg)
    
    df
  }) %>% bindEvent(input$ld_num, input$ld_dates, input$game_year_ld,
                   input$ld_game_type, input$ld_opponents, input$ld_home_away,
                   input$ld_outcome, input$ld_opp_rank_side, input$ld_opp_rank_n,
                   input$ld_opp_rank_metric, input$main_tabs, input$ld_view_mode)
  
  # --- Full ranked Summary data (ranks computed BEFORE any local filtering) ---
  # Same pattern as ld_ff_ranked_df but for the Summary view.
  ld_summary_ranked_df <- reactive({
    req(identical(input$main_tabs, "lineup_data"))
    gy <- as.integer(input$game_year_ld)
    num <- as.integer(input$ld_num)
    
    game_type_csv <- {
      x <- input$ld_game_type
      if (is.null(x) || !length(x) || !any(nzchar(x))) NA_character_ else paste(x[nzchar(x)], collapse = ",")
    }
    opp_ids_csv <- {
      ids <- selected_opp_ids_ld()
      if (is.null(ids) || !length(ids)) NA_character_ else paste(ids, collapse = ",")
    }
    home_away <- if (!nzchar(input$ld_home_away %||% "")) NA_character_ else input$ld_home_away
    outcome <- if (!nzchar(input$ld_outcome %||% "")) NA_character_ else input$ld_outcome
    rank_side <- if (!nzchar(input$ld_opp_rank_side %||% "")) NA_character_ else input$ld_opp_rank_side
    rank_n <- suppressWarnings(as.integer(if (!nzchar(input$ld_opp_rank_n %||% "")) NA_character_ else input$ld_opp_rank_n))
    metric <- if (!nzchar(input$ld_opp_rank_metric %||% "")) NA_character_ else input$ld_opp_rank_metric
    
    start_date <- if (!is.null(input$ld_dates[1]) && !is.na(input$ld_dates[1])) as.Date(input$ld_dates[1]) else NA
    end_date <- if (!is.null(input$ld_dates[2]) && !is.na(input$ld_dates[2])) as.Date(input$ld_dates[2]) else NA
    
    df <- run_fetch_lineups_16(pg_pool,
                               num = num, team_csv = NA_character_, player_csv = NA_character_,
                               player_off_csv = NA_character_, exact = TRUE,
                               start_date = start_date, end_date = end_date,
                               min_poss = 0L, game_year = gy,
                               game_type_csv = game_type_csv, opp_ids_csv = opp_ids_csv,
                               home_away = home_away, outcome = outcome,
                               opp_rank_side = rank_side, opp_rank_n = rank_n, opp_rank_metric = metric)
    
    if (is.null(df) || NROW(df) == 0L) return(df)
    
    df$total_poss <- dplyr::coalesce(df$off_poss, 0L) + dplyr::coalesce(df$def_poss, 0L)
    df$plus_minus <- dplyr::coalesce(df$off_pts, 0) - dplyr::coalesce(df$def_pts, 0)
    
    # Adaptive baseline: lowers threshold when data is sparse
    rank_thresh <- adaptive_baseline(df$total_poss)
    qualified <- df$total_poss >= rank_thresh
    
    pr_vec <- function(x, invert = FALSE) {
      vals <- ifelse(qualified, x, NA_real_)
      n <- sum(!is.na(vals))
      if (n <= 1) return(rep(NA_real_, length(vals)))
      r <- rank(vals, na.last = "keep", ties.method = "average")
      p <- (r - 1) / (n - 1)
      if (invert) p <- 1 - p
      as.numeric(p)
    }
    
    if ("net_rtg" %in% names(df)) df$pr_ld_net       <- pr_vec(df$net_rtg)
    if ("off_ppp" %in% names(df)) df$pr_ld_off_ppp   <- pr_vec(df$off_ppp)
    if ("def_ppp" %in% names(df)) df$pr_ld_def_ppp_i <- pr_vec(df$def_ppp, invert = TRUE)
    
    df
  }) %>% bindEvent(input$ld_num, input$ld_dates, input$game_year_ld,
                   input$ld_game_type, input$ld_opponents, input$ld_home_away,
                   input$ld_outcome, input$ld_opp_rank_side, input$ld_opp_rank_n,
                   input$ld_opp_rank_metric, input$main_tabs, input$ld_view_mode)
  
  ld_params <- reactive({
    req(identical(input$main_tabs, "lineup_data"))
    team_id <- if (!is.null(input$ld_team) && !is.na(input$ld_team) && nzchar(input$ld_team)) as.integer(input$ld_team) else NA_integer_
    player_on_ids <- if (!is.na(team_id)) as.integer(input$ld_players_on) else integer(0)
    player_off_ids <- if (!is.na(team_id)) as.integer(input$ld_players_off) else integer(0)
    ld_game_type_csv <- {
      x <- input$ld_game_type
      if (is.null(x) || !length(x) || !any(nzchar(x))) NA_character_ else paste(x[nzchar(x)], collapse = ",")
    }
    ld_opp_ids_csv <- {
      ids <- selected_opp_ids_ld()
      if (is.null(ids) || !length(ids)) NA_character_ else paste(ids, collapse = ",")
    }
    ld_home_away <- if (!nzchar(input$ld_home_away %||% "")) NA_character_ else input$ld_home_away
    ld_outcome <- if (!nzchar(input$ld_outcome %||% "")) NA_character_ else input$ld_outcome
    ld_rank_side <- if (!nzchar(input$ld_opp_rank_side %||% "")) NA_character_ else input$ld_opp_rank_side
    ld_rank_n <- suppressWarnings(as.integer(if (!nzchar(input$ld_opp_rank_n %||% "")) NA_character_ else input$ld_opp_rank_n))
    ld_metric <- if (!nzchar(input$ld_opp_rank_metric %||% "")) NA_character_ else input$ld_opp_rank_metric
    
    list(num = as.integer(input$ld_num), team_csv = if (!is.na(team_id)) as.character(team_id) else NA_character_, player_csv = if (length(player_on_ids)) paste(player_on_ids, collapse = ",") else NA_character_, player_off_csv = if (length(player_off_ids)) paste(player_off_ids, collapse = ",") else NA_character_, exact = TRUE, start_date = if (!is.null(input$ld_dates[1]) && !is.na(input$ld_dates[1])) as.Date(input$ld_dates[1]) else NA, end_date = if (!is.null(input$ld_dates[2]) && !is.na(input$ld_dates[2])) as.Date(input$ld_dates[2]) else NA, min_poss = as.integer(input$ld_minposs), game_type_csv = ld_game_type_csv, opp_ids_csv = ld_opp_ids_csv, home_away = ld_home_away, outcome = ld_outcome, opp_rank_side = ld_rank_side, opp_rank_n = ld_rank_n, opp_rank_metric = ld_metric)
  }) %>% bindEvent(input$ld_num, input$ld_team, input$ld_players_on, input$ld_players_off, input$ld_dates, input$ld_minposs, input$main_tabs, input$ld_game_type, input$ld_opponents, input$ld_home_away, input$ld_outcome, input$ld_opp_rank_side, input$ld_opp_rank_n, input$ld_opp_rank_metric, input$ld_view_mode)
  
  ld_data <- reactive({
    req(ld_params())
    p <- ld_params()
    gy <- as.integer(input$game_year_ld)
    mode <- input$ld_view_mode
    
    if (identical(mode, "Four Factors")) {
      # Get pre-ranked data (ranks computed on full unfiltered population)
      df <- ld_ff_ranked_df()
      
      if (is.null(df) || NROW(df) == 0L) {
        return(data.frame(
          team_id = integer(0), player_names_str = character(0),
          off_ts = numeric(0), off_oreb = numeric(0), off_tov = numeric(0), off_ftr = numeric(0),
          off_poss = integer(0), off_pts = integer(0), off_ppp = numeric(0),
          def_ts = numeric(0), def_oreb = numeric(0), def_tov = numeric(0), def_ftr = numeric(0),
          def_poss = integer(0), def_pts = integer(0), def_ppp = numeric(0),
          net_rtg = numeric(0), num_lineup = integer(0), sub_lineup_hash = character(0),
          total_poss = integer(0),
          stringsAsFactors = FALSE
        ))
      }
      
      # --- Filter LOCALLY (ranks already computed on full data) ---
      
      # Filter by team
      if (!is.na(p$team_csv) && nzchar(p$team_csv)) {
        team_ids <- as.integer(strsplit(p$team_csv, ",")[[1]])
        df <- df %>% filter(team_id %in% team_ids)
      }
      
      # Filter by players on (lineup must contain all selected players)
      if (!is.na(p$player_csv) && nzchar(p$player_csv)) {
        on_ids <- as.integer(strsplit(p$player_csv, ",")[[1]])
        pid_list <- if (is.list(df$player_ids)) df$player_ids else lapply(df$player_ids, function(s) as.integer(strsplit(gsub("[{}]", "", as.character(s)), ",")[[1]]))
        keep <- vapply(pid_list, function(x) all(on_ids %in% x), logical(1))
        df <- df[keep, , drop = FALSE]
      }
      
      # Filter by players off (lineup must NOT contain any excluded players)
      if (!is.na(p$player_off_csv) && nzchar(p$player_off_csv)) {
        off_ids <- as.integer(strsplit(p$player_off_csv, ",")[[1]])
        pid_list <- if (is.list(df$player_ids)) df$player_ids else lapply(df$player_ids, function(s) as.integer(strsplit(gsub("[{}]", "", as.character(s)), ",")[[1]]))
        keep <- vapply(pid_list, function(x) !any(off_ids %in% x), logical(1))
        df <- df[keep, , drop = FALSE]
      }
      
      # Filter by min poss
      df <- df %>% filter(total_poss >= !!p$min_poss)
      
      df
    } else {
      # Get pre-ranked data (ranks computed on full unfiltered population)
      df <- ld_summary_ranked_df()
      
      if (is.null(df) || NROW(df) == 0L) {
        return(data.frame(
          team_id = integer(0), player_names_str = character(0),
          total_poss = integer(0), plus_minus = numeric(0),
          off_poss = integer(0), def_poss = integer(0),
          off_pts = numeric(0), def_pts = numeric(0),
          off_ppp = numeric(0), def_ppp = numeric(0),
          net_rtg = numeric(0), num_lineup = integer(0),
          sub_lineup_hash = character(0),
          stringsAsFactors = FALSE
        ))
      }
      
      # --- Filter LOCALLY (ranks already computed on full data) ---
      
      # Filter by team
      if (!is.na(p$team_csv) && nzchar(p$team_csv)) {
        team_ids <- as.integer(strsplit(p$team_csv, ",")[[1]])
        df <- df %>% filter(team_id %in% team_ids)
      }
      
      # Filter by players on (lineup must contain all selected players)
      if (!is.na(p$player_csv) && nzchar(p$player_csv)) {
        on_ids <- as.integer(strsplit(p$player_csv, ",")[[1]])
        pid_list <- if (is.list(df$player_ids)) df$player_ids else lapply(df$player_ids, function(s) as.integer(strsplit(gsub("[{}]", "", as.character(s)), ",")[[1]]))
        keep <- vapply(pid_list, function(x) all(on_ids %in% x), logical(1))
        df <- df[keep, , drop = FALSE]
      }
      
      # Filter by players off (lineup must NOT contain any excluded players)
      if (!is.na(p$player_off_csv) && nzchar(p$player_off_csv)) {
        off_ids <- as.integer(strsplit(p$player_off_csv, ",")[[1]])
        pid_list <- if (is.list(df$player_ids)) df$player_ids else lapply(df$player_ids, function(s) as.integer(strsplit(gsub("[{}]", "", as.character(s)), ",")[[1]]))
        keep <- vapply(pid_list, function(x) !any(off_ids %in% x), logical(1))
        df <- df[keep, , drop = FALSE]
      }
      
      # Filter by min poss
      df <- df %>% filter(total_poss >= !!p$min_poss)
      
      df
    }
  })
  
  team_name_vec <- reactive({
    tdf <- isolate(ld_ref$teams)
    if (is.null(tdf)) return(character(0))
    setNames(tdf$team_name, as.character(tdf$team_id))
  })
  
  output$ld_table <- DT::renderDataTable({
    req(ld_params())
    df <- ld_data()
    mode <- input$ld_view_mode
    tmap <- team_name_vec()
    
    # Common: Map team names and create Players column
    if ("team_id" %in% names(df)) {
      df$Team <- unname(tmap[as.character(df$team_id)])
      df$Team[is.na(df$Team)] <- as.character(df$team_id[is.na(df$Team)])
    }
    if ("player_names_str" %in% names(df)) df$Players <- df$player_names_str
    
    cuts <- seq(0.05, 0.95, by = 0.05)
    cols_grad <- colorRampPalette(c("#d73027", "#fee08b", "#1a9850"))(20)
    cols_rev  <- rev(cols_grad)
    
    if (identical(mode, "Four Factors")) {
      # ============================================================
      # FOUR FACTORS LINEUP TABLE
      # Ranks are pre-computed on the full unfiltered population
      # in ld_ff_ranked_df(), so colors stay stable across local filters.
      # ============================================================
      
      pr_cols <- c("pr_off_ppp", "pr_off_ts", "pr_off_oreb", "pr_off_tov", "pr_off_ftr",
                   "pr_def_ppp", "pr_def_ts", "pr_def_oreb", "pr_def_tov", "pr_def_ftr", "pr_net")
      
      keep_cols <- c("Team", "Players",
                     "off_ppp", "off_ts", "off_oreb", "off_tov", "off_ftr", "off_poss",
                     "def_ppp", "def_ts", "def_oreb", "def_tov", "def_ftr", "def_poss",
                     "total_poss", "net_rtg")
      df <- df %>% select(any_of(c(keep_cols, pr_cols)))
      df$is_total <- rep(1, nrow(df))
      df <- df %>% arrange(desc(total_poss))
      
      # --- TOTAL row (rates from summed raw counts) ---
      if (nrow(df) > 0) {
        raw <- ld_data()
        sum_off_poss <- sum(df$off_poss, na.rm = TRUE)
        sum_def_poss <- sum(df$def_poss, na.rm = TRUE)
        sum_off_pts  <- sum(raw$off_pts, na.rm = TRUE)
        sum_def_pts  <- sum(raw$def_pts, na.rm = TRUE)
        tot_off_ppp <- if (sum_off_poss > 0) round((sum_off_pts / sum_off_poss) * 100, 1) else NA_real_
        tot_def_ppp <- if (sum_def_poss > 0) round((sum_def_pts / sum_def_poss) * 100, 1) else NA_real_
        tot_net_rtg <- if (!is.na(tot_off_ppp) && !is.na(tot_def_ppp)) round(tot_off_ppp - tot_def_ppp, 1) else NA_real_
        
        # Sum raw counts for four-factor rates
        s_off_ts_poss   <- sum(raw$off_ts_poss, na.rm = TRUE)
        s_off_oreb_cnt  <- sum(raw$off_oreb_cnt, na.rm = TRUE)
        s_off_oreb_opps <- sum(raw$off_oreb_opps, na.rm = TRUE)
        s_off_tov_cnt   <- sum(raw$off_tov_cnt, na.rm = TRUE)
        s_off_fta       <- sum(raw$off_fta, na.rm = TRUE)
        s_off_fga       <- sum(raw$off_fga_cnt, na.rm = TRUE)
        s_def_ts_poss   <- sum(raw$def_ts_poss, na.rm = TRUE)
        s_def_oreb_cnt  <- sum(raw$def_oreb_cnt, na.rm = TRUE)
        s_def_oreb_opps <- sum(raw$def_oreb_opps, na.rm = TRUE)
        s_def_tov_cnt   <- sum(raw$def_tov_cnt, na.rm = TRUE)
        s_def_fta       <- sum(raw$def_fta, na.rm = TRUE)
        s_def_fga       <- sum(raw$def_fga_cnt, na.rm = TRUE)
        
        tot_off_ts   <- if (s_off_ts_poss > 0) round(sum_off_pts / (2 * s_off_ts_poss) * 100, 1) else NA_real_
        tot_off_oreb <- if (s_off_oreb_opps > 0) round(s_off_oreb_cnt / s_off_oreb_opps * 100, 1) else NA_real_
        tot_off_tov  <- if (sum_off_poss > 0) round(s_off_tov_cnt / sum_off_poss * 100, 1) else NA_real_
        tot_off_ftr  <- if (s_off_fga > 0) round(s_off_fta / s_off_fga * 100, 1) else NA_real_
        tot_def_ts   <- if (s_def_ts_poss > 0) round(sum_def_pts / (2 * s_def_ts_poss) * 100, 1) else NA_real_
        tot_def_oreb <- if (s_def_oreb_opps > 0) round(s_def_oreb_cnt / s_def_oreb_opps * 100, 1) else NA_real_
        tot_def_tov  <- if (sum_def_poss > 0) round(s_def_tov_cnt / sum_def_poss * 100, 1) else NA_real_
        tot_def_ftr  <- if (s_def_fga > 0) round(s_def_fta / s_def_fga * 100, 1) else NA_real_
        
        total_row <- data.frame(
          Team = "TOTAL", Players = "— All Lineups —",
          off_ppp = tot_off_ppp, off_ts = tot_off_ts, off_oreb = tot_off_oreb, off_tov = tot_off_tov, off_ftr = tot_off_ftr,
          off_poss = sum_off_poss,
          def_ppp = tot_def_ppp, def_ts = tot_def_ts, def_oreb = tot_def_oreb, def_tov = tot_def_tov, def_ftr = tot_def_ftr,
          def_poss = sum_def_poss,
          total_poss = sum_off_poss + sum_def_poss,
          net_rtg = tot_net_rtg,
          is_total = 0, stringsAsFactors = FALSE
        )
        df <- dplyr::bind_rows(total_row, df)
      }
      
      df <- df %>% select(is_total, everything())
      
      # Build custom sketch header
      # Note: first th("") in each row accounts for hidden is_total column at position 0
      sketch_ff <- htmltools::withTags(table(class = 'display', thead(
        tr(
          th(""),
          th(class = "group-head", colspan = 2, ""),
          th(class = "group-head section-left-border", colspan = 6, "Offense"),
          th(class = "group-head section-left-border", colspan = 6, "Defense"),
          th(class = "group-head section-left-border", colspan = 2, "Usage")
        ),
        tr(
          th(""),
          th(class = "sub-head", "Team"), th(class = "sub-head", "Players"),
          th(class = "sub-head section-left-border", "PPP"), th(class = "sub-head", "TS%"),
          th(class = "sub-head", "OREB%"), th(class = "sub-head", "TOV%"),
          th(class = "sub-head", "FTR"), th(class = "sub-head", "Poss"),
          th(class = "sub-head section-left-border", "PPP"), th(class = "sub-head", "TS%"),
          th(class = "sub-head", "OREB%"), th(class = "sub-head", "TOV%"),
          th(class = "sub-head", "FTR"), th(class = "sub-head", "Poss"),
          th(class = "sub-head section-left-border", "Total"), th(class = "sub-head", "Net")
        )
      )))
      
      # Column indices for section borders
      hide_idx <- c(0, which(colnames(df) %in% pr_cols) - 1L)
      off_ppp_idx  <- which(names(df) == "off_ppp") - 1L
      def_ppp_idx  <- which(names(df) == "def_ppp") - 1L
      total_idx    <- which(names(df) == "total_poss") - 1L
      
      col_defs <- list(
        list(targets = hide_idx, visible = FALSE),
        list(targets = "_all", className = "dt-center")
      )
      if (length(off_ppp_idx)) col_defs[[length(col_defs) + 1]] <- list(targets = off_ppp_idx, className = "section-left-border dt-center")
      if (length(def_ppp_idx)) col_defs[[length(col_defs) + 1]] <- list(targets = def_ppp_idx, className = "section-left-border dt-center")
      if (length(total_idx))   col_defs[[length(col_defs) + 1]] <- list(targets = total_idx, className = "section-left-border dt-center")
      
      dt <- DT::datatable(df, container = sketch_ff, rownames = FALSE,
                          options = list(
                            dom = "tip", pageLength = 50,
                            lengthMenu = c(25, 50, 100, 200),
                            orderFixed = list(list(0, 'asc')),
                            deferRender = TRUE, scrollX = TRUE,
                            columnDefs = col_defs
                          ))
      
      # Format numbers
      rate_cols <- intersect(c("off_ts", "off_oreb", "off_tov", "off_ftr", "def_ts", "def_oreb", "def_tov", "def_ftr"), names(df))
      ppp_cols  <- intersect(c("off_ppp", "def_ppp", "net_rtg"), names(df))
      poss_cols <- intersect(c("off_poss", "def_poss", "total_poss"), names(df))
      
      if (length(rate_cols)) dt <- DT::formatRound(dt, rate_cols, 1)
      if (length(ppp_cols))  dt <- DT::formatRound(dt, ppp_cols, 1)
      if (length(poss_cols)) dt <- DT::formatCurrency(dt, poss_cols, currency = "", interval = 3, mark = ",", digits = 0)
      
      # TOTAL row styling
      dt <- DT::formatStyle(dt, "Team", target = "row",
                            backgroundColor = styleEqual("TOTAL", "#f0f0f0"),
                            fontWeight = styleEqual("TOTAL", "bold"))
      
      # Color logic
      if ("pr_off_ppp"  %in% names(df)) dt <- DT::formatStyle(dt, "off_ppp",  backgroundColor = styleInterval(cuts, cols_grad), valueColumns = "pr_off_ppp")
      if ("pr_off_ts"   %in% names(df)) dt <- DT::formatStyle(dt, "off_ts",   backgroundColor = styleInterval(cuts, cols_grad), valueColumns = "pr_off_ts")
      if ("pr_off_oreb" %in% names(df)) dt <- DT::formatStyle(dt, "off_oreb", backgroundColor = styleInterval(cuts, cols_grad), valueColumns = "pr_off_oreb")
      if ("pr_off_tov"  %in% names(df)) dt <- DT::formatStyle(dt, "off_tov",  backgroundColor = styleInterval(cuts, cols_grad), valueColumns = "pr_off_tov")
      if ("pr_off_ftr"  %in% names(df)) dt <- DT::formatStyle(dt, "off_ftr",  backgroundColor = styleInterval(cuts, cols_grad), valueColumns = "pr_off_ftr")
      if ("pr_def_ppp"  %in% names(df)) dt <- DT::formatStyle(dt, "def_ppp",  backgroundColor = styleInterval(cuts, cols_grad), valueColumns = "pr_def_ppp")
      if ("pr_def_ts"   %in% names(df)) dt <- DT::formatStyle(dt, "def_ts",   backgroundColor = styleInterval(cuts, cols_grad), valueColumns = "pr_def_ts")
      if ("pr_def_oreb" %in% names(df)) dt <- DT::formatStyle(dt, "def_oreb", backgroundColor = styleInterval(cuts, cols_grad), valueColumns = "pr_def_oreb")
      if ("pr_def_tov"  %in% names(df)) dt <- DT::formatStyle(dt, "def_tov",  backgroundColor = styleInterval(cuts, cols_grad), valueColumns = "pr_def_tov")
      if ("pr_def_ftr"  %in% names(df)) dt <- DT::formatStyle(dt, "def_ftr",  backgroundColor = styleInterval(cuts, cols_grad), valueColumns = "pr_def_ftr")
      if ("pr_net"      %in% names(df)) dt <- DT::formatStyle(dt, "net_rtg",  backgroundColor = styleInterval(cuts, cols_grad), valueColumns = "pr_net")
      
      return(dt)
      
    } else {
      # ============================================================
      # SUMMARY LINEUP TABLE (existing behavior)
      # ============================================================
      
      pr_cols <- c("pr_ld_net", "pr_ld_off_ppp", "pr_ld_def_ppp_i")
      keep_cols <- c("Team", "Players", "total_poss", "plus_minus", "off_poss", "def_poss", "off_pts", "def_pts", "off_ppp", "def_ppp", "net_rtg", "num_lineup", "sub_lineup_hash")
      df <- df %>% select(any_of(c(keep_cols, pr_cols)))
      df$is_total <- rep(1, nrow(df))
      if ("net_rtg" %in% names(df)) df <- df %>% arrange(desc(total_poss))
      if (nrow(df) > 0) {
        sum_off_poss <- sum(df$off_poss, na.rm = TRUE)
        sum_def_poss <- sum(df$def_poss, na.rm = TRUE)
        sum_off_pts <- sum(df$off_pts, na.rm = TRUE)
        sum_def_pts <- sum(df$def_pts, na.rm = TRUE)
        tot_off_ppp <- if (sum_off_poss > 0) (sum_off_pts / sum_off_poss) * 100 else 0
        tot_def_ppp <- if (sum_def_poss > 0) (sum_def_pts / sum_def_poss) * 100 else 0
        tot_net_rtg <- tot_off_ppp - tot_def_ppp
        total_row <- data.frame(Team = "TOTAL", Players = "— All Lineups —", total_poss = sum_off_poss + sum_def_poss, off_ppp = tot_off_ppp, def_ppp = tot_def_ppp, net_rtg = tot_net_rtg, plus_minus = sum_off_pts - sum_def_pts, off_poss = sum_off_poss, off_pts = sum_off_pts, def_poss = sum_def_poss, def_pts = sum_def_pts, num_lineup = NA_integer_, sub_lineup_hash = "TOTAL", is_total = 0, stringsAsFactors = FALSE)
        df <- dplyr::bind_rows(total_row, df)
      }
      df <- df %>% select(is_total, everything())
      show_cols <- c("Team", "Players", "total_poss", "off_ppp", "def_ppp", "net_rtg", "plus_minus", "off_poss", "off_pts", "def_poss", "def_pts", "num_lineup", "sub_lineup_hash")
      
      keep <- intersect(show_cols, names(df))
      df <- df[, unique(c("is_total", keep, pr_cols[pr_cols %in% names(df)])), drop = FALSE]
      pretty_labels <- c(Team = "Team", Players = "Players", num_lineup = "Size", total_poss = "Total Poss", net_rtg = "Net RTG", plus_minus = "+/-", off_ppp = "Off PPP", def_ppp = "Def PPP", off_poss = "Off Poss", off_pts = "Off Pts", def_poss = "Def Poss", def_pts = "Def Pts", sub_lineup_hash = "Lineup ID")
      data_col_names <- colnames(df)[-1]
      data_col_names <- setdiff(data_col_names, pr_cols)
      col_labels <- unname(pretty_labels[data_col_names])
      final_labels <- c("", col_labels)
      pr_indices <- which(colnames(df) %in% pr_cols) - 1L
      hidden_indices <- c(0, pr_indices)
      
      dt <- DT::datatable(df, colnames = final_labels, rownames = FALSE, filter = "top", options = list(pageLength = 50, lengthMenu = c(25, 50, 100, 200, 1000), orderFixed = list(list(0, 'asc')), deferRender = TRUE, scrollX = TRUE, processing = TRUE, columnDefs = list(list(targets = hidden_indices, visible = FALSE)))) |>
        DT::formatRound(c("off_ppp", "def_ppp", "net_rtg")[c("off_ppp", "def_ppp", "net_rtg") %in% names(df)], 1) |>
        DT::formatCurrency(c("total_poss", "off_poss", "def_poss")[c("total_poss", "off_poss", "def_poss") %in% names(df)], currency = "", interval = 3, mark = ",", digits = 0) |>
        DT::formatCurrency(c("off_pts", "def_pts", "plus_minus")[c("off_pts", "def_pts", "plus_minus") %in% names(df)], currency = "", interval = 3, mark = ",", digits = 0)
      dt <- DT::formatStyle(dt, "Team", target = "row", backgroundColor = styleEqual("TOTAL", "#f0f0f0"), fontWeight = styleEqual("TOTAL", "bold"))
      if (all(c("net_rtg", "pr_ld_net") %in% colnames(df))) dt <- DT::formatStyle(dt, "net_rtg", backgroundColor = styleInterval(cuts, cols_grad), valueColumns = "pr_ld_net")
      if (all(c("off_ppp", "pr_ld_off_ppp") %in% colnames(df))) dt <- DT::formatStyle(dt, "off_ppp", backgroundColor = styleInterval(cuts, cols_grad), valueColumns = "pr_ld_off_ppp")
      if (all(c("def_ppp", "pr_ld_def_ppp_i") %in% colnames(df))) dt <- DT::formatStyle(dt, "def_ppp", backgroundColor = styleInterval(cuts, cols_grad), valueColumns = "pr_ld_def_ppp_i")
      return(dt)
    }
  })
  
  # -------------------------------------------------------------
  # Tab 3: Team Ratings (Fully Expanded Logic)
  # -------------------------------------------------------------
  observeEvent(input$tr_reset, {
    updateRadioButtons(session, "tr_view_mode", selected = "Summary")
    updateDateRangeInput(session, "tr_dates", start = NA, end = NA)
    updateSelectizeInput(session, "tr_game_type", selected = "")
    updateSelectizeInput(session, "tr_opponents", selected = character(0))
    updateSelectInput(session, "tr_home_away", selected = "")
    updateSelectInput(session, "tr_outcome", selected = "")
    updateSelectInput(session, "tr_opp_rank_side", selected = "")
    updateSelectInput(session, "tr_opp_rank_n", selected = "")
    updateSelectInput(session, "tr_opp_rank_metric", selected = "")
  })
  
  observeEvent(list(input$tr_game_year, input$main_tabs), {
    req(input$tr_game_year)
    gy_int <- as.integer(input$tr_game_year)
    td <- full_rosters %>%
      filter(game_year == !!gy_int) %>%
      distinct(team_id, team_name) %>%
      arrange(team_name) %>%
      collect()
    updateSelectizeInput(session, "tr_opponents", choices = td$team_name, selected = character(0), server = TRUE)
  })
  
  run_team_ratings_dynamic <- function(pool, game_year, start_d, end_d, game_type_csv, opp_ids_csv, home_away, outcome, opp_rank_side, opp_rank_n, opp_rank_metric) {
    DBI::dbGetQuery(pool, paste0("SELECT * FROM basketball_test.get_team_ratings_dynamic(", "$1::int4,$2::date,$3::date,$4::text,$5::text,$6::text,$7::text,$8::text,$9::int4,$10::text", ")"), params = list(as.integer(game_year), if (!is.na(start_d)) as.Date(start_d) else NA, if (!is.na(end_d)) as.Date(end_d) else NA, game_type_csv, opp_ids_csv, home_away, outcome, opp_rank_side, opp_rank_n, opp_rank_metric))
  }
  
  run_team_ff_dynamic <- function(pool, game_year, start_d, end_d, game_type_csv, opp_ids_csv, home_away, outcome, opp_rank_side, opp_rank_n, opp_rank_metric) {
    DBI::dbGetQuery(pool, paste0("SELECT * FROM basketball_test.get_team_four_factors_dynamic(", "$1::int4,$2::date,$3::date,$4::text,$5::text,$6::text,$7::text,$8::text,$9::int4,$10::text", ")"), params = list(as.integer(game_year), if (!is.na(start_d)) as.Date(start_d) else NA, if (!is.na(end_d)) as.Date(end_d) else NA, game_type_csv, opp_ids_csv, home_away, outcome, opp_rank_side, opp_rank_n, opp_rank_metric))
  }
  
  tr_params <- reactive({
    gy <- as.integer(input$tr_game_year)
    req(gy)
    start_d <- if (!is.null(input$tr_dates[1]) && !is.na(input$tr_dates[1])) as.Date(input$tr_dates[1]) else NA
    end_d <- if (!is.null(input$tr_dates[2]) && !is.na(input$tr_dates[2])) as.Date(input$tr_dates[2]) else NA
    tr_game_type_csv <- {
      x <- input$tr_game_type
      if (is.null(x) || !length(x) || !any(nzchar(x))) NA_character_ else paste(x[nzchar(x)], collapse = ",")
    }
    td_map <- full_rosters %>%
      filter(game_year == !!gy) %>%
      distinct(team_id, team_name) %>%
      collect()
    tr_opp_ids_csv <- {
      sel <- input$tr_opponents
      if (is.null(sel) || !length(sel)) NA_character_ else {
        ids <- td_map %>% filter(team_name %in% sel) %>% pull(team_id)
        paste(ids, collapse = ",")
      }
    }
    tr_home_away <- if (!nzchar(input$tr_home_away %||% "")) NA_character_ else input$tr_home_away
    tr_outcome <- if (!nzchar(input$tr_outcome %||% "")) NA_character_ else input$tr_outcome
    tr_rank_side <- if (!nzchar(input$tr_opp_rank_side %||% "")) NA_character_ else input$tr_opp_rank_side
    tr_rank_n <- suppressWarnings(as.integer(if (!nzchar(input$tr_opp_rank_n %||% "")) NA_character_ else input$tr_opp_rank_n))
    tr_metric <- if (!nzchar(input$tr_opp_rank_metric %||% "")) NA_character_ else input$tr_opp_rank_metric
    
    list(game_year = gy, start_d = start_d, end_d = end_d, game_type_csv = tr_game_type_csv, opp_ids_csv = tr_opp_ids_csv, home_away = tr_home_away, outcome = tr_outcome, rank_side = tr_rank_side, rank_n = tr_rank_n, metric = tr_metric)
  }) %>% debounce(300)
  
  tr_fallback_needed <- reactive({
    p <- tr_params()
    has_dates <- !is.na(p$start_d) || !is.na(p$end_d)
    has_gt <- !is.na(p$game_type_csv)
    has_opp <- !is.na(p$opp_ids_csv)
    has_ha <- !is.na(p$home_away)
    has_out <- !is.na(p$outcome)
    has_rank <- !is.na(p$rank_side) || !is.na(p$rank_n)
    has_dates || has_gt || has_opp || has_ha || has_out || has_rank
  })
  
  tr_data <- reactive({
    p <- tr_params()
    if (tr_fallback_needed()) {
      run_team_ratings_dynamic(pg_pool, game_year = p$game_year, start_d = p$start_d, end_d = p$end_d, game_type_csv = p$game_type_csv, opp_ids_csv = p$opp_ids_csv, home_away = p$home_away, outcome = p$outcome, opp_rank_side = p$rank_side, opp_rank_n = p$rank_n, opp_rank_metric = p$metric)
    } else {
      team_ratings_mv %>%
        filter(game_year == !!p$game_year) %>%
        select(game_year, team_name, off_ppp, def_ppp, net_rtg, rank_net_rtg, rank_off_ppp, rank_def_ppp) %>%
        arrange(rank_net_rtg) %>%
        collect()
    }
  })
  
  tr_ff_data <- reactive({
    p <- tr_params()
    if (tr_fallback_needed()) {
      df <- run_team_ff_dynamic(pg_pool, game_year = p$game_year, start_d = p$start_d, end_d = p$end_d, game_type_csv = p$game_type_csv, opp_ids_csv = p$opp_ids_csv, home_away = p$home_away, outcome = p$outcome, opp_rank_side = p$rank_side, opp_rank_n = p$rank_n, opp_rank_metric = p$metric)
    } else {
      df <- team_ff_mv %>%
        filter(game_year == !!p$game_year) %>%
        collect()
    }
    
    if (is.null(df) || nrow(df) == 0) return(df)
    
    # Compute percentile ranks — all teams qualify (>>100 poss)
    pr_vec <- function(x, invert = FALSE) {
      n <- sum(!is.na(x))
      if (n <= 1) return(rep(NA_real_, length(x)))
      r <- rank(x, na.last = "keep", ties.method = "average")
      p <- (r - 1) / (n - 1)
      if (invert) p <- 1 - p
      as.numeric(p)
    }
    
    df$pr_off_ppp  <- pr_vec(df$off_ppp)
    df$pr_off_ts   <- pr_vec(df$off_ts)
    df$pr_off_oreb <- pr_vec(df$off_oreb)
    df$pr_off_tov  <- pr_vec(df$off_tov, invert = TRUE)
    df$pr_off_ftr  <- pr_vec(df$off_ftr)
    df$pr_def_ppp  <- pr_vec(df$def_ppp, invert = TRUE)
    df$pr_def_ts   <- pr_vec(df$def_ts, invert = TRUE)
    df$pr_def_oreb <- pr_vec(df$def_oreb, invert = TRUE)
    df$pr_def_tov  <- pr_vec(df$def_tov)
    df$pr_def_ftr  <- pr_vec(df$def_ftr, invert = TRUE)
    df$pr_net      <- pr_vec(df$net_rtg)
    
    df
  })
  
  output$tr_table <- renderDT({
    mode <- input$tr_view_mode
    
    if (identical(mode, "Four Factors")) {
      # ============================================================
      # FOUR FACTORS TEAM TABLE
      # ============================================================
      df <- tr_ff_data()
      if (is.null(df) || nrow(df) == 0) return(NULL)
      
      pr_cols <- c("pr_off_ppp", "pr_off_ts", "pr_off_oreb", "pr_off_tov", "pr_off_ftr",
                   "pr_def_ppp", "pr_def_ts", "pr_def_oreb", "pr_def_tov", "pr_def_ftr", "pr_net")
      
      keep_cols <- c("team_name",
                     "off_ppp", "off_ts", "off_oreb", "off_tov", "off_ftr", "off_poss",
                     "def_ppp", "def_ts", "def_oreb", "def_tov", "def_ftr", "def_poss",
                     "net_rtg")
      df <- df %>% select(any_of(c(keep_cols, pr_cols)))
      df <- df %>% arrange(desc(net_rtg))
      
      cuts <- seq(0.05, 0.95, by = 0.05)
      cols_grad <- colorRampPalette(c("#d73027", "#fee08b", "#1a9850"))(20)
      
      sketch_ff <- htmltools::withTags(table(class = 'display', thead(
        tr(
          th(class = "group-head", ""),
          th(class = "group-head section-left-border", colspan = 6, "Offense"),
          th(class = "group-head section-left-border", colspan = 6, "Defense"),
          th(class = "group-head section-left-border", "")
        ),
        tr(
          th(class = "sub-head", "Team"),
          th(class = "sub-head section-left-border", "PPP"), th(class = "sub-head", "TS%"),
          th(class = "sub-head", "OREB%"), th(class = "sub-head", "TOV%"),
          th(class = "sub-head", "FTR"), th(class = "sub-head", "Poss"),
          th(class = "sub-head section-left-border", "PPP"), th(class = "sub-head", "TS%"),
          th(class = "sub-head", "OREB%"), th(class = "sub-head", "TOV%"),
          th(class = "sub-head", "FTR"), th(class = "sub-head", "Poss"),
          th(class = "sub-head section-left-border", "Net")
        )
      )))
      
      hide_idx <- which(colnames(df) %in% pr_cols) - 1L
      off_ppp_idx <- which(names(df) == "off_ppp") - 1L
      def_ppp_idx <- which(names(df) == "def_ppp") - 1L
      net_idx     <- which(names(df) == "net_rtg") - 1L
      
      col_defs <- list(
        list(targets = hide_idx, visible = FALSE),
        list(targets = "_all", className = "dt-center")
      )
      if (length(off_ppp_idx)) col_defs[[length(col_defs) + 1]] <- list(targets = off_ppp_idx, className = "section-left-border dt-center")
      if (length(def_ppp_idx)) col_defs[[length(col_defs) + 1]] <- list(targets = def_ppp_idx, className = "section-left-border dt-center")
      if (length(net_idx))     col_defs[[length(col_defs) + 1]] <- list(targets = net_idx, className = "section-left-border dt-center")
      
      dt <- DT::datatable(df, container = sketch_ff, rownames = FALSE,
                          options = list(
                            dom = "t", pageLength = 50,
                            deferRender = TRUE, scrollX = TRUE,
                            order = list(list(net_idx, "desc")),
                            columnDefs = col_defs
                          ))
      
      rate_cols <- intersect(c("off_ts", "off_oreb", "off_tov", "off_ftr", "def_ts", "def_oreb", "def_tov", "def_ftr"), names(df))
      ppp_cols  <- intersect(c("off_ppp", "def_ppp", "net_rtg"), names(df))
      poss_cols <- intersect(c("off_poss", "def_poss"), names(df))
      
      if (length(rate_cols)) dt <- DT::formatRound(dt, rate_cols, 1)
      if (length(ppp_cols))  dt <- DT::formatRound(dt, ppp_cols, 1)
      if (length(poss_cols)) dt <- DT::formatCurrency(dt, poss_cols, currency = "", interval = 3, mark = ",", digits = 0)
      
      # Color logic — same polarity as Tab 2 FF
      if ("pr_off_ppp"  %in% names(df)) dt <- DT::formatStyle(dt, "off_ppp",  backgroundColor = styleInterval(cuts, cols_grad), valueColumns = "pr_off_ppp")
      if ("pr_off_ts"   %in% names(df)) dt <- DT::formatStyle(dt, "off_ts",   backgroundColor = styleInterval(cuts, cols_grad), valueColumns = "pr_off_ts")
      if ("pr_off_oreb" %in% names(df)) dt <- DT::formatStyle(dt, "off_oreb", backgroundColor = styleInterval(cuts, cols_grad), valueColumns = "pr_off_oreb")
      if ("pr_off_tov"  %in% names(df)) dt <- DT::formatStyle(dt, "off_tov",  backgroundColor = styleInterval(cuts, cols_grad), valueColumns = "pr_off_tov")
      if ("pr_off_ftr"  %in% names(df)) dt <- DT::formatStyle(dt, "off_ftr",  backgroundColor = styleInterval(cuts, cols_grad), valueColumns = "pr_off_ftr")
      if ("pr_def_ppp"  %in% names(df)) dt <- DT::formatStyle(dt, "def_ppp",  backgroundColor = styleInterval(cuts, cols_grad), valueColumns = "pr_def_ppp")
      if ("pr_def_ts"   %in% names(df)) dt <- DT::formatStyle(dt, "def_ts",   backgroundColor = styleInterval(cuts, cols_grad), valueColumns = "pr_def_ts")
      if ("pr_def_oreb" %in% names(df)) dt <- DT::formatStyle(dt, "def_oreb", backgroundColor = styleInterval(cuts, cols_grad), valueColumns = "pr_def_oreb")
      if ("pr_def_tov"  %in% names(df)) dt <- DT::formatStyle(dt, "def_tov",  backgroundColor = styleInterval(cuts, cols_grad), valueColumns = "pr_def_tov")
      if ("pr_def_ftr"  %in% names(df)) dt <- DT::formatStyle(dt, "def_ftr",  backgroundColor = styleInterval(cuts, cols_grad), valueColumns = "pr_def_ftr")
      if ("pr_net"      %in% names(df)) dt <- DT::formatStyle(dt, "net_rtg",  backgroundColor = styleInterval(cuts, cols_grad), valueColumns = "pr_net")
      
      return(dt)
      
    } else {
      # ============================================================
      # SUMMARY TEAM TABLE (existing behavior)
      # ============================================================
      df <- tr_data()
      if (is.null(df) || nrow(df) == 0) return(NULL)
      pretty_names <- c("Season", "Team", "Off PPP", "Def PPP", "Net Rtg", "Net Rank", "Off Rank", "Def Rank")
      disp_df <- df %>% select(game_year, team_name, off_ppp, def_ppp, net_rtg, rank_net_rtg, rank_off_ppp, rank_def_ppp)
      max_rank <- max(c(disp_df$rank_net_rtg, disp_df$rank_off_ppp, disp_df$rank_def_ppp), na.rm = TRUE)
      if (max_rank < 2) max_rank <- 2
      cuts <- seq(1.5, max_rank - 0.5, 1)
      cols_rank <- colorRampPalette(c("#1a9850", "#fee08b", "#d73027"))(length(cuts) + 1)
      
      dt <- datatable(disp_df, colnames = pretty_names, rownames = FALSE, options = list(dom = "t", pageLength = 50, scrollX = TRUE, columnDefs = list(list(className = 'dt-center', targets = "_all")))) %>%
        formatRound(c("off_ppp", "def_ppp", "net_rtg"), 1) %>%
        formatStyle(columns = c("rank_net_rtg", "rank_off_ppp", "rank_def_ppp"), backgroundColor = styleInterval(cuts, cols_rank))
      return(dt)
    }
  })
}

shinyApp(ui, server)