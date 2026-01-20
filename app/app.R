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

# ---------------- Defaults ----------------
DEFAULT_START <- as.Date("2024-10-01")
DEFAULT_END   <- as.Date("2025-07-01")

# default selected season (matches schedule.game_year)
DEFAULT_GAME_YEAR <- "2026"   # 2025-26

DEFAULT_MIN_ALL <- 100L   # eligibility: On/Off tab
DEFAULT_MIN_ON  <- 300L

# keep internal "no filter" value for DB function signature
DEFAULT_MIN_NET <- -1e9

# Lineup Data defaults
LD_DEFAULT_MIN_POSS <- 20L          # minimal gate for showing "all"
LD_DEFAULT_NUM       <- "5"         # default group size

# ---------------- PostgreSQL pool ----------------
pg_pool <- dbPool(
  drv      = Postgres(),
  host     = Sys.getenv("PG_HOST"),
  port     = as.integer(Sys.getenv("PG_PORT", "6543")),  # Supabase default
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
full_rosters <- tbl(pg_pool, in_schema("basketball_test","full_rosters"))
onoff_mv     <- tbl(pg_pool, in_schema("basketball_test","onoff_default_mv"))
schedule_tbl <- tbl(pg_pool, in_schema("basketball_test","schedule"))
team_ratings_mv <- tbl(pg_pool, in_schema("basketball_test", "team_ppp_ratings_mv"))

# ---------------- UI ----------------
ui <- navbarPage(
  id = "main_tabs",
  title = "Player Analytics",
  theme = bslib::bs_theme(version = 5), # Required for accordion support
  
  # -------- Tab 1: On/Off Impact --------
  tabPanel(
    title = "On/Off Impact", value = "onoff",
    fluidPage(
      tags$head(
        tags$style(HTML("
          th.group-head { background:#f7efe5 !important; font-weight:700; text-align:center; }
          th.sub-head   { background:#fafafa !important; font-weight:700; }
          table.dataTable thead th.sep-left { box-shadow: inset 3px 0 0 #222 !important; }
          table.dataTable tbody td.block-start { box-shadow: inset 3px 0 0 #222 !important; }
          .accordion-button { padding: 0.5rem 1rem; font-weight: 600; background-color: #f8f9fa; }
        "))
      ),
      titlePanel("Player ON/OFF Impact (PostgreSQL + dbplyr + DT)"),
      sidebarLayout(
        sidebarPanel(
          actionButton("reset_defaults", "Reset to defaults"),
          tags$hr(),
          
          selectInput(
            "game_year", "Season",
            choices  = c("2025-26" = "2026",
                         "2024-25" = "2025"),
            selected = DEFAULT_GAME_YEAR
          ),
          
          uiOutput("date_filter_ui"),
          uiOutput("team_filter_ui"),
          
          tags$hr(),
          
          # Clickable Menus for Tab 1
          bslib::accordion(
            bslib::accordion_panel(
              "Game Filters",
              selectizeInput(
                "on_game_type", "Game type",
                choices = c(
                  "All" = "",
                  "Regular season" = "5",
                  "Playoffs – Quarterfinals" = "16",
                  "Playoffs – Finals" = "17",
                  "Playoffs – Semifinals" = "26",
                  "Play-in" = "33",
                  "Winner Cup" = "34"
                ),
                selected = "",
                multiple = TRUE,
                options = list(placeholder = "All game types")
              ),
              selectizeInput(
                "on_opponents", "Opponents",
                choices = NULL,
                selected = character(0),
                multiple = TRUE,
                options = list(placeholder = "All opponents")
              ),
              selectInput(
                "on_home_away", "Home/Away",
                choices = c("All" = "", "Home" = "home", "Away" = "away"),
                selected = ""
              ),
              selectInput(
                "on_outcome", "Outcome",
                choices = c("All" = "", "Win" = "win", "Loss" = "loss"),
                selected = ""
              )
            ),
            bslib::accordion_panel(
              "Opponent Strength",
              selectInput(
                "on_opp_rank_side", "Top / Bottom",
                choices = c("Off" = "", "Top" = "top", "Bottom" = "bottom"),
                selected = ""
              ),
              selectInput(
                "on_opp_rank_n", "Rank N",
                choices = c("—" = "", as.character(1:12)),
                selected = ""
              ),
              selectInput(
                "on_opp_rank_metric", "Metric",
                choices = c("—" = "", "Offense" = "off", "Defense" = "def", "Net rating" = "net"),
                selected = ""
              )
            ),
            open = FALSE # Closed by default
          ),
          
          tags$hr(),
          sliderInput("min_all_poss", "Min possessions per side (eligibility):",
                      min = 0, max = 2000, value = DEFAULT_MIN_ALL, step = 10),
          sliderInput("min_on_poss", "Min ON possessions (eligibility):",
                      min = 0, max = 3000, value = DEFAULT_MIN_ON, step = 10),
          
          tags$hr(),
          downloadButton("download_csv", "Download CSV")
        ),
        mainPanel(
          DTOutput("onoff_dt")
        )
      )
    )
  ),
  
  # -------- Tab 2: Lineup Data --------
  tabPanel(
    title = "Lineup Data", value = "lineup_data",
    fluidPage(
      sidebarLayout(
        sidebarPanel(
          actionButton("ld_reset", "Reset Lineup Filters"),
          tags$hr(),
          
          # --- Slider at top ---
          sliderInput("ld_minposs", "Min possessions (sum of Off/Def)",
                      min = 0, max = 2000, value = LD_DEFAULT_MIN_POSS, step = 10),
          tags$hr(),
          
          selectizeInput("ld_team", "Team", choices = NULL, multiple = FALSE),
          helpText("Pick a team to enable player filtering."),
          selectizeInput("ld_players_on",  "Players On (exact/contains)", choices = NULL, multiple = TRUE,
                         options = list(placeholder = "Select a team first…")),
          selectizeInput("ld_players_off", "Players Off (exclude any)",   choices = NULL, multiple = TRUE,
                         options = list(placeholder = "Select a team first…")),
          
          tags$hr(),
          
          selectInput(
            "game_year_ld", "Season",
            choices  = c("2025-26" = "2026",
                         "2024-25" = "2025"),
            selected = DEFAULT_GAME_YEAR
          ),
          
          radioButtons("ld_num", "Group size",
                       choices = c("2","3","4","5"),
                       selected = LD_DEFAULT_NUM, inline = TRUE),
          
          dateRangeInput("ld_dates", "Date range", start = NA, end = NA),
          
          tags$hr(),
          
          # Clickable Menus for Tab 2
          bslib::accordion(
            bslib::accordion_panel(
              "Game Filters",
              selectizeInput(
                "ld_game_type", "Game type",
                choices = c(
                  "All" = "",
                  "Regular season" = "5",
                  "Playoffs – Quarterfinals" = "16",
                  "Playoffs – Finals" = "17",
                  "Playoffs – Semifinals" = "26",
                  "Play-in" = "33",
                  "Winner Cup" = "34"
                ),
                selected = "",
                multiple = TRUE,
                options = list(placeholder = "All game types")
              ),
              selectizeInput(
                "ld_opponents", "Opponents",
                choices = NULL,
                selected = character(0),
                multiple = TRUE,
                options = list(placeholder = "All opponents")
              ),
              selectInput(
                "ld_home_away", "Home/Away",
                choices = c("All" = "", "Home" = "home", "Away" = "away"),
                selected = ""
              ),
              selectInput(
                "ld_outcome", "Outcome",
                choices = c("All" = "", "Win" = "win", "Loss" = "loss"),
                selected = ""
              )
            ),
            bslib::accordion_panel(
              "Opponent Strength",
              selectInput(
                "ld_opp_rank_side", "Top / Bottom",
                choices = c("Off" = "", "Top" = "top", "Bottom" = "bottom"),
                selected = ""
              ),
              selectInput(
                "ld_opp_rank_n", "Rank N",
                choices = c("—" = "", as.character(1:12)),
                selected = ""
              ),
              selectInput(
                "ld_opp_rank_metric", "Metric",
                choices = c("—" = "", "Offense" = "off", "Defense" = "def", "Net rating" = "net"),
                selected = ""
              )
            ),
            open = FALSE # Closed by default
          )
        ),
        mainPanel(
          DTOutput("ld_table")
        )
      )
    )
  ),
  
  # -------- Tab 3: Team Ratings (NEW with Filters) --------
  tabPanel(
    title = "Team Ratings", value = "team_ratings",
    fluidPage(
      sidebarLayout(
        sidebarPanel(
          width = 3,
          actionButton("tr_reset", "Reset Filters"),
          tags$hr(),
          selectInput(
            "tr_game_year", "Season",
            choices  = c("2025-26" = "2026",
                         "2024-25" = "2025"),
            selected = DEFAULT_GAME_YEAR
          ),
          
          dateRangeInput("tr_dates", "Date range", start = NA, end = NA),
          
          tags$hr(),
          
          # Clickable Menus for Tab 3
          bslib::accordion(
            bslib::accordion_panel(
              "Game Filters",
              selectizeInput(
                "tr_game_type", "Game type",
                choices = c(
                  "All" = "",
                  "Regular season" = "5",
                  "Playoffs – Quarterfinals" = "16",
                  "Playoffs – Finals" = "17",
                  "Playoffs – Semifinals" = "26",
                  "Play-in" = "33",
                  "Winner Cup" = "34"
                ),
                selected = "",
                multiple = TRUE,
                options = list(placeholder = "All game types")
              ),
              selectizeInput(
                "tr_opponents", "Opponents",
                choices = NULL,
                selected = character(0),
                multiple = TRUE,
                options = list(placeholder = "All opponents")
              ),
              selectInput(
                "tr_home_away", "Home/Away",
                choices = c("All" = "", "Home" = "home", "Away" = "away"),
                selected = ""
              ),
              selectInput(
                "tr_outcome", "Outcome",
                choices = c("All" = "", "Win" = "win", "Loss" = "loss"),
                selected = ""
              )
            ),
            bslib::accordion_panel(
              "Opponent Strength",
              selectInput(
                "tr_opp_rank_side", "Top / Bottom",
                choices = c("Off" = "", "Top" = "top", "Bottom" = "bottom"),
                selected = ""
              ),
              selectInput(
                "tr_opp_rank_n", "Rank N",
                choices = c("—" = "", as.character(1:12)),
                selected = ""
              ),
              selectInput(
                "tr_opp_rank_metric", "Metric",
                choices = c("—" = "", "Offense" = "off", "Defense" = "def", "Net rating" = "net"),
                selected = ""
              )
            ),
            open = FALSE 
          )
        ),
        mainPanel(
          width = 9,
          DTOutput("tr_table")
        )
      )
    )
  )
)

# ---------------- Server ----------------
server <- function(input, output, session) {
  
  # ---- Season helpers -------------------------------------------------------
  season_date_bounds <- function(gy) {
    if (identical(gy, "2026")) {
      list(start = as.Date("2025-10-01"), end = as.Date("2026-07-01"))
    } else {
      list(start = DEFAULT_START, end = DEFAULT_END)
    }
  }
  
  selected_game_year <- reactive({
    gy <- input$game_year
    if (is.null(gy) || !nzchar(gy)) DEFAULT_GAME_YEAR else gy
  })
  
  # helper
  `%||%` <- function(a, b) if (!is.null(a)) a else b
  
  # ===== Opponents dropdown choices: ALL teams in season (non-null menu) =====
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
    
    updateSelectizeInput(session, "on_opponents",
                         choices  = td$team_name,
                         selected = character(0),
                         server   = TRUE)
    
    updateSelectizeInput(session, "ld_opponents",
                         choices  = td$team_name,
                         selected = character(0),
                         server   = TRUE)
    
    # Update Tab 3 opponents handled separately via observeEvent on tr_game_year
  }, ignoreInit = FALSE)
  
  # helper: names -> ids
  selected_opp_ids_on <- reactive({
    td  <- teams_for_year_df()
    sel <- input$on_opponents
    if (is.null(sel) || !length(sel)) return(NULL)
    td %>% filter(team_name %in% sel) %>% pull(team_id)
  })
  
  selected_opp_ids_ld <- reactive({
    td  <- teams_for_year_df()
    sel <- input$ld_opponents
    if (is.null(sel) || !length(sel)) return(NULL)
    td %>% filter(team_name %in% sel) %>% pull(team_id)
  })
  
  # ======== On/Off tab ===================================
  
  output$date_filter_ui <- renderUI({
    gy     <- selected_game_year()
    bounds <- season_date_bounds(gy)
    
    dateRangeInput(
      "date_range", "Game Date Range",
      start = bounds$start, end = bounds$end,
      min   = bounds$start, max   = bounds$end,
      format = "yyyy-mm-dd", weekstart = 0
    )
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
    selectizeInput("teams", "Teams",
                   choices  = teams$team_name,
                   multiple = TRUE,
                   options  = list(placeholder = "All teams"))
  })
  
  debounced_range <- reactive(input$date_range) %>% debounce(300)
  debounced_teams <- reactive(input$teams)        %>% debounce(300)
  
  debounced_on_filters <- reactive(list(
    game_type = input$on_game_type,
    opp_names = input$on_opponents,
    home_away = input$on_home_away,
    outcome   = input$on_outcome,
    rank_side = input$on_opp_rank_side,
    rank_n    = input$on_opp_rank_n,
    metric    = input$on_opp_rank_metric
  )) %>% debounce(300)
  
  selected_team_ids <- reactive({
    teams <- teams_for_year()
    teams_in <- debounced_teams()
    if (is.null(teams_in) || !length(teams_in)) return(NULL)
    teams %>% filter(team_name %in% teams_in) %>% pull(team_id)
  })
  
  fallback_needed <- reactive({
    rng <- debounced_range()
    if (is.null(rng)) return(TRUE)
    
    start_d <- as.Date(rng[1])
    end_d   <- as.Date(rng[2])
    
    gy             <- selected_game_year()
    season_bounds <- season_date_bounds(gy)
    
    team_changed <- !is.null(debounced_teams()) && length(debounced_teams()) > 0
    elig_changed <- (isTruthy(input$min_all_poss) && input$min_all_poss != DEFAULT_MIN_ALL) ||
      (isTruthy(input$min_on_poss)  && input$min_on_poss  != DEFAULT_MIN_ON)
    date_changed <- (start_d != season_bounds$start) || (end_d != season_bounds$end)
    
    f <- debounced_on_filters()
    has_game_type <- !is.null(f$game_type) && length(f$game_type) && any(nzchar(f$game_type))
    has_opp       <- !is.null(f$opp_names) && length(f$opp_names) > 0
    has_homeaway  <- nzchar(f$home_away %||% "")
    has_outcome   <- nzchar(f$outcome   %||% "")
    has_rank_side <- nzchar(f$rank_side %||% "")
    has_rank_n    <- nzchar(f$rank_n    %||% "")
    has_metric    <- nzchar(f$metric    %||% "")
    
    extra_filters <- has_game_type || has_opp || has_homeaway || has_outcome ||
      has_rank_side || has_rank_n || has_metric
    
    team_changed || elig_changed || date_changed || extra_filters
  })
  
  # ---------------- On/Off compute (14 params) ----------------
  run_onoff_compute_14 <- function(pool,
                                   start_d, end_d,
                                   team_ids, min_all, min_on, min_net, game_year,
                                   game_type_csv, opp_ids_csv, home_away, outcome,
                                   opp_rank_side, opp_rank_n, opp_rank_metric) {
    
    team_csv <- if (is.null(team_ids) || !length(team_ids)) NA_character_ else paste(team_ids, collapse = ",")
    
    DBI::dbGetQuery(
      pool,
      paste0(
        "SELECT * FROM basketball_test.onoff_compute(",
        "$1::date,$2::date,$3::text,$4::int4,$5::int4,$6::numeric,$7::text,",
        "$8::text,$9::text,$10::text,$11::text,$12::text,$13::int4,$14::text",
        ")"
      ),
      params = list(
        as.Date(start_d),
        as.Date(end_d),
        team_csv,
        as.integer(min_all),
        as.integer(min_on),
        as.numeric(min_net),
        as.character(game_year),
        
        game_type_csv,
        opp_ids_csv,
        home_away,
        outcome,
        opp_rank_side,
        opp_rank_n,
        opp_rank_metric
      )
    )
  }
  
  live_result_df <- reactive({
    req(input$min_all_poss, input$min_on_poss)
    rng  <- debounced_range(); req(rng)
    tids <- selected_team_ids()
    gy   <- selected_game_year()
    f    <- debounced_on_filters()
    
    game_type_csv <- {
      x <- f$game_type
      if (is.null(x) || !length(x) || !any(nzchar(x))) NA_character_
      else paste(x[nzchar(x)], collapse = ",")
    }
    
    opp_ids_csv <- {
      ids <- selected_opp_ids_on()
      if (is.null(ids) || !length(ids)) NA_character_
      else paste(ids, collapse = ",")
    }
    
    home_away <- if (!nzchar(f$home_away %||% "")) NA_character_ else f$home_away
    outcome   <- if (!nzchar(f$outcome   %||% "")) NA_character_ else f$outcome
    
    opp_rank_side    <- if (!nzchar(f$rank_side %||% "")) NA_character_ else f$rank_side
    opp_rank_n       <- suppressWarnings(as.integer(if (!nzchar(f$rank_n %||% "")) NA_character_ else f$rank_n))
    opp_rank_metric <- if (!nzchar(f$metric %||% "")) NA_character_ else f$metric
    
    run_onoff_compute_14(
      pg_pool,
      start_d = as.Date(rng[1]),
      end_d   = as.Date(rng[2]),
      team_ids = tids,
      min_all  = input$min_all_poss,
      min_on   = input$min_on_poss,
      min_net  = DEFAULT_MIN_NET,
      game_year = gy,
      
      game_type_csv    = game_type_csv,
      opp_ids_csv      = opp_ids_csv,
      home_away        = home_away,
      outcome          = outcome,
      opp_rank_side    = opp_rank_side,
      opp_rank_n       = opp_rank_n,
      opp_rank_metric = opp_rank_metric
    )
  })
  
  mv_result_df <- reactive({
    gy <- as.integer(selected_game_year())
    
    onoff_mv %>%
      filter(`Year` == !!gy) %>%
      arrange(desc(`Net RTG Diff`), `Team`, `Last Name`, `First Name`) %>%
      collect()
  })
  
  result_df <- reactive({
    if (isTRUE(fallback_needed())) live_result_df() else mv_result_df()
  }) %>% bindEvent(
    debounced_range(), debounced_teams(), debounced_on_filters(),
    input$min_all_poss, input$min_on_poss,
    input$game_year
  )
  
  output$onoff_dt <- renderDT({
    df <- result_df()
    
    if (!"Player" %in% names(df) && all(c("First Name","Last Name") %in% names(df))) {
      df <- df %>% mutate(Player = paste(`First Name`, `Last Name`))
    }
    
    keep_cols <- c(
      "Team","Player",
      "Net RTG Diff","Off ON Diff","Def ON Diff",
      "Off ON PPP","Def ON PPP","On Net RTG",
      "Off OFF PPP","Def OFF PPP","Off Net RTG",
      "ON Poss","OFF Poss",
      "pr_net","pr_off_on","pr_def_on","pr_on_net",
      "pr_off_off","pr_def_off","pr_off_net",
      "pr_def_on_d","pr_off_on_d","pr_def_on_inv","pr_def_off_inv","pr_def_on_d_inv"
    )
    df <- df[, intersect(keep_cols, names(df))]
    
    sketch <- htmltools::withTags(table(
      class = 'display',
      thead(
        tr(
          th(class="group-head",           colspan=2, ""),
          th(class="group-head",           colspan=3, "Summary"),
          th(class="group-head sep-left",  colspan=3, "On Court"),
          th(class="group-head sep-left",  colspan=3, "Off Court"),
          th(class="group-head",           colspan=2, "Usage")
        ),
        tr(
          th(class="sub-head","Team"),
          th(class="sub-head","Player"),
          th(class="sub-head","Net RTG Diff"),
          th(class="sub-head","Off Diff"),
          th(class="sub-head","Def Diff"),
          th(class="sub-head sep-left","Off ON"),
          th(class="sub-head","Def ON"),
          th(class="sub-head","On Net RTG"),
          th(class="sub-head sep-left","Off OFF"),
          th(class="sub-head","Def OFF"),
          th(class="sub-head","Off Net RTG"),
          th(class="sub-head","ON Poss"),
          th(class="sub-head","OFF Poss")
        )
      )
    ))
    
    idx_on_start  <- which(names(df) == "Off ON PPP")  - 1
    idx_off_start <- which(names(df) == "Off OFF PPP") - 1
    
    pr_cols  <- c("pr_net","pr_off_on","pr_def_on","pr_on_net",
                  "pr_off_off","pr_def_off","pr_off_net",
                  "pr_def_on_d","pr_off_on_d","pr_def_on_inv","pr_def_off_inv","pr_def_on_d_inv")
    hide_idx <- which(names(df) %in% pr_cols) - 1
    
    cuts      <- seq(0.05, 0.95, by = 0.05)
    cols_grad <- colorRampPalette(c("#d73027","#fee08b","#1a9850"))(20)
    
    dt <- datatable(
      df,
      container = sketch,
      rownames  = FALSE,
      options = list(
        dom = "tip",
        pageLength = 30,
        deferRender = TRUE,
        processing  = TRUE,
        autoWidth   = TRUE,
        scrollX     = TRUE,
        order = list(list(which(names(df)=="Net RTG Diff")-1, "desc")),
        columnDefs = list(
          list(targets = c(idx_on_start, idx_off_start), className = "block-start"),
          list(targets = hide_idx, visible = FALSE)
        )
      )
    ) |>
      formatRound(c("Net RTG Diff","On Net RTG","Off Net RTG"), 2) |>
      formatRound(c("Off ON PPP","Def ON PPP","Off OFF PPP","Def OFF PPP","Off ON Diff","Def ON Diff"), 1) |>
      formatCurrency(c("ON Poss","OFF Poss"), currency = "", interval = 3, mark = ",", digits = 0)
    
    for (m in list(
      c("Net RTG Diff","pr_net"),
      c("Off ON PPP","pr_off_on"),
      c("Off OFF PPP","pr_off_off"),
      c("On Net RTG","pr_on_net"),
      c("Off Net RTG","pr_off_net"),
      c("Off ON Diff","pr_off_on_d")
    )) dt <- formatStyle(dt, m[1], backgroundColor = styleInterval(cuts, cols_grad), valueColumns = m[2])
    
    for (m in list(
      c("Def ON PPP","pr_def_on_inv"),
      c("Def OFF PPP","pr_def_off_inv"),
      c("Def ON Diff","pr_def_on_d_inv")
    )) dt <- formatStyle(dt, m[1], backgroundColor = styleInterval(cuts, cols_grad), valueColumns = m[2])
    
    dt
  }) %>% bindEvent(
    debounced_range(), debounced_teams(), debounced_on_filters(),
    input$min_all_poss, input$min_on_poss,
    input$game_year
  )
  
  observeEvent(input$reset_defaults, {
    gy     <- selected_game_year()
    bounds <- season_date_bounds(gy)
    
    updateDateRangeInput(session, "date_range", start = bounds$start, end = bounds$end)
    updateSelectizeInput(session, "teams", selected = character(0))
    updateSliderInput(session, "min_all_poss", value = DEFAULT_MIN_ALL)
    updateSliderInput(session, "min_on_poss",  value = DEFAULT_MIN_ON)
    
    updateSelectizeInput(session, "on_game_type", selected = "")
    updateSelectizeInput(session, "on_opponents", selected = character(0))
    updateSelectInput(session, "on_home_away", selected = "")
    updateSelectInput(session, "on_outcome", selected = "")
    updateSelectInput(session, "on_opp_rank_side", selected = "")
    updateSelectInput(session, "on_opp_rank_n", selected = "")
    updateSelectInput(session, "on_opp_rank_metric", selected = "")
  })
  
  output$download_csv <- downloadHandler(
    filename = function() paste0("onoff_summary_", Sys.Date(), ".csv"),
    content = function(file) {
      df <- result_df() %>%
        mutate(Player = if ("Player" %in% names(.)) Player else paste(`First Name`, `Last Name`)) %>%
        arrange(desc(`Net RTG Diff`), Team, `Last Name`, `First Name`) %>%
        select(
          Team, Player,
          `Net RTG Diff`, `Off ON Diff`, `Def ON Diff`,
          `Off ON PPP`, `Def ON PPP`, `On Net RTG`,
          `Off OFF PPP`, `Def OFF PPP`, `Off Net RTG`,
          `ON Poss`, `OFF Poss`,
          dplyr::any_of(c("player_id","team_id"))
        )
      readr::write_csv(df, file, na = "")
    }
  )
  
  # ======== Lineup Data tab =========================================================
  
  ld_ref <- reactiveValues(teams=NULL, players=NULL)
  
  observeEvent(list(input$main_tabs, input$game_year), ignoreInit = TRUE, {
    if (!identical(input$main_tabs, "lineup_data")) return(NULL)
    
    gy_int <- as.integer(selected_game_year())
    
    teams_ld <- DBI::dbGetQuery(
      pg_pool,
      "SELECT DISTINCT team_id, MIN(team_name) AS team_name
         FROM basketball_test.full_rosters
        WHERE game_year = $1
        GROUP BY team_id
        ORDER BY MIN(team_name)",
      params = list(gy_int)
    )
    ld_ref$teams <- teams_ld
    
    team_values <- c("", as.character(teams_ld$team_id))
    names(team_values) <- c("— All teams —", teams_ld$team_name)
    
    updateSelectizeInput(session, "ld_team",
                         choices  = team_values,
                         selected = "", server = TRUE)
    
    players_map <- DBI::dbGetQuery(
      pg_pool,
      "SELECT team_id, player_id,
              MIN(btrim(firstname)||' '||btrim(lastname)) AS name
         FROM basketball_test.full_rosters
        WHERE game_year = $1
        GROUP BY team_id, player_id
        ORDER BY MIN(btrim(firstname)||' '||btrim(lastname))",
      params = list(gy_int)
    )
    ld_ref$players <- players_map
    
    updateSelectizeInput(session, "ld_players_on",
                         choices  = setNames(integer(0), character(0)),
                         selected = character(0), server = TRUE)
    updateSelectizeInput(session, "ld_players_off",
                         choices  = setNames(integer(0), character(0)),
                         selected = character(0), server = TRUE)
  })
  
  observeEvent(input$ld_team, {
    req(identical(input$main_tabs, "lineup_data"))
    
    if (is.null(input$ld_team) || is.na(input$ld_team) || !nzchar(input$ld_team)) {
      updateSelectizeInput(session, "ld_players_on",
                           choices = setNames(integer(0), character(0)),
                           selected = character(0), server = TRUE)
      updateSelectizeInput(session, "ld_players_off",
                           choices = setNames(integer(0), character(0)),
                           selected = character(0), server = TRUE)
      return(invisible(NULL))
    }
    
    team_id <- as.integer(input$ld_team)
    players <- ld_ref$players %>% filter(team_id == !!team_id)
    choices <- setNames(players$player_id, players$name)
    updateSelectizeInput(session, "ld_players_on",
                         choices  = choices,
                         selected = character(0), server = TRUE)
    updateSelectizeInput(session, "ld_players_off",
                         choices  = choices,
                         selected = character(0), server = TRUE)
  }, ignoreInit = TRUE)
  
  observeEvent(input$ld_players_on, {
    on_sel  <- input$ld_players_on  %||% character(0)
    off_sel <- input$ld_players_off %||% character(0)
    inter   <- intersect(on_sel, off_sel)
    if (length(inter)) {
      updateSelectizeInput(session, "ld_players_off",
                           selected = setdiff(off_sel, inter), server = TRUE)
    }
  }, ignoreInit = TRUE)
  
  observeEvent(input$ld_players_off, {
    on_sel  <- input$ld_players_on  %||% character(0)
    off_sel <- input$ld_players_off %||% character(0)
    inter   <- intersect(on_sel, off_sel)
    if (length(inter)) {
      updateSelectizeInput(session, "ld_players_on",
                           selected = setdiff(on_sel, inter), server = TRUE)
    }
  }, ignoreInit = TRUE)
  
  observeEvent(input$ld_reset, {
    updateRadioButtons(session, "ld_num", selected = LD_DEFAULT_NUM)
    updateDateRangeInput(session, "ld_dates", start = NA, end = NA)
    
    if (!is.null(ld_ref$teams)) {
      team_values <- c("", as.character(ld_ref$teams$team_id))
      names(team_values) <- c("— All teams —", ld_ref$teams$team_name)
      updateSelectizeInput(session, "ld_team",
                           choices = team_values, selected = "", server = TRUE)
    } else {
      updateSelectizeInput(session, "ld_team", selected = "", server = TRUE)
    }
    
    updateSelectizeInput(session, "ld_players_on",
                         choices = setNames(integer(0), character(0)),
                         selected = character(0), server = TRUE)
    updateSelectizeInput(session, "ld_players_off",
                         choices = setNames(integer(0), character(0)),
                         selected = character(0), server = TRUE)
    
    updateSliderInput(session, "ld_minposs", value = LD_DEFAULT_MIN_POSS)
    
    updateSelectizeInput(session, "ld_game_type", selected = "")
    updateSelectizeInput(session, "ld_opponents", selected = character(0))
    updateSelectInput(session, "ld_home_away", selected = "")
    updateSelectInput(session, "ld_outcome", selected = "")
    updateSelectInput(session, "ld_opp_rank_side", selected = "")
    updateSelectInput(session, "ld_opp_rank_n", selected = "")
    updateSelectInput(session, "ld_opp_rank_metric", selected = "")
  })
  
  # ----------- Lineup DB call (16 params: 9 old + 7 new) ---------------------
  run_fetch_lineups_16 <- function(pool,
                                   num, team_csv, player_csv, player_off_csv,
                                   exact, start_date, end_date, min_poss, game_year,
                                   game_type_csv, opp_ids_csv, home_away, outcome,
                                   opp_rank_side, opp_rank_n, opp_rank_metric) {
    
    DBI::dbGetQuery(
      pool,
      paste0(
        "SELECT * FROM basketball_test.fetch_lineups_csv_v2(",
        "$1::int4,$2::text,$3::text,$4::text,$5::bool,$6::date,$7::date,$8::int4,$9::int4,",
        "$10::text,$11::text,$12::text,$13::text,$14::text,$15::int4,$16::text",
        ")"
      ),
      params = list(
        as.integer(num),
        team_csv,
        player_csv,
        player_off_csv,
        as.logical(exact),
        as.Date(start_date),
        as.Date(end_date),
        as.integer(min_poss),
        as.integer(game_year),
        game_type_csv,
        opp_ids_csv,
        home_away,
        outcome,
        opp_rank_side,
        opp_rank_n,
        opp_rank_metric
      )
    )
  }
  
  ld_params <- reactive({
    req(identical(input$main_tabs, "lineup_data"))
    
    team_id <- if (!is.null(input$ld_team) && !is.na(input$ld_team) && nzchar(input$ld_team)) {
      as.integer(input$ld_team)
    } else {
      NA_integer_
    }
    
    player_on_ids  <- if (!is.na(team_id)) as.integer(input$ld_players_on)  else integer(0)
    player_off_ids <- if (!is.na(team_id)) as.integer(input$ld_players_off) else integer(0)
    
    ld_game_type_csv <- {
      x <- input$ld_game_type
      if (is.null(x) || !length(x) || !any(nzchar(x))) NA_character_
      else paste(x[nzchar(x)], collapse = ",")
    }
    
    ld_opp_ids_csv <- {
      ids <- selected_opp_ids_ld()
      if (is.null(ids) || !length(ids)) NA_character_
      else paste(ids, collapse = ",")
    }
    
    ld_home_away <- if (!nzchar(input$ld_home_away %||% "")) NA_character_ else input$ld_home_away
    ld_outcome   <- if (!nzchar(input$ld_outcome   %||% "")) NA_character_ else input$ld_outcome
    
    ld_rank_side <- if (!nzchar(input$ld_opp_rank_side %||% "")) NA_character_ else input$ld_opp_rank_side
    ld_rank_n    <- suppressWarnings(as.integer(if (!nzchar(input$ld_opp_rank_n %||% "")) NA_character_ else input$ld_opp_rank_n))
    ld_metric    <- if (!nzchar(input$ld_opp_rank_metric %||% "")) NA_character_ else input$ld_opp_rank_metric
    
    list(
      num              = as.integer(input$ld_num),
      team_csv         = if (!is.na(team_id)) as.character(team_id) else NA_character_,
      player_csv       = if (length(player_on_ids))  paste(player_on_ids,  collapse = ",") else NA_character_,
      player_off_csv = if (length(player_off_ids)) paste(player_off_ids, collapse = ",") else NA_character_,
      exact        = TRUE,
      start_date = if (!is.null(input$ld_dates[1]) && !is.na(input$ld_dates[1])) as.Date(input$ld_dates[1]) else NA,
      end_date   = if (!is.null(input$ld_dates[2]) && !is.na(input$ld_dates[2])) as.Date(input$ld_dates[2]) else NA,
      min_poss   = as.integer(input$ld_minposs),
      
      game_type_csv    = ld_game_type_csv,
      opp_ids_csv      = ld_opp_ids_csv,
      home_away        = ld_home_away,
      outcome          = ld_outcome,
      opp_rank_side    = ld_rank_side,
      opp_rank_n       = ld_rank_n,
      opp_rank_metric = ld_metric
    )
  }) %>% bindEvent(
    input$ld_num, input$ld_team, input$ld_players_on, input$ld_players_off,
    input$ld_dates, input$ld_minposs, input$main_tabs,
    input$ld_game_type, input$ld_opponents, input$ld_home_away, input$ld_outcome,
    input$ld_opp_rank_side, input$ld_opp_rank_n, input$ld_opp_rank_metric
  )
  
  ld_data <- reactive({
    req(ld_params())
    p  <- ld_params()
    gy <- as.integer(selected_game_year())
    
    df <- run_fetch_lineups_16(
      pg_pool,
      num            = p$num,
      team_csv       = p$team_csv,
      player_csv     = p$player_csv,
      player_off_csv = p$player_off_csv,
      exact          = p$exact,
      start_date     = p$start_date,
      end_date       = p$end_date,
      min_poss       = p$min_poss,
      game_year      = gy,
      
      game_type_csv    = p$game_type_csv,
      opp_ids_csv      = p$opp_ids_csv,
      home_away        = p$home_away,
      outcome          = p$outcome,
      opp_rank_side    = p$opp_rank_side,
      opp_rank_n       = p$opp_rank_n,
      opp_rank_metric = p$opp_rank_metric
    )
    
    if (is.null(df) || NROW(df) == 0L) return(df[0, , drop = FALSE])
    
    df$total_poss <- dplyr::coalesce(df$off_poss, 0L) + dplyr::coalesce(df$def_poss, 0L)
    df$plus_minus <- dplyr::coalesce(df$off_pts, 0)  - dplyr::coalesce(df$def_pts, 0)
    
    df
  })
  
  team_name_vec <- reactive({
    tdf <- isolate(ld_ref$teams)
    if (is.null(tdf)) return(character(0))
    setNames(tdf$team_name, as.character(tdf$team_id))
  })
  
  output$ld_table <- DT::renderDataTable({
    req(ld_params())
    df <- ld_data()
    
    # 1. Map Names
    tmap <- team_name_vec()
    if ("team_id" %in% names(df)) df$Team <- unname(tmap[as.character(df$team_id)])
    if ("player_names_str" %in% names(df)) df$Players <- df$player_names_str
    
    # 2. SELECT display columns only (prevents crash on array types)
    keep_cols <- c("Team", "Players", "total_poss", "plus_minus", 
                   "off_poss", "def_poss", "off_pts", "def_pts", 
                   "off_ppp", "def_ppp", "net_rtg", 
                   "num_lineup", "sub_lineup_hash")
    df <- df %>% select(any_of(keep_cols))
    
    # 3. Add hidden sort helper (1 for data rows)
    df$is_total <- 1
    
    # 4. Sort Data in R (by Net RTG descending)
    if ("net_rtg" %in% names(df)) {
      df <- df %>% arrange(desc(net_rtg))
    }
    
    # 5. Calculate and Prepend Total Row
    if (nrow(df) > 0) {
      sum_off_poss <- sum(df$off_poss, na.rm = TRUE)
      sum_def_poss <- sum(df$def_poss, na.rm = TRUE)
      sum_off_pts  <- sum(df$off_pts, na.rm = TRUE)
      sum_def_pts  <- sum(df$def_pts, na.rm = TRUE)
      
      tot_off_ppp <- if (sum_off_poss > 0) (sum_off_pts / sum_off_poss) * 100 else 0
      tot_def_ppp <- if (sum_def_poss > 0) (sum_def_pts / sum_def_poss) * 100 else 0
      tot_net_rtg <- tot_off_ppp - tot_def_ppp
      
      total_row <- data.frame(
        Team            = "TOTAL",
        Players         = "— All Lineups —",
        total_poss      = sum_off_poss + sum_def_poss,
        off_ppp         = tot_off_ppp,
        def_ppp         = tot_def_ppp,
        net_rtg         = tot_net_rtg,
        plus_minus      = sum_off_pts - sum_def_pts,
        off_poss        = sum_off_poss,
        off_pts         = sum_off_pts,
        def_poss        = sum_def_poss,
        def_pts         = sum_def_pts,
        num_lineup      = NA_integer_,
        sub_lineup_hash = "TOTAL",
        is_total        = 0, # <--- 0 for Total
        stringsAsFactors = FALSE
      )
      
      df <- dplyr::bind_rows(total_row, df)
    }
    
    # 6. Move helper to first column (Index 0 in JS)
    df <- df %>% select(is_total, everything())
    
    show_cols <- c(
      "Team","Players","total_poss",
      "off_ppp","def_ppp","net_rtg","plus_minus",
      "off_poss","off_pts","def_poss","def_pts","num_lineup",
      "sub_lineup_hash"
    )
    
    # Safe ranking ignoring TOTAL row
    is_data <- if (nrow(df) > 0) df$Team != "TOTAL" else logical(0)
    
    pr_safe <- function(x, invert = FALSE) {
      vals <- x[is_data]
      n <- sum(!is.na(vals))
      if (n <= 1) return(rep(NA_real_, length(x)))
      r <- rank(vals, na.last = "keep", ties.method = "average")
      p <- (r - 1) / (n - 1)
      if (invert) p <- 1 - p
      
      out <- rep(NA_real_, length(x))
      out[is_data] <- as.numeric(p)
      out
    }
    
    df$pr_ld_net        <- pr_safe(df$net_rtg,  invert = FALSE)
    df$pr_ld_off_ppp    <- pr_safe(df$off_ppp,  invert = FALSE)
    df$pr_ld_def_ppp_i  <- pr_safe(df$def_ppp,  invert = TRUE)
    
    pr_cols <- c("pr_ld_net","pr_ld_off_ppp","pr_ld_def_ppp_i")
    # Note: Keep "is_total" separate from "keep" calculation
    keep    <- intersect(show_cols, names(df))
    # Final DF structure: is_total | Display Cols | PR Cols
    df      <- df[, unique(c("is_total", keep, pr_cols[pr_cols %in% names(df)])), drop = FALSE]
    
    pretty_labels <- c(
      Team                = "Team",
      Players             = "Players",
      num_lineup          = "Size",
      total_poss          = "Total Poss",
      net_rtg             = "Net RTG",
      plus_minus          = "+/- (Off-Def)",
      off_ppp             = "Off PPP",
      def_ppp             = "Def PPP",
      off_poss            = "Off Poss",
      off_pts             = "Off Pts",
      def_poss            = "Def Poss",
      def_pts             = "Def Pts",
      sub_lineup_hash     = "Lineup ID"
    )
    # The actual data column names (excluding is_total and PR cols)
    data_col_names <- colnames(df)[-1] 
    # Remove PR cols from that list for labeling
    data_col_names <- setdiff(data_col_names, pr_cols)
    
    col_labels <- unname(pretty_labels[data_col_names])
    # Add blank label for the hidden "is_total" column (first col)
    final_labels <- c("", col_labels) 
    
    cuts      <- seq(0.05, 0.95, by = 0.05)
    cols_grad <- colorRampPalette(c("#d73027","#fee08b","#1a9850"))(20)
    
    # Calculate indices to hide (is_total + PR cols)
    # is_total is index 0
    # PR cols are at the end
    pr_indices <- which(colnames(df) %in% pr_cols) - 1L
    hidden_indices <- c(0, pr_indices)
    
    dt <- DT::datatable(
      df,
      colnames = final_labels,
      rownames = FALSE,
      filter   = "top",
      options  = list(
        pageLength  = 50,
        lengthMenu  = c(25, 50, 100, 200, 1000),
        # FREEZE TOTAL ROW: Sort by is_total (asc) first, then allow user sorting
        orderFixed  = list(list(0, 'asc')), 
        deferRender = TRUE,
        scrollX     = TRUE,
        processing  = TRUE,
        columnDefs  = list(
          list(targets = hidden_indices, visible = FALSE)
        )
      )
    ) |>
      DT::formatRound(
        c("off_ppp","def_ppp","net_rtg")[c("off_ppp","def_ppp","net_rtg") %in% names(df)],
        1
      ) |>
      DT::formatCurrency(
        c("total_poss","off_poss","def_poss")[c("total_poss","off_poss","def_poss") %in% names(df)],
        currency = "", interval = 3, mark = ",", digits = 0
      ) |>
      DT::formatCurrency(
        c("off_pts","def_pts","plus_minus")[c("off_pts","def_pts","plus_minus") %in% names(df)],
        currency = "", interval = 3, mark = ",", digits = 0
      )
    
    # Style for TOTAL row
    dt <- DT::formatStyle(
      dt, "Team",
      target = "row",
      backgroundColor = styleEqual("TOTAL", "#f0f0f0"),
      fontWeight      = styleEqual("TOTAL", "bold")
    )
    
    if (all(c("net_rtg","pr_ld_net") %in% colnames(df))) {
      dt <- DT::formatStyle(dt, "net_rtg",
                            backgroundColor = styleInterval(cuts, cols_grad),
                            valueColumns    = "pr_ld_net")
    }
    if (all(c("off_ppp","pr_ld_off_ppp") %in% colnames(df))) {
      dt <- DT::formatStyle(dt, "off_ppp",
                            backgroundColor = styleInterval(cuts, cols_grad),
                            valueColumns    = "pr_ld_off_ppp")
    }
    if (all(c("def_ppp","pr_ld_def_ppp_i") %in% colnames(df))) {
      dt <- DT::formatStyle(dt, "def_ppp",
                            backgroundColor = styleInterval(cuts, cols_grad),
                            valueColumns    = "pr_ld_def_ppp_i")
    }
    
    dt
  })
  
  # ======== Tab 3: Team Ratings Logic (Updated) =================================
  
  observeEvent(input$tr_reset, {
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
    # Update Tab 3 opponents when its season changes or tab is opened
    req(input$tr_game_year)
    gy_int <- as.integer(input$tr_game_year)
    
    # We fetch teams for the year selected in Tab 3 specifically
    td <- full_rosters %>%
      filter(game_year == !!gy_int) %>%
      distinct(team_id, team_name) %>%
      arrange(team_name) %>%
      collect()
    
    updateSelectizeInput(session, "tr_opponents",
                         choices  = td$team_name,
                         selected = character(0),
                         server   = TRUE)
  })
  
  # New Function: Dynamic Team Ratings
  run_team_ratings_dynamic <- function(pool, 
                                       game_year, start_d, end_d,
                                       game_type_csv, opp_ids_csv, home_away, outcome,
                                       opp_rank_side, opp_rank_n, opp_rank_metric) {
    DBI::dbGetQuery(
      pool,
      paste0(
        "SELECT * FROM basketball_test.get_team_ratings_dynamic(",
        "$1::int4,$2::date,$3::date,$4::text,$5::text,$6::text,$7::text,$8::text,$9::int4,$10::text",
        ")"
      ),
      params = list(
        as.integer(game_year),
        if (!is.na(start_d)) as.Date(start_d) else NA,
        if (!is.na(end_d)) as.Date(end_d) else NA,
        game_type_csv,
        opp_ids_csv,
        home_away,
        outcome,
        opp_rank_side,
        opp_rank_n,
        opp_rank_metric
      )
    )
  }
  
  tr_params <- reactive({
    gy <- as.integer(input$tr_game_year)
    req(gy)
    
    start_d <- if (!is.null(input$tr_dates[1]) && !is.na(input$tr_dates[1])) as.Date(input$tr_dates[1]) else NA
    end_d   <- if (!is.null(input$tr_dates[2]) && !is.na(input$tr_dates[2])) as.Date(input$tr_dates[2]) else NA
    
    tr_game_type_csv <- {
      x <- input$tr_game_type
      if (is.null(x) || !length(x) || !any(nzchar(x))) NA_character_
      else paste(x[nzchar(x)], collapse = ",")
    }
    
    # Need to map Opponent Names -> IDs for Tab 3
    # Helper to get map for current tab 3 year
    td_map <- full_rosters %>%
      filter(game_year == !!gy) %>%
      distinct(team_id, team_name) %>%
      collect()
    
    tr_opp_ids_csv <- {
      sel <- input$tr_opponents
      if (is.null(sel) || !length(sel)) NA_character_
      else {
        ids <- td_map %>% filter(team_name %in% sel) %>% pull(team_id)
        paste(ids, collapse = ",")
      }
    }
    
    tr_home_away <- if (!nzchar(input$tr_home_away %||% "")) NA_character_ else input$tr_home_away
    tr_outcome   <- if (!nzchar(input$tr_outcome   %||% "")) NA_character_ else input$tr_outcome
    
    tr_rank_side <- if (!nzchar(input$tr_opp_rank_side %||% "")) NA_character_ else input$tr_opp_rank_side
    tr_rank_n    <- suppressWarnings(as.integer(if (!nzchar(input$tr_opp_rank_n %||% "")) NA_character_ else input$tr_opp_rank_n))
    tr_metric    <- if (!nzchar(input$tr_opp_rank_metric %||% "")) NA_character_ else input$tr_opp_rank_metric
    
    list(
      game_year = gy,
      start_d = start_d,
      end_d = end_d,
      game_type_csv = tr_game_type_csv,
      opp_ids_csv = tr_opp_ids_csv,
      home_away = tr_home_away,
      outcome = tr_outcome,
      rank_side = tr_rank_side,
      rank_n = tr_rank_n,
      metric = tr_metric
    )
  }) %>% debounce(300)
  
  # Determine if we need fallback (Dynamic) or default (Static MV)
  tr_fallback_needed <- reactive({
    p <- tr_params()
    
    has_dates <- !is.na(p$start_d) || !is.na(p$end_d)
    has_gt    <- !is.na(p$game_type_csv)
    has_opp   <- !is.na(p$opp_ids_csv)
    has_ha    <- !is.na(p$home_away)
    has_out   <- !is.na(p$outcome)
    has_rank  <- !is.na(p$rank_side) || !is.na(p$rank_n)
    
    has_dates || has_gt || has_opp || has_ha || has_out || has_rank
  })
  
  tr_data <- reactive({
    p <- tr_params()
    
    if (tr_fallback_needed()) {
      # Use Dynamic Function
      run_team_ratings_dynamic(
        pg_pool,
        game_year = p$game_year,
        start_d   = p$start_d,
        end_d     = p$end_d,
        game_type_csv = p$game_type_csv,
        opp_ids_csv   = p$opp_ids_csv,
        home_away     = p$home_away,
        outcome       = p$outcome,
        opp_rank_side = p$rank_side,
        opp_rank_n    = p$rank_n,
        opp_rank_metric = p$metric
      )
    } else {
      # Use Static Materialized View (Fast)
      team_ratings_mv %>%
        filter(game_year == !!p$game_year) %>%
        select(game_year, team_name, off_ppp, def_ppp, net_rtg, 
               rank_net_rtg, rank_off_ppp, rank_def_ppp) %>%
        arrange(rank_net_rtg) %>%
        collect()
    }
  })
  
  output$tr_table <- renderDT({
    df <- tr_data()
    
    if (is.null(df) || nrow(df) == 0) return(NULL)
    
    # 2. Pretty Names map
    # Cols: game_year, team_name, off_ppp, def_ppp, net_rtg, rank_net_rtg, rank_off_ppp, rank_def_ppp
    # Note: Dynamic function returns team_id, static select didn't. 
    # Adjust logic: Just display the common columns.
    pretty_names <- c(
      "Season", "Team", 
      "Off PPP", "Def PPP", "Net Rtg",
      "Net Rank", "Off Rank", "Def Rank"
    )
    
    # Ensure columns match for display
    # Dynamic func returns: game_year, team_id, team_name, off_ppp, def_ppp, net_rtg, rank_net...
    # Static logic above selects: game_year, team_name, ...
    
    # Normalize for display
    disp_df <- df %>%
      select(game_year, team_name, off_ppp, def_ppp, net_rtg, 
             rank_net_rtg, rank_off_ppp, rank_def_ppp)
    
    # 3. Create Color Palette for Ranks (1=Green ... 30=Red)
    max_rank <- max(c(disp_df$rank_net_rtg, disp_df$rank_off_ppp, disp_df$rank_def_ppp), na.rm = TRUE)
    if (max_rank < 2) max_rank <- 2
    
    cuts <- seq(1.5, max_rank - 0.5, 1)
    cols_rank <- colorRampPalette(c("#1a9850", "#fee08b", "#d73027"))(length(cuts) + 1)
    
    # 4. Render
    dt <- datatable(
      disp_df,
      colnames = pretty_names,
      rownames = FALSE,
      options = list(
        dom = "t", 
        pageLength = 50,
        scrollX = TRUE,
        columnDefs = list(
          list(className = 'dt-center', targets = "_all")
        )
      )
    ) %>%
      formatRound(c("off_ppp", "def_ppp", "net_rtg"), 1) %>%
      formatStyle(
        columns = c("rank_net_rtg", "rank_off_ppp", "rank_def_ppp"),
        backgroundColor = styleInterval(cuts, cols_rank)
      )
    
    dt
  })
  
}

shinyApp(ui, server)