# app.R -------------------------------------------------------------
library(shiny)
library(DBI)
library(dplyr)
library(dbplyr)
library(pool)
library(RPostgres)
library(DT)
library(purrr)

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

# ---------------- UI ----------------
ui <- navbarPage(
  id = "main_tabs",
  title = "Player Analytics",
  
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
        "))
      ),
      titlePanel("Player ON/OFF Impact (PostgreSQL + dbplyr + DT)"),
      sidebarLayout(
        sidebarPanel(
          actionButton("reset_defaults", "Reset to defaults"),
          tags$hr(),
          
          # Season selector (maps to schedule.game_year)
          selectInput(
            "game_year", "Season",
            choices  = c("2025-26" = "2026",
                         "2024-25" = "2025"),
            selected = DEFAULT_GAME_YEAR
          ),
          
          uiOutput("date_filter_ui"),
          uiOutput("team_filter_ui"),
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
          # *** moved to top ***
          actionButton("ld_reset", "Reset Lineup Filters"),
          tags$hr(),
          
          # Season selector (shared inputId, same default)
          selectInput(
            "game_year", "Season",
            choices  = c("2025-26" = "2026",
                         "2024-25" = "2025"),
            selected = DEFAULT_GAME_YEAR
          ),
          tags$hr(),
          
          radioButtons("ld_num", "Group size",
                       choices = c("2","3","4","5"),
                       selected = LD_DEFAULT_NUM, inline = TRUE),
          
          dateRangeInput("ld_dates", "Date range", start = NA, end = NA),
          
          # Team: single-select (default is empty -> all teams)
          selectizeInput("ld_team", "Team", choices = NULL, multiple = FALSE),
          
          helpText("Pick a team to enable player filtering."),
          # Players ON / OFF (both disabled until a team is chosen)
          selectizeInput("ld_players_on",  "Players On (exact/contains)", choices = NULL, multiple = TRUE,
                         options = list(placeholder = "Select a team first…")),
          selectizeInput("ld_players_off", "Players Off (exclude any)",   choices = NULL, multiple = TRUE,
                         options = list(placeholder = "Select a team first…")),
          
          sliderInput("ld_minposs", "Min possessions (sum of Off/Def)",
                      min = 0, max = 2000, value = LD_DEFAULT_MIN_POSS, step = 10)
        ),
        mainPanel(
          DTOutput("ld_table")
        )
      )
    )
  )
)

# ---------------- Server ----------------
server <- function(input, output, session) {
  
  # ---- Season helpers -------------------------------------------------------
  # Map a game_year to its default date window
  season_date_bounds <- function(gy) {
    if (identical(gy, "2026")) {
      list(
        start = as.Date("2025-10-01"),
        end   = as.Date("2026-07-01")
      )
    } else {
      # 2024-25 (game_year 2025) → original window
      list(
        start = DEFAULT_START,
        end   = DEFAULT_END
      )
    }
  }
  
  # Reactive wrapper for the currently selected game_year,
  # with a fallback to DEFAULT_GAME_YEAR
  selected_game_year <- reactive({
    gy <- input$game_year
    if (is.null(gy) || !nzchar(gy)) DEFAULT_GAME_YEAR else gy
  })
  
  # ======== On/Off tab ===================================
  
  # Date range UI reacts to selected season
  output$date_filter_ui <- renderUI({
    gy     <- selected_game_year()
    bounds <- season_date_bounds(gy)
    
    dateRangeInput(
      "date_range", "Game Date Range",
      start = bounds$start, end   = bounds$end,
      min   = bounds$start, max   = bounds$end,
      format = "yyyy-mm-dd", weekstart = 0
    )
  })
  
  # Year-aware teams: only teams that appear in games for the selected game_year
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
                   choices = teams$team_name,
                   multiple = TRUE,
                   options  = list(placeholder = "All teams"))
  })
  
  debounced_range <- reactive(input$date_range) %>% debounce(300)
  debounced_teams <- reactive(input$teams)       %>% debounce(300)
  
  fallback_needed <- reactive({
    rng <- debounced_range()
    if (is.null(rng)) return(TRUE)
    
    start_d <- as.Date(rng[1])
    end_d   <- as.Date(rng[2])
    
    gy            <- selected_game_year()
    season_bounds <- season_date_bounds(gy)
    
    team_changed <- !is.null(debounced_teams()) && length(debounced_teams()) > 0
    elig_changed <- (isTruthy(input$min_all_poss) && input$min_all_poss != DEFAULT_MIN_ALL) ||
      (isTruthy(input$min_on_poss)  && input$min_on_poss  != DEFAULT_MIN_ON)
    date_changed <- (start_d != season_bounds$start) || (end_d != season_bounds$end)
    
    team_changed || elig_changed || date_changed
  })
  
  selected_team_ids <- reactive({
    teams <- teams_for_year()
    teams_in <- debounced_teams()
    if (is.null(teams_in) || !length(teams_in)) return(NULL)
    teams %>% filter(team_name %in% teams_in) %>% pull(team_id)
  })
  
  # DB function must accept game_year as 7th parameter
  run_onoff_compute <- function(pool, start_d, end_d, team_ids, min_all, min_on, min_net, game_year) {
    team_csv <- if (is.null(team_ids) || !length(team_ids)) NA_character_ else paste(team_ids, collapse = ",")
    DBI::dbGetQuery(
      pool,
      "SELECT * FROM basketball_test.onoff_compute($1,$2,$3,$4,$5,$6,$7)",
      params = list(
        as.Date(start_d),
        as.Date(end_d),
        team_csv,
        as.integer(min_all),
        as.integer(min_on),
        as.numeric(min_net),
        as.character(game_year)
      )
    )
  }
  
  live_result_df <- reactive({
    req(input$min_all_poss, input$min_on_poss)
    rng  <- debounced_range(); req(rng)
    tids <- selected_team_ids()
    gy   <- selected_game_year()
    
    run_onoff_compute(
      pg_pool,
      start_d   = as.Date(rng[1]),
      end_d     = as.Date(rng[2]),
      team_ids  = tids,
      min_all   = input$min_all_poss,
      min_on    = input$min_on_poss,
      min_net   = DEFAULT_MIN_NET,   # no UI filter, keep DB signature
      game_year = gy
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
    debounced_range(), debounced_teams(),
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
  }) %>% bindEvent(debounced_range(), debounced_teams(),
                   input$min_all_poss, input$min_on_poss,
                   input$game_year)
  
  observeEvent(input$reset_defaults, {
    gy     <- selected_game_year()
    bounds <- season_date_bounds(gy)
    
    updateDateRangeInput(session, "date_range",
                         start = bounds$start, end = bounds$end)
    updateSelectizeInput(session, "teams", selected = character(0))
    updateSliderInput(session, "min_all_poss", value = DEFAULT_MIN_ALL)
    updateSliderInput(session, "min_on_poss",  value = DEFAULT_MIN_ON)
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
  
  # Storage for team & player name maps (used to present names in UI table)
  ld_ref <- reactiveValues(teams=NULL, players=NULL)
  
  # init teams & name maps when tab opens OR season changes
  observeEvent(list(input$main_tabs, input$game_year), ignoreInit = TRUE, {
    if (!identical(input$main_tabs, "lineup_data")) return(NULL)
    
    gy_int <- as.integer(selected_game_year())
    
    # teams for this season
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
    
    # add an explicit empty option at the top
    team_values <- c("", as.character(teams_ld$team_id))
    names(team_values) <- c("— All teams —", teams_ld$team_name)
    
    updateSelectizeInput(session, "ld_team",
                         choices  = team_values,
                         selected = "", server = TRUE)
    
    # full player name map (team_id + player_id -> name), season-aware
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
    
    # clear players on init/season change
    updateSelectizeInput(
      session, "ld_players_on",
      choices  = setNames(integer(0), character(0)),
      selected = character(0), server = TRUE
    )
    updateSelectizeInput(
      session, "ld_players_off",
      choices  = setNames(integer(0), character(0)),
      selected = character(0), server = TRUE
    )
  })
  
  # update Players ON/OFF lists when a specific team is chosen; clear if "All teams"
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
  
  # Optional: prevent overlaps (if a player is ON, drop from OFF and vice versa)
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
  
  # RESET button (Lineup Data) – restore defaults
  observeEvent(input$ld_reset, {
    updateRadioButtons(session, "ld_num", selected = LD_DEFAULT_NUM)
    updateDateRangeInput(session, "ld_dates", start = NA, end = NA)
    
    # keep the same choices but select the empty option
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
  })
  
  # Build params continuously (no Apply button)
  ld_params <- reactive({
    req(identical(input$main_tabs, "lineup_data"))
    
    # team: empty string => no filter (NULL)
    team_id <- if (!is.null(input$ld_team) && !is.na(input$ld_team) && nzchar(input$ld_team)) {
      as.integer(input$ld_team)
    } else {
      NA_integer_
    }
    # players: only valid when a concrete team is chosen
    player_on_ids  <- if (!is.na(team_id)) as.integer(input$ld_players_on)  else integer(0)
    player_off_ids <- if (!is.na(team_id)) as.integer(input$ld_players_off) else integer(0)
    
    list(
      num            = as.integer(input$ld_num),
      team_csv       = if (!is.na(team_id)) as.character(team_id) else NA_character_,
      player_csv     = if (length(player_on_ids))  paste(player_on_ids,  collapse = ",") else NA_character_,
      player_off_csv = if (length(player_off_ids)) paste(player_off_ids, collapse = ",") else NA_character_,
      exact      = TRUE,   # hybrid semantics implemented in SQL (set-equality when full set)
      start_date = if (!is.null(input$ld_dates[1]) && !is.na(input$ld_dates[1])) as.Date(input$ld_dates[1]) else NA,
      end_date   = if (!is.null(input$ld_dates[2]) && !is.na(input$ld_dates[2])) as.Date(input$ld_dates[2]) else NA,
      min_poss   = as.integer(input$ld_minposs)
    )
  }) %>% bindEvent(input$ld_num, input$ld_team, input$ld_players_on, input$ld_players_off,
                   input$ld_dates, input$ld_minposs, input$main_tabs)
  
  # Query DB (9-arg CSV wrapper; season-aware)
  # Requires SQL function: basketball_test.fetch_lineups_csv_v2(..., p_game_year)
  ld_data <- reactive({
    req(ld_params())
    p  <- ld_params()
    gy <- as.integer(selected_game_year())
    
    df <- DBI::dbGetQuery(
      pg_pool,
      "SELECT * FROM basketball_test.fetch_lineups_csv_v2($1,$2,$3,$4,$5,$6,$7,$8,$9)",
      params = list(
        p$num, p$team_csv, p$player_csv, p$player_off_csv,
        p$exact, as.Date(p$start_date), as.Date(p$end_date), p$min_poss,
        gy
      )
    )
    if (is.null(df) || NROW(df) == 0L) return(df[0, , drop = FALSE])
    
    # Total possessions (sum)
    df$total_poss <- dplyr::coalesce(df$off_poss, 0L) + dplyr::coalesce(df$def_poss, 0L)
    
    # NEW: Plus/Minus = Off Pts - Def Pts
    df$plus_minus <- dplyr::coalesce(df$off_pts, 0) - dplyr::coalesce(df$def_pts, 0)
    
    # "Initial" means: no team/players/dates, min poss is default (20). Group size can vary.
    no_team  <- is.null(input$ld_team) || is.na(input$ld_team) || !nzchar(input$ld_team)
    no_p_on  <- is.null(input$ld_players_on)  || length(input$ld_players_on)  == 0
    no_p_off <- is.null(input$ld_players_off) || length(input$ld_players_off) == 0
    no_dates <- is.null(input$ld_dates) || all(is.na(input$ld_dates))
    is_def_mp <- isTRUE(all.equal(as.integer(input$ld_minposs), LD_DEFAULT_MIN_POSS))  # 20
    
    is_initial <- no_team && no_p_on && no_p_off && no_dates && is_def_mp
    
    if (is_initial) {
      # Pick the set by total_poss (desc), keep only top 50; table still orders by Net RTG.
      ord <- order(-df$total_poss)
      take <- min(50L, NROW(df))
      df <- df[ord[seq_len(take)], , drop = FALSE]
    }
    
    df
  })
  
  # Helper: map team_id -> team_name (vector)
  team_name_vec <- reactive({
    tdf <- isolate(ld_ref$teams)
    if (is.null(tdf)) return(character(0))
    setNames(tdf$team_name, as.character(tdf$team_id))
  })
  
  # Render DT for Lineup Data with requested column order + percentile coloring
  output$ld_table <- DT::renderDataTable({
    req(ld_params())
    df <- ld_data()
    
    # Resolve team names
    tmap <- team_name_vec()
    if ("team_id" %in% names(df)) {
      df$Team <- unname(tmap[as.character(df$team_id)])
    }
    
    # Player names (already a single string)
    if ("player_names_str" %in% names(df)) {
      df$Players <- df$player_names_str
    }
    
    # Requested display order (added plus_minus)
    show_cols <- c(
      "Team","Players","total_poss",
      "off_ppp","def_ppp","net_rtg","plus_minus",
      "off_poss","off_pts","def_poss","def_pts","num_lineup",
      "sub_lineup_hash"
    )
    
    # ---- Percentile helpers (like On/Off, but computed client-side) ----
    pr_safe <- function(x, invert = FALSE) {
      n <- sum(!is.na(x))
      if (n <= 1) return(rep(NA_real_, length(x)))
      r <- rank(x, na.last = "keep", ties.method = "average")
      p <- (r - 1) / (n - 1)
      if (invert) p <- 1 - p
      as.numeric(p)
    }
    
    # Compute percentiles on the full df
    df$pr_ld_net       <- pr_safe(df$net_rtg,  invert = FALSE)  # higher better
    df$pr_ld_off_ppp   <- pr_safe(df$off_ppp,  invert = FALSE)  # higher better
    df$pr_ld_def_ppp_i <- pr_safe(df$def_ppp,  invert = TRUE)   # lower better -> invert
    
    # Keep display + hidden percentile columns
    pr_cols <- c("pr_ld_net","pr_ld_off_ppp","pr_ld_def_ppp_i")
    keep    <- intersect(show_cols, names(df))
    df      <- df[, unique(c(keep, pr_cols[pr_cols %in% names(df)])), drop = FALSE]
    
    # Friendly headers (added plus_minus)
    pretty_labels <- c(
      Team            = "Team",
      Players         = "Players",
      num_lineup      = "Size",
      total_poss      = "Total Poss",
      net_rtg         = "Net RTG",
      plus_minus      = "+/- (Off-Def)",
      off_ppp         = "Off PPP",
      def_ppp         = "Def PPP",
      off_poss        = "Off Poss",
      off_pts         = "Off Pts",
      def_poss        = "Def Poss",
      def_pts         = "Def Pts",
      sub_lineup_hash = "Lineup ID"
    )
    col_labels <- unname(pretty_labels[colnames(df)[colnames(df) %in% names(pretty_labels)]])
    
    cuts      <- seq(0.05, 0.95, by = 0.05)
    cols_grad <- colorRampPalette(c("#d73027","#fee08b","#1a9850"))(20)
    
    hide_idx <- which(colnames(df) %in% pr_cols) - 1L  # 0-based for DT
    
    dt <- DT::datatable(
      df,
      colnames = col_labels,
      rownames = FALSE,
      filter   = "top",
      options  = list(
        pageLength  = 50,
        lengthMenu  = c(25, 50, 100, 200, 1000),
        order       = list(list(which(colnames(df)=="net_rtg")-1L, "desc")),
        deferRender = TRUE,
        scrollX     = TRUE,
        processing  = TRUE,
        columnDefs  = list(
          list(targets = hide_idx, visible = FALSE)
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
  
}

shinyApp(ui, server)
