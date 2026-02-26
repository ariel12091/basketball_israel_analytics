# app.R - Main entry point
# Sources modular files and assembles the Shiny app

# Source all modules
source("R/global.R", local = TRUE)
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

# ---------------- UI ----------------
ui <- navbarPage(
  id = "main_tabs",
  title = tags$span(
    tags$i(class = "bi bi-activity", style = "margin-right: 6px;"),
    "IBPL Analytics"
  ),
  theme = bslib::bs_theme(
    version = 5,
    bg = "#0d1117",
    fg = "#e6edf3",
    primary = "#e8a435",
    secondary = "#21262d",
    success = "#34d399",
    danger = "#f87171",
    info = "#60a5fa",
    base_font = "DM Sans, Inter, -apple-system, sans-serif",
    code_font = "JetBrains Mono, monospace",
    "navbar-bg" = "#0d1117"
  ),
  header = tags$div(
    style = "position: fixed; right: 16px; top: 8px; font-size: 0.82rem; color: #8b949e; z-index: 9999; display: flex; align-items: center; gap: 8px;",
    tags$div(
      class = "navbar-season-select",
      selectInput("game_year", NULL,
                  choices = c("25-26" = "2026", "24-25" = "2025"),
                  selected = DEFAULT_GAME_YEAR)
    ),
    actionButton("open_glossary",
                 tags$span(tags$i(class = "bi bi-book"), " Glossary"),
                 class = "btn btn-sm btn-outline-secondary nav-help-btn"),
    tags$span(
      style = "display: inline-flex; align-items: center; gap: 4px;",
      tags$span(style = "width: 6px; height: 6px; background: #34d399; border-radius: 50%; display: inline-block;"),
      textOutput("last_updated", inline = TRUE)
    )
  ),
  ui_tab1_onoff,
  ui_tab2_lineup,
  ui_tab3_team,
  ui_tab4_gamelogs,
  ui_tab5_traditional
)

# ---------------- Server ----------------
server <- function(input, output, session) {
  startup_t0 <- proc.time()[["elapsed"]]
  init_session_request_guard(session)
  log_startup <- function(step) {
    elapsed <- proc.time()[["elapsed"]] - startup_t0
    message(sprintf("[startup] %s (%.3fs)", step, elapsed))
  }

  # ---- Shared helpers & reactives ----
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

  # ===== Teams dropdown choices =====
  teams_for_year_df <- reactive({
    gy_int <- as.integer(selected_game_year())
    req(gy_int)
    cached_ref_query(
      key = sprintf("teams_for_year_%d", gy_int),
      query_fun = function() {
        DBI::dbGetQuery(
          pg_pool,
          sprintf("SELECT DISTINCT team_id, team_name FROM basketball_test.full_rosters WHERE game_year = %d ORDER BY team_name", gy_int)
        )
      }
    )
  })

  prewarm_for_year <- function(gy_chr) {
    gy_int <- suppressWarnings(as.integer(gy_chr))
    if (!is.finite(gy_int) || is.na(gy_int)) return(invisible(NULL))

    # Warm teams cache used across tabs.
    invisible(cached_ref_query(
      key = sprintf("teams_for_year_%d", gy_int),
      query_fun = function() {
        DBI::dbGetQuery(
          pg_pool,
          sprintf("SELECT DISTINCT team_id, team_name FROM basketball_test.full_rosters WHERE game_year = %d ORDER BY team_name", gy_int)
        )
      }
    ))

    # Warm ON tab GN cache (primary first-view path).
    # Avoid running duplicate GN queries for every tab at startup.
    gn_query <- function() {
      DBI::dbGetQuery(
        pg_pool,
        sprintf("SELECT DISTINCT gn FROM basketball_test.final_schedule_mv WHERE game_year = %d ORDER BY gn", gy_int)
      )
    }
    invisible(cached_ref_query(key = sprintf("on_gn_%d", gy_int), query_fun = gn_query))
  }

  observeEvent(selected_game_year(), {
    td <- teams_for_year_df()
    updateSelectizeInput(session, "teams", choices = td$team_name, selected = character(0), server = TRUE)
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

  last_success_path <- function() {
    candidates <- c(
      file.path(getwd(), "etl", "logs", "last_success.txt"),
      file.path(getwd(), "..", "etl", "logs", "last_success.txt")
    )
    existing <- candidates[file.exists(candidates)]
    if (length(existing)) existing[[1]] else NA_character_
  }

  last_success_db <- function() {
    tryCatch({
      q <- DBI::dbGetQuery(
        pg_pool,
        "SELECT value FROM basketball_test.app_meta WHERE key = 'etl_full_last_success' LIMIT 1"
      )
      if (nrow(q) && nzchar(q$value[1])) q$value[1] else NA_character_
    }, error = function(e) NA_character_)
  }

  last_updated_cache <- reactiveVal(NA_character_)

  refresh_last_updated <- function() {
    ts <- last_success_db()
    if (is.na(ts)) {
      p <- last_success_path()
      if (is.na(p)) {
        last_updated_cache("Last updated: unavailable")
        return(invisible(NULL))
      }
      lines <- tryCatch(readLines(p, warn = FALSE), error = function(e) character(0))
      ts <- if (length(lines)) trimws(lines[[1]]) else ""
    }
    txt <- if (!nzchar(ts)) "Last updated: unavailable" else paste("Last updated:", ts)
    last_updated_cache(txt)
    invisible(NULL)
  }

  observe({
    invalidateLater(60000, session)
    refresh_last_updated()
  })

  output$last_updated <- renderText({
    last_updated_cache() %||% "Last updated: unavailable"
  })

  prewarm_enabled <- tolower(Sys.getenv("APP_PREWARM_ENABLED", "1")) %in% c("1", "true", "yes")
  if (isTRUE(prewarm_enabled)) {
    observeEvent(selected_game_year(), {
      prewarm_for_year(selected_game_year())
      log_startup(sprintf("prewarm complete for season %s", selected_game_year()))
    }, ignoreInit = FALSE)
  }

  observeEvent(input$open_glossary, {
    showModal(
      modalDialog(
        title = "Glossary",
        tags$ul(
          tags$li(tags$b("PPP"), ": Points per possession."),
          tags$li(tags$b("Net Rating"), ": Offensive PPP minus Defensive PPP."),
          tags$li(tags$b("TS%"), ": Shooting efficiency including free throws and threes."),
          tags$li(tags$b("OREB%"), ": Share of available offensive rebounds secured."),
          tags$li(tags$b("TOV%"), ": Turnovers per offensive possession."),
          tags$li(tags$b("FTR"), ": Free throw attempts relative to field goal attempts."),
          tags$li(tags$b("Possessions"), ": Estimated offensive/defensive trips."),
          tags$li(tags$b("Game Number (GN)"), ": League game sequence index."),
          tags$li(tags$b("Clutch"), ": Late-game possessions filtered by margin/time settings.")
        ),
        easyClose = TRUE,
        footer = modalButton("Close")
      )
    )
  }, ignoreInit = TRUE)

  # Create shared context for tab servers
  shared <- list(
    season_date_bounds = season_date_bounds,
    selected_game_year = selected_game_year,
    teams_for_year_df = teams_for_year_df,
    selected_opp_ids_on = selected_opp_ids_on,
    selected_opp_ids_ld = selected_opp_ids_ld
  )

  # Call tab server modules
  server_tab1(input, output, session, shared)
  server_tab2(input, output, session, shared)
  server_tab3(input, output, session, shared)
  server_tab4(input, output, session, shared)
  server_tab5_traditional(input, output, session, shared)
  log_startup("server modules initialized")
}

shinyApp(ui, server)
