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
  title = "Player Analytics",
  theme = bslib::bs_theme(version = 5),
  header = tags$div(
    style = "position: fixed; right: 16px; top: 8px; font-size: 0.85rem; color: #666; z-index: 9999; display: flex; align-items: center;",
    actionButton("open_glossary", "Glossary", class = "btn btn-sm btn-outline-secondary nav-help-btn"),
    textOutput("last_updated", inline = TRUE)
  ),
  ui_tab1_onoff,
  ui_tab2_lineup,
  ui_tab3_team,
  ui_tab4_gamelogs,
  ui_tab5_traditional
)

# ---------------- Server ----------------
server <- function(input, output, session) {
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
          "SELECT DISTINCT team_id, team_name FROM basketball_test.full_rosters WHERE game_year = $1 ORDER BY team_name",
          params = list(gy_int)
        )
      }
    )
  })

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

  output$last_updated <- renderText({
    ts <- last_success_db()
    if (is.na(ts)) {
      p <- last_success_path()
      if (is.na(p)) return("Last updated: unavailable")
      lines <- tryCatch(readLines(p, warn = FALSE), error = function(e) character(0))
      ts <- if (length(lines)) trimws(lines[[1]]) else ""
    }
    if (!nzchar(ts)) "Last updated: unavailable" else paste("Last updated:", ts)
  })

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
}

shinyApp(ui, server)
