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

# ---------------- UI ----------------
ui <- navbarPage(
  id = "main_tabs",
  title = "Player Analytics",
  theme = bslib::bs_theme(version = 5),
  ui_tab1_onoff,
  ui_tab2_lineup,
  ui_tab3_team,
  ui_tab4_gamelogs
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
    DBI::dbGetQuery(pg_pool,
      "SELECT DISTINCT team_id, team_name FROM basketball_test.full_rosters WHERE game_year = $1 ORDER BY team_name",
      params = list(gy_int))
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
}

shinyApp(ui, server)
