# ui_tab2_lineup.R - Tab 2: Lineup Data UI

ui_tab2_lineup <- tabPanel(
  title = "Lineup Data",
  value = "lineup_data",
  fluidPage(
    sidebarLayout(
      sidebarPanel(
        width = 3,
        div(
          class = "view-mode-container",
          radioButtons("ld_view_mode", label = "View:",
                       choices = c("Summary", "Four Factors"),
                       selected = "Summary", inline = TRUE)
        ),
        tags$hr(),
        tags$button(class = "btn btn-outline-secondary d-md-none w-100 mb-2",
                    `data-bs-toggle` = "collapse", `data-bs-target` = "#ld-filters",
                    "Show Filters"),
        div(
          id = "ld-filters", class = "collapse d-md-block",
          actionButton("ld_reset", "Reset Lineup Filters"),
          tags$hr(),
          sliderInput("ld_minposs", "Min possessions (sum of Off/Def)", min = 0, max = 2000, value = LD_DEFAULT_MIN_POSS, step = 10),
          radioButtons("ld_num", "Group size", choices = c("2", "3", "4", "5"), selected = LD_DEFAULT_NUM, inline = TRUE),
          tags$hr(),
          selectizeInput("ld_team", "Team", choices = NULL, multiple = FALSE),
          helpText("Pick a team to enable player filtering."),
          selectizeInput("ld_players_on", "Players On (exact/contains)", choices = NULL, multiple = TRUE, options = list(placeholder = "Select a team first…")),
          selectizeInput("ld_players_off", "Players Off (exclude any)", choices = NULL, multiple = TRUE, options = list(placeholder = "Select a team first…")),
          tags$hr(),
          selectInput("game_year_ld", "Season", choices = c("2025-26" = "2026", "2024-25" = "2025"), selected = DEFAULT_GAME_YEAR),
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
            bslib::accordion_panel(
              "Clutch Time",
              checkboxInput("ld_clutch_enabled", "Enable clutch filter", value = FALSE),
              conditionalPanel(
                condition = "input.ld_clutch_enabled == true",
                sliderInput("ld_clutch_margin", "Max point margin", min = 0, max = 10, value = 5, step = 1),
                selectInput("ld_clutch_status", "Score status", choices = c("All" = "all", "Leading" = "leading", "Trailing" = "trailing", "Tied" = "tied"), selected = "all"),
                sliderInput("ld_clutch_minutes", "Max minutes remaining", min = 1, max = 5, value = 5, step = 1),
                checkboxInput("ld_clutch_ot_margin", "Exclude OT if margin exceeded", value = FALSE),
                helpText("By default, overtime always qualifies. Check above to apply margin filter to OT.")
              )
            ),
            open = FALSE
          )
        )
      ),
      mainPanel(width = 9, DTOutput("ld_table"))
    )
  )
)
