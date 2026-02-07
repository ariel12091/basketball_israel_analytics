# ui_tab3_team.R - Tab 3: Team Ratings UI

ui_tab3_team <- tabPanel(
  title = "Team Ratings",
  value = "team_ratings",
  fluidPage(
    sidebarLayout(
      sidebarPanel(
        width = 3,
        div(
          class = "view-mode-container",
          radioButtons("tr_view_mode", label = "View:",
                       choices = c("Summary", "Four Factors"),
                       selected = "Summary", inline = TRUE)
        ),
        tags$hr(),
        tags$button(class = "btn btn-outline-secondary d-md-none w-100 mb-2",
                    `data-bs-toggle` = "collapse", `data-bs-target` = "#tr-filters",
                    "Show Filters"),
        div(
          id = "tr-filters", class = "collapse d-md-block",
          actionButton("tr_reset", "Reset Filters"),
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
            bslib::accordion_panel(
              "Clutch Time",
              checkboxInput("tr_clutch_enabled", "Enable clutch filter", value = FALSE),
              conditionalPanel(
                condition = "input.tr_clutch_enabled == true",
                sliderInput("tr_clutch_margin", "Max point margin", min = 0, max = 10, value = 5, step = 1),
                selectInput("tr_clutch_status", "Score status", choices = c("All" = "all", "Leading" = "leading", "Trailing" = "trailing", "Tied" = "tied"), selected = "all"),
                sliderInput("tr_clutch_minutes", "Max minutes remaining", min = 1, max = 5, value = 5, step = 1),
                checkboxInput("tr_clutch_ot_margin", "Exclude OT if margin exceeded", value = FALSE),
                helpText("By default, overtime always qualifies. Check above to apply margin filter to OT.")
              )
            ),
            open = FALSE
          )
        )
      ),
      mainPanel(width = 9, DTOutput("tr_table"))
    )
  )
)
