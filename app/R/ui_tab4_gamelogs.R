# ui_tab4_gamelogs.R - Tab 4: Game Logs UI

ui_tab4_gamelogs <- tabPanel(
  title = "Game Logs",
  value = "game_logs",
  fluidPage(
    sidebarLayout(
      sidebarPanel(
        width = 3,
        div(
          class = "view-mode-container",
          radioButtons("gl_view_mode", label = "View:",
                       choices = c("Summary", "Four Factors"),
                       selected = "Summary", inline = TRUE)
        ),
        tags$hr(),
        tags$button(class = "btn btn-outline-secondary d-md-none w-100 mb-2",
                    `data-bs-toggle` = "collapse", `data-bs-target` = "#gl-filters",
                    "Show Filters"),
        div(
          id = "gl-filters", class = "collapse d-md-block",
          actionButton("gl_reset", "Reset Filters"),
          tags$hr(),
          selectInput("gl_game_year", "Season",
                      choices = c("2025-26" = "2026", "2024-25" = "2025"),
                      selected = DEFAULT_GAME_YEAR),
          selectizeInput("gl_team", "Team", choices = NULL, multiple = FALSE),
          dateRangeInput("gl_dates", "Date range", start = NA, end = NA),
          tags$hr(),
          bslib::accordion(
            bslib::accordion_panel(
              "Game Filters",
              selectizeInput("gl_game_type", "Game type",
                             choices = c("All" = "", "Regular season" = "5",
                                         "Playoffs \u2013 Quarterfinals" = "16",
                                         "Playoffs \u2013 Finals" = "17",
                                         "Playoffs \u2013 Semifinals" = "26",
                                         "Play-in" = "33", "Winner Cup" = "34"),
                             selected = "", multiple = TRUE,
                             options = list(placeholder = "All game types")),
              selectizeInput("gl_opponents", "Opponents", choices = NULL,
                             selected = character(0), multiple = TRUE,
                             options = list(placeholder = "All opponents")),
              selectInput("gl_home_away", "Home/Away",
                          choices = c("All" = "", "Home" = "home", "Away" = "away"),
                          selected = ""),
              selectInput("gl_outcome", "Outcome",
                          choices = c("All" = "", "Win" = "win", "Loss" = "loss"),
                          selected = ""),
              tags$hr(),
              fluidRow(
                column(6, selectizeInput("gl_gn_min", "From GN", choices = NULL, selected = "", multiple = FALSE,
                                         options = list(placeholder = "Any"))),
                column(6, selectizeInput("gl_gn_max", "To GN", choices = NULL, selected = "", multiple = FALSE,
                                         options = list(placeholder = "Any")))
              ),
              selectizeInput("gl_last_n", "Last N games", choices = NULL, selected = "", multiple = FALSE,
                             options = list(placeholder = "Any"))
            ),
            open = FALSE
          )
        )
      ),
      mainPanel(
        width = 9,
        conditionalPanel(
          condition = "input.gl_view_mode == 'Summary'",
          div(
            class = "legend-box",
            span(style = "font-weight:700; margin-right:10px;", "Shot Splits:"),
            div(class = "legend-item",
                div(style = "display:flex; flex-direction:column; align-items:center; gap:2px;",
                    span(style = "font-size:0.75em; color:#888; text-transform:uppercase; letter-spacing:0.5px;", "Frequency"),
                    div(style = "display:flex; align-items:center; gap:8px;",
                        div(style = "width:14px; height:14px; background:#5b8abd; border-radius:3px;"),
                        span("2PT"),
                        div(style = "width:14px; height:14px; background:#d4843e; border-radius:3px; margin-left:6px;"),
                        span("3PT")
                    )
                )
            ),
            span(style = "margin:0 12px; color:#555;", "|"),
            div(class = "legend-item",
                div(style = "display:flex; flex-direction:column; align-items:center; gap:2px;",
                    span(style = "font-size:0.75em; color:#888; text-transform:uppercase; letter-spacing:0.5px;", "Accuracy"),
                    div(style = "display:flex; align-items:center; gap:6px;",
                        span(style = "color:#c84040; font-weight:600;", "FG%"),
                        span(style = "color:#888; margin:0 2px;", "\u2192"),
                        span(style = "color:#3a9a3a; font-weight:600;", "FG%")
                    )
                )
            )
          )
        ),
        DTOutput("gl_table")
      )
    )
  )
)
