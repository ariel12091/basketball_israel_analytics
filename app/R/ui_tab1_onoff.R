# ui_tab1_onoff.R - Tab 1: On/Off Impact UI

ui_tab1_onoff <- tabPanel(
  title = "On/Off Impact",
  value = "onoff",
  fluidPage(
    shared_head_tags(),

    sidebarLayout(
      sidebarPanel(
        width = 3,
        div(
          class = "view-mode-container",
          radioButtons("onoff_view_mode", label = "Select View:",
                       choices = c("Summary", "Four Factors"),
                       selected = "Summary",
                       inline = TRUE)
        ),
        tags$hr(),
        tags$button(class = "btn btn-outline-secondary d-md-none w-100 mb-2",
                    `data-bs-toggle` = "collapse", `data-bs-target` = "#onoff-filters",
                    "Show Filters"),
        div(
          id = "onoff-filters", class = "collapse d-md-block",
          actionButton("reset_defaults", "Reset to defaults"),
          tags$hr(),

          selectInput(
            "game_year", "Season",
            choices = c("2025-26" = "2026", "2024-25" = "2025"),
            selected = DEFAULT_GAME_YEAR
          ),

          dateRangeInput("date_range", "Game Date Range",
                         start = as.Date("2025-10-01"), end = as.Date("2026-07-01"),
                         min = as.Date("2025-10-01"), max = as.Date("2026-07-01"),
                         format = "yyyy-mm-dd"),
          selectizeInput("teams", "Teams", choices = NULL, multiple = TRUE,
                         options = list(placeholder = "All teams")),
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
              selectInput("on_outcome", "Outcome", choices = c("All" = "", "Win" = "win", "Loss" = "loss"), selected = ""),
              tags$hr(),
              fluidRow(
                column(6, selectizeInput("on_gn_min", "From GN", choices = NULL, selected = "", multiple = FALSE,
                                         options = list(placeholder = "Any"))),
                column(6, selectizeInput("on_gn_max", "To GN", choices = NULL, selected = "", multiple = FALSE,
                                         options = list(placeholder = "Any")))
              ),
              selectizeInput("on_last_n", "Last N games", choices = NULL, selected = "", multiple = FALSE,
                             options = list(placeholder = "Any"))
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
        )
      ),

      mainPanel(
        width = 9,
        # --- LEGEND (Summary mode: shot split legend) ---
        conditionalPanel(
          condition = "input.onoff_view_mode == 'Summary'",
          div(
            class = "legend-box",
            span(style = "font-weight:700; margin-right:5px;", "Shot Splits:"),
            div(class = "legend-item",
                div(style = "width:14px; height:14px; background:#5b8abd; border-radius:3px;"),
                span("2PT")),
            div(class = "legend-item",
                div(style = "width:14px; height:14px; background:#d4843e; border-radius:3px;"),
                span("3PT")),
            span(style = "margin-left:15px; color:#555;", "|"),
            div(class = "legend-item",
                span(style = "color:#c84040; font-weight:600;", "FG%"),
                span(style = "color:#888; margin:0 3px;", "\u2192"),
                span(style = "color:#3a9a3a; font-weight:600;", "FG%")),
            span(style = "font-size:0.8em; color:#888;",
                 "(accuracy vs league avg)")
          )
        ),
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
)
