# ui_tab5_traditional.R - Tab 5: Traditional Player Stats UI

ui_tab5_traditional <- tabPanel(
  title = "Player Stats",
  value = "traditional_stats",
  fluidPage(
    sidebarLayout(
      sidebarPanel(
        width = 3,
        tags$button(
          class = "btn btn-outline-secondary d-md-none w-100 mb-2",
          `data-bs-toggle` = "collapse",
          `data-bs-target` = "#ts-filters",
          "Show Filters"
        ),
        div(
          id = "ts-filters",
          class = "collapse d-md-block",
          actionButton("ts_reset", "Reset Filters"),
          tags$hr(),
          selectInput(
            "ts_game_year", "Season",
            choices = c("2025-26" = "2026", "2024-25" = "2025"),
            selected = DEFAULT_GAME_YEAR
          ),
          dateRangeInput("ts_dates", "Date range", start = NA, end = NA),
          selectizeInput("ts_teams", "Teams", choices = NULL, selected = character(0), multiple = TRUE,
                         options = list(placeholder = "All teams")),
          selectInput(
            "ts_display_mode",
            "Display mode",
            choices = c("Totals", "Per Game", "Per 60 Possessions", "Per 30 Minutes"),
            selected = "Totals"
          ),
          checkboxInput("ts_show_ineligible", "Show non-eligible players in rate modes", value = FALSE),
          tags$hr(),
          bslib::accordion(
            bslib::accordion_panel(
              "Game Filters",
              selectizeInput(
                "ts_game_type", "Game type",
                choices = c(
                  "All" = "",
                  "Regular season" = "5",
                  "Playoffs - Quarterfinals" = "16",
                  "Playoffs - Finals" = "17",
                  "Playoffs - Semifinals" = "26",
                  "Play-in" = "33",
                  "Winner Cup" = "34"
                ),
                selected = "", multiple = TRUE,
                options = list(placeholder = "All game types")
              ),
              selectizeInput("ts_opponents", "Opponents", choices = NULL, selected = character(0), multiple = TRUE,
                             options = list(placeholder = "All opponents")),
              selectInput("ts_home_away", "Home/Away", choices = c("All" = "", "Home" = "home", "Away" = "away"), selected = ""),
              selectInput("ts_outcome", "Outcome", choices = c("All" = "", "Win" = "win", "Loss" = "loss"), selected = ""),
              tags$hr(),
              fluidRow(
                column(
                  6,
                  selectizeInput("ts_gn_min", "From Game Number (GN)", choices = NULL, selected = "", multiple = FALSE,
                                 options = list(placeholder = "Any"))
                ),
                column(
                  6,
                  selectizeInput("ts_gn_max", "To Game Number (GN)", choices = NULL, selected = "", multiple = FALSE,
                                 options = list(placeholder = "Any"))
                )
              ),
              selectizeInput("ts_last_n", "Last N Team Games", choices = NULL, selected = "", multiple = FALSE,
                             options = list(placeholder = "Any"))
            ),
            bslib::accordion_panel(
              "Opponent Strength",
              selectInput("ts_opp_rank_side", "Top / Bottom", choices = c("Off" = "", "Top" = "top", "Bottom" = "bottom"), selected = ""),
              selectInput("ts_opp_rank_n", "Rank N", choices = c("-" = "", as.character(1:12)), selected = ""),
              selectInput("ts_opp_rank_metric", "Metric", choices = c("-" = "", "Offense" = "off", "Defense" = "def", "Net rating" = "net"), selected = "")
            ),
            bslib::accordion_panel(
              "Clutch Time",
              checkboxInput("ts_clutch_enabled", "Enable clutch filter", value = FALSE),
              conditionalPanel(
                condition = "input.ts_clutch_enabled == true",
                sliderInput("ts_clutch_margin", "Max point margin", min = 0, max = 10, value = 5, step = 1),
                selectInput("ts_clutch_status", "Score status", choices = c("All" = "all", "Leading" = "leading", "Trailing" = "trailing", "Tied" = "tied"), selected = "all"),
                sliderInput("ts_clutch_minutes", "Max minutes remaining", min = 1, max = 5, value = 5, step = 1),
                checkboxInput("ts_clutch_ot_margin", "Exclude OT if margin exceeded", value = FALSE),
                helpText("By default, overtime always qualifies. Check above to apply margin filter to OT.")
              )
            ),
            open = FALSE
          ),
          tags$hr(),
          downloadButton("ts_download_csv", "Download CSV")
        )
      ),
      mainPanel(
        width = 9,
        tab_explainer(
          id = "traditional_explainer",
          title = "What This Tab Answers",
          intro = "How do players produce in traditional box-score terms under your selected game filters?",
          bullets = c(
            "All counting stats are attributed from offense-side player event rows in this dataset mapping.",
            "Use minutes as context to compare high-volume production with role-sized output.",
            "Use eFG% and TS% to compare scoring efficiency alongside volume."
          )
        ),
        uiOutput("ts_mode_warning"),
        DTOutput("ts_table")
      )
    )
  )
)
