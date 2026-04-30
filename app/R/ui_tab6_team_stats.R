# ui_tab6_team_stats.R - Tab 6: Traditional Team Stats UI

ui_tab6_team_stats <- tabPanel(
  title = tags$span(tags$i(class = "bi bi-people-fill"), "Team Stats"),
  value = "team_stats",
  fluidPage(
    sidebarLayout(
      sidebarPanel(
        width = 3,
        tags$button(
          class = "btn btn-outline-secondary d-md-none w-100 mb-2",
          `data-bs-toggle` = "collapse",
          `data-bs-target` = "#tst-filters",
          "Show Filters"
        ),
        div(
          id = "tst-filters",
          class = "collapse d-md-block",
          actionButton("tst_reset", "Reset Filters"),
          tags$hr(),
          selectInput(
            "tst_display_mode",
            "Display mode",
            choices = c("Per Game", "Per 100 Possessions", "Per 40 Minutes"),
            selected = "Per Game"
          ),
          selectInput(
            "tst_rank_change_basis",
            "Rank change vs",
            choices = c("Last Week" = "week", "Last Match Day" = "match"),
            selected = "week"
          ),
          fluidRow(
            column(
              7,
              sliderInput("tst_min_gp_slider", "Min GP", min = 1, max = 40, value = 1, step = 1)
            ),
            column(
              5,
              numericInput("tst_min_gp", "Min GP (type)", value = 1, min = 1, max = 40, step = 1)
            )
          ),
          tags$hr(),
          dateRangeInput(
            "tst_dates", "Date range",
            start = season_date_bounds_for_year(DEFAULT_GAME_YEAR)$start,
            end = season_date_bounds_for_year(DEFAULT_GAME_YEAR)$end
          ),
          selectizeInput("tst_teams", "Teams", choices = NULL, selected = character(0), multiple = TRUE,
                         options = list(placeholder = "All teams")),
          checkboxInput("tst_clutch_enabled", tt("Clutch", "clutch"), value = FALSE),
          conditionalPanel(
            condition = "input.tst_clutch_enabled == true",
            sliderInput("tst_clutch_margin", "Max point margin", min = 0, max = 10, value = 5, step = 1),
            selectInput("tst_clutch_status", "Score status", choices = c("All" = "all", "Leading" = "leading", "Trailing" = "trailing", "Tied" = "tied"), selected = "all"),
            sliderInput("tst_clutch_minutes", "Max minutes remaining", min = 1, max = 5, value = 5, step = 1),
            checkboxInput("tst_clutch_ot_margin", "Exclude OT if margin exceeded", value = FALSE),
            helpText("By default, overtime always qualifies. Check above to apply margin filter to OT.")
          ),
          tags$hr(),
          tags$div(
            class = "text-end mb-2",
            tags$a(
              href = "#",
              class = "small text-muted fw-bold js-accordion-toggle-all",
              style = "text-decoration: none;",
              "Collapse/Expand All"
            )
          ),
          bslib::accordion(
            bslib::accordion_panel(
              "Game Filters",
              selectizeInput(
                "tst_game_type", "Game type",
                choices = c(
                  "All" = "",
                  "Regular season" = "5",
                  "Playoffs - Quarterfinals" = "16",
                  "Playoffs - Finals" = "17",
                  "Playoffs - Semifinals" = "26",
                  "Play-in" = "33",
                  "Winner Cup" = "34",
                  "State Cup" = "35"
                ),
                selected = "", multiple = TRUE,
                options = list(placeholder = "All game types")
              ),
              selectizeInput("tst_opponents", "Opponents", choices = NULL, selected = character(0), multiple = TRUE,
                             options = list(placeholder = "All opponents")),
              selectInput("tst_home_away", "Home/Away", choices = c("All" = "", "Home" = "home", "Away" = "away"), selected = ""),
              selectInput("tst_outcome", "Outcome", choices = c("All" = "", "Win" = "win", "Loss" = "loss"), selected = ""),
              tags$hr(),
              fluidRow(
                column(
                  6,
                  selectizeInput("tst_gn_min", tt("From Game Number (GN)", "gn"), choices = NULL, selected = "", multiple = FALSE,
                                 options = list(placeholder = "Any"))
                ),
                column(
                  6,
                  selectizeInput("tst_gn_max", tt("To Game Number (GN)", "gn"), choices = NULL, selected = "", multiple = FALSE,
                                 options = list(placeholder = "Any"))
                )
              ),
              selectizeInput("tst_last_n", tt("Last N Team Games", "last_n"), choices = NULL, selected = "", multiple = FALSE,
                             options = list(placeholder = "Any"))
            ),
            bslib::accordion_panel(
              tt("Opponent Strength", "opp_strength"), value = "Opponent Strength",
              selectInput("tst_opp_rank_side", "Top / Bottom", choices = c("Off" = "", "Top" = "top", "Bottom" = "bottom"), selected = ""),
              selectInput("tst_opp_rank_n", "Rank N", choices = c("-" = "", as.character(1:12)), selected = ""),
              selectInput("tst_opp_rank_metric", "Metric", choices = c("-" = "", "Offense" = "off", "Defense" = "def", "Net rating" = "net"), selected = "")
            ),
            open = TRUE
          ),
          tags$hr(),
          downloadButton("tst_download_csv", "Download CSV")
        )
      ),
      mainPanel(
        width = 9,
        tab_explainer(
          id = "team_stats_explainer",
          title = "What This Tab Answers",
          intro = "How are teams producing in traditional box-score terms under your selected game filters?",
          bullets = c(
            "Each row shows Team, GP, Poss on Floor, MIN, PTS, REB, OREB, DREB, AST, STL, BLK, TOV, FGM, FGA, FG%, 3PM, 3PA, 3P%, FTM, FTA, FT%, eFG%, and TS%.",
            "Cells show value, current league rank, and rank movement (\u25b2/\u25bc) compared to the selected baseline.",
            "Switch between Per Game, Per 60 Possessions, and Per 30 Minutes to compare teams fairly across tempo differences.",
            "Heat colors follow Team Ratings polarity \u2014 green is better, red is worse."
          )
        ),
        uiOutput("tst_filter_chips"),
        DTOutput("tst_table")
      )
    )
  )
)
