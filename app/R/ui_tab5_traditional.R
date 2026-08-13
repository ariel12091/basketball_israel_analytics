# ui_tab5_traditional.R - Tab 5: Traditional Player Stats UI

ui_tab5_traditional <- function() tabPanel(
  title = tags$span(tags$i(class = "bi bi-bar-chart-line"), "Player Stats"),
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
          dateRangeInput("ts_dates", "Date range", start = DEFAULT_START, end = DEFAULT_END),
          selectizeInput("ts_teams", "Teams", choices = NULL, selected = character(0), multiple = TRUE,
                         options = list(placeholder = "All teams")),
          selectizeInput("ts_players", "Players", choices = NULL, selected = character(0), multiple = TRUE,
                         options = list(placeholder = "All players")),
          div(style = "display: none;",
            selectInput(
              "ts_display_mode",
              "Display mode",
              choices = c("Totals", "Per Game", "Per 60 Possessions", "Per 30 Minutes"),
              selected = "Per Game"
            )
          ),
          fluidRow(
            column(
              7,
              sliderInput("ts_min_gp_slider", "Min GP", min = 1, max = 40, value = 1, step = 1)
            ),
            column(
              5,
              numericInput("ts_min_gp", "Min GP (type)", value = 1, min = 1, max = 40, step = 1)
            )
          ),
          checkboxInput("ts_show_ineligible", "Show non-eligible players in rate modes", value = FALSE),
          checkboxInput("ts_clutch_enabled", tt("Enable clutch filter", "clutch"), value = FALSE),
          conditionalPanel(
            condition = "input.ts_clutch_enabled == true",
            sliderInput("ts_clutch_margin", "Max point margin", min = 0, max = 10, value = 5, step = 1),
            selectInput("ts_clutch_status", "Score status", choices = c("All" = "all", "Leading" = "leading", "Trailing" = "trailing", "Tied" = "tied"), selected = "all"),
            sliderInput("ts_clutch_minutes", "Max minutes remaining", min = 1, max = 5, value = 5, step = 1),
            checkboxInput("ts_clutch_ot_margin", "Exclude OT if margin exceeded", value = FALSE),
            helpText("By default, overtime always qualifies. Check above to apply margin filter to OT.")
          ),
          tags$hr(),
          accordion_toggle_link(),
          uiOutput("ts_game_context_filters")
        )
      ),
      mainPanel(
        width = 9,
        tab_explainer(
          id = "traditional_explainer",
          title = "What This Tab Answers",
          intro = "How do players produce in traditional box-score terms under your selected game filters?",
          bullets = c(
            "Each row shows Team, Player, GP, Poss on Floor, MIN, scoring, rebounding, playmaking, defensive events, shooting splits, eFG%, TS%, and USG%. Non-total modes also show Total Poss from the totals-mode possession count.",
            "Use MIN, Poss on Floor, and Total Poss where shown as context \u2014 high counting stats on low minutes may signal efficiency, while low stats on high minutes may signal a role player.",
            "FG%, 2P%, 3P%, FT%, eFG%, TS%, and USG% let you compare scoring efficiency and role alongside volume \u2014 heat colors show where each stat ranks league-wide."
          )
        ),
        tags$a(
          href = "#traditional-example-box",
          class = "explainer-toggle",
          `data-bs-toggle` = "collapse",
          `data-bs-target` = "#traditional-example-box",
          "Show/Hide Example"
        ),
        div(
          id = "traditional-example-box",
          class = "collapse example-wrapper league-only-il",
          div(
            class = "example-grid",
            div(
              class = "example-card",
              div(class = "example-card-title", "How to Read Player Stats (Real Example)"),
              tags$p(style = "margin-bottom: 6px;", "Kyler Edwards (Hapoel Haemek) averages 19.4 PTS on 14.3 FGA with 43.4 FG% and 36.2 3P% across 19 games."),
              tags$p(style = "margin-bottom: 6px;", "Heat colors show where each stat ranks league-wide: green means above average, red means below."),
              tags$p(style = "margin-bottom: 0;", "Use Poss on Floor (58.8) and MIN (30.6) as context to judge whether production comes from high usage or efficiency.")
            ),
            div(
              class = "example-snippet",
              tags$img(src = app_image_src("player-stats-row-snippet.png"), alt = "Player traditional stats table snippet"),
              div(class = "example-snippet-caption", "Real Player Stats snippet")
            )
          )
        ),
        uiOutput("ts_filter_chips"),
        uiOutput("ts_mode_warning"),
        DTOutput("ts_table")
      )
    )
  )
)
