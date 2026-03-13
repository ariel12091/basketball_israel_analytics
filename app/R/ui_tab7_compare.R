# ui_tab7_compare.R - Tab 7: Compare

ui_tab7_compare <- tabPanel(
  title = tags$span(tags$i(class = "bi bi-arrow-left-right"), "Compare"),
  value = "compare",
  fluidPage(
    shared_head_tags(),
    sidebarLayout(
      sidebarPanel(
        width = 3,

        # Mode tabs
        radioButtons("cmp_mode", label = NULL,
                     choices = c("Teams", "Lineups", "Players"),
                     selected = "Teams", inline = TRUE),

        # Preset dropdown
        selectInput("cmp_preset", "Quick preset",
                    choices = c("\u2014 Custom \u2014" = "",
                                "Starters vs Bench" = "starters_bench",
                                "Clutch vs Non-Clutch" = "clutch",
                                "Home vs Away" = "home_away",
                                "Win vs Loss" = "win_loss"),
                    selected = ""),

        tags$hr(),

        # ── Side A ──
        tags$div(
          class = "mb-2",
          tags$span(class = "badge rounded-pill",
                    style = "background: rgba(123,140,222,.2); color: #7b8cde; border: 1px solid rgba(123,140,222,.4);",
                    "Side A")
        ),

        # Player picker (Players mode only)
        conditionalPanel(
          condition = "input.cmp_mode == 'Players'",
          selectizeInput("cmp_player_a", "Player A", choices = NULL,
                         options = list(placeholder = "Search player..."))
        ),

        # Starters filter (Teams + Lineups only)
        conditionalPanel(
          condition = "input.cmp_mode != 'Players'",
          fluidRow(
            column(6, selectInput("cmp_a_starters_mode", "Starters",
                                  choices = c("ALL" = "", "At least (>=)" = "gte", "At most (<=)" = "lte"), selected = "")),
            column(6, selectInput("cmp_a_starters_val", "Value",
                                  choices = c("\u2014" = "", as.character(0:5)), selected = ""))
          )
        ),

        selectInput("cmp_a_home_away", "Home/Away",
                    choices = c("All" = "", "Home" = "home", "Away" = "away"), selected = ""),
        selectInput("cmp_a_outcome", "Win/Loss",
                    choices = c("All" = "", "Win" = "win", "Loss" = "loss"), selected = ""),
        checkboxInput("cmp_a_clutch", "Clutch", value = FALSE),
        conditionalPanel(
          condition = "input.cmp_a_clutch == true",
          sliderInput("cmp_a_clutch_margin", "Margin", min = 0, max = 10, value = 5, step = 1),
          sliderInput("cmp_a_clutch_minutes", "Minutes left", min = 1, max = 5, value = 5, step = 1)
        ),
        fluidRow(
          column(6, selectInput("cmp_a_cutoff_type", "Before/After",
                                choices = c("\u2014" = "", "Before GN" = "before_gn", "After GN" = "after_gn",
                                            "Before date" = "before_date", "After date" = "after_date"), selected = "")),
          column(6, uiOutput("cmp_a_cutoff_value_ui"))
        ),
        selectizeInput("cmp_a_opponents", "Opponents", choices = NULL, multiple = TRUE,
                       options = list(placeholder = "All opponents")),
        selectizeInput("cmp_a_game_type", "Game type",
                       choices = c("All" = "", "Regular season" = "5", "Playoffs \u2013 QF" = "16",
                                   "Playoffs \u2013 Finals" = "17", "Playoffs \u2013 SF" = "26",
                                   "Play-in" = "33", "Winner Cup" = "34", "State Cup" = "35"),
                       selected = "", multiple = TRUE, options = list(placeholder = "All")),
        fluidRow(
          column(6, selectInput("cmp_a_opp_rank_side", "Opp rank",
                                choices = c("Off" = "", "Top" = "top", "Bottom" = "bottom"), selected = "")),
          column(6, selectInput("cmp_a_opp_rank_n", "N",
                                choices = c("\u2014" = "", as.character(1:12)), selected = ""))
        ),

        # ── vs divider ──
        tags$div(class = "text-center text-muted fw-bold my-2", "\u2014 vs \u2014"),

        # ── Side B ──
        tags$div(
          class = "mb-2",
          tags$span(class = "badge rounded-pill",
                    style = "background: rgba(232,164,53,.15); color: #e8a435; border: 1px solid rgba(232,164,53,.35);",
                    "Side B")
        ),

        conditionalPanel(
          condition = "input.cmp_mode == 'Players'",
          selectizeInput("cmp_player_b", "Player B", choices = NULL,
                         options = list(placeholder = "Search player..."))
        ),

        conditionalPanel(
          condition = "input.cmp_mode != 'Players'",
          fluidRow(
            column(6, selectInput("cmp_b_starters_mode", "Starters",
                                  choices = c("ALL" = "", "At least (>=)" = "gte", "At most (<=)" = "lte"), selected = "")),
            column(6, selectInput("cmp_b_starters_val", "Value",
                                  choices = c("\u2014" = "", as.character(0:5)), selected = ""))
          )
        ),

        selectInput("cmp_b_home_away", "Home/Away",
                    choices = c("All" = "", "Home" = "home", "Away" = "away"), selected = ""),
        selectInput("cmp_b_outcome", "Win/Loss",
                    choices = c("All" = "", "Win" = "win", "Loss" = "loss"), selected = ""),
        checkboxInput("cmp_b_clutch", "Clutch", value = FALSE),
        conditionalPanel(
          condition = "input.cmp_b_clutch == true",
          sliderInput("cmp_b_clutch_margin", "Margin", min = 0, max = 10, value = 5, step = 1),
          sliderInput("cmp_b_clutch_minutes", "Minutes left", min = 1, max = 5, value = 5, step = 1)
        ),
        fluidRow(
          column(6, selectInput("cmp_b_cutoff_type", "Before/After",
                                choices = c("\u2014" = "", "Before GN" = "before_gn", "After GN" = "after_gn",
                                            "Before date" = "before_date", "After date" = "after_date"), selected = "")),
          column(6, uiOutput("cmp_b_cutoff_value_ui"))
        ),
        selectizeInput("cmp_b_opponents", "Opponents", choices = NULL, multiple = TRUE,
                       options = list(placeholder = "All opponents")),
        selectizeInput("cmp_b_game_type", "Game type",
                       choices = c("All" = "", "Regular season" = "5", "Playoffs \u2013 QF" = "16",
                                   "Playoffs \u2013 Finals" = "17", "Playoffs \u2013 SF" = "26",
                                   "Play-in" = "33", "Winner Cup" = "34", "State Cup" = "35"),
                       selected = "", multiple = TRUE, options = list(placeholder = "All")),
        fluidRow(
          column(6, selectInput("cmp_b_opp_rank_side", "Opp rank",
                                choices = c("Off" = "", "Top" = "top", "Bottom" = "bottom"), selected = "")),
          column(6, selectInput("cmp_b_opp_rank_n", "N",
                                choices = c("\u2014" = "", as.character(1:12)), selected = ""))
        ),

        tags$hr(),
        actionButton("cmp_go", "Compare \u2192",
                     class = "btn btn-warning w-100 fw-bold"),
        actionButton("cmp_reset", "Reset",
                     class = "btn btn-outline-secondary btn-sm w-100 mt-2")
      ),

      mainPanel(
        width = 9,

        # Metric chips
        div(
          class = "d-flex align-items-center gap-2 mb-3 flex-wrap",
          tags$span(class = "text-muted small text-uppercase", "Metric"),
          uiOutput("cmp_metric_chips_ui")
        ),

        # Rate mode (Players mode only)
        conditionalPanel(
          condition = "input.cmp_mode == 'Players'",
          div(
            class = "mb-3",
            radioButtons("cmp_rate_mode", NULL,
                         choices = c("Per Game", "Per 75 Possessions", "Totals"),
                         selected = "Per Game", inline = TRUE)
          )
        ),

        # Summary cards
        fluidRow(
          column(4, div(class = "card bg-dark border-secondary p-3 mb-3",
            tags$div(class = "small text-uppercase", style = "color: #7b8cde;", "Side A"),
            tags$div(class = "fs-4 fw-bold", style = "color: #4caf7d;", textOutput("cmp_summary_a", inline = TRUE)),
            tags$div(class = "small text-muted", textOutput("cmp_summary_a_label", inline = TRUE))
          )),
          column(4, div(class = "card bg-dark border-secondary p-3 mb-3",
            tags$div(class = "small text-uppercase", style = "color: #e8a435;", "Side B"),
            tags$div(class = "fs-4 fw-bold", style = "color: #e05c5c;", textOutput("cmp_summary_b", inline = TRUE)),
            tags$div(class = "small text-muted", textOutput("cmp_summary_b_label", inline = TRUE))
          )),
          column(4, div(class = "card bg-dark border-secondary p-3 mb-3",
            tags$div(class = "small text-uppercase text-muted", "Avg Gap"),
            tags$div(class = "fs-4 fw-bold", style = "color: #e8a435;", textOutput("cmp_summary_gap", inline = TRUE)),
            tags$div(class = "small text-muted", "league-wide")
          ))
        ),

        # Results table
        DT::dataTableOutput("cmp_table"),

        # Filter chips
        uiOutput("cmp_filter_chips")
      )
    )
  )
)
