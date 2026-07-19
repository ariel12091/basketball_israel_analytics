# ui_tab3_team.R - Tab 3: Team Ratings UI

ui_tab3_team <- tabPanel(
  title = tags$span(tags$i(class = "bi bi-trophy-fill"), "Team Ratings"),
  value = "team_ratings",
  fluidPage(
    sidebarLayout(
      sidebarPanel(
        width = 3,
        div(
          class = "view-mode-container",
          radioButtons(
            "tr_view_mode",
            label = "View:",
            choices = c("Summary", "Four Factors", "Shot Profile", "Traditional"),
            selected = "Summary",
            inline = TRUE
          )
        ),
        conditionalPanel(
          condition = "input.tr_view_mode == 'Traditional'",
          tags$div(
            class = "d-flex align-items-center gap-2 mb-2",
            tags$span("Team"),
            bslib::input_switch(
              "tr_trad_defense_mode",
              label = NULL,
              value = FALSE
            ),
            tags$span("Opponent")
          ),
          selectInput(
            "tr_trad_display_mode",
            "Traditional display mode",
            choices = c("Per Game", "Per 75 Possessions", "Per 40 Minutes"),
            selected = "Per Game"
          )
        ),
        tags$hr(),
        tags$button(
          class = "btn btn-outline-secondary d-md-none w-100 mb-2",
          `data-bs-toggle` = "collapse",
          `data-bs-target` = "#tr-filters",
          "Show Filters"
        ),
        div(
          id = "tr-filters",
          class = "collapse d-md-block",
          actionButton("tr_reset", "Reset Filters"),
          tags$hr(),
          dateRangeInput("tr_dates", "Date range", start = DEFAULT_START, end = DEFAULT_END),
          checkboxInput("tr_clutch_enabled", tt("Clutch", "clutch"), value = FALSE),
          conditionalPanel(
            condition = "input.tr_clutch_enabled == true",
            sliderInput("tr_clutch_margin", "Max point margin", min = 0, max = 10, value = 5, step = 1),
            selectInput(
              "tr_clutch_status",
              "Score status",
              choices = c("All" = "all", "Leading" = "leading", "Trailing" = "trailing", "Tied" = "tied"),
              selected = "all"
            ),
            sliderInput("tr_clutch_minutes", "Max minutes remaining", min = 1, max = 5, value = 5, step = 1),
            checkboxInput("tr_clutch_ot_margin", "Exclude OT if margin exceeded", value = FALSE),
            helpText("By default, overtime always qualifies. Check above to apply margin filter to OT.")
          ),
          fluidRow(
            column(
              6,
              selectInput(
                "tr_num_starters_off_mode",
                tt("Own lineup starters", "own_starters"),
                choices = c("ALL" = "", "At least (>=)" = "gte", "At most (<=)" = "lte"),
                selected = ""
              )
            ),
            column(6, selectInput("tr_num_starters_off", "Own value", choices = c("\u2014" = "", as.character(0:5)), selected = ""))
          ),
          fluidRow(
            column(
              6,
              selectInput(
                "tr_num_starters_def_mode",
                tt("Opponent lineup starters", "opp_starters"),
                choices = c("ALL" = "", "At least (>=)" = "gte", "At most (<=)" = "lte"),
                selected = ""
              )
            ),
            column(6, selectInput("tr_num_starters_def", "Opp value", choices = c("\u2014" = "", as.character(0:5)), selected = ""))
          ),
          accordion_toggle_link(),
          game_context_filters_ui("tr")
        )
      ),
      mainPanel(
        width = 9,
        conditionalPanel(
          condition = "input.tr_view_mode == 'Summary'",
          tab_explainer(
            id = "team_explainer_summary",
            title = "What This Tab Answers (Summary)",
            intro = "How strong are teams overall in offense, defense, and net rating?",
            bullets = c(
              "Each row shows Season, Team, GP, W, L, Off PPP, Def PPP, Net RTG (with league rank and rank delta), Off Pace, Def Pace, Off Poss, and Def Poss.",
              "Start with Net RTG to rank teams quickly, then inspect Off PPP and Def PPP to see which end drives the edge.",
              "Pace columns show possessions per 40 minutes - they help compare fast vs slow teams fairly.",
              "GP, W, and L provide context, but possession-based rates are more reliable than win-loss record for evaluating strength."
            )
          ),
          tags$a(
            href = "#team-example-box",
            class = "explainer-toggle",
            `data-bs-toggle` = "collapse",
            `data-bs-target` = "#team-example-box",
            "Show/Hide Example"
          ),
          div(
            id = "team-example-box",
            class = "collapse example-wrapper",
            div(
              class = "example-grid",
              div(
                class = "example-card",
                div(class = "example-card-title", "How to Read Team Ratings (Real Example)"),
                tags$p(style = "margin-bottom: 6px;", "Maccabi Tel Aviv shows Off PPP 128.3 and Def PPP 108.0, which gives Net RTG +20.4."),
                tags$p(style = "margin-bottom: 6px;", "So over 100 possessions, they outperform opponents by 20.4 points."),
                tags$p(style = "margin-bottom: 0;", "The same row also shows context: 21 games and a 19-2 record.")
              ),
              div(
                class = "example-snippet",
                tags$img(src = app_image_src("team-row-snippet.png"), alt = "Team summary table snippet"),
                div(class = "example-snippet-caption", "Real summary snippet (Team Ratings)")
              )
            )
          )
        ),
        conditionalPanel(
          condition = "input.tr_view_mode == 'Four Factors'",
          tab_explainer(
            id = "team_explainer_ff",
            title = "What This Tab Answers (Four Factors)",
            intro = "Which team-level factors are driving strong or weak performance? See each team's PPP, eFG%, OREB%, TOV%, FTR, and possessions on offense and defense, plus league rank, rank delta, and net rating.",
            bullets = c(
              "Read offense and defense factor profiles side by side - ranks (#1-#14) and rank deltas (\u25b2/\u25bc) help spot movement.",
              "Interpret TOV% with opposite polarity: lower is better on offense, higher is better on defense.",
              "Use Poss columns to judge sample reliability, especially after applying date or clutch filters.",
              "Cross-reference with the Summary view's 2PT/3PT frequency and accuracy splits to check whether a high eFG% is driven by sustainable shot selection or a hot-hand streak."
            )
          ),
          tags$a(
            href = "#team-ff-example-box",
            class = "explainer-toggle",
            `data-bs-toggle` = "collapse",
            `data-bs-target` = "#team-ff-example-box",
            "Show/Hide Example"
          ),
          div(
            id = "team-ff-example-box",
            class = "collapse example-wrapper",
            div(
              class = "example-grid",
              div(
                class = "example-card",
                div(class = "example-card-title", "How to Read Four Factors (Real Example)"),
                tags$p(style = "margin-bottom: 6px;", "Maccabi Tel Aviv combines elite offense (PPP 128.3 #1, eFG% 59.5 #2, OREB% 38.6 #1) with strong defense (PPP allowed 108.0 #3)."),
                tags$p(style = "margin-bottom: 6px;", "Their factor profile supports Net +20.4: efficient scoring, dominant offensive rebounding, and league-best turnover rate (14.4%)."),
                tags$p(style = "margin-bottom: 0;", "Possession volume is high (Off 1,592, Def 1,596), which strengthens confidence in this profile.")
              ),
              div(
                class = "example-snippet",
                tags$img(src = app_image_src("team-ff-row-snippet.png"), alt = "Team four factors table snippet"),
                div(class = "example-snippet-caption", "Real Four Factors snippet (Team Ratings)")
              )
            )
          )
        ),
        conditionalPanel(
          condition = "input.tr_view_mode == 'Shot Profile'",
          tab_explainer(
            id = "team_explainer_sp",
            title = "What This Tab Answers (Shot Profile)",
            intro = "What does each team's shot diet look like, on offense and defense? Shares of FGA by shot type: lay-up, dunk, rim (lay-up + dunk), 3PA, corner-3 share of 3PA, and mid-range.",
            bullets = c(
              "Shares are descriptive — they describe the mix, not its quality. #1 means most of that shot type, not best.",
              "Defense columns are the shot diet teams allow their opponents.",
              "C3% of 3PA uses shots with known court location; — means location unknown.",
              "The same date/clutch-free filters apply as in Summary; use Poss columns to judge sample size."
            )
          )
        ),
        conditionalPanel(
          condition = "input.tr_view_mode == 'Traditional'",
          tab_explainer(
            id = "team_explainer_traditional",
            title = "What This Tab Answers (Traditional)",
            intro = "How do teams produce in traditional box-score terms under your selected filters?",
            bullets = c(
              "Shows team-level counting, assist rate, and shooting stats using the same game/clutch filters.",
              "Ranks are season-contextual inside the filtered sample.",
              "Rank delta is vs previous matchday only, and shown only on baseline date/GN scope."
            )
          ),
          tags$a(
            href = "#team-traditional-example-box",
            class = "explainer-toggle",
            `data-bs-toggle` = "collapse",
            `data-bs-target` = "#team-traditional-example-box",
            "Show/Hide Example"
          ),
          div(
            id = "team-traditional-example-box",
            class = "collapse example-wrapper",
            div(
              class = "example-grid",
              div(
                class = "example-card",
                div(class = "example-card-title", "How to Read Traditional Stats (Real Example)"),
                tags$p(style = "margin-bottom: 6px;", "Maccabi Tel Aviv leads the league in PTS (41.6 #1), REB (27.1 #5), and AST (23.6 #2) per game."),
                tags$p(style = "margin-bottom: 6px;", "Each cell shows the value, league rank, and rank movement arrows compared to the previous matchday."),
                tags$p(style = "margin-bottom: 0;", "Heat colors follow the same polarity as Team Ratings: green is better, red is worse.")
              ),
              div(
                class = "example-snippet",
                tags$img(src = app_image_src("team-traditional-row-snippet.png"), alt = "Team traditional stats table snippet"),
                div(class = "example-snippet-caption", "Real Traditional snippet (Team Ratings)")
              )
            )
          )
        ),
        uiOutput("tr_filter_chips"),
        DTOutput("tr_table")
      )
    )
  )
)
