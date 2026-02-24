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
          dateRangeInput("tr_dates", "Date range", start = NA, end = NA),
          fluidRow(
            column(6, selectInput("tr_num_starters_off_mode", "Own lineup starters", choices = c("ALL" = "", "At least (>=)" = "gte", "At most (<=)" = "lte"), selected = "")),
            column(6, selectInput("tr_num_starters_off", "Own value", choices = c("—" = "", as.character(0:5)), selected = ""))
          ),
          fluidRow(
            column(6, selectInput("tr_num_starters_def_mode", "Opponent lineup starters", choices = c("ALL" = "", "At least (>=)" = "gte", "At most (<=)" = "lte"), selected = "")),
            column(6, selectInput("tr_num_starters_def", "Opp value", choices = c("—" = "", as.character(0:5)), selected = ""))
          ),
          tags$hr(),
          tags$div(
            class = "text-end mb-2",
            tags$a(
              href = "#",
              class = "small text-muted fw-bold",
              style = "text-decoration: none;",
              onclick = "var acc=this.parentElement.nextElementSibling; if(!acc) return false; var items=acc.querySelectorAll('.accordion-collapse'); var anyOpen=false; items.forEach(function(el){ if(el.classList.contains('show')) anyOpen=true; }); items.forEach(function(el){ if(anyOpen){ el.classList.remove('show'); } else { el.classList.add('show'); }}); return false;",
              "Collapse/Expand All"
            )
          ),
          bslib::accordion(
            bslib::accordion_panel(
              "Game Filters",
              selectizeInput("tr_game_type", "Game type", choices = c("All" = "", "Regular season" = "5", "Playoffs – Quarterfinals" = "16", "Playoffs – Finals" = "17", "Playoffs – Semifinals" = "26", "Play-in" = "33", "Winner Cup" = "34", "State Cup" = "35"), selected = "", multiple = TRUE, options = list(placeholder = "All game types")),
              selectizeInput("tr_opponents", "Opponents", choices = NULL, selected = character(0), multiple = TRUE, options = list(placeholder = "All opponents")),
              selectInput("tr_home_away", "Home/Away", choices = c("All" = "", "Home" = "home", "Away" = "away"), selected = ""),
              selectInput("tr_outcome", "Outcome", choices = c("All" = "", "Win" = "win", "Loss" = "loss"), selected = ""),
              tags$hr(),
              fluidRow(
                column(6, selectizeInput("tr_gn_min", "From Game Number (GN)", choices = NULL, selected = "", multiple = FALSE,
                                         options = list(placeholder = "Any"))),
                column(6, selectizeInput("tr_gn_max", "To Game Number (GN)", choices = NULL, selected = "", multiple = FALSE,
                                         options = list(placeholder = "Any")))
              ),
              selectizeInput("tr_last_n", "Last N Team Games", choices = NULL, selected = "", multiple = FALSE,
                             options = list(placeholder = "Any"))
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
            open = TRUE
          )
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
              "Start with Net RTG to rank teams quickly.",
              "Then inspect Off PPP and Def PPP to see where edges come from.",
              "Use games and W-L as context, not as replacement for possession-based rates."
            )
          ),
          tags$a(
            href = "#",
            class = "explainer-toggle",
            onclick = "return false;",
            `data-bs-toggle` = "collapse",
            `data-bs-target` = "#team-example-box",
            "Show/Hide Example"
          ),
          div(
            id = "team-example-box",
            class = "collapse",
            div(
              class = "example-grid",
              div(
                class = "example-card",
                div(class = "example-card-title", "How to Read Team Ratings (Real Example)"),
                tags$p(style = "margin-bottom: 6px;", "Maccabi Tel Aviv shows Off PPP 126.4 and Def PPP 107.2, which gives Net RTG +19.2."),
                tags$p(style = "margin-bottom: 6px;", "So over 100 possessions, they outperform opponents by 19.2 points."),
                tags$p(style = "margin-bottom: 0;", "The same row also shows context: 17 games and a 15-2 record.")
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
            intro = "Which team-level factors are driving strong or weak performance?",
            bullets = c(
              "Read offense and defense factor profiles side by side.",
              "Interpret TOV% with opposite polarity: lower is better on offense, higher can be better on defense.",
              "Use Poss columns to judge sample reliability."
            )
          ),
          tags$a(
            href = "#",
            class = "explainer-toggle",
            onclick = "return false;",
            `data-bs-toggle` = "collapse",
            `data-bs-target` = "#team-ff-example-box",
            "Show/Hide Example"
          ),
          div(
            id = "team-ff-example-box",
            class = "collapse",
            div(
              class = "example-grid",
              div(
                class = "example-card",
                div(class = "example-card-title", "How to Read Four Factors (Real Example)"),
                tags$p(style = "margin-bottom: 6px;", "Maccabi Tel Aviv combines elite offense (PPP 126.4, TS% 58.9, OREB% 37.9) with strong defense (PPP allowed 107.2)."),
                tags$p(style = "margin-bottom: 6px;", "Their factor profile supports Net +19.2: efficient scoring, offensive rebounding, and manageable turnover rates."),
                tags$p(style = "margin-bottom: 0;", "Possession volume is high (Off 1282, Def 1289), which strengthens confidence in this profile.")
              ),
              div(
                class = "example-snippet",
                tags$img(src = app_image_src("team-ff-row-snippet.png"), alt = "Team four factors table snippet"),
                div(class = "example-snippet-caption", "Real Four Factors snippet (Team Ratings)")
              )
            )
          )
        ),
        uiOutput("tr_filter_chips"),
        DTOutput('tr_table')
      )
    )
  )
)

