# ui_tab4_gamelogs.R - Tab 4: Game Logs UI

ui_tab4_gamelogs <- tabPanel(
  title = tags$span(tags$i(class = "bi bi-calendar-event"), "Game Logs"),
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
          selectizeInput("gl_team", "Team", choices = NULL, multiple = FALSE),
          dateRangeInput("gl_dates", "Date range", start = DEFAULT_START, end = DEFAULT_END),
          fluidRow(
            column(6, selectInput("gl_num_starters_off_mode", tt("Own lineup starters", "own_starters"), choices = c("ALL" = "", "At least (>=)" = "gte", "At most (<=)" = "lte"), selected = "")),
            column(6, selectInput("gl_num_starters_off", "Own value", choices = c("—" = "", as.character(0:5)), selected = ""))
          ),
          fluidRow(
            column(6, selectInput("gl_num_starters_def_mode", tt("Opponent lineup starters", "opp_starters"), choices = c("ALL" = "", "At least (>=)" = "gte", "At most (<=)" = "lte"), selected = "")),
            column(6, selectInput("gl_num_starters_def", "Opp value", choices = c("—" = "", as.character(0:5)), selected = ""))
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
              selectizeInput("gl_game_type", "Game type",
                             choices = c("All" = "", "Regular season" = "5",
                                         "Playoffs \u2013 Quarterfinals" = "16",
                                         "Playoffs \u2013 Finals" = "17",
                                         "Playoffs \u2013 Semifinals" = "26",
                                         "Play-in" = "33", "Winner Cup" = "34", "State Cup" = "35"),
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
                column(6, selectizeInput("gl_gn_min", tt("From Game Number (GN)", "gn"), choices = NULL, selected = "", multiple = FALSE,
                                         options = list(placeholder = "Any"))),
                column(6, selectizeInput("gl_gn_max", tt("To Game Number (GN)", "gn"), choices = NULL, selected = "", multiple = FALSE,
                                         options = list(placeholder = "Any")))
              ),
              selectizeInput("gl_last_n", tt("Last N Team Games", "last_n"), choices = NULL, selected = "", multiple = FALSE,
                             options = list(placeholder = "Any"))
            ),
            open = TRUE
          )
        )
      ),
      mainPanel(
        width = 9,
        conditionalPanel(
          condition = "input.gl_view_mode == 'Summary'",
          tab_explainer(
            id = "gamelogs_explainer_summary",
            title = "What This Tab Answers (Summary)",
            intro = "How did each game look in outcomes and pace-adjusted efficiency terms?",
            bullets = c(
              "Each row shows GN, Game Type, Date, Team, Opponent, W/L, Score, Off PPP, Def PPP, Net, Off Shot, Def Shot, Off Poss, and Def Poss.",
              "Off Shot and Def Shot cells show 2PT/3PT frequency and accuracy \u2014 use them to check whether efficiency is driven by sustainable shot selection or a hot-hand streak.",
              "Compare Off PPP and Def PPP across games to spot trends; the score alone can mislead when pace varies.",
              "Switch to Four Factors view if you need the cause-level breakdown (TS%, OREB%, TOV%, FTR)."
            )
          ),
          tags$a(
            href = "#",
            class = "explainer-toggle",
            onclick = "return false;",
            `data-bs-toggle` = "collapse",
            `data-bs-target` = "#gamelogs-example-box",
            "Show/Hide Example"
          ),
          div(
            id = "gamelogs-example-box",
            class = "collapse example-wrapper",
            div(
              class = "example-grid",
              div(
                class = "example-card",
                div(class = "example-card-title", "How to Read Game Logs (Real Example)"),
                tags$p(style = "margin-bottom: 6px;", "For GN 17, Bnei Herzliya vs Hapoel Galil Elion finished 105-89."),
                tags$p(style = "margin-bottom: 6px;", "In that game, Off PPP was 128.0 and Def PPP was 107.2, so Net was +20.8."),
                tags$p(style = "margin-bottom: 0;", "This single-game view helps you compare whether dominant results came from offense, defense, or both.")
              ),
              div(
                class = "example-snippet",
                tags$img(src = app_image_src("gamelogs-row-snippet.png"), alt = "Game logs summary table snippet"),
                div(class = "example-snippet-caption", "Real summary snippet (Game Logs)")
              )
            )
          )
        ),
        conditionalPanel(
          condition = "input.gl_view_mode == 'Four Factors'",
          tab_explainer(
            id = "gamelogs_explainer_ff",
            title = "What This Tab Answers (Four Factors)",
            intro = "Which four-factor components drove each single-game result? Each row shows one game with GN, date, teams, W/L, score, and the full PPP + TS% + OREB% + TOV% + FTR breakdown for both offense and defense.",
            bullets = c(
              "Scan TS%, OREB%, TOV%, and FTR together to diagnose why a game was won or lost.",
              "Compare offense and defense factor columns side by side \u2014 a win can mask poor defense if offense was exceptional.",
              "Look for patterns across games: consistent factor profiles suggest sustainability, while one-off spikes may be variance.",
              "Cross-reference with the Summary view's 2PT/3PT frequency and accuracy splits to check whether a high TS% game was driven by sustainable shot selection or a hot-hand streak."
            )
          ),
          tags$a(
            href = "#",
            class = "explainer-toggle",
            onclick = "return false;",
            `data-bs-toggle` = "collapse",
            `data-bs-target` = "#gamelogs-ff-example-box",
            "Show/Hide Example"
          ),
          div(
            id = "gamelogs-ff-example-box",
            class = "collapse example-wrapper",
            div(
              class = "example-grid",
              div(
                class = "example-card",
                div(class = "example-card-title", "How to Read Four Factors (Real Example)"),
                tags$p(style = "margin-bottom: 6px;", "GN 17 (Bnei Herzliya vs Hapoel Galil Elion, 105-89) shows strong offense: PPP 128.0 with TS% 63.3."),
                tags$p(style = "margin-bottom: 6px;", "Defense was solid too: PPP allowed 107.2 with opponent TS% 57.8 and FTR 50.8 (opponents got to the line frequently)."),
                tags$p(style = "margin-bottom: 0;", "The split across TS%, OREB%, TOV%, and FTR shows offense carried this win, despite allowing a high free-throw rate.")
              ),
              div(
                class = "example-snippet",
                tags$img(src = app_image_src("gamelogs-ff-row-snippet.png"), alt = "Game logs four factors table snippet"),
                div(class = "example-snippet-caption", "Real Four Factors snippet (Game Logs)")
              )
            )
          )
        ),
        conditionalPanel(
          condition = "input.gl_view_mode == 'Summary'",
          div(
            class = "legend-box",
            span(style = "font-weight:700; margin-right:10px;", "Shot Splits:"),
            div(class = "legend-item",
                div(style = "display:flex; flex-direction:column; align-items:center; gap:2px;",
                    span(style = "font-size:0.75em; color:#6e7681; text-transform:uppercase; letter-spacing:0.5px;", "Frequency"),
                    div(style = "display:flex; align-items:center; gap:8px;",
                        div(style = "width:14px; height:14px; background:#5b8abd; border-radius:3px;"),
                        span("2PT"),
                        div(style = "width:14px; height:14px; background:#d4843e; border-radius:3px; margin-left:6px;"),
                        span("3PT")
                    )
                )
            ),
            span(style = "margin:0 12px; color:#30363d;", "|"),
            div(class = "legend-item",
                div(style = "display:flex; flex-direction:column; align-items:center; gap:2px;",
                    span(style = "font-size:0.75em; color:#6e7681; text-transform:uppercase; letter-spacing:0.5px;", "Accuracy"),
                    div(style = "display:flex; align-items:center; gap:6px;",
                        span(style = "color:#f87171; font-weight:600;", "FG%"),
                        span(style = "color:#6e7681; margin:0 2px;", "\u2192"),
                        span(style = "color:#34d399; font-weight:600;", "FG%")
                    )
                )
            )
          )
        ),
        uiOutput("gl_filter_chips"),
        DTOutput("gl_table")
      )
    )
  )
)

