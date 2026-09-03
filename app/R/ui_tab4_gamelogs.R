# ui_tab4_gamelogs.R - Tab 4: Game Logs UI

ui_tab4_gamelogs <- function() tabPanel(
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
          accordion_toggle_link(),
          game_context_filters_ui("gl", include_opp_rank = FALSE)
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
              "Switch to Four Factors view if you need the cause-level breakdown (eFG%, OREB%, TOV%, FTR)."
            )
          ),
          tags$a(
            href = "#gamelogs-example-box",
            class = "explainer-toggle",
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
            intro = "Which four-factor components drove each single-game result? Each row shows one game with GN, date, teams, W/L, score, and the full PPP + eFG% + OREB% + TOV% + FTR breakdown for both offense and defense.",
            bullets = c(
              "Scan eFG%, OREB%, TOV%, and FTR together to diagnose why a game was won or lost.",
              "Compare offense and defense factor columns side by side \u2014 a win can mask poor defense if offense was exceptional.",
              "Look for patterns across games: consistent factor profiles suggest sustainability, while one-off spikes may be variance.",
              "Cross-reference with the Summary view's 2PT/3PT frequency and accuracy splits to check whether a high eFG% game was driven by sustainable shot selection or a hot-hand streak."
            )
          ),
          tags$a(
            href = "#gamelogs-ff-example-box",
            class = "explainer-toggle",
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
                tags$p(style = "margin-bottom: 6px;", "GN 17 (Bnei Herzliya vs Hapoel Galil Elion, 105-89) shows strong offense: PPP 128.0 with eFG% 63.3."),
                tags$p(style = "margin-bottom: 6px;", "Defense was solid too: PPP allowed 107.2 with opponent eFG% 57.8 and FTR 50.8 (opponents got to the line frequently)."),
                tags$p(style = "margin-bottom: 0;", "The split across eFG%, OREB%, TOV%, and FTR shows offense carried this win, despite allowing a high free-throw rate.")
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
                    span(style = "font-size:0.75em; color:var(--ibpl-text-dim); text-transform:uppercase; letter-spacing:0.5px;", "Frequency"),
                    div(style = "display:flex; align-items:center; gap:8px;",
                        div(style = "width:14px; height:14px; background:var(--ibpl-fg2); border-radius:3px;"),
                        span("2PT"),
                        div(style = "width:14px; height:14px; background:var(--ibpl-fg3); border-radius:3px; margin-left:6px;"),
                        span("3PT")
                    )
                )
            ),
            span(style = "margin:0 12px; color:var(--ibpl-border);", "|"),
            div(class = "legend-item",
                div(style = "display:flex; flex-direction:column; align-items:center; gap:2px;",
                    span(style = "font-size:0.75em; color:var(--ibpl-text-dim); text-transform:uppercase; letter-spacing:0.5px;", "Accuracy"),
                    div(style = "display:flex; align-items:center; gap:6px;",
                        span(style = "color:var(--ibpl-neg); font-weight:600;", "FG%"),
                        span(style = "color:var(--ibpl-text-dim); margin:0 2px;", "\u2192"),
                        span(style = "color:var(--ibpl-pos); font-weight:600;", "FG%")
                    )
                )
            )
          )
        ),
        filter_chips_row("gl_filter_chips"),
        DTOutput("gl_table")
      )
    )
  )
)

