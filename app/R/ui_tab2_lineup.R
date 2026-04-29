# ui_tab2_lineup.R - Tab 2: Lineup Data UI

ui_tab2_lineup <- tabPanel(
  title = tags$span(tags$i(class = "bi bi-people-fill"), "Lineup Data"),
  value = "lineup_data",
  fluidPage(
    sidebarLayout(
      sidebarPanel(
        width = 3,
        div(
          class = "view-mode-container",
          radioButtons(
            "ld_view_mode",
            label = "View:",
            choices = c("Summary", "Four Factors"),
            selected = "Summary",
            inline = TRUE
          )
        ),
        tags$hr(),
        tags$button(
          class = "btn btn-outline-secondary d-md-none w-100 mb-2",
          `data-bs-toggle` = "collapse",
          `data-bs-target` = "#ld-filters",
          "Show Filters"
        ),
        div(
          id = "ld-filters",
          class = "collapse d-md-block",
          actionButton("ld_reset", "Reset Lineup Filters"),
          tags$hr(),
          sliderInput(
            "ld_minposs",
            tt("Minimum possessions (Off + Def)", "min_poss_lineup"),
            min = 0,
            max = 2000,
            value = LD_DEFAULT_MIN_POSS,
            step = 10
          ),
          helpText("Higher minimums improve stability but remove smaller-sample lineups."),
          radioButtons("ld_num", tt("Group size", "group_size"), choices = c("2", "3", "4", "5"), selected = LD_DEFAULT_NUM, inline = TRUE),
          tags$hr(),
          lineup_player_filter_ui(
            "ld_lineup_filter",
            team_label = "Team",
            team_help = "Pick a team to enable player filtering.",
            team_placeholder = "All teams",
            players_on_placeholder = "Select a team first...",
            players_off_placeholder = "Select a team first..."
          ),
          fluidRow(
            column(
              6,
              selectInput(
                "ld_num_starters_off_mode",
                tt("Own lineup starters", "own_starters"),
                choices = c("ALL" = "", "At least (>=)" = "gte", "At most (<=)" = "lte"),
                selected = ""
              )
            ),
            column(6, selectInput("ld_num_starters_off", "Own value", choices = c("\u2014" = "", as.character(0:5)), selected = ""))
          ),
          fluidRow(
            column(
              6,
              selectInput(
                "ld_num_starters_def_mode",
                tt("Opponent lineup starters", "opp_starters"),
                choices = c("ALL" = "", "At least (>=)" = "gte", "At most (<=)" = "lte"),
                selected = ""
              )
            ),
            column(6, selectInput("ld_num_starters_def", "Opp value", choices = c("\u2014" = "", as.character(0:5)), selected = ""))
          ),
          tags$hr(),
          dateRangeInput("ld_dates", "Date range", start = NA, end = NA),
          checkboxInput("ld_clutch_enabled", tt("Clutch", "clutch"), value = FALSE),
          conditionalPanel(
            condition = "input.ld_clutch_enabled == true",
            sliderInput("ld_clutch_margin", "Max point margin", min = 0, max = 10, value = 5, step = 1),
            selectInput(
              "ld_clutch_status",
              "Score status",
              choices = c("All" = "all", "Leading" = "leading", "Trailing" = "trailing", "Tied" = "tied"),
              selected = "all"
            ),
            sliderInput("ld_clutch_minutes", "Max minutes remaining", min = 1, max = 5, value = 5, step = 1),
            checkboxInput("ld_clutch_ot_margin", "Exclude OT if margin exceeded", value = FALSE),
            helpText("By default, overtime always qualifies. Check above to apply margin filter to OT.")
          ),
          tags$hr(),
          accordion_toggle_link(),
          game_context_filters_ui("ld")
        )
      ),
      mainPanel(
        width = 9,
        conditionalPanel(
          condition = "input.ld_view_mode == 'Summary'",
          tab_explainer(
            id = "lineup_explainer_summary",
            title = "What This Tab Answers (Summary)",
            intro = "Which player combinations perform best, and how reliable is the sample?",
            bullets = c(
              "Each row shows a lineup's MIN, Total Poss, Off PPP, Def PPP, Net RTG, and +/- (point differential).",
              "Off Shot and Def Shot cells show 2PT/3PT frequency and accuracy - use them to check whether efficiency is driven by sustainable shot selection or a hot-hand streak.",
              "Off Poss, Off Pts, Def Poss, Def Pts, and # Starters round out the row for sample-size and context.",
              "The TOTAL row at the top aggregates all lineups matching your filters - use it as a baseline for comparison."
            )
          ),
          tags$a(
            href = "#",
            class = "explainer-toggle",
            onclick = "return false;",
            `data-bs-toggle` = "collapse",
            `data-bs-target` = "#lineup-example-box",
            "Show/Hide Example"
          ),
          div(
            id = "lineup-example-box",
            class = "collapse example-wrapper",
            div(
              class = "example-grid",
              div(
                class = "example-card",
                div(class = "example-card-title", "How to Read Lineup Data (Real Example)"),
                tags$p(style = "margin-bottom: 6px;", "This Rishon Lezion lineup has Off PPP 109.6 and Def PPP 111.8, so Net RTG is -2.2."),
                tags$p(style = "margin-bottom: 6px;", "That means this five-man unit is outscored by 2.2 points per 100 possessions while on court."),
                tags$p(style = "margin-bottom: 0;", "Sample size here is meaningful: 153.8 minutes and 560 total possessions.")
              ),
              div(
                class = "example-snippet",
                tags$img(src = app_image_src("lineup-row-snippet.png"), alt = "Lineup summary table snippet"),
                div(class = "example-snippet-caption", "Real summary snippet (Lineup Data)")
              )
            )
          )
        ),
        conditionalPanel(
          condition = "input.ld_view_mode == 'Four Factors'",
          tab_explainer(
            id = "lineup_explainer_ff",
            title = "What This Tab Answers (Four Factors)",
            intro = "Which lineup-level factors drive good or bad results? See each lineup's PPP, eFG%, OREB%, TOV%, and FTR on offense and defense, plus minutes, possessions, and net rating.",
            bullets = c(
              "The TOTAL row at the top aggregates all lineups matching your filters - use it as a baseline.",
              "Compare eFG%, OREB%, TOV%, and FTR together; a single dominant factor often explains the PPP gap.",
              "Check MIN and POSS columns before concluding - small-sample lineups can show extreme rates.",
              "Cross-reference with the Summary view's 2PT/3PT frequency and accuracy splits to check whether a high eFG% is driven by sustainable shot selection or a hot-hand streak."
            )
          ),
          tags$a(
            href = "#",
            class = "explainer-toggle",
            onclick = "return false;",
            `data-bs-toggle` = "collapse",
            `data-bs-target` = "#lineup-ff-example-box",
            "Show/Hide Example"
          ),
          div(
            id = "lineup-ff-example-box",
            class = "collapse example-wrapper",
            div(
              class = "example-grid",
              div(
                class = "example-card",
                div(class = "example-card-title", "How to Read Four Factors (Real Example)"),
                tags$p(style = "margin-bottom: 6px;", "This Rishon Lezion lineup has Off PPP 109.6 vs Def PPP 111.8, giving Net -2.2."),
                tags$p(style = "margin-bottom: 6px;", "Offense profile: eFG% 50.5 and OREB% 33.8. Defense allows 111.8 PPP with opponents shooting 57.6 eFG%."),
                tags$p(style = "margin-bottom: 0;", "Workload is substantial (153.8 minutes, 560 total possessions), so factor-level interpretation is reliable.")
              ),
              div(
                class = "example-snippet",
                tags$img(src = app_image_src("lineup-ff-row-snippet.png"), alt = "Lineup four factors table snippet"),
                div(class = "example-snippet-caption", "Real Four Factors snippet (Lineup Data)")
              )
            )
          )
        ),
        conditionalPanel(
          condition = "input.ld_view_mode == 'Summary'",
          div(
            class = "legend-box",
            span(style = "font-weight:700; margin-right:10px;", "Shot Splits:"),
            div(
              class = "legend-item",
              div(
                style = "display:flex; flex-direction:column; align-items:center; gap:2px;",
                span(style = "font-size:0.75em; color:#6e7681; text-transform:uppercase; letter-spacing:0.5px;", "Frequency"),
                div(
                  style = "display:flex; align-items:center; gap:8px;",
                  div(style = "width:14px; height:14px; background:#5b8abd; border-radius:3px;"),
                  span("2PT"),
                  div(style = "width:14px; height:14px; background:#d4843e; border-radius:3px; margin-left:6px;"),
                  span("3PT")
                )
              )
            ),
            span(style = "margin:0 12px; color:#30363d;", "|"),
            div(
              class = "legend-item",
              div(
                style = "display:flex; flex-direction:column; align-items:center; gap:2px;",
                span(style = "font-size:0.75em; color:#6e7681; text-transform:uppercase; letter-spacing:0.5px;", "Accuracy"),
                div(
                  style = "display:flex; align-items:center; gap:6px;",
                  span(style = "color:#f87171; font-weight:600;", "FG%"),
                  span(style = "color:#6e7681; margin:0 2px;", "\u2192"),
                  span(style = "color:#34d399; font-weight:600;", "FG%")
                )
              )
            )
          )
        ),
        uiOutput("ld_filter_chips"),
        DTOutput("ld_table")
      )
    )
  )
)
