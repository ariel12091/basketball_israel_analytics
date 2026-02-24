# ui_tab1_onoff.R - Tab 1: On/Off Impact UI

ui_tab1_onoff <- tabPanel(
  title = tags$span(tags$i(class = "bi bi-toggles"), "On/Off Impact"),
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

          dateRangeInput("date_range", "Game Date Range",
                         start = as.Date("2025-10-01"), end = as.Date("2026-07-01"),
                         min = as.Date("2025-10-01"), max = as.Date("2026-07-01"),
                         format = "yyyy-mm-dd"),
          selectizeInput("teams", "Teams", choices = NULL, multiple = TRUE,
                         options = list(placeholder = "All teams")),
          fluidRow(
            column(6, selectInput("on_num_starters_off_mode", "Own lineup starters", choices = c("ALL" = "", "At least (>=)" = "gte", "At most (<=)" = "lte"), selected = "")),
            column(6, selectInput("on_num_starters_off", "Own value", choices = c("—" = "", as.character(0:5)), selected = ""))
          ),
          fluidRow(
            column(6, selectInput("on_num_starters_def_mode", "Opponent lineup starters", choices = c("ALL" = "", "At least (>=)" = "gte", "At most (<=)" = "lte"), selected = "")),
            column(6, selectInput("on_num_starters_def", "Opp value", choices = c("—" = "", as.character(0:5)), selected = ""))
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
              selectizeInput("on_game_type", "Game type",
                             choices = c("All" = "", "Regular season" = "5", "Playoffs – Quarterfinals" = "16",
                                         "Playoffs – Finals" = "17", "Playoffs – Semifinals" = "26",
                                         "Play-in" = "33", "Winner Cup" = "34", "State Cup" = "35"),
                             selected = "", multiple = TRUE, options = list(placeholder = "All game types")),
              selectizeInput("on_opponents", "Opponents", choices = NULL, selected = character(0), multiple = TRUE, options = list(placeholder = "All opponents")),
          selectInput("on_home_away", "Home/Away", choices = c("All" = "", "Home" = "home", "Away" = "away"), selected = ""),
          selectInput("on_outcome", "Outcome", choices = c("All" = "", "Win" = "win", "Loss" = "loss"), selected = ""),
          tags$hr(),
          fluidRow(
                column(6, selectizeInput("on_gn_min", "From Game Number (GN)", choices = NULL, selected = "", multiple = FALSE,
                                         options = list(placeholder = "Any"))),
                column(6, selectizeInput("on_gn_max", "To Game Number (GN)", choices = NULL, selected = "", multiple = FALSE,
                                         options = list(placeholder = "Any")))
              ),
              selectizeInput("on_last_n", "Last N Team Games", choices = NULL, selected = "", multiple = FALSE,
                             options = list(placeholder = "Any"))
            ),
            bslib::accordion_panel(
              "Opponent Strength",
              selectInput("on_opp_rank_side", "Top / Bottom", choices = c("Off" = "", "Top" = "top", "Bottom" = "bottom"), selected = ""),
              selectInput("on_opp_rank_n", "Rank N", choices = c("—" = "", as.character(1:12)), selected = ""),
              selectInput("on_opp_rank_metric", "Metric", choices = c("—" = "", "Offense" = "off", "Defense" = "def", "Net rating" = "net"), selected = "")
            ),
            open = TRUE
          ),

          tags$hr(),
          sliderInput("min_all_poss", "Min possessions per side (eligibility):", min = 0, max = 2000, value = DEFAULT_MIN_ALL, step = 10),
          sliderInput("min_on_poss", "Minimum ON possessions (for ranking):", min = 0, max = 3000, value = DEFAULT_MIN_ON, step = 10),
          helpText("If rows disappear, reduce possession minimums or widen the date range."),
          tags$hr(),
          downloadButton("download_csv", "Download CSV")
        )
      ),

      mainPanel(
        width = 9,
        conditionalPanel(
          condition = "input.onoff_view_mode == 'Summary'",
          tab_explainer(
            id = "onoff_explainer_summary",
            title = "What This Tab Answers (Summary)",
            intro = "Which players change team offense and defense most when they are on the floor?",
            bullets = c(
              "Start broad, then add filters only if needed.",
              "Use Net/Off/Def impact first, then compare on-court vs off-court PPP.",
              "Validate that usage (ON/OFF possessions) is large enough."
            )
          ),
          tags$a(
            href = "#",
            class = "explainer-toggle",
            onclick = "return false;",
            `data-bs-toggle` = "collapse",
            `data-bs-target` = "#onoff-example-box",
            "Show/Hide Example"
          ),
          div(
            id = "onoff-example-box",
            class = "collapse",
            div(
              class = "example-grid",
              div(
                class = "example-card",
                div(class = "example-card-title", "How to Read On/Off (Real Example)"),
                tags$p(
                  style = "margin-bottom: 6px;",
                  "In this example, Hapoel Tel Aviv is better with Tyler Ennis on the floor by +29.5 points per 100 possessions."
                ),
                tags$p(
                  style = "margin-bottom: 6px;",
                  "That comes from offense being +15.7 points better and defense being 13.8 points better (shown as Def Diff -13.8, because lower defensive PPP allowed is better)."
                ),
                tags$p(
                  style = "margin-bottom: 0;",
                  "With him on court, Hapoel scores 129.4 and allows 93.2. With him off court, they score 113.7 and allow 107.0."
                )
              ),
              div(
                class = "example-snippet",
                tags$img(src = app_image_src("onoff-row-snippet.png"), alt = "On/Off summary table snippet"),
                div(class = "example-snippet-caption", "Real summary snippet (On/Off Impact)")
              )
            )
          )
        ),
        conditionalPanel(
          condition = "input.onoff_view_mode == 'Four Factors'",
          tab_explainer(
            id = "onoff_explainer_ff",
            title = "What This Tab Answers (Four Factors)",
            intro = "Why is player impact happening: shooting efficiency, rebounding, turnovers, or free-throw pressure?",
            bullets = c(
              "Read Offense and Defense factor diffs separately.",
              "Use TS%, OREB%, TOV%, FTR diffs to explain the Net impact.",
              "Check ON/OFF possessions so factor diffs are not driven by tiny samples."
            )
          ),
          tags$a(
            href = "#",
            class = "explainer-toggle",
            onclick = "return false;",
            `data-bs-toggle` = "collapse",
            `data-bs-target` = "#onoff-ff-example-box",
            "Show/Hide Example"
          ),
          div(
            id = "onoff-ff-example-box",
            class = "collapse",
            div(
              class = "example-grid",
              div(
                class = "example-card",
                div(class = "example-card-title", "How to Read Four Factors (Real Example)"),
                tags$p(style = "margin-bottom: 6px;", "Darius Hannah (Kiryat Ata) shows Net Diff +23.5 with Off Diff +14.0 and Def Diff -9.5."),
                tags$p(style = "margin-bottom: 6px;", "Offense factors: TS% Diff +9.1 and OREB% Diff +2.4 support stronger on-court offense."),
                tags$p(style = "margin-bottom: 0;", "Usage is substantial (633 ON possessions vs 676 OFF possessions), so this is a stable directional signal.")
              ),
              div(
                class = "example-snippet",
                tags$img(src = app_image_src("onoff-ff-row-snippet.png"), alt = "On/Off four factors table snippet"),
                div(class = "example-snippet-caption", "Real Four Factors snippet (On/Off Impact)")
              )
            )
          )
        ),
        # --- LEGEND (Summary mode: shot split legend) ---
        conditionalPanel(
          condition = "input.onoff_view_mode == 'Summary'",
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
                div(class = "legend-tick", style = "left:50%; height:12px; top:-2px; background:#6e7681;"),
                div(class = "legend-tick", style = "right:0;")
              ),
              span("100% Rank")
            ),
            span(style="margin-left: 15px; font-size: 0.8em; color: #6e7681;", paste0("(Ranked Players: > ", RANKING_BASELINE, " poss)"))
          )
        ),
        uiOutput("on_filter_chips"),
        DTOutput("onoff_dt")
      )
    )
  )
)

