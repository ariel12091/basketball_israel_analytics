# ui_tab8_euro.R - Tab 8: EuroLeague / EuroCup On/Off Impact UI
#
# Mirrors Tab 1's layout so the shared DT renderers and stat filters behave
# identically, with three deliberate differences:
#   * Its own competition + season selectors. The navbar season selector is the
#     Israeli season-ending year (2026 = 2025-26); EuroLeague uses the provider
#     season (2025 = 2025-26), so one control cannot serve both.
#   * "Game type" is provider phase text ('RS', ...), populated from the data.
#   * GN means ROUND number, not gamecode.
# Summary + Four Factors only; Shot Profile is deferred until shot coordinates
# are collected.

ui_tab8_euro <- function() tabPanel(
  title = tags$span(tags$i(class = "bi bi-globe2"), "EuroLeague"),
  value = "euro",
  fluidPage(
    shared_head_tags(),

    sidebarLayout(
      sidebarPanel(
        width = 3,
        div(
          class = "view-mode-container",
          radioButtons("euro_view_mode", label = "Select View:",
                       choices = c("Summary", "Four Factors"),
                       selected = "Summary",
                       inline = TRUE)
        ),
        tags$hr(),
        tags$button(class = "btn btn-outline-secondary d-md-none w-100 mb-2",
                    `data-bs-toggle` = "collapse", `data-bs-target` = "#euro-filters",
                    "Show Filters"),
        div(
          id = "euro-filters", class = "collapse d-md-block",
          actionButton("euro_reset_defaults", "Reset to defaults"),
          tags$hr(),

          fluidRow(
            column(6, selectInput("euro_competition", "Competition",
                                  choices = c("EuroLeague" = "E"),
                                  selected = EURO_DEFAULT_COMPETITION)),
            column(6, selectInput("euro_game_year", "Season",
                                  choices = stats::setNames(
                                    EURO_DEFAULT_SEASON,
                                    euro_season_label(EURO_DEFAULT_SEASON)
                                  ),
                                  selected = EURO_DEFAULT_SEASON))
          ),

          dateRangeInput("euro_date_range", "Game Date Range",
                         start = EURO_DEFAULT_START, end = EURO_DEFAULT_END,
                         min = EURO_DEFAULT_START, max = EURO_DEFAULT_END,
                         format = "yyyy-mm-dd"),
          selectizeInput("euro_teams", "Teams", choices = NULL, multiple = TRUE,
                         options = list(placeholder = "All teams")),
          fluidRow(
            column(6, selectInput("euro_num_starters_off_mode", tt("Own lineup starters", "own_starters"), choices = c("ALL" = "", "At least (>=)" = "gte", "At most (<=)" = "lte"), selected = "")),
            column(6, selectInput("euro_num_starters_off", "Own value", choices = c("—" = "", as.character(0:5)), selected = ""))
          ),
          fluidRow(
            column(6, selectInput("euro_num_starters_def_mode", tt("Opponent lineup starters", "opp_starters"), choices = c("ALL" = "", "At least (>=)" = "gte", "At most (<=)" = "lte"), selected = "")),
            column(6, selectInput("euro_num_starters_def", "Opp value", choices = c("—" = "", as.character(0:5)), selected = ""))
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
              selectizeInput("euro_phase", "Phase", choices = NULL,
                             selected = character(0), multiple = TRUE,
                             options = list(placeholder = "All phases")),
              selectizeInput("euro_opponents", "Opponents", choices = NULL, selected = character(0), multiple = TRUE, options = list(placeholder = "All opponents")),
              selectInput("euro_home_away", "Home/Away", choices = c("All" = "", "Home" = "home", "Away" = "away"), selected = ""),
              selectInput("euro_outcome", "Outcome", choices = c("All" = "", "Win" = "win", "Loss" = "loss"), selected = ""),
              tags$hr(),
              fluidRow(
                column(6, selectizeInput("euro_gn_min", "From Round", choices = NULL, selected = "", multiple = FALSE,
                                         options = list(placeholder = "Any"))),
                column(6, selectizeInput("euro_gn_max", "To Round", choices = NULL, selected = "", multiple = FALSE,
                                         options = list(placeholder = "Any")))
              ),
              selectizeInput("euro_last_n", tt("Last N Team Games", "last_n"), choices = NULL, selected = "", multiple = FALSE,
                             options = list(placeholder = "Any"))
            ),
            bslib::accordion_panel(
              tt("Opponent Strength", "opp_strength"), value = "Opponent Strength",
              selectInput("euro_opp_rank_side", "Top / Bottom", choices = c("Off" = "", "Top" = "top", "Bottom" = "bottom"), selected = ""),
              selectInput("euro_opp_rank_n", "Rank N", choices = c("—" = "", as.character(1:20)), selected = ""),
              selectInput("euro_opp_rank_metric", "Metric", choices = c("—" = "", "Offense" = "off", "Defense" = "def", "Net rating" = "net"), selected = "")
            ),
            open = TRUE
          ),

          tags$hr(),
          sliderInput("euro_min_all_poss", tt("Min possessions per side (eligibility):", "min_poss_side"), min = 0, max = 2000, value = 0, step = 10),
          sliderInput("euro_min_on_poss", tt("Minimum ON possessions (for ranking):", "min_on_poss"), min = 0, max = 3000, value = 0, step = 10),
          helpText("If rows disappear, reduce possession minimums or widen the date range.")
        )
      ),

      mainPanel(
        width = 9,
        conditionalPanel(
          condition = "input.euro_view_mode == 'Summary'",
          tab_explainer(
            id = "euro_explainer_summary",
            title = "What This Tab Answers (Summary)",
            intro = "Which EuroLeague players change their team's offense and defense most when they are on the floor?",
            bullets = c(
              "Net Impact shows the on-minus-off PPP gap; Off and Def break it into which end the player affects.",
              "On Court Stats (Off PPP, Def PPP, Net RTG) and Off Court Stats show the team's actual rates with and without the player.",
              "Off Shot and Def Shot cells show 2PT/3PT frequency and accuracy.",
              "ON Poss and OFF Poss (scrolled right) indicate sample size — small samples produce noisy diffs.",
              "Possessions and ratings come from an independent EuroLeague possession engine. Never compare these numbers against the Israeli-league tabs: different competitions, different derivations."
            )
          )
        ),
        conditionalPanel(
          condition = "input.euro_view_mode == 'Four Factors'",
          tab_explainer(
            id = "euro_explainer_ff",
            title = "What This Tab Answers (Four Factors)",
            intro = "Why is a player's on/off impact happening? Break the Net diff into shooting (eFG%), rebounding (OREB%), turnovers (TOV%), and free-throw pressure (FTR) on both ends.",
            bullets = c(
              "Each cell shows the ON minus OFF diff, with on-court and off-court values below and a percentile-rank slider.",
              "Read Offense Impact and Defense Impact blocks separately — a player can help on one end and hurt on the other.",
              "Gray (unranked) cells mean a small on-court possession sample; factor diffs from small samples are unreliable.",
              "TS% uses the same denominator as the Israeli tabs (FGA plus the last free throw of a shooting-foul trip), not the 0.44 x FTA estimate."
            )
          )
        ),
        # --- LEGEND (Summary mode: shot split legend) ---
        conditionalPanel(
          condition = "input.euro_view_mode == 'Summary'",
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
                        span(style = "color:#6e7681; margin:0 2px;", "→"),
                        span(style = "color:#34d399; font-weight:600;", "FG%")
                    )
                )
            )
          )
        ),
        # --- LEGEND (Only visible in Four Factors mode) ---
        conditionalPanel(
          condition = "input.euro_view_mode == 'Four Factors'",
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
            span(style = "margin-left: 15px; font-size: 0.8em; color: #6e7681;",
                 paste0("(Ranked Players: > ", RANKING_BASELINE, " poss)"))
          )
        ),
        uiOutput("euro_filter_chips"),
        DTOutput("euro_dt")
      )
    )
  )
)
