# ui_tab1_onoff.R - Tab 1: On/Off Impact UI

ui_tab1_onoff <- function() tabPanel(
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
                       choices = c("Summary", "Four Factors", "Shot Profile"),
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
                         start = DEFAULT_START, end = DEFAULT_END,
                         min = DEFAULT_START, max = DEFAULT_END,
                         format = "yyyy-mm-dd"),
          selectizeInput("teams", "Teams", choices = NULL, multiple = TRUE,
                         options = list(placeholder = "All teams")),
          fluidRow(
            column(6, selectInput("on_num_starters_off_mode", tt("Own lineup starters", "own_starters"), choices = c("ALL" = "", "At least (>=)" = "gte", "At most (<=)" = "lte"), selected = "")),
            column(6, selectInput("on_num_starters_off", "Own value", choices = c("—" = "", as.character(0:5)), selected = ""))
          ),
          fluidRow(
            column(6, selectInput("on_num_starters_def_mode", tt("Opponent lineup starters", "opp_starters"), choices = c("ALL" = "", "At least (>=)" = "gte", "At most (<=)" = "lte"), selected = "")),
            column(6, selectInput("on_num_starters_def", "Opp value", choices = c("—" = "", as.character(0:5)), selected = ""))
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
                column(6, selectizeInput("on_gn_min", tt("From Game Number (GN)", "gn"), choices = NULL, selected = "", multiple = FALSE,
                                         options = list(placeholder = "Any"))),
                column(6, selectizeInput("on_gn_max", tt("To Game Number (GN)", "gn"), choices = NULL, selected = "", multiple = FALSE,
                                         options = list(placeholder = "Any")))
              ),
              selectizeInput("on_last_n", tt("Last N Team Games", "last_n"), choices = NULL, selected = "", multiple = FALSE,
                             options = list(placeholder = "Any"))
            ),
            bslib::accordion_panel(
              tt("Opponent Strength", "opp_strength"), value = "Opponent Strength",
              selectInput("on_opp_rank_side", "Top / Bottom", choices = c("Off" = "", "Top" = "top", "Bottom" = "bottom"), selected = ""),
              selectInput("on_opp_rank_n", "Rank N", choices = c("—" = "", as.character(1:12)), selected = ""),
              selectInput("on_opp_rank_metric", "Metric", choices = c("—" = "", "Offense" = "off", "Defense" = "def", "Net rating" = "net"), selected = "")
            ),
            open = TRUE
          ),

          tags$hr(),
          sliderInput("min_all_poss", tt("Min possessions per side (eligibility):", "min_poss_side"), min = 0, max = 2000, value = DEFAULT_MIN_ALL, step = 10),
          sliderInput("min_on_poss", tt("Minimum ON possessions (for ranking):", "min_on_poss"), min = 0, max = 3000, value = DEFAULT_MIN_ON, step = 10),
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
              "Net Impact shows the on-minus-off PPP gap; Off and Def break it into which end the player affects.",
              "On Court Stats (Off PPP, Def PPP, Net RTG) and Off Court Stats show the team's actual rates with and without the player.",
              "Off Shot and Def Shot cells show 2PT/3PT frequency and accuracy \u2014 use them to check whether efficiency is driven by sustainable shot selection or a hot-hand streak.",
              "ON Poss and OFF Poss (scrolled right) indicate sample size \u2014 small samples produce noisy diffs."
            )
          ),
          tags$a(
            href = "#onoff-example-box",
            class = "explainer-toggle",
            `data-bs-toggle` = "collapse",
            `data-bs-target` = "#onoff-example-box",
            "Show/Hide Example"
          ),
          div(
            id = "onoff-example-box",
            class = "collapse example-wrapper",
            div(
              class = "example-grid",
              div(
                class = "example-card",
                div(class = "example-card-title", "How to Read On/Off (Real Example)"),
                tags$p(
                  style = "margin-bottom: 6px;",
                  "In this example, Maccabi Tel Aviv is better with Jimmy Clark III on the floor by +28.1 points per 100 possessions."
                ),
                tags$p(
                  style = "margin-bottom: 6px;",
                  "That comes from offense being +14.4 points better and defense being 13.7 points better (shown as Def -13.7, because lower defensive PPP allowed is better)."
                ),
                tags$p(
                  style = "margin-bottom: 0;",
                  "With him on court, Maccabi scores 135.5 and allows 101.1. With him off court, they score 121.1 and allow 114.8."
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
            intro = "Why is a player's on/off impact happening? Break down the Net diff into shooting (eFG%), rebounding (OREB%), turnovers (TOV%), and free-throw pressure (FTR) on both ends.",
            bullets = c(
              "Each cell shows the ON minus OFF diff, with on-court and off-court values below and a percentile-rank slider.",
              "Read Offense Impact and Defense Impact blocks separately \u2014 a player can help on one end and hurt on the other.",
              "Gray (unranked) cells mean fewer than 100 on-court possessions; factor diffs from small samples are unreliable.",
              "Cross-reference with the Summary view's 2PT/3PT frequency and accuracy splits to check whether a high eFG% is driven by sustainable shot selection or a hot-hand streak."
            )
          ),
          tags$a(
            href = "#onoff-ff-example-box",
            class = "explainer-toggle",
            `data-bs-toggle` = "collapse",
            `data-bs-target` = "#onoff-ff-example-box",
            "Show/Hide Example"
          ),
          div(
            id = "onoff-ff-example-box",
            class = "collapse example-wrapper",
            div(
              class = "example-grid",
              div(
                class = "example-card",
                div(class = "example-card-title", "How to Read Four Factors (Real Example)"),
                tags$p(style = "margin-bottom: 6px;", "Jimmy Clark III (Maccabi Tel Aviv) shows Diff +28.1 with Off Diff +14.4 and Def Diff -13.7."),
                tags$p(style = "margin-bottom: 6px;", "Offense factors: eFG% Diff +7.7 explains most of the offensive edge, while OREB% Diff -1.3 is slightly negative."),
                tags$p(style = "margin-bottom: 0;", "Defense factors: OREB% Diff +2.2 and TOV% Diff +5.8 show opponents rebound and turn the ball over more with Clark on court.")
              ),
              div(
                class = "example-snippet",
                tags$img(src = app_image_src("onoff-ff-row-snippet.png"), alt = "On/Off four factors table snippet"),
                div(class = "example-snippet-caption", "Real Four Factors snippet (On/Off Impact)")
              )
            )
          )
        ),
        conditionalPanel(
          condition = "input.onoff_view_mode == 'Shot Profile'",
          tab_explainer(
            id = "onoff_explainer_sp",
            title = "What This Tab Answers (Shot Profile)",
            intro = "How does the team's shot diet shift with the player on vs off the floor? Each cell shows the ON-minus-OFF change in share of team FGA, with ON | OFF values below. Each group leads with the team eFG% swing — the efficiency context the diet shares feed into.",
            bullets = c(
              "eFG% is the ON-minus-OFF change in team effective FG% (offense: higher is better; defense: lower is better). It is computed from the same shooting splits as the diet shares.",
              "Colors follow the league value hierarchy (interior and 3s beat 2PT jumpers): green = shift toward higher-value shots, red = away; the 2PT Jumper column flips, like TOV% in Four Factors. No point-impact estimate is attached — efficiency itself lives in the Summary and Four Factors views.",
              "Each cell mirrors Four Factors: the Δ headline, on/off percentile dots on the rank bar, and ON | OFF share values below.",
              "Columns follow the play-by-play shot-type tags: Lay+Dunk = attempts tagged lay-up or dunk (the tag describes execution, not court location — a running floater can be tagged lay-up); 2PT Jumper = attempts tagged 2-point jump-shot. 3PA% is share of all FGA.",
              "C3% of 3PA splits threes into corner vs above-break, using shots with known court location; — means location unknown.",
              "Cells gray out below 50 team FGA on the ON side — small samples produce noisy shares."
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
        conditionalPanel(
          condition = "input.onoff_view_mode == 'Shot Profile'",
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
                 "(Ranked: ≥ 50 team FGA with player on · eFG% + shares of team FGA, Δ = ON − OFF pp · C3% of known-location 3PA, — = unknown)")
          )
        ),
        uiOutput("on_filter_chips"),
        DTOutput("onoff_dt"),
        conditionalPanel(
          condition = "input.onoff_view_mode == 'Four Factors'",
          tags$div(class = "ff-impact-legend", ff_impact_legend())
        )
      )
    )
  )
)

