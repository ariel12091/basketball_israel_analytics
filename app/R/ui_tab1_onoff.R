# ui_tab1_onoff.R - Tab 1: On/Off Impact UI

ui_tab1_onoff <- function() {
  onoff_cfg <- onoff_tab_descriptor("israel")
  tabPanel(
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
                       choices = onoff_cfg$view_choices,
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
          onoff_starter_filters_ui(onoff_cfg$prefix),
          tags$hr(),
          accordion_toggle_link(),
          onoff_game_context_filters_ui(onoff_cfg),

          tags$hr(),
          helpText("If rows disappear, reduce the possession minimums above the table or widen the date range."),
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
              "Corner 3 Share splits threes into corner vs above-break, using shots with known court location; — means location unknown.",
              "Cells gray out below 50 team FGA on the ON side — small samples produce noisy shares."
            )
          )
        ),
        onoff_summary_legend_ui(onoff_cfg$view_id),
        onoff_rank_legend_ui(onoff_cfg$view_id),
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
                div(class = "legend-tick", style = "left:50%; height:12px; top:-2px; background:var(--ibpl-text-dim);"),
                div(class = "legend-tick", style = "right:0;")
              ),
              span("100% Rank")
            ),
            span(style = "margin-left: 15px; font-size: 0.8em; color: var(--ibpl-text-dim);",
                 "(Ranked: ≥ 50 team FGA with player on · eFG% + shares of team FGA, Δ = ON − OFF pp · Corner 3 Share uses known-location 3PA, — = unknown)")
          )
        ),
        filter_chips_row(
          "on_filter_chips",
          minposs_slider("min_all_poss", "Min Poss / side", "min_poss_side",
                         max = 2000, value = onoff_cfg$initial_min_all),
          minposs_slider("min_on_poss", "Min ON Poss", "min_on_poss",
                         max = 3000, value = onoff_cfg$initial_min_on),
          ff_ranges_toggle("onoff_view_mode")
        ),
        DTOutput("onoff_dt"),
        conditionalPanel(
          condition = "input.onoff_view_mode == 'Four Factors'",
          tags$div(class = "ff-impact-legend", ff_impact_legend())
        )
      )
    )
  )
  )
}

