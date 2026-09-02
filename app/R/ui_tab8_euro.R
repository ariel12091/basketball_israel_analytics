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

ui_tab8_euro <- function() {
  onoff_cfg <- onoff_tab_descriptor("euroleague")
  tabPanel(
  title = tags$span(tags$i(class = "bi bi-toggles"), "On/Off Impact"),
  value = "euro",
  fluidPage(
    shared_head_tags(),

    sidebarLayout(
      sidebarPanel(
        width = 3,
        div(
          class = "view-mode-container",
          radioButtons("euro_view_mode", label = "Select View:",
                       choices = onoff_cfg$view_choices,
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

          # Competition and season are section-wide and live in the navbar,
          # shared with every other EuroLeague tab.

          dateRangeInput("euro_date_range", "Game Date Range",
                         start = EURO_DEFAULT_START, end = EURO_DEFAULT_END,
                         min = EURO_DEFAULT_START, max = EURO_DEFAULT_END,
                         format = "yyyy-mm-dd"),
          selectizeInput("euro_teams", "Teams", choices = NULL, multiple = TRUE,
                         options = list(placeholder = "All teams")),
          onoff_starter_filters_ui(onoff_cfg$prefix),
          tags$hr(),
          accordion_toggle_link(),
          onoff_game_context_filters_ui(onoff_cfg),

          tags$hr(),
          helpText("If rows disappear, reduce the possession minimums above the table or widen the date range.")
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
        onoff_summary_legend_ui(onoff_cfg$view_id),
        onoff_rank_legend_ui(onoff_cfg$view_id),
        filter_chips_row(
          "euro_filter_chips",
          minposs_slider("euro_min_all_poss", "Min Poss / side", "min_poss_side",
                         max = 2000, value = onoff_cfg$initial_min_all),
          minposs_slider("euro_min_on_poss", "Min ON Poss", "min_on_poss",
                         max = 3000, value = onoff_cfg$initial_min_on)
        ),
        DTOutput("euro_dt")
      )
    )
  )
  )
}
