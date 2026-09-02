# ui_tab9_euro_team.R - Tab 9: EuroLeague / EuroCup Team Ratings
#
# Mirrors Israeli Tab 3's Summary and Four Factors views. Deliberately absent:
#   * Shot Profile     - needs shot coordinates/types, never collected.
#   * Traditional      - needs the box score promoted out of jsonb.
# Team minutes and pace reuse the canonical matchup-segment duration fact.

ui_tab9_euro_team <- function() tabPanel(
  title = tags$span(tags$i(class = "bi bi-trophy-fill"), "Team Ratings"),
  value = "euro_team",
  fluidPage(
    shared_head_tags(),

    sidebarLayout(
      sidebarPanel(
        width = 3,
        div(
          class = "view-mode-container",
          radioButtons("euroteam_view_mode", label = "Select View:",
                       choices = c("Summary", "Four Factors"),
                       selected = "Summary", inline = TRUE)
        ),
        tags$hr(),
        tags$button(class = "btn btn-outline-secondary d-md-none w-100 mb-2",
                    `data-bs-toggle` = "collapse", `data-bs-target` = "#euroteam-filters",
                    "Show Filters"),
        div(
          id = "euroteam-filters", class = "collapse d-md-block",
          actionButton("euroteam_reset", "Reset to defaults"),
          tags$hr(),

          # Competition and season are section-wide and live in the navbar,
          # shared with every other EuroLeague tab.

          dateRangeInput("euroteam_dates", "Game Date Range",
                         start = EURO_DEFAULT_START, end = EURO_DEFAULT_END,
                         min = EURO_DEFAULT_START, max = EURO_DEFAULT_END,
                         format = "yyyy-mm-dd"),
          selectizeInput("euroteam_teams", "Teams", choices = NULL, multiple = TRUE,
                         options = list(placeholder = "All teams")),
          clutch_filter_ui("euroteam"),
          starter_context_filters_ui("euroteam"),
          tags$hr(),
          accordion_toggle_link(),
          game_context_filters_from_descriptor(
            game_context_descriptor("euroteam", "euroleague")
          )
        )
      ),

      mainPanel(
        width = 9,
        conditionalPanel(
          condition = "input.euroteam_view_mode == 'Summary'",
          tab_explainer(
            id = "euroteam_explainer_summary",
            title = "What This Tab Answers (Summary)",
            intro = "How good is each EuroLeague team, per possession, on each end?",
            bullets = c(
              "Off PPP is points scored per 100 possessions; Def PPP is points allowed per 100. Net Rtg is the gap.",
              "Minutes are summed from each selected game's canonical lineup segments; pace is possessions per 40 minutes.",
              "Each cell shows the value, the team's rank in it, and the rank change versus the previous matchday.",
              "Colour runs by rank, green best to red worst, so Def PPP colours in reverse — allowing fewer points is better.",
              "Opponent-strength filters rank opponents over the whole season, so they do not shift as you narrow the date range."
            )
          )
        ),
        conditionalPanel(
          condition = "input.euroteam_view_mode == 'Four Factors'",
          tab_explainer(
            id = "euroteam_explainer_ff",
            title = "What This Tab Answers (Four Factors)",
            intro = "What drives each team's rating? Shooting (eFG%), rebounding (OREB%), turnovers (TOV%), and free-throw pressure (FTR) on both ends.",
            bullets = c(
              "Offence green-high on every factor except TOV%, where fewer is better; defence flips — green means the team suppresses that factor.",
              "Rates are computed once from summed raw counts, never averaged across games.",
              "TS% uses the Israeli denominator (FGA plus the last free throw of a shooting-foul trip), not the 0.44 x FTA estimate.",
              "These counts are derived independently of the Israeli league's possession engine. Do not compare the two leagues' numbers directly."
            )
          )
        ),
        conditionalPanel(
          condition = "input.euroteam_view_mode == 'Four Factors'",
          div(
            class = "legend-box",
            span(style = "font-weight:700; margin-right:5px;", "Legend:"),
            span(style = "font-size: 0.82em; color: var(--ibpl-text-dim);",
                 "Green = better for that team. Offence: higher eFG%/OREB%/FTR, lower TOV%. ",
                 "Defence: lower eFG%/OREB%/FTR allowed, higher opponent TOV%.")
          )
        ),
        uiOutput("euroteam_filter_chips"),
        DTOutput("euroteam_table")
      )
    )
  )
)
