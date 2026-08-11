# ui_tab10_euro_lineups.R - Tab 10: EuroLeague 2-5 player lineup units.
#
# Tab 8's euro filter vocabulary plus the lineup-specific controls Tab 2 has:
# group size, team + players-on/off, and a minimum-possessions threshold.
#
# Clutch controls are deliberately absent. They arrive with the query path that
# backs them (a per-event margin test the pre-aggregated fact cannot answer);
# a disabled control that silently does nothing is worse than no control.

ui_tab10_euro_lineups <- function() tabPanel(
  title = tags$span(tags$i(class = "bi bi-people"), "EL Lineups"),
  value = "euro_lineups",
  fluidPage(
    shared_head_tags(),

    sidebarLayout(
      sidebarPanel(
        width = 3,
        div(
          class = "view-mode-container",
          radioButtons("euro_ld_view_mode", label = "Select View:",
                       choices = c("Summary", "Four Factors"),
                       selected = "Summary",
                       inline = TRUE)
        ),
        tags$hr(),
        tags$button(class = "btn btn-outline-secondary d-md-none w-100 mb-2",
                    `data-bs-toggle` = "collapse",
                    `data-bs-target` = "#euro-ld-filters",
                    "Show Filters"),
        div(
          id = "euro-ld-filters", class = "collapse d-md-block",
          actionButton("euro_ld_reset", "Reset to defaults"),
          tags$hr(),

          selectInput("euro_ld_group_size", tt("Group size", "group_size"),
                      choices = c("2 players" = "2", "3 players" = "3",
                                  "4 players" = "4", "5 players" = "5"),
                      selected = "5"),

          lineup_player_filter_ui(
            "euro_ld_lineup_filter",
            layout = "stacked",
            team_label = "Team",
            team_placeholder = "All teams"
          ),

          sliderInput("euro_ld_minposs", tt("Minimum possessions", "min_poss_lineup"),
                      min = 0, max = 500, value = 0, step = 10),

          tags$hr(),

          dateRangeInput("euro_ld_date_range", "Game Date Range",
                         start = EURO_DEFAULT_START, end = EURO_DEFAULT_END,
                         min = EURO_DEFAULT_START, max = EURO_DEFAULT_END,
                         format = "yyyy-mm-dd"),
          accordion_toggle_link(),
          game_context_filters_from_descriptor(
            game_context_descriptor(
              "euro_ld", "euroleague", opp_rank_metric_selected = "net"
            )
          ),
          starter_context_filters_ui("euro_ld")
        )
      ),
      mainPanel(
        width = 9,
        conditionalPanel(
          condition = "input.euro_ld_view_mode == 'Summary'",
          tab_explainer(
            id = "euro_ld_explainer_summary",
            title = "What This Tab Answers (Summary)",
            intro = "Which EuroLeague 2-, 3-, 4-, or 5-player combinations outscore their opponents while sharing the floor?",
            bullets = c(
              "A unit's row covers every possession where all of its players were on court together, across any five-man lineup containing them.",
              "Off PPP and Def PPP are the team's points per 100 possessions with that unit on the floor; Net is the gap.",
              "Units overlap by design: a pair's possessions include every lineup that contained the pair, so unit rows do not sum to a team total.",
              "Minutes and possessions indicate sample size. Small samples produce noisy rates.",
              "Possessions come from an independent EuroLeague possession engine. Never compare these numbers against the Israeli-league tabs."
            )
          )
        ),
        conditionalPanel(
          condition = "input.euro_ld_view_mode == 'Four Factors'",
          tab_explainer(
            id = "euro_ld_explainer_ff",
            title = "What This Tab Answers (Four Factors)",
            intro = "Why does a unit outscore or get outscored? Break its Net into shooting (TS%), rebounding (OREB%), turnovers (TOV%), and free-throw pressure (FTR) on both ends.",
            bullets = c(
              "Offense columns are what the unit's team did; Defense columns are what opponents did against it.",
              "Lower is better for defensive TS%, OREB%, and FTR; higher is better for defensive TOV%.",
              "TS% uses the EuroLeague denominator (FGA plus the last free throw of a committed-foul trip), not the 0.44 x FTA estimate.",
              "Rates are computed after aggregating the selected games, never averaged from per-game ratios."
            )
          )
        ),
        uiOutput("euro_ld_filter_chips"),
        DTOutput("euro_ld_dt")
      )
    )
  )
)
