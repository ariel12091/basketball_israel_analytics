# ui_tab10_euro_lineups.R - Tab 10: EuroLeague 2-5 player lineup units.
#
# The sidebar is Tab 2's, control for control and in Tab 2's order: the two
# tabs render the same table through the same helpers, so the filters that
# drive it must not be arranged or labelled differently. Only the ids carry a
# euro_ prefix, and only the explainer prose is EuroLeague-specific -- it makes
# caveats the Israeli tab has no reason to make.
#
ui_tab10_euro_lineups <- function() tabPanel(
  title = tags$span(tags$i(class = "bi bi-people"), "Lineup Data"),
  value = "euro_lineups",
  fluidPage(
    shared_head_tags(),

    sidebarLayout(
      sidebarPanel(
        width = 3,
        div(
          class = "view-mode-container",
          radioButtons("euro_ld_view_mode", label = "View:",
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
          actionButton("euro_ld_reset", "Reset Lineup Filters"),
          tags$hr(),

          sliderInput("euro_ld_minposs",
                      tt("Minimum possessions (Off + Def)", "min_poss_lineup"),
                      min = 0, max = 500, value = 0, step = 10),
          helpText("Higher minimums improve stability but remove smaller-sample lineups."),

          radioButtons("euro_ld_group_size", tt("Group size", "group_size"),
                       choices = c("2", "3", "4", "5"),
                       selected = "5", inline = TRUE),
          tags$hr(),

          lineup_player_filter_ui(
            "euro_ld_lineup_filter",
            team_label = "Team",
            team_help = "Pick a team to enable player filtering.",
            team_placeholder = "All teams",
            players_on_placeholder = "Select a team first...",
            players_off_placeholder = "Select a team first..."
          ),
          starter_context_filters_ui("euro_ld"),
          tags$hr(),

          dateRangeInput("euro_ld_date_range", "Date range",
                         start = EURO_DEFAULT_START, end = EURO_DEFAULT_END,
                         min = EURO_DEFAULT_START, max = EURO_DEFAULT_END,
                         format = "yyyy-mm-dd"),
          clutch_filter_ui("euro_ld"),
          tags$hr(),

          accordion_toggle_link(),
          game_context_filters_from_descriptor(
            game_context_descriptor(
              "euro_ld", "euroleague", opp_rank_metric_selected = "net"
            )
          )
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
              "Off Shot and Def Shot cells show 2PT/3PT frequency and accuracy - use them to check whether efficiency is driven by sustainable shot selection or a hot-hand streak.",
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
            intro = "Why does a unit outscore or get outscored? Break its Net into shooting (eFG%), rebounding (OREB%), turnovers (TOV%), and free-throw pressure (FTR) on both ends.",
            bullets = c(
              "Offense columns are what the unit's team did; Defense columns are what opponents did against it.",
              "Lower is better for defensive eFG%, OREB%, and FTR; higher is better for defensive TOV%.",
              "eFG% credits a made three as 1.5 field goals: (FGM + 0.5 x 3PM) / FGA.",
              "Rates are computed after aggregating the selected games, never averaged from per-game ratios."
            )
          )
        ),
        conditionalPanel(
          condition = "input.euro_ld_view_mode == 'Summary'",
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
        uiOutput("euro_ld_filter_chips"),
        DTOutput("euro_ld_dt")
      )
    )
  )
)
