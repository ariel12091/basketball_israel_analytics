# ui_tab7_compare.R - Tab 7: Compare

ui_tab7_compare <- function() tabPanel(
  title = tags$span(tags$i(class = "bi bi-arrow-left-right"), "Compare"),
  value = "compare",
  fluidPage(
    shared_head_tags(),
    sidebarLayout(
      sidebarPanel(
        width = 3,

        # Mode tabs
        radioButtons("cmp_mode", label = NULL,
                     choices = c("Teams", "Lineups", "Players"),
                     selected = "Teams", inline = TRUE),

        # Preset dropdown (teams/lineups only)
        conditionalPanel(
          condition = "input.cmp_mode != 'Players'",
          selectInput("cmp_preset", tt("Quick preset", "quick_preset"),
                      choices = c("\u2014 Custom \u2014" = "",
                                  "Starters vs Bench" = "starters_bench",
                                  "Vs Starters vs Vs Bench" = "opp_starters_bench",
                                  "Clutch vs Non-Clutch" = "clutch",
                                  "Home vs Away" = "home_away",
                                  "Win vs Loss" = "win_loss",
                                  "Top vs Bottom (Opp Rank)" = "top_bottom_rank",
                                  "Date split" = "date_split",
                                  "GN split" = "gn_split"),
                      selected = "")
        ),
        conditionalPanel(
          condition = "input.cmp_mode != 'Players'",
          sliderInput("cmp_min_poss", tt("Min possessions", "min_poss_lineup"), min = 0, max = 2000, value = 10, step = 10)
        ),
        conditionalPanel(
          condition = "input.cmp_mode != 'Players' && input.cmp_preset == 'date_split'",
          dateInput("cmp_split_date", "Split date", value = DEFAULT_END)
        ),
        conditionalPanel(
          condition = "input.cmp_mode != 'Players' && input.cmp_preset == 'gn_split'",
          selectizeInput("cmp_split_gn", "Split GN",
                         choices = NULL, selected = "", multiple = FALSE,
                         options = list(placeholder = "Choose GN"))
        ),

        tags$hr(),
        tags$button(class = "btn btn-outline-secondary d-md-none w-100 mb-2",
                    `data-bs-toggle` = "collapse", `data-bs-target` = "#cmp-filters",
                    "Show Filters"),
        div(
          id = "cmp-filters", class = "collapse d-md-block",
          style = "max-height: calc(100vh - 160px); overflow-y: auto; padding-right: 4px; padding-bottom: 60px;",
          actionButton("cmp_reset", "Reset Filters"),
          conditionalPanel(
            condition = "input.cmp_mode == 'Players'",
            radioButtons(
              "cmp_player_compare_mode", "Compare",
              choices = c("Other players" = "other", "Against himself" = "self"),
              selected = "other", inline = FALSE
            )
          ),
          tags$hr(),
          conditionalPanel(
            condition = "input.cmp_mode != 'Players' && input.cmp_preset != 'date_split'",
            dateRangeInput("cmp_dates", "Date range", start = DEFAULT_START, end = DEFAULT_END)
          ),
          conditionalPanel(
            condition = "input.cmp_mode != 'Players' && input.cmp_preset != 'gn_split'",
            fluidRow(
              column(6, selectizeInput("cmp_gn_min", tt("From Game Number (GN)", "gn"),
                                       choices = NULL, selected = "", multiple = FALSE,
                                       options = list(placeholder = "Any"))),
              column(6, selectizeInput("cmp_gn_max", tt("To Game Number (GN)", "gn"),
                                       choices = NULL, selected = "", multiple = FALSE,
                                       options = list(placeholder = "Any")))
            )
          ),
          conditionalPanel(
            condition = "input.cmp_mode != 'Players'",
            tags$hr()
          ),
          conditionalPanel(
            condition = "input.cmp_mode == 'Players' && input.cmp_player_compare_mode != 'self'",
            tagList(
              dateRangeInput("cmp_players_dates", "Date range", start = DEFAULT_START, end = DEFAULT_END),
              fluidRow(
                column(6, selectizeInput("cmp_players_gn_min", tt("From Game Number (GN)", "gn"),
                                         choices = NULL, selected = "", multiple = FALSE,
                                         options = list(placeholder = "Any"))),
                column(6, selectizeInput("cmp_players_gn_max", tt("To Game Number (GN)", "gn"),
                                         choices = NULL, selected = "", multiple = FALSE,
                                         options = list(placeholder = "Any")))
              ),
              tags$hr()
            )
          ),

          # ── A ──
          tags$div(
            class = "d-flex align-items-center gap-2 mb-2",
            tags$span(class = "badge rounded-pill",
                      style = "background: rgba(123,140,222,.2); color: var(--ibpl-side-a); border: 1px solid rgba(123,140,222,.4); font-size: .7rem;",
                      "A")
          ),

          conditionalPanel(
            condition = "input.cmp_mode == 'Players' && input.cmp_player_compare_mode == 'self'",
            tagList(
              dateRangeInput("cmp_player_a_dates", "Date range", start = DEFAULT_START, end = DEFAULT_END),
              fluidRow(
                column(6, selectizeInput("cmp_player_a_gn_min", tt("From Game Number (GN)", "gn"),
                                         choices = NULL, selected = "", multiple = FALSE,
                                         options = list(placeholder = "Any"))),
                column(6, selectizeInput("cmp_player_a_gn_max", tt("To Game Number (GN)", "gn"),
                                         choices = NULL, selected = "", multiple = FALSE,
                                         options = list(placeholder = "Any")))
              ),
              tags$hr()
            )
          ),

          # Player picker (Players mode only)
          conditionalPanel(
            condition = "input.cmp_mode == 'Players'",
            tagList(
              selectizeInput("cmp_player_a_list_team_filter", "Player List Team Filter",
                             choices = NULL, multiple = TRUE,
                             options = list(placeholder = "All teams")),
              selectizeInput("cmp_player_a", "Player", choices = NULL,
                             options = list(placeholder = "Search player...")),
              uiOutput("cmp_player_a_team_ui")
            )
          ),

          # Starters filter (Teams + Lineups only)
          conditionalPanel(
            condition = "input.cmp_mode != 'Players'",
            tagList(
              fluidRow(
                column(6, selectInput("cmp_a_starters_mode", tt("Own lineup starters", "own_starters"),
                                    choices = c("ALL" = "", "At least (>=)" = "gte", "At most (<=)" = "lte"), selected = "")),
                column(6, selectInput("cmp_a_starters_val", "Own value",
                                    choices = c("\u2014" = "", as.character(0:5)), selected = ""))
              ),
              fluidRow(
                column(6, selectInput("cmp_a_opp_starters_mode", tt("Opponent lineup starters", "opp_starters"),
                                    choices = c("ALL" = "", "At least (>=)" = "gte", "At most (<=)" = "lte"), selected = "")),
                column(6, selectInput("cmp_a_opp_starters_val", "Opp value",
                                    choices = c("\u2014" = "", as.character(0:5)), selected = ""))
              )
            )
          ),
          conditionalPanel(
            condition = "input.cmp_mode != 'Players'",
            selectizeInput("cmp_a_teams", "Teams", choices = NULL, multiple = TRUE,
                           options = list(placeholder = "All teams"))
          ),

          selectInput("cmp_a_home_away", "Home/Away",
                      choices = c("All" = "", "Home" = "home", "Away" = "away"), selected = ""),
          selectInput("cmp_a_outcome", "Win/Loss",
                      choices = c("All" = "", "Win" = "win", "Loss" = "loss"), selected = ""),
          checkboxInput("cmp_a_clutch", tt("Clutch", "clutch"), value = FALSE),
          conditionalPanel(
            condition = "input.cmp_a_clutch == true",
            sliderInput("cmp_a_clutch_margin", "Max point margin", min = 0, max = 10, value = 5, step = 1),
            sliderInput("cmp_a_clutch_minutes", "Max minutes remaining", min = 1, max = 5, value = 5, step = 1)
          ),
          selectizeInput("cmp_a_opponents", "Opponents", choices = NULL, multiple = TRUE,
                         options = list(placeholder = "All opponents")),
          selectizeInput("cmp_a_game_type", "Game type",
                         choices = c("All" = "", "Regular season" = "5", "Playoffs \u2013 QF" = "16",
                                     "Playoffs \u2013 Finals" = "17", "Playoffs \u2013 SF" = "26",
                                     "Play-in" = "33", "Winner Cup" = "34", "State Cup" = "35"),
                         selected = "", multiple = TRUE, options = list(placeholder = "All")),
          fluidRow(
            column(4, selectInput("cmp_a_opp_rank_side", "Top / Bottom",
                                  choices = c("Off" = "", "Top" = "top", "Bottom" = "bottom"), selected = "")),
            column(4, selectInput("cmp_a_opp_rank_n", "Rank N",
                                  choices = c("\u2014" = "", as.character(1:12)), selected = "")),
            column(4, selectInput("cmp_a_opp_rank_metric", "Metric",
                                  choices = c("\u2014" = "", "Offense" = "off", "Defense" = "def", "Net rating" = "net"), selected = ""))
          ),

          # ── vs divider ──
          tags$div(class = "text-center text-muted fw-bold my-2", "\u2014 vs \u2014"),

          # ── B ──
          tags$div(
            class = "d-flex align-items-center gap-2 mb-2",
            tags$span(class = "badge rounded-pill",
                      style = "background: rgba(232,164,53,.15); color: var(--ibpl-accent); border: 1px solid rgba(232,164,53,.35); font-size: .7rem;",
                      "B")
          ),

          conditionalPanel(
            condition = "input.cmp_mode == 'Players' && input.cmp_player_compare_mode != 'self'",
            tagList(
              selectizeInput("cmp_player_b_list_team_filter", "Player List Team Filter",
                             choices = NULL, multiple = TRUE,
                             options = list(placeholder = "All teams")),
              selectizeInput("cmp_player_b", "Player", choices = NULL,
                             options = list(placeholder = "Search player...")),
              uiOutput("cmp_player_b_team_ui")
            )
          ),

          conditionalPanel(
            condition = "input.cmp_mode == 'Players' && input.cmp_player_compare_mode == 'self'",
            tagList(
              dateRangeInput("cmp_player_b_dates", "Date range", start = DEFAULT_START, end = DEFAULT_END),
              fluidRow(
                column(6, selectizeInput("cmp_player_b_gn_min", tt("From Game Number (GN)", "gn"),
                                         choices = NULL, selected = "", multiple = FALSE,
                                         options = list(placeholder = "Any"))),
                column(6, selectizeInput("cmp_player_b_gn_max", tt("To Game Number (GN)", "gn"),
                                         choices = NULL, selected = "", multiple = FALSE,
                                         options = list(placeholder = "Any")))
              )
            )
          ),

          conditionalPanel(
            condition = "input.cmp_mode != 'Players'",
            tagList(
              fluidRow(
                column(6, selectInput("cmp_b_starters_mode", tt("Own lineup starters", "own_starters"),
                                    choices = c("ALL" = "", "At least (>=)" = "gte", "At most (<=)" = "lte"), selected = "")),
                column(6, selectInput("cmp_b_starters_val", "Own value",
                                    choices = c("\u2014" = "", as.character(0:5)), selected = ""))
              ),
              fluidRow(
                column(6, selectInput("cmp_b_opp_starters_mode", tt("Opponent lineup starters", "opp_starters"),
                                    choices = c("ALL" = "", "At least (>=)" = "gte", "At most (<=)" = "lte"), selected = "")),
                column(6, selectInput("cmp_b_opp_starters_val", "Opp value",
                                    choices = c("\u2014" = "", as.character(0:5)), selected = ""))
              )
            )
          ),
          conditionalPanel(
            condition = "input.cmp_mode != 'Players'",
            selectizeInput("cmp_b_teams", "Teams", choices = NULL, multiple = TRUE,
                           options = list(placeholder = "All teams"))
          ),

          selectInput("cmp_b_home_away", "Home/Away",
                      choices = c("All" = "", "Home" = "home", "Away" = "away"), selected = ""),
          selectInput("cmp_b_outcome", "Win/Loss",
                      choices = c("All" = "", "Win" = "win", "Loss" = "loss"), selected = ""),
          checkboxInput("cmp_b_clutch", tt("Clutch", "clutch"), value = FALSE),
          conditionalPanel(
            condition = "input.cmp_b_clutch == true",
            sliderInput("cmp_b_clutch_margin", "Max point margin", min = 0, max = 10, value = 5, step = 1),
            sliderInput("cmp_b_clutch_minutes", "Max minutes remaining", min = 1, max = 5, value = 5, step = 1)
          ),
          selectizeInput("cmp_b_opponents", "Opponents", choices = NULL, multiple = TRUE,
                         options = list(placeholder = "All opponents")),
          selectizeInput("cmp_b_game_type", "Game type",
                         choices = c("All" = "", "Regular season" = "5", "Playoffs \u2013 QF" = "16",
                                     "Playoffs \u2013 Finals" = "17", "Playoffs \u2013 SF" = "26",
                                     "Play-in" = "33", "Winner Cup" = "34", "State Cup" = "35"),
                         selected = "", multiple = TRUE, options = list(placeholder = "All")),
          fluidRow(
            column(4, selectInput("cmp_b_opp_rank_side", "Top / Bottom",
                                  choices = c("Off" = "", "Top" = "top", "Bottom" = "bottom"), selected = "")),
            column(4, selectInput("cmp_b_opp_rank_n", "Rank N",
                                  choices = c("\u2014" = "", as.character(1:12)), selected = "")),
            column(4, selectInput("cmp_b_opp_rank_metric", "Metric",
                                  choices = c("\u2014" = "", "Offense" = "off", "Defense" = "def", "Net rating" = "net"), selected = ""))
          )
        )
      ),

      mainPanel(
        width = 9,
        tab_explainer(
          id = "compare_explainer",
          title = "What This Tab Answers",
          intro = "How do two situations compare side-by-side: starters vs bench, home vs away, clutch vs non-clutch, or any custom split?",
          bullets = c(
            "Metric chips at the top (Net Rtg, Offense, Defense, eFG%, TOV%, OREB%, FTR) control which stat is shown in the table and summary cards.",
            "Summary cards show Side A value, Side B value, delta, and possessions. The table shows #, Team, Side A metric, Total Poss A, Side B metric, Total Poss B, and Gap.",
            "Teams and Lineups modes compare the same team/lineup to itself under two different conditions (e.g. home vs away, clutch vs non-clutch). Players mode compares either two players under the same time filters or one player across separate A/B time filters.",
            "Click any row in League view for a detailed breakdown of that team or lineup."
          )
        ),
        tags$a(
          href = "#compare-example-box",
          class = "explainer-toggle",
          `data-bs-toggle` = "collapse",
          `data-bs-target` = "#compare-example-box",
          "Show/Hide Example"
        ),
        div(
          id = "compare-example-box",
          class = "collapse example-wrapper",
          div(
            class = "example-grid",
            div(
              class = "example-card",
              div(class = "example-card-title", "How to Read Compare (Real Example)"),
              tags$p(style = "margin-bottom: 6px;", "Using the Starters vs Bench preset: Side A (3+ starters) averages -0.2 Net Rtg across 1,048 possessions, while Side B (2 or fewer starters) averages +1.2 across 375 possessions."),
              tags$p(style = "margin-bottom: 6px;", "The table breaks this down per team. Maccabi Tel Aviv's starters lead the league with a +17.6 gap over their bench."),
              tags$p(style = "margin-bottom: 0;", "Use metric chips (Offense, Defense, eFG%, etc.) to switch which stat drives the comparison. Click any row for a full detail breakdown.")
            ),
            div(
              class = "example-snippet",
              tags$img(src = app_image_src("compare-row-snippet.png"), alt = "Compare tab table snippet"),
              div(class = "example-snippet-caption", "Real Compare snippet (Starters vs Bench)")
            )
          )
        ),

        # ── Teams / Lineups mode ──
        conditionalPanel(
          condition = "input.cmp_mode != 'Players'",

          # Toggle bar
          div(
            class = "d-flex align-items-center gap-2 mb-3 flex-wrap",
            tags$span(class = "text-muted small text-uppercase", "View"),
            tags$button(id = "cmp_view_league_btn", type = "button",
              class = "btn btn-sm btn-warning cmp-view-toggle-btn js-shiny-event",
              `data-input-id` = "cmp_detail_toggle",
              `data-shiny-value` = "league",
              "League"),
            tags$button(id = "cmp_view_detail_btn", type = "button",
              class = "btn btn-sm btn-outline-secondary cmp-view-toggle-btn js-shiny-event",
              `data-input-id` = "cmp_detail_toggle",
              `data-shiny-value` = "detail",
              "Detail"),
            uiOutput("cmp_team_players_view_btn_ui"),
            uiOutput("cmp_detail_entity_dropdown_ui")
          ),

          # League view (existing content)
          div(
            id = "cmp_league_container",

            div(
              class = "text-muted mb-2",
              style = "font-size: .72rem;",
              "Click a row for a detailed team or lineup breakdown."
            ),

            # Metric chips
            div(
              class = "d-flex align-items-center gap-2 mb-3 flex-wrap",
              tags$span(class = "text-muted small text-uppercase", "Metric"),
              uiOutput("cmp_metric_chips_ui")
            ),

            # Summary cards
            fluidRow(
              column(4, div(class = "card bg-dark border-secondary p-3 mb-3",
                tags$div(class = "small text-uppercase", style = "color: var(--ibpl-side-a);",
                         textOutput("cmp_summary_a_title", inline = TRUE)),
                tags$div(class = "fs-4 fw-bold", textOutput("cmp_summary_a", inline = TRUE)),
                tags$div(class = "small text-muted", textOutput("cmp_summary_a_label", inline = TRUE)),
                tags$div(class = "small text-muted", textOutput("cmp_summary_a_delta", inline = TRUE)),
                tags$div(class = "small text-muted", textOutput("cmp_summary_a_poss", inline = TRUE))
              )),
              column(4, div(class = "card bg-dark border-secondary p-3 mb-3",
                tags$div(class = "small text-uppercase", style = "color: var(--ibpl-accent);",
                         textOutput("cmp_summary_b_title", inline = TRUE)),
                tags$div(class = "fs-4 fw-bold", textOutput("cmp_summary_b", inline = TRUE)),
                tags$div(class = "small text-muted", textOutput("cmp_summary_b_label", inline = TRUE)),
                tags$div(class = "small text-muted", textOutput("cmp_summary_b_delta", inline = TRUE)),
                tags$div(class = "small text-muted", textOutput("cmp_summary_b_poss", inline = TRUE))
              )),
              column(4, div(class = "card bg-dark border-secondary p-3 mb-3",
                tags$div(class = "small text-uppercase text-muted", "Avg Gap"),
                tags$div(class = "fs-4 fw-bold", style = "color: var(--ibpl-accent);", textOutput("cmp_summary_gap", inline = TRUE)),
                tags$div(class = "small text-muted", "league-wide")
              ))
            ),

            # Lineup controls (Lineups mode only)
            conditionalPanel(
              condition = "input.cmp_mode == 'Lineups'",
              div(
                class = "d-flex align-items-center gap-3 mb-3 flex-wrap",
                div(
                  class = "d-flex align-items-center gap-2",
                  tags$span(class = "text-muted small text-uppercase", "Size"),
                  radioButtons("cmp_lu_num", NULL,
                               choices = c("2", "3", "4", "5"), selected = "5",
                               inline = TRUE)
                ),
                lineup_player_filter_ui(
                  "cmp_lu_filter",
                  layout = "inline",
                  team_label = NULL,
                  team_placeholder = "All teams",
                  players_on_label = NULL,
                  players_off_label = NULL,
                  players_on_placeholder = "Any",
                  players_off_placeholder = "Any"
                )
              )
            ),

            # Results table
            DT::dataTableOutput("cmp_table")
          ),

          # Detail view (hidden by default)
          div(
            id = "cmp_detail_container",
            class = "cmp-view-hidden",
            uiOutput("cmp_detail_view_ui")
          ),

          div(
            id = "cmp_team_players_container",
            class = "cmp-view-hidden",
            div(
              class = "d-flex align-items-center gap-2 mb-3 flex-wrap",
              tags$span(class = "text-muted small text-uppercase", "Metric"),
              uiOutput("cmp_team_player_metric_chips_ui"),
              tags$span(class = "text-muted small text-uppercase ms-2", "Rate"),
              radioButtons("cmp_team_player_rate_mode", NULL,
                           choices = c("Per Game", "Per 60 Possessions", "Per 30 Minutes", "Totals"),
                           selected = "Per Game", inline = TRUE)
            ),
            uiOutput("cmp_team_players_panel_ui")
          )
        ),

        # ── Players mode: PvP comparison view ──
        conditionalPanel(
          condition = "input.cmp_mode == 'Players'",
          div(
            class = "d-flex align-items-center gap-2 mb-3 flex-wrap",
            tags$span(class = "text-muted small text-uppercase", "View"),
            uiOutput("cmp_player_chips_ui")
          ),
          conditionalPanel(
            condition = "input.cmp_player_view != 'ff_swing'",
            div(
              class = "mb-3",
              radioButtons("cmp_rate_mode", NULL,
                           choices = c("Per Game", "Per 75 Possessions", "Totals"),
                           selected = "Per Game", inline = TRUE)
            )
          ),
          uiOutput("cmp_pvp_ui")
        ),

        # Filter chips (always)
        uiOutput("cmp_filter_chips")
      )
    )
  )
)
