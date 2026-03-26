# ui_tab7_compare.R - Tab 7: Compare

ui_tab7_compare <- tabPanel(
  title = tags$span(tags$i(class = "bi bi-arrow-left-right"), "Compare"),
  value = "compare",
  fluidPage(
    shared_head_tags(),
    tags$script(HTML("
      Shiny.addCustomMessageHandler('toggle_cmp_view', function(msg) {
        var league = document.getElementById('cmp_view_league_btn');
        var detail = document.getElementById('cmp_view_detail_btn');
        if (!league || !detail) return;
        if (msg.detail) {
          league.className = 'btn btn-sm btn-outline-secondary';
          league.style.borderRadius = '20px';
          detail.className = 'btn btn-sm btn-warning';
          detail.style.borderRadius = '20px';
        } else {
          league.className = 'btn btn-sm btn-warning';
          league.style.borderRadius = '20px';
          detail.className = 'btn btn-sm btn-outline-secondary';
          detail.style.borderRadius = '20px';
        }
        var leagueC = document.getElementById('cmp_league_container');
        var detailC = document.getElementById('cmp_detail_container');
        if (leagueC) leagueC.style.display = msg.detail ? 'none' : '';
        if (detailC) detailC.style.display = msg.detail ? '' : 'none';
      });
    ")),
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
          selectInput("cmp_preset", "Quick preset",
                      choices = c("\u2014 Custom \u2014" = "",
                                  "Starters vs Bench" = "starters_bench",
                                  "Clutch vs Non-Clutch" = "clutch",
                                  "Home vs Away" = "home_away",
                                  "Win vs Loss" = "win_loss",
                                  "Date split" = "date_split",
                                  "GN split" = "gn_split"),
                      selected = "")
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
          style = "max-height: calc(100vh - 200px); overflow-y: auto; padding-right: 4px;",
          actionButton("cmp_reset", "Reset Filters"),
          tags$hr(),
          conditionalPanel(
            condition = "input.cmp_mode == 'Players'",
            tagList(
              dateRangeInput("cmp_players_dates", "Date range", start = DEFAULT_START, end = DEFAULT_END),
              fluidRow(
                column(6, selectizeInput("cmp_players_gn_min", "From Game Number (GN)",
                                         choices = NULL, selected = "", multiple = FALSE,
                                         options = list(placeholder = "Any"))),
                column(6, selectizeInput("cmp_players_gn_max", "To Game Number (GN)",
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
                      style = "background: rgba(123,140,222,.2); color: #7b8cde; border: 1px solid rgba(123,140,222,.4); font-size: .7rem;",
                      "A")
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
            fluidRow(
              column(6, selectInput("cmp_a_starters_mode", "Starters",
                                    choices = c("ALL" = "", "At least (>=)" = "gte", "At most (<=)" = "lte"), selected = "")),
              column(6, selectInput("cmp_a_starters_val", "Value",
                                    choices = c("\u2014" = "", as.character(0:5)), selected = ""))
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
          checkboxInput("cmp_a_clutch", "Clutch", value = FALSE),
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
            column(4, selectInput("cmp_a_opp_rank_side", "Opp rank",
                                  choices = c("Off" = "", "Top" = "top", "Bottom" = "bottom"), selected = "")),
            column(4, selectInput("cmp_a_opp_rank_n", "N",
                                  choices = c("\u2014" = "", as.character(1:12)), selected = "")),
            column(4, selectInput("cmp_a_opp_rank_metric", "Metric",
                                  choices = c("\u2014" = "", "Offense" = "off", "Defense" = "def", "Net" = "net"), selected = ""))
          ),

          # ── vs divider ──
          tags$div(class = "text-center text-muted fw-bold my-2", "\u2014 vs \u2014"),

          # ── B ──
          tags$div(
            class = "d-flex align-items-center gap-2 mb-2",
            tags$span(class = "badge rounded-pill",
                      style = "background: rgba(232,164,53,.15); color: #e8a435; border: 1px solid rgba(232,164,53,.35); font-size: .7rem;",
                      "B")
          ),

          conditionalPanel(
            condition = "input.cmp_mode == 'Players'",
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
            condition = "input.cmp_mode != 'Players'",
            fluidRow(
              column(6, selectInput("cmp_b_starters_mode", "Starters",
                                    choices = c("ALL" = "", "At least (>=)" = "gte", "At most (<=)" = "lte"), selected = "")),
              column(6, selectInput("cmp_b_starters_val", "Value",
                                    choices = c("\u2014" = "", as.character(0:5)), selected = ""))
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
          checkboxInput("cmp_b_clutch", "Clutch", value = FALSE),
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
            column(4, selectInput("cmp_b_opp_rank_side", "Opp rank",
                                  choices = c("Off" = "", "Top" = "top", "Bottom" = "bottom"), selected = "")),
            column(4, selectInput("cmp_b_opp_rank_n", "N",
                                  choices = c("\u2014" = "", as.character(1:12)), selected = "")),
            column(4, selectInput("cmp_b_opp_rank_metric", "Metric",
                                  choices = c("\u2014" = "", "Offense" = "off", "Defense" = "def", "Net" = "net"), selected = ""))
          )
        )
      ),

      mainPanel(
        width = 9,

        # ── Teams / Lineups mode ──
        conditionalPanel(
          condition = "input.cmp_mode != 'Players'",

          # Toggle bar
          div(
            class = "d-flex align-items-center gap-2 mb-3 flex-wrap",
            tags$span(class = "text-muted small text-uppercase", "View"),
            tags$button(id = "cmp_view_league_btn", type = "button",
              class = "btn btn-sm btn-warning",
              style = "border-radius: 20px; padding: 2px 12px; font-size: .76rem;",
              onclick = "Shiny.setInputValue('cmp_detail_toggle', 'league', {priority: 'event'})",
              "League"),
            tags$button(id = "cmp_view_detail_btn", type = "button",
              class = "btn btn-sm btn-outline-secondary",
              style = "border-radius: 20px; padding: 2px 12px; font-size: .76rem;",
              onclick = "Shiny.setInputValue('cmp_detail_toggle', 'detail', {priority: 'event'})",
              "Detail"),
            uiOutput("cmp_detail_entity_dropdown_ui")
          ),

          # League view (existing content)
          div(
            id = "cmp_league_container",

            # Metric chips
            div(
              class = "d-flex align-items-center gap-2 mb-3 flex-wrap",
              tags$span(class = "text-muted small text-uppercase", "Metric"),
              uiOutput("cmp_metric_chips_ui")
            ),

            # Summary cards
            fluidRow(
              column(4, div(class = "card bg-dark border-secondary p-3 mb-3",
                tags$div(class = "small text-uppercase", style = "color: #7b8cde;",
                         textOutput("cmp_summary_a_title", inline = TRUE)),
                tags$div(class = "fs-4 fw-bold", style = "color: #4caf7d;", textOutput("cmp_summary_a", inline = TRUE)),
                tags$div(class = "small text-muted", textOutput("cmp_summary_a_label", inline = TRUE)),
                tags$div(class = "small text-muted", textOutput("cmp_summary_a_delta", inline = TRUE)),
                tags$div(class = "small text-muted", textOutput("cmp_summary_a_poss", inline = TRUE))
              )),
              column(4, div(class = "card bg-dark border-secondary p-3 mb-3",
                tags$div(class = "small text-uppercase", style = "color: #e8a435;",
                         textOutput("cmp_summary_b_title", inline = TRUE)),
                tags$div(class = "fs-4 fw-bold", style = "color: #e05c5c;", textOutput("cmp_summary_b", inline = TRUE)),
                tags$div(class = "small text-muted", textOutput("cmp_summary_b_label", inline = TRUE)),
                tags$div(class = "small text-muted", textOutput("cmp_summary_b_delta", inline = TRUE)),
                tags$div(class = "small text-muted", textOutput("cmp_summary_b_poss", inline = TRUE))
              )),
              column(4, div(class = "card bg-dark border-secondary p-3 mb-3",
                tags$div(class = "small text-uppercase text-muted", "Avg Gap"),
                tags$div(class = "fs-4 fw-bold", style = "color: #e8a435;", textOutput("cmp_summary_gap", inline = TRUE)),
                tags$div(class = "small text-muted", "league-wide")
              ))
            ),

            # Results table
            DT::dataTableOutput("cmp_table")
          ),

          # Detail view (hidden by default)
          div(
            id = "cmp_detail_container",
            style = "display: none;",
            uiOutput("cmp_detail_view_ui")
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
