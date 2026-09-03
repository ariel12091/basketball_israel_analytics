# ui_tab11_euro_gamelogs.R - Tab 11: EuroLeague / EuroCup Game Logs

ui_tab11_euro_gamelogs <- function() tabPanel(
  title = tags$span(tags$i(class = "bi bi-calendar-event"), "Game Logs"),
  value = "euro_game_logs",
  fluidPage(
    shared_head_tags(),
    sidebarLayout(
      sidebarPanel(
        width = 3,
        div(
          class = "view-mode-container",
          radioButtons("eurogl_view_mode", "View:",
                       choices = c("Summary", "Four Factors"),
                       selected = "Summary", inline = TRUE)
        ),
        tags$hr(),
        tags$button(class = "btn btn-outline-secondary d-md-none w-100 mb-2",
                    `data-bs-toggle` = "collapse", `data-bs-target` = "#eurogl-filters",
                    "Show Filters"),
        div(
          id = "eurogl-filters", class = "collapse d-md-block",
          actionButton("eurogl_reset", "Reset to defaults"),
          tags$hr(),
          dateRangeInput("eurogl_dates", "Game Date Range",
                         start = EURO_DEFAULT_START, end = EURO_DEFAULT_END,
                         min = EURO_DEFAULT_START, max = EURO_DEFAULT_END,
                         format = "yyyy-mm-dd"),
          selectizeInput("eurogl_teams", "Teams", choices = NULL, multiple = TRUE,
                         options = list(placeholder = "All teams")),
          starter_context_filters_ui("eurogl"),
          tags$hr(),
          accordion_toggle_link(),
          game_context_filters_ui(
            "eurogl", include_opp_rank = FALSE,
            game_type_id = "eurogl_phase", game_type_label = "Phase",
            game_type_choices = NULL, game_type_selected = character(0),
            game_type_placeholder = "All phases",
            gn_min_label = "From Round", gn_max_label = "To Round",
            opp_rank_max = 20L
          )
        )
      ),
      mainPanel(
        width = 9,
        conditionalPanel(
          condition = "input.eurogl_view_mode == 'Summary'",
          tab_explainer(
            id = "eurogl_explainer_summary",
            title = "What This Tab Answers (Summary)",
            intro = "How did each EuroLeague game look in result and possession-adjusted efficiency terms?",
            bullets = c(
              "Each row is one team's perspective on one game, with round, opponent, result, score, and ratings.",
              "Off PPP is points scored per 100 possessions; Def PPP is points allowed per 100. Net is the gap.",
              "Compare games by efficiency rather than score alone, because pace and possession counts vary."
            )
          )
        ),
        conditionalPanel(
          condition = "input.eurogl_view_mode == 'Four Factors'",
          tab_explainer(
            id = "eurogl_explainer_ff",
            title = "What This Tab Answers (Four Factors)",
            intro = "Which shooting, rebounding, turnover, and free-throw components drove each game?",
            bullets = c(
              "Read offense and defense side by side to diagnose why a game was won or lost.",
              "Rates are calculated from additive game counts, never by averaging stored percentages.",
              "EuroLeague possessions and free-throw trips use the EuroLeague engine and source semantics."
            )
          )
        ),
        filter_chips_row("eurogl_filter_chips"),
        DTOutput("eurogl_table")
      )
    )
  )
)
