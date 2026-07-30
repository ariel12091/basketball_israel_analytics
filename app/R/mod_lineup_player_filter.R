lineup_player_filter_ui <- function(id,
                                    layout = c("stacked", "inline"),
                                    team_label = "Team",
                                    team_help = NULL,
                                    team_placeholder = "All teams",
                                    players_on_label = tt("Players On (exact/contains)", "players_on"),
                                    players_off_label = tt("Players Off (exclude any)", "players_off"),
                                    players_on_placeholder = "Any",
                                    players_off_placeholder = "Any") {
  layout <- match.arg(layout)
  ns <- shiny::NS(id)

  team_input <- selectizeInput(
    ns("team"),
    team_label,
    choices = NULL,
    multiple = FALSE,
    options = list(placeholder = team_placeholder),
    width = "100%"
  )
  players_on_input <- selectizeInput(
    ns("players_on"),
    players_on_label,
    choices = NULL,
    multiple = TRUE,
    options = list(placeholder = players_on_placeholder),
    width = "100%"
  )
  players_off_input <- selectizeInput(
    ns("players_off"),
    players_off_label,
    choices = NULL,
    multiple = TRUE,
    options = list(placeholder = players_off_placeholder),
    width = "100%"
  )

  if (identical(layout, "inline")) {
    return(
      div(
        class = "d-flex align-items-center gap-2 flex-grow-1",
        tags$span(class = "text-muted small text-uppercase text-nowrap", "Team"),
        div(style = "min-width: 140px;", team_input),
        tags$span(class = "text-muted small text-uppercase text-nowrap", "On"),
        div(style = "min-width: 160px;", players_on_input),
        tags$span(class = "text-muted small text-uppercase text-nowrap", "Off"),
        div(style = "min-width: 160px;", players_off_input)
      )
    )
  }

  tagList(
    team_input,
    if (!is.null(team_help)) helpText(team_help),
    players_on_input,
    players_off_input
  )
}

lineup_player_filter_server <- function(id, players_ref) {
  moduleServer(id, function(input, output, session) {
    empty_choices <- setNames(character(0), character(0))

    clear_player_choices <- function() {
      updateSelectizeInput(session, "players_on", choices = empty_choices, selected = character(0), server = FALSE)
      updateSelectizeInput(session, "players_off", choices = empty_choices, selected = character(0), server = FALSE)
    }

    update_team_choices <- function(choices, selected = "") {
      selected <- restore_aware_selection(session, "team", selected, choices)
      updateSelectizeInput(session, "team", choices = choices, selected = selected, server = FALSE)
    }

    current_team_value <- function() {
      sanitize_single_choice(input$team, numeric_only = TRUE)
    }

    current_player_values <- function(input_id) {
      sanitize_persisted_choices(input[[input_id]], numeric_only = TRUE)
    }

    selected_in_choices <- function(selected, choices) {
      selected <- sanitize_persisted_choices(selected, numeric_only = TRUE)
      if (!length(selected) || !length(choices)) return(character(0))
      intersect(selected, as.character(unname(choices)))
    }

    refresh_player_choices <- function() {
      team_val <- current_team_value()
      team_val <- team_val[nzchar(team_val)]
      if (!length(team_val)) {
        clear_player_choices()
        return(invisible(NULL))
      }
      team_val <- team_val[[1]]

      players_df <- players_ref()
      has_player_cols <- !is.null(players_df) && all(c("team_id", "player_id", "name") %in% names(players_df))
      if (!isTRUE(has_player_cols)) return(invisible(NULL))

      tid <- suppressWarnings(as.integer(team_val))
      roster <- players_df[players_df$team_id == tid, , drop = FALSE]
      choices <- if (nrow(roster)) {
        setNames(as.character(roster$player_id), roster$name)
      } else {
        empty_choices
      }

      updateSelectizeInput(
        session, "players_on",
        choices = choices,
        selected = selected_in_choices(input$players_on, choices),
        server = FALSE
      )
      updateSelectizeInput(
        session, "players_off",
        choices = choices,
        selected = selected_in_choices(input$players_off, choices),
        server = FALSE
      )
      invisible(NULL)
    }

    reset_inputs <- function(team_choices = NULL, team_selected = "") {
      if (!is.null(team_choices)) {
        update_team_choices(team_choices, selected = team_selected)
      } else {
        updateSelectizeInput(session, "team", selected = team_selected, server = FALSE)
      }
      clear_player_choices()
    }

    observeEvent(list(input$team, players_ref()), {
      refresh_player_choices()
    }, ignoreInit = FALSE)

    observeEvent(input$players_on, {
      on_sel <- current_player_values("players_on")
      off_sel <- current_player_values("players_off")
      inter <- intersect(on_sel, off_sel)
      if (length(inter)) {
        updateSelectizeInput(session, "players_off", selected = setdiff(off_sel, inter))
      }
    }, ignoreInit = TRUE)

    observeEvent(input$players_off, {
      on_sel <- current_player_values("players_on")
      off_sel <- current_player_values("players_off")
      inter <- intersect(on_sel, off_sel)
      if (length(inter)) {
        updateSelectizeInput(session, "players_on", selected = setdiff(on_sel, inter))
      }
    }, ignoreInit = TRUE)

    list(
      team = reactive(current_team_value()),
      players_on = reactive(current_player_values("players_on")),
      players_off = reactive(current_player_values("players_off")),
      update_team_choices = update_team_choices,
      refresh_player_choices = refresh_player_choices,
      clear_player_choices = clear_player_choices,
      reset_inputs = reset_inputs
    )
  })
}
