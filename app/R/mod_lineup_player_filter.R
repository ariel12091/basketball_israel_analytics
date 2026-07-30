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
    restore_seed <- new.env(parent = emptyenv())
    restore_seed$team <- sanitize_persisted_choices(
      restored_input_value(session, "team"),
      max_len = 1L,
      numeric_only = TRUE
    )
    restore_seed$players_on <- sanitize_persisted_choices(
      restored_input_value(session, "players_on"),
      numeric_only = TRUE
    )
    restore_seed$players_off <- sanitize_persisted_choices(
      restored_input_value(session, "players_off"),
      numeric_only = TRUE
    )
    restore_seed$available <- any(lengths(list(
      restore_seed$team,
      restore_seed$players_on,
      restore_seed$players_off
    )) > 0L)
    # Values pushed with updateSelectizeInput() that the browser has not echoed
    # back yet. Without them an observer that runs between the push and the echo
    # sees a blank input and overwrites what was just restored.
    restore_seed$pending <- list()

    remember_pending <- function(input_id, value) {
      restore_seed$pending[[input_id]] <- value
      invisible(value)
    }

    clear_pending <- function(input_id) {
      restore_seed$pending[[input_id]] <- NULL
      invisible(NULL)
    }

    clear_player_choices <- function() {
      remember_pending("players_on", character(0))
      remember_pending("players_off", character(0))
      updateSelectizeInput(session, "players_on", choices = empty_choices, selected = character(0), server = FALSE)
      updateSelectizeInput(session, "players_off", choices = empty_choices, selected = character(0), server = FALSE)
    }

    selection_with_restore_seed <- function(input_id, current, choices, max_len = 80L) {
      pick <- function(x) {
        sanitize_persisted_choices(x, max_len = max_len, numeric_only = TRUE)
      }
      selected <- pick(current)
      if (!length(selected)) selected <- pick(restore_seed$pending[[input_id]])
      if (!length(selected) && isTRUE(restore_seed$available)) {
        selected <- pick(restore_seed[[input_id]])
      }
      intersect(selected, as.character(unname(choices)))
    }

    update_team_choices <- function(choices, selected = "") {
      selected <- selection_with_restore_seed(
        "team", selected, choices, max_len = 1L
      )
      remember_pending("team", selected)
      updateSelectizeInput(session, "team", choices = choices, selected = selected, server = FALSE)
      invisible(selected)
    }

    current_team_value <- function() {
      sanitize_single_choice(input$team, numeric_only = TRUE)
    }

    current_player_values <- function(input_id) {
      sanitize_persisted_choices(input[[input_id]], numeric_only = TRUE)
    }

    refresh_player_choices <- function(team_value) {
      team_val <- if (missing(team_value)) {
        current_team_value()
      } else {
        sanitize_single_choice(team_value, numeric_only = TRUE)
      }
      team_val <- team_val[nzchar(team_val)]
      if (!length(team_val)) {
        clear_player_choices()
        restore_seed$available <- FALSE
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
      selected_on <- selection_with_restore_seed(
        "players_on", input$players_on, choices
      )
      selected_off <- selection_with_restore_seed(
        "players_off", input$players_off, choices
      )
      selected_off <- setdiff(selected_off, selected_on)
      restore_seed$available <- FALSE
      remember_pending("players_on", selected_on)
      remember_pending("players_off", selected_off)

      updateSelectizeInput(
        session, "players_on",
        choices = choices,
        selected = selected_on,
        server = FALSE
      )
      updateSelectizeInput(
        session, "players_off",
        choices = choices,
        selected = selected_off,
        server = FALSE
      )
      invisible(list(
        team = team_val,
        players_on = selected_on,
        players_off = selected_off
      ))
    }

    reset_inputs <- function(team_choices = NULL, team_selected = "") {
      restore_seed$available <- FALSE
      if (!is.null(team_choices)) {
        update_team_choices(team_choices, selected = team_selected)
      } else {
        remember_pending("team", team_selected)
        updateSelectizeInput(session, "team", selected = team_selected, server = FALSE)
      }
      clear_player_choices()
    }

    # ignoreNULL = FALSE so that clearing a multi-select still retires its
    # pending value instead of leaving a stale one to be re-applied.
    observeEvent(input$team, {
      clear_pending("team")
      refresh_player_choices()
    }, ignoreInit = TRUE, ignoreNULL = FALSE)

    observeEvent(input$players_on, {
      clear_pending("players_on")
      on_sel <- current_player_values("players_on")
      off_sel <- current_player_values("players_off")
      inter <- intersect(on_sel, off_sel)
      if (length(inter)) {
        updateSelectizeInput(session, "players_off", selected = setdiff(off_sel, inter))
      }
    }, ignoreInit = TRUE, ignoreNULL = FALSE)

    observeEvent(input$players_off, {
      clear_pending("players_off")
      on_sel <- current_player_values("players_on")
      off_sel <- current_player_values("players_off")
      inter <- intersect(on_sel, off_sel)
      if (length(inter)) {
        updateSelectizeInput(session, "players_on", selected = setdiff(on_sel, inter))
      }
    }, ignoreInit = TRUE, ignoreNULL = FALSE)

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
