lineup_player_filter_ui <- function(id,
                                    layout = c("stacked", "inline"),
                                    team_label = "Team",
                                    team_help = NULL,
                                    team_placeholder = "All teams",
                                    players_on_label = tt("every one of", "players_on"),
                                    players_on_any_label = tt("and at least one of", "players_on_any"),
                                    players_off_label = NULL,
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
  # The second on-selector contributes one "at least one of these" clause, so
  # "A here, B and C there" reads A AND (B OR C). On its own it is the plain
  # any-of filter; left empty, the filter is exactly the historical all-of one.
  players_on_any_input <- selectizeInput(
    ns("players_on_any"),
    players_on_any_label,
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
        tags$span(class = "text-muted small text-nowrap", "Team"),
        div(style = "min-width: 140px;", team_input),
        tags$span(class = "text-muted small text-nowrap", "include all of"),
        div(style = "min-width: 150px;", players_on_input),
        tags$span(class = "text-muted small text-nowrap", "and any of"),
        div(style = "min-width: 150px;", players_on_any_input),
        tags$span(class = "text-muted small text-nowrap", "exclude"),
        div(style = "min-width: 150px;", players_off_input)
      )
    )
  }

  # The two on-selectors are read as one sentence: the connective "and" sits
  # between them, where the relationship actually is, rather than inside a
  # trailing parenthetical on two labels that would otherwise be identical
  # until their last word. The exclusion is a separate sentence because it is a
  # separate clause, not a third alternative.
  tagList(
    team_input,
    if (!is.null(team_help)) helpText(team_help),
    div(
      class = "mb-3",
      tags$p(class = "mt-3 mb-2 fw-semibold", "Lineups must include"),
      players_on_input,
      players_on_any_input,
      helpText("Leave the second box empty to match on the first alone.")
    ),
    tags$p(class = "mt-1 mb-2 fw-semibold", tt("Lineups must exclude", "players_off")),
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
    restore_seed$players_on_any <- sanitize_persisted_choices(
      restored_input_value(session, "players_on_any"),
      numeric_only = TRUE
    )
    restore_seed$players_off <- sanitize_persisted_choices(
      restored_input_value(session, "players_off"),
      numeric_only = TRUE
    )
    restore_seed$available <- any(lengths(list(
      restore_seed$team,
      restore_seed$players_on,
      restore_seed$players_on_any,
      restore_seed$players_off
    )) > 0L)

    # The three player boxes, in precedence order: a player picked in an earlier
    # one is removed from the later ones.
    PLAYER_BOXES <- c("players_on", "players_on_any", "players_off")

    clear_player_choices <- function() {
      for (box_id in PLAYER_BOXES) {
        updateSelectizeInput(session, box_id, choices = empty_choices, selected = character(0), server = FALSE)
      }
    }

    selection_with_restore_seed <- function(input_id, current, choices, max_len = 80L) {
      selected <- sanitize_persisted_choices(
        current,
        max_len = max_len,
        numeric_only = TRUE
      )
      if (!length(selected) && isTRUE(restore_seed$available)) {
        selected <- sanitize_persisted_choices(
          restore_seed[[input_id]],
          max_len = max_len,
          numeric_only = TRUE
        )
      }
      intersect(selected, as.character(unname(choices)))
    }

    update_team_choices <- function(choices, selected = "") {
      selected <- selection_with_restore_seed(
        "team", selected, choices, max_len = 1L
      )
      updateSelectizeInput(session, "team", choices = choices, selected = selected, server = FALSE)
      invisible(selected)
    }

    current_team_value <- function() {
      sanitize_single_choice(input$team, numeric_only = TRUE)
    }

    current_player_values <- function(input_id) {
      sanitize_persisted_choices(input[[input_id]], numeric_only = TRUE)
    }

    refresh_player_choices <- function(team_value, players_on = NULL) {
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
      # NULL means no pivot: preserve whatever is selected. A value states the
      # selection outright, including character(0) for a team-level pivot, which
      # has to clear a player left behind by an earlier one. Team-scoped choices,
      # so a player off this roster is dropped either way.
      selected_on <- if (is.null(players_on)) {
        selection_with_restore_seed("players_on", input$players_on, choices)
      } else {
        intersect(sanitize_persisted_choices(players_on, numeric_only = TRUE),
                  unname(choices))
      }
      selected_any <- selection_with_restore_seed(
        "players_on_any", input$players_on_any, choices
      )
      selected_off <- selection_with_restore_seed(
        "players_off", input$players_off, choices
      )
      # Same precedence the exclusion observers use, applied here too because a
      # team pivot can reinstate a player into more than one box at once.
      selected_any <- setdiff(selected_any, selected_on)
      selected_off <- setdiff(selected_off, c(selected_on, selected_any))
      restore_seed$available <- FALSE

      updateSelectizeInput(
        session, "players_on",
        choices = choices,
        selected = selected_on,
        server = FALSE
      )
      updateSelectizeInput(
        session, "players_on_any",
        choices = choices,
        selected = selected_any,
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
        players_on_any = selected_any,
        players_off = selected_off
      ))
    }

    reset_inputs <- function(team_choices = NULL, team_selected = "") {
      restore_seed$available <- FALSE
      if (!is.null(team_choices)) {
        update_team_choices(team_choices, selected = team_selected)
      } else {
        updateSelectizeInput(session, "team", selected = team_selected, server = FALSE)
      }
      clear_player_choices()
    }

    observeEvent(input$team, {
      refresh_player_choices()
    }, ignoreInit = TRUE)

    # A player belongs to exactly one of the three boxes. With three of them the
    # old pairwise observers would need six copies, so one handler is registered
    # per box and clears that box's picks out of the other two. Precedence is
    # the PLAYER_BOXES order, which is also the order they read on screen.
    enforce_exclusive_boxes <- function(changed_id) {
      changed <- current_player_values(changed_id)
      if (!length(changed)) return(invisible(NULL))
      for (other_id in setdiff(PLAYER_BOXES, changed_id)) {
        vals <- current_player_values(other_id)
        keep <- setdiff(vals, changed)
        if (length(keep) != length(vals)) {
          updateSelectizeInput(session, other_id, selected = keep)
        }
      }
      invisible(NULL)
    }

    lapply(PLAYER_BOXES, function(box_id) {
      observeEvent(input[[box_id]], enforce_exclusive_boxes(box_id), ignoreInit = TRUE)
    })

    list(
      team = reactive(current_team_value()),
      players_on = reactive(current_player_values("players_on")),
      players_on_any = reactive(current_player_values("players_on_any")),
      players_off = reactive(current_player_values("players_off")),
      update_team_choices = update_team_choices,
      refresh_player_choices = refresh_player_choices,
      clear_player_choices = clear_player_choices,
      reset_inputs = reset_inputs
    )
  })
}
