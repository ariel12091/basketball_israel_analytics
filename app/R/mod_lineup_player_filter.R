lineup_player_filter_ui <- function(id,
                                    layout = c("stacked", "inline", "chips"),
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

  # One roster of player chips in place of the three boxes: a mode switch says
  # what a tap does (On / Any of / Off), and a tap on a chip already in that
  # mode clears it. The three selectizes stay in the DOM, hidden, and remain
  # the source of truth -- the chips read and write them -- so restore, row
  # pivots, the filter-chip bar's clears and every server-side reader keep
  # working unchanged. The client half lives in www/app.js ("Lineup player
  # chips"); the group count ("at least k of") is the one input only it sets.
  if (identical(layout, "chips")) {
    mode_button <- function(mode, label, title, checked = FALSE) {
      tags$button(
        type = "button", class = "lineup-chips-mode", role = "radio",
        `data-mode` = mode, title = title,
        `aria-checked` = if (checked) "true" else "false",
        tags$i(class = paste("lineup-chips-mark", mode), `aria-hidden` = "true"),
        label
      )
    }
    return(tagList(
      team_input,
      div(
        id = ns("chips"), class = "lineup-chips", `data-ns` = ns(""),
        `data-mode` = "on",
        div(
          class = "lineup-chips-head",
          tags$span(class = "lineup-chips-eyebrow", id = ns("chips_label"), "Players"),
          div(
            class = "lineup-chips-modes", role = "radiogroup",
            `aria-label` = "What tapping a player does",
            mode_button("on", "On", "Must be on the floor", checked = TRUE),
            # "Any of" matches the filter-chip bar's label for this clause; the
            # summary line gives the exact count once there is a choice.
            mode_button("any", "Any of", "At least one of these must be on the floor -- raise the count in the line below"),
            mode_button("off", "Off", "Must be off the floor")
          )
        ),
        div(class = "lineup-chips-list", role = "group", `aria-labelledby` = ns("chips_label")),
        div(class = "lineup-chips-summary", `aria-live` = "polite"),
        div(
          class = "lineup-chips-model", style = "display: none;", `aria-hidden` = "true",
          players_on_input, players_on_any_input, players_off_input
        )
      )
    ))
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
      tags$p(
        class = "mt-3 mb-2 fw-semibold", "Lineups must include",
        tags$span(class = "ms-1 fw-normal small text-muted", "(players on)")
      ),
      players_on_input,
      players_on_any_input,
      helpText("Leave the second box empty to match on the first alone.")
    ),
    tags$p(
      class = "mt-1 mb-2 fw-semibold", tt("Lineups must exclude", "players_off"),
      tags$span(class = "ms-1 fw-normal small text-muted", "(players off)")
    ),
    players_off_input
  )
}

# Season minutes onto a roster frame, so the chip roster can lead with the
# rotation instead of the alphabet. Best effort: a missing or failed stats pull
# leaves the roster as it was and the chips fall back to name order.
with_season_minutes <- function(players_df, season_df) {
  if (is.null(players_df) || !NROW(players_df)) return(players_df)
  if (is.null(season_df) || !NROW(season_df) ||
      !all(c("team_id", "player_id", "minutes") %in% names(season_df))) {
    return(players_df)
  }
  season_key <- paste(season_df$team_id, season_df$player_id)
  mins <- tapply(suppressWarnings(as.numeric(season_df$minutes)), season_key, sum, na.rm = TRUE)
  players_df$minutes <- as.numeric(mins[paste(players_df$team_id, players_df$player_id)])
  players_df
}

# chips = TRUE pairs with lineup_player_filter_ui(layout = "chips"): the boxes
# are hidden, so they are not pooled against each other (a player moving
# between modes must be settable in any box), and the roster is also sent to
# the chip widget, ordered by season minutes when players_ref carries them.
lineup_player_filter_server <- function(id, players_ref, chips = FALSE) {
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
    restore_seed$players_on_any_min <- sanitize_single_choice(
      restored_input_value(session, "players_on_any_min"),
      numeric_only = TRUE
    )
    restore_seed$available <- any(lengths(list(
      restore_seed$team,
      restore_seed$players_on,
      restore_seed$players_on_any,
      restore_seed$players_off
    )) > 0L)

    # The three player boxes, in precedence order: a player picked in an earlier
    # one is withheld from the later ones' option pools.
    PLAYER_BOXES <- c("players_on", "players_on_any", "players_off")

    # The roster behind those boxes, kept so a pool can be recomputed as players
    # move between them without re-querying.
    roster_choices <- reactiveVal(empty_choices)

    # The chip widget's copy of the roster. any_min is only sent when there is
    # something to restore; the widget otherwise keeps its own count.
    send_chip_roster <- function(roster = NULL, any_min = NULL) {
      if (!isTRUE(chips)) return(invisible(NULL))
      players <- if (is.null(roster) || !NROW(roster)) list() else lapply(
        seq_len(NROW(roster)),
        function(i) list(
          id = as.character(roster$player_id[[i]]),
          name = as.character(roster$name[[i]]),
          min = if ("minutes" %in% names(roster) && !is.na(roster$minutes[[i]])) {
            round(roster$minutes[[i]])
          }
        )
      )
      session$sendCustomMessage("lineup-chips-roster", list(
        id = session$ns("chips"),
        players = players,
        any_min = any_min
      ))
      invisible(NULL)
    }

    clear_player_choices <- function() {
      roster_choices(empty_choices)
      for (box_id in PLAYER_BOXES) {
        updateSelectizeInput(session, box_id, choices = empty_choices, selected = character(0), server = FALSE)
      }
      send_chip_roster()
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
      if (isTRUE(chips) && "minutes" %in% names(roster)) {
        roster <- roster[order(-roster$minutes, roster$name, na.last = TRUE), , drop = FALSE]
      }
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
      # A team pivot can reinstate a player into more than one box at once, so
      # the same precedence the pools enforce is applied to the selections
      # first: earlier boxes in PLAYER_BOXES win.
      selected_any <- setdiff(selected_any, selected_on)
      selected_off <- setdiff(selected_off, c(selected_on, selected_any))
      restored_any_min <- if (isTRUE(restore_seed$available)) restore_seed$players_on_any_min else NULL
      restore_seed$available <- FALSE
      roster_choices(choices)

      selections <- list(
        players_on = selected_on,
        players_on_any = selected_any,
        players_off = selected_off
      )
      for (box_id in PLAYER_BOXES) {
        mine <- selections[[box_id]]
        taken <- unlist(selections[setdiff(PLAYER_BOXES, box_id)], use.names = FALSE)
        updateSelectizeInput(
          session, box_id,
          choices = if (isTRUE(chips)) choices else lineup_box_pool(choices, mine, taken),
          selected = mine,
          server = FALSE
        )
      }
      send_chip_roster(
        roster,
        any_min = if (length(restored_any_min) && length(selected_any) >= 2L) {
          parse_any_min(restored_any_min, length(selected_any) - 1L)
        }
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

    # Each box offers the roster minus whatever the OTHER boxes hold, so a
    # player already claimed simply is not in the list. Registering one handler
    # per box off the vector keeps this to a single rule rather than the six
    # pairwise observers three boxes would otherwise need.
    #
    # Only the other boxes are re-pooled, never the one that just changed: a box
    # updating itself would echo its own value back and risk a feedback loop,
    # and its own pool is already maintained by the other boxes' handlers.
    refresh_other_box_pools <- function(changed_id) {
      choices <- roster_choices()
      if (!length(choices)) return(invisible(NULL))
      for (other_id in setdiff(PLAYER_BOXES, changed_id)) {
        mine <- current_player_values(other_id)
        taken <- unlist(
          lapply(setdiff(PLAYER_BOXES, other_id), current_player_values),
          use.names = FALSE
        )
        updateSelectizeInput(
          session, other_id,
          choices = lineup_box_pool(choices, mine, taken),
          selected = mine,
          server = FALSE
        )
      }
      invisible(NULL)
    }

    # ignoreNULL = FALSE: clearing a box has to return its players to the other
    # pools, and a cleared multi-select reports NULL, which the default would
    # swallow.
    #
    # Chips hide the boxes, so there is no dropdown to narrow.
    if (!isTRUE(chips)) {
      lapply(PLAYER_BOXES, function(box_id) {
        observeEvent(input[[box_id]], refresh_other_box_pools(box_id),
                     ignoreInit = TRUE, ignoreNULL = FALSE)
      })
    }

    list(
      team = reactive(current_team_value()),
      players_on = reactive(current_player_values("players_on")),
      players_on_any = reactive(current_player_values("players_on_any")),
      players_off = reactive(current_player_values("players_off")),
      # "At least k of" the any-of box. Only the chip widget sets it; every
      # other layout leaves it NULL, which is 1 -- the plain any-of.
      players_on_any_min = reactive(parse_any_min(
        input$players_on_any_min,
        length(current_player_values("players_on_any"))
      )),
      update_team_choices = update_team_choices,
      refresh_player_choices = refresh_player_choices,
      clear_player_choices = clear_player_choices,
      reset_inputs = reset_inputs
    )
  })
}
