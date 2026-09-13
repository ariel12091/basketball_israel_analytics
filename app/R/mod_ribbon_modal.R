ribbon_inline_ui <- function(prefix) {
  div(
    id = paste0(prefix, "_ribbon_inline_panel"),
    class = "ibpl-ribbon-inline-panel",
    hidden = "hidden",
    div(
      class = "ibpl-ribbon-inline-head",
      tags$strong("Gameflow"),
      tags$button(type = "button", class = "ibpl-ribbon-inline-close",
                  `aria-label` = "Close gameflow", "Close")
    ),
    div(class = "ibpl-ribbon-inline-loading", "Loading gameflow…"),
    uiOutput(paste0(prefix, "_ribbon_inline"))
  )
}

ribbon_modal_server <- function(input, output, session, prefix, league,
                                 data_version_fn, svg_id_prefix = prefix) {
  observeEvent(input[[paste0(prefix, "_ribbon_click")]], {
    click <- input[[paste0(prefix, "_ribbon_click")]]
    req(click$game_id, click$team_id)
    mobile <- isTRUE(click$mobile)

    allowed <- guard_heavy_request(
      session,
      key = "ribbon_open",
      max_calls = 20L,
      window_sec = 60L
    )
    if (!isTRUE(allowed)) {
      if (mobile) {
        output[[paste0(prefix, "_ribbon_inline")]] <- renderUI({
          div(class = "ibpl-ribbon-inline-result",
              `data-game-id` = as.character(click$game_id),
              div(class = "alert alert-warning mb-0", "Please try again in a moment."))
        })
      }
      return()
    }

    ribbon <- fetch_stint_ribbon(
      pg_pool, league, click$game_id, click$team_id,
      data_version = data_version_fn()
    )

    if (is.null(ribbon) || !nrow(ribbon$lanes)) {
      if (mobile) {
        output[[paste0(prefix, "_ribbon_inline")]] <- renderUI({
          div(class = "ibpl-ribbon-inline-result",
              `data-game-id` = as.character(click$game_id),
              div(class = "alert alert-warning mb-0",
                  "This game has no segment data to draw."))
        })
      } else {
        showModal(modalDialog(title = "No lineup data",
                              "This game has no segment data to draw.",
                              easyClose = TRUE))
      }
      return()
    }

    meta <- ribbon$meta
    own_team <- click$own_team %||% ""
    opp_team <- click$opp_team %||% ""
    meta$own_team <- if (nzchar(own_team)) own_team else "Own"
    meta$opp_team <- if (nzchar(opp_team)) opp_team else "Opponent"
    meta$game_label <- if (nzchar(own_team) && nzchar(opp_team)) {
      sprintf("%s vs %s", own_team, opp_team)
    } else {
      sprintf("Game %s", click$game_id)
    }

    health_ui <- if (!is.null(ribbon$health)) {
      div(class = "alert alert-warning py-2 px-3 mb-2", ribbon$health)
    }
    # A phone gets the compact layout: the whole game across the screen
    # rather than the 1070-unit desktop chart panned sideways.
    svg <- build_stint_ribbon_svg(ribbon$lanes, ribbon$margin, meta,
                                  id_prefix = paste0(svg_id_prefix, click$game_id),
                                  steps = ribbon$steps,
                                  layout = ribbon_layout(compact = mobile))

    if (mobile) {
      output[[paste0(prefix, "_ribbon_inline")]] <- renderUI({
        div(class = "ibpl-ribbon-inline-result",
            `data-game-id` = as.character(click$game_id),
          div(class = "ibpl-ribbon-inline-title", meta$game_label),
          health_ui,
          div(class = "ibpl-ribbon-inline-hint",
              "Swipe sideways for the rest of the game. Tap a player's row for that stint and its lineups; the number beside each name is their game +/-."),
          div(class = "ibpl-ribbon-inline-scroll",
              `aria-label` = paste("Gameflow for", meta$game_label), svg)
        )
      })
      return()
    }

    output[[paste0(prefix, "_ribbon_svg")]] <- renderUI({ tagList(health_ui, svg) })

    showModal(modalDialog(
      title = meta$game_label,
      uiOutput(paste0(prefix, "_ribbon_svg")),
      size = "xl",
      easyClose = TRUE
    ))
  })
}
