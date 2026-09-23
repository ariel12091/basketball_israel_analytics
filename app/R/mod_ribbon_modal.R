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
    div(
      class = "ibpl-ribbon-inline-loading",
      paste0("Loading gameflow", intToUtf8(8230L))
    ),
    uiOutput(paste0(prefix, "_ribbon_inline"))
  )
}

ribbon_mobile_overview_ui <- function(margin, bounds) {
  total <- bounds[length(bounds)]
  width <- 320
  height <- 56
  plot_top <- 4
  plot_height <- 44
  path <- ribbon_margin_path(margin, total, width, plot_top,
                             plot_height, gutter = 0)
  div(
    class = "ibpl-ribbon-mobile-overview",
    div(class = "ibpl-ribbon-overview-title", "Score margin across the game"),
    tags$svg(
      class = "ibpl-ribbon-overview-svg",
      viewBox = sprintf("0 0 %d %d", width, height),
      role = "img",
      `aria-label` = "Full-game score margin; positive values favor the first team",
      tags$line(class = "ibpl-ribbon-overview-zero", x1 = 0, x2 = width,
                y1 = plot_top + plot_height / 2,
                y2 = plot_top + plot_height / 2),
      lapply(bounds[-length(bounds)], function(end) {
        x <- end / total * width
        tags$line(class = "ibpl-ribbon-overview-period", x1 = x, x2 = x,
                  y1 = 0, y2 = height)
      }),
      tags$path(class = "ibpl-ribbon-overview-line", d = path)
    ),
    div(class = "ibpl-ribbon-quarter-nav", role = "group",
        `aria-label` = "Jump to quarter",
        lapply(seq_along(bounds), function(i) {
          label <- if (i <= 4L) paste0("Q", i) else paste0("OT", i - 4L)
          tags$button(type = "button", class = "ibpl-ribbon-quarter-jump",
                      `data-quarter` = i,
                      `aria-label` = paste("Jump to", label), label)
        }))
  )
}

# Game 406's Q4 clock is reconstructed from wall-entry time because the
# provider froze every real-Q4 action at 00:00/00:01. Put that qualification
# in the chart's warning area, where there is room to explain its scope.
ribbon_game_warning <- function(league, game_id, health = NULL) {
  id <- suppressWarnings(as.integer(game_id))
  if (!identical(as.character(league), "israel") ||
      length(id) != 1L || is.na(id) || id != 406L) {
    return(health)
  }
  paste(
    "Q4 timing is approximate:",
    "the provider's game clock was unusable, so Q4 events were positioned",
    "on the Gameflow using their wall-clock timestamps."
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
    title_ui <- meta$game_label
    warning <- ribbon_game_warning(league, click$game_id, ribbon$health)
    health_ui <- if (!is.null(warning)) {
      div(class = "alert alert-warning py-2 px-3 mb-2", warning)
    }
    # A phone gets the compact layout: the whole game across the screen
    # rather than the 1070-unit desktop chart panned sideways.
    svg <- build_stint_ribbon_svg(ribbon$lanes, ribbon$margin, meta,
                                  id_prefix = paste0(svg_id_prefix, click$game_id),
                                  steps = ribbon$steps,
                                  layout = ribbon_layout(compact = mobile))

    if (mobile) {
      bounds <- ribbon_period_bounds(meta$n_periods)
      output[[paste0(prefix, "_ribbon_inline")]] <- renderUI({
        div(class = "ibpl-ribbon-inline-result",
            `data-game-id` = as.character(click$game_id),
          div(class = "ibpl-ribbon-inline-title", title_ui),
          health_ui,
          div(class = "ibpl-ribbon-inline-hint",
              "Scroll through the quarters. Tap a player's row for that stint and its lineups."),
          ribbon_mobile_overview_ui(ribbon$margin, bounds),
          div(class = "ibpl-ribbon-inline-scroll",
              `aria-label` = paste("Gameflow for", meta$game_label), svg)
        )
      })
      return()
    }

    output[[paste0(prefix, "_ribbon_svg")]] <- renderUI({ tagList(health_ui, svg) })

    showModal(modalDialog(
      title = title_ui,
      uiOutput(paste0(prefix, "_ribbon_svg")),
      size = "xl",
      easyClose = TRUE
    ))
  })
}
