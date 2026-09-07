ribbon_modal_server <- function(input, output, session, prefix, league,
                                 data_version_fn, svg_id_prefix = prefix) {
  observeEvent(input[[paste0(prefix, "_ribbon_click")]], {
    click <- input[[paste0(prefix, "_ribbon_click")]]
    req(click$game_id, click$team_id)

    allowed <- guard_heavy_request(
      session,
      key = "ribbon_open",
      max_calls = 20L,
      window_sec = 60L
    )
    if (!isTRUE(allowed)) return()

    ribbon <- fetch_stint_ribbon(
      pg_pool, league, click$game_id, click$team_id,
      data_version = data_version_fn()
    )

    if (is.null(ribbon) || !nrow(ribbon$lanes)) {
      showModal(modalDialog(title = "No lineup data",
                            "This game has no segment data to draw.",
                            easyClose = TRUE))
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

    output[[paste0(prefix, "_ribbon_svg")]] <- renderUI({
      tagList(
        if (!is.null(ribbon$health)) {
          div(class = "alert alert-warning py-2 px-3 mb-2", ribbon$health)
        },
        build_stint_ribbon_svg(ribbon$lanes, ribbon$margin, meta,
                               id_prefix = paste0(svg_id_prefix, click$game_id),
                               steps = ribbon$steps),
        ribbon_detail_strip()
      )
    })

    showModal(modalDialog(
      title = meta$game_label,
      uiOutput(paste0(prefix, "_ribbon_svg")),
      size = "xl",
      easyClose = TRUE
    ))
  })
}
