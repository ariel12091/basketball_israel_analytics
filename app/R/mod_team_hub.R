# mod_team_hub.R — Home team hub: identity card, key players, best/worst
# lineups, storylines. Plain server-function pattern (not a namespaced module)
# because it drives top-level inputs (main_tabs, home_team, teams).

# File-scope fetchers let app.R prewarm the same cache keys.
hub_fetch_team_ratings <- function(gy, ver) {
  cached_season_df(
    list("team_ppp_ratings_mv", as.integer(gy), ver),
    function() {
      tryCatch(
        db_get_query(
          pg_pool,
          "SELECT game_year, team_id, team_name, off_ppp, def_ppp, net_rtg,
                  games_played, wins, losses, off_poss, def_poss,
                  rank_net_rtg, rank_off_ppp, rank_def_ppp
             FROM basketball_test.team_ppp_ratings_mv
            WHERE game_year = $1::int4
            ORDER BY rank_net_rtg",
          params = list(as.integer(gy))
        ),
        error = function(e) NULL
      )
    }
  )
}

hub_fetch_team_ff <- function(gy, ver) {
  cached_season_df(
    list("team_four_factors_mv", as.integer(gy), ver),
    function() {
      tryCatch(
        db_get_query(
          pg_pool,
          "SELECT *
             FROM basketball_test.team_four_factors_mv
            WHERE game_year = $1::int4",
          params = list(as.integer(gy))
        ),
        error = function(e) NULL
      )
    }
  )
}

team_hub_ui <- function() {
  div(
    id = "team_hub_section",
    uiOutput("hub_identity"),
    fluidRow(
      style = "align-items: stretch;",
      column(width = 6, uiOutput("hub_players")),
      column(width = 6, uiOutput("hub_lineups"))
    ),
    uiOutput("hub_storylines")
  )
}

server_team_hub <- function(input, output, session, shared) {
  hub_auto_selected <- reactiveVal("")
  hub_remembered_seen <- reactiveVal(FALSE)

  # Populate the selector and choose remembered team when it first arrives.
  # If localStorage arrives just after the leader fallback, it may replace that
  # automatic choice, but it never replaces a different valid user selection.
  observe({
    teams <- shared$teams_for_year_df()
    req(!is.null(teams), nrow(teams) > 0)
    team_ids <- as.character(teams$team_id)
    choices <- c(
      "",
      stats::setNames(team_ids, as.character(teams$team_name))
    )
    current <- as.character(input$home_team %||% "")
    current_valid <- length(current) == 1L &&
      nzchar(current) &&
      current %in% team_ids
    remembered_received <- !is.null(input$hub_remembered_team)
    may_apply_remembered <- remembered_received &&
      !isTRUE(hub_remembered_seen()) &&
      (!current_valid || identical(current, hub_auto_selected()))

    ratings <- tryCatch(
      hub_fetch_team_ratings(
        as.integer(shared$selected_game_year()),
        shared_data_version(shared)
      ),
      error = function(e) NULL
    )
    selected <- if (may_apply_remembered) {
      hub_default_team(input$hub_remembered_team, teams, ratings)
    } else if (current_valid) {
      current
    } else {
      hub_default_team("", teams, ratings)
    }

    updateSelectizeInput(
      session,
      "home_team",
      choices = choices,
      selected = selected,
      server = TRUE
    )
    if (!identical(selected, current)) hub_auto_selected(selected)
    if (remembered_received) hub_remembered_seen(TRUE)
  }) |>
    bindEvent(
      shared$teams_for_year_df(),
      input$hub_remembered_team,
      ignoreNULL = FALSE
    )

  observeEvent(input$home_team, {
    tid <- as.character(input$home_team %||% "")
    if (length(tid) == 1L && nzchar(tid)) {
      session$sendCustomMessage(
        "ibpl-store-hub-team",
        list(teamId = tid)
      )
    }
  }, ignoreInit = TRUE)

  hub_ver <- reactive(shared_data_version(shared))
  hub_gy <- reactive({
    gy <- suppressWarnings(as.integer(shared$selected_game_year()))
    req(is.finite(gy))
    gy
  })
  hub_team_id <- reactive({
    tid <- as.character(input$home_team %||% "")
    req(nzchar(tid))
    tid
  })

  hub_ratings_df <- reactive(hub_fetch_team_ratings(hub_gy(), hub_ver()))
  hub_ff_df <- reactive(hub_fetch_team_ff(hub_gy(), hub_ver()))

  hub_onoff_df <- reactive({
    gy <- hub_gy()
    cached_season_df(
      list("onoff_default_mv", gy, hub_ver()),
      function() {
        tryCatch(
          db_get_query(
            pg_pool,
            'SELECT *
               FROM basketball_test.onoff_default_mv
              WHERE "Year" = $1::int4
              ORDER BY "Net RTG Diff" DESC, "Team", "Last Name", "First Name"',
            params = list(gy)
          ),
          error = function(e) NULL
        )
      }
    )
  })

  hub_ts_df <- reactive({
    gy <- hub_gy()
    cached_season_df(
      list("player_traditional_stats_mv", gy, hub_ver()),
      function() {
        raw <- tryCatch(
          db_get_query(
            pg_pool,
            "SELECT *
               FROM basketball_test.player_traditional_stats_mv
              WHERE game_year = $1",
            params = list(gy)
          ),
          error = function(e) NULL
        )
        if (is.null(raw)) return(NULL)
        normalize_ts_result_cols(raw)
      }
    )
  })

  hub_lineups_df <- reactive({
    gy <- hub_gy()
    tid <- hub_team_id()
    bounds <- shared$season_date_bounds(as.character(gy))
    allowed <- guard_heavy_request(
      session,
      key = "hub_lineups",
      max_calls = 20L,
      window_sec = 60L
    )
    if (!isTRUE(allowed)) return(NULL)
    cached_season_df(
      list("hub_lineups", tid, gy, hub_ver()),
      function() {
        tryCatch(
          db_get_query(
            pg_pool,
            paste0(
              "SELECT * FROM basketball_test.fetch_lineups_csv_v2(",
              "$1::int4,$2::text,$3::text,$4::text,$5::bool,$6::date,$7::date,$8::int4,$9::int4,",
              "$10::text,$11::text,$12::text,$13::text,$14::text,$15::int4,$16::text,$17::int4,$18::text,$19::int4,$20::bool,",
              "$21::int4,$22::int4,$23::int4,$24::int4,$25::int4,$26::int4,$27::int4,$28::int4,$29::int4",
              ")"
            ),
            params = list(
              5L,
              tid,
              NA_character_,
              NA_character_,
              FALSE,
              as.Date(bounds$start),
              as.Date(bounds$end),
              100L,
              gy,
              NA_character_,
              NA_character_,
              NA_character_,
              NA_character_,
              NA_character_,
              NA_integer_,
              NA_character_,
              NA_integer_,
              NA_character_,
              NA_integer_,
              FALSE,
              NA_integer_,
              NA_integer_,
              NA_integer_,
              NA_integer_,
              NA_integer_,
              NA_integer_,
              NA_integer_,
              NA_integer_,
              NA_integer_
            )
          ),
          error = function(e) NULL
        )
      }
    )
  })

  # League-wide dynamic pulls: one per storyline variant, season and ETL cycle.
  hub_dyn_variants <- list(
    starters_hi = list(off_min = 3L, off_max = 5L),
    starters_lo = list(off_min = 0L, off_max = 2L),
    clutch = list(max_margin = 5L, max_time = 300L),
    last10 = list(last_n = 10L),
    top4 = list(opp_side = "top", opp_n = 4L, opp_metric = "net"),
    bottom4 = list(opp_side = "bottom", opp_n = 4L, opp_metric = "net")
  )

  hub_dyn_df <- function(variant) {
    gy <- hub_gy()
    variant_args <- hub_dyn_variants[[variant]]
    if (is.null(variant_args)) return(NULL)
    cached_season_df(
      list("hub_team_dyn", variant, gy, hub_ver()),
      function() {
        allowed <- guard_heavy_request(
          session,
          key = "hub_storylines",
          max_calls = 20L,
          window_sec = 60L
        )
        if (!isTRUE(allowed)) return(NULL)
        tryCatch(
          db_get_query(
            pg_pool,
            paste0(
              "SELECT * FROM basketball_test.get_team_ratings_dynamic(",
              "$1::int4,$2::date,$3::date,$4::text,$5::text,$6::text,$7::text,$8::text,$9::int4,$10::text,",
              "$11::int4,$12::text,$13::int4,$14::bool,$15::int4,$16::int4,$17::int4,",
              "$18::int4,$19::int4,$20::int4,$21::int4,$22::int4,$23::int4",
              ")"
            ),
            params = list(
              gy,
              NA,
              NA,
              NA_character_,
              NA_character_,
              NA_character_,
              NA_character_,
              variant_args$opp_side %||% NA_character_,
              variant_args$opp_n %||% NA_integer_,
              variant_args$opp_metric %||% NA_character_,
              variant_args$max_margin %||% NA_integer_,
              NA_character_,
              variant_args$max_time %||% NA_integer_,
              FALSE,
              NA_integer_,
              NA_integer_,
              variant_args$last_n %||% NA_integer_,
              NA_integer_,
              NA_integer_,
              variant_args$off_min %||% NA_integer_,
              variant_args$off_max %||% NA_integer_,
              NA_integer_,
              NA_integer_
            )
          ),
          error = function(e) NULL
        )
      }
    )
  }

  hub_team_row <- function(df) {
    tid <- suppressWarnings(as.integer(hub_team_id()))
    if (is.null(df) || !nrow(df) || !is.finite(tid)) return(NULL)
    row <- df[as.integer(df$team_id) == tid, , drop = FALSE]
    if (!nrow(row)) return(NULL)
    row[1, , drop = FALSE]
  }

  hub_story_pair <- function(df_a, df_b) {
    row_a <- hub_team_row(df_a)
    row_b <- hub_team_row(df_b)
    if (!is.null(row_a)) {
      row_a$net_rtg_rank <- hub_net_rtg_rank(df_a, hub_team_id())
    }
    if (!is.null(row_b)) {
      row_b$net_rtg_rank <- hub_net_rtg_rank(df_b, hub_team_id())
    }
    list(a = row_a, b = row_b)
  }

  # ---- Identity card ----
  output$hub_identity <- renderUI({
    info <- hub_identity_data(hub_ratings_df(), hub_ff_df(), hub_team_id())
    if (is.null(info)) return(NULL)
    row <- info$row
    n_teams <- info$n_teams
    mini <- hub_ff_mini(hub_ff_df(), hub_team_id())
    stat <- function(label, value, stat_rank) {
      div(
        class = "hub-stat",
        div(class = "hub-stat-value", value),
        div(class = "hub-stat-label", label),
        div(
          class = "hub-stat-rank",
          sprintf("%s of %d", hub_ordinal(stat_rank), n_teams)
        )
      )
    }
    div(
      class = "card bg-dark border-secondary mb-4 hub-card js-shiny-event",
      `data-input-id` = "hub_go_team",
      role = "button",
      div(
        class = "card-body",
        div(
          class = "d-flex justify-content-between align-items-baseline mb-2",
          tags$h5(class = "card-title mb-0", as.character(row$team_name)),
          tags$span(
            class = "hub-record",
            sprintf("%d–%d", as.integer(row$wins), as.integer(row$losses))
          )
        ),
        div(
          class = "hub-stat-row",
          stat(
            "Off PPP",
            sprintf("%.1f", as.numeric(row$off_ppp)),
            row$rank_off_ppp
          ),
          stat(
            "Def PPP",
            sprintf("%.1f", as.numeric(row$def_ppp)),
            row$rank_def_ppp
          ),
          stat(
            "Net",
            sprintf("%+.1f", as.numeric(row$net_rtg)),
            row$rank_net_rtg
          )
        ),
        if (!is.null(mini)) {
          div(
            class = "hub-ff-row",
            lapply(seq_len(nrow(mini)), function(i) {
              tags$span(
                class = "hub-ff-chip",
                sprintf(
                  "%s %.1f (%s)",
                  mini$label[[i]],
                  mini$value[[i]],
                  hub_ordinal(mini$rank[[i]])
                )
              )
            })
          )
        }
      )
    )
  })

  # ---- Key players ----
  output$hub_players <- renderUI({
    key_players <- hub_key_players(hub_onoff_df(), hub_team_id())
    scorer <- hub_top_scorer(hub_ts_df(), hub_team_id())
    if (is.null(key_players) && is.null(scorer)) return(NULL)
    div(
      class = "card bg-dark border-secondary mb-4 h-100 hub-card js-shiny-event",
      `data-input-id` = "hub_go_players",
      role = "button",
      div(
        class = "card-body",
        tags$h6(class = "hub-block-title", "Key players (on/off impact)"),
        if (!is.null(key_players)) {
          tags$ul(
            class = "hub-list",
            lapply(seq_len(nrow(key_players)), function(i) {
              delta <- as.numeric(key_players[["Net RTG Diff"]][[i]])
              tags$li(
                tags$span(
                  class = "hub-player-name",
                  paste(
                    key_players[["First Name"]][[i]],
                    key_players[["Last Name"]][[i]]
                  )
                ),
                tags$span(
                  class = if (delta >= 0) "hub-pos" else "hub-neg",
                  sprintf("%+.1f / 100", delta)
                )
              )
            })
          )
        },
        if (!is.null(scorer)) {
          tags$p(
            class = "hub-footnote",
            sprintf(
              "Top scorer: %s — %.1f ppg",
              as.character(scorer$player_name %||% scorer$Player),
              scorer$ppg
            )
          )
        }
      )
    )
  })

  # ---- Best/worst lineups ----
  output$hub_lineups <- renderUI({
    best_worst <- hub_best_worst_lineups(hub_lineups_df())
    if (is.null(best_worst)) return(NULL)
    lineup_row <- function(label, row, class_name) {
      div(
        class = "hub-lineup",
        tags$span(class = paste("hub-lineup-tag", class_name), label),
        tags$span(
          class = "hub-lineup-players",
          as.character(row$player_names_str)
        ),
        tags$span(
          class = class_name,
          sprintf(
            "%+.1f net, %d poss",
            as.numeric(row$net_rtg),
            as.integer(row$total_poss)
          )
        )
      )
    }
    div(
      class = "card bg-dark border-secondary mb-4 h-100 hub-card js-shiny-event",
      `data-input-id` = "hub_go_lineups",
      role = "button",
      div(
        class = "card-body",
        tags$h6(class = "hub-block-title", "Lineups (min 100 poss)"),
        lineup_row("Best", best_worst$best, "hub-pos"),
        lineup_row("Worst", best_worst$worst, "hub-neg")
      )
    )
  })

  # ---- Deep links ----
  observeEvent(input$hub_go_team, {
    updateTabsetPanel(session, "main_tabs", selected = "team_ratings")
  })

  observeEvent(input$hub_go_players, {
    teams_df <- shared$teams_for_year_df()
    team_choices <- stats::setNames(
      as.character(teams_df$team_id),
      as.character(teams_df$team_name)
    )
    tid <- as.character(input$home_team %||% "")
    if (nzchar(tid) && tid %in% unname(team_choices)) {
      updateSelectizeInput(
        session,
        "teams",
        choices = team_choices,
        selected = tid,
        server = TRUE
      )
    }
    updateTabsetPanel(session, "main_tabs", selected = "onoff")
  })

  observeEvent(input$hub_go_lineups, {
    tid <- as.character(input$home_team %||% "")
    if (nzchar(tid)) shared$pending_ld_team(tid)
    updateRadioButtons(session, "ld_num", selected = "5")
    updateTabsetPanel(session, "main_tabs", selected = "lineup_data")
  })

  output$hub_storylines <- renderUI({
    fetch_pair <- function(id) {
      switch(
        id,
        starters_bench = hub_story_pair(
          hub_dyn_df("starters_hi"),
          hub_dyn_df("starters_lo")
        ),
        clutch = hub_story_pair(
          hub_dyn_df("clutch"),
          hub_ratings_df()
        ),
        last10 = hub_story_pair(
          hub_dyn_df("last10"),
          hub_ratings_df()
        ),
        top_bottom_4 = hub_story_pair(
          hub_dyn_df("top4"),
          hub_dyn_df("bottom4")
        ),
        NULL
      )
    }
    lines <- hub_storyline_lines(hub_storyline_specs(), fetch_pair)
    if (!length(lines)) return(NULL)
    div(
      class = "card bg-dark border-secondary mb-4 hub-card-static",
      div(
        class = "card-body",
        tags$h6(class = "hub-block-title", "Storylines"),
        lapply(lines, function(line) {
          tags$span(
            class = "hub-story-line js-shiny-event",
            `data-input-id` = "hub_story_click",
            `data-shiny-value` = line$id,
            line$text
          )
        })
      )
    )
  })

  observeEvent(input$hub_story_click, {
    storyline_id <- as.character(input$hub_story_click %||% "")
    specs <- hub_storyline_specs()
    spec <- NULL
    for (candidate in specs) {
      if (identical(candidate$id, storyline_id)) spec <- candidate
    }
    if (is.null(spec)) return()
    if (nzchar(spec$preset)) {
      shared$pending_compare_preset(list(
        preset = spec$preset,
        team_id = as.character(input$home_team %||% ""),
        open_detail = TRUE
      ))
      updateTabsetPanel(session, "main_tabs", selected = "compare")
    } else {
      updateTabsetPanel(session, "main_tabs", selected = "team_ratings")
    }
  })
}
