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
}
