# mod_team_hub.R — Home team hub: identity card, key players, best/worst
# lineups, storylines. Plain server-function pattern (not a namespaced module)
# because it drives top-level inputs (main_tabs, home_team, teams).

# File-scope fetchers let app.R prewarm the same cache keys.
hub_db_get_query_timed <- function(
  statement,
  params = NULL,
  label,
  session = NULL
) {
  # Server-test mocks are not real pool objects; preserve their normal query
  # path while profiling production pool checkout and SQL separately.
  if (!inherits(pg_pool, "Pool")) {
    return(db_get_query(pg_pool, statement, params = params))
  }

  total_started <- proc.time()[["elapsed"]]
  checkout_ms <- NA_real_
  sql_ms <- NA_real_
  status <- "ok"
  conn <- NULL

  on.exit({
    total_ms <- (proc.time()[["elapsed"]] - total_started) * 1000
    if (exists("app_log", mode = "function")) {
      app_log(
        "hub_storylines_perf",
        sprintf(
          "%s checkout_ms=%.1f sql_ms=%.1f total_ms=%.1f status=%s",
          label,
          checkout_ms,
          sql_ms,
          total_ms,
          status
        ),
        session = session
      )
    }
  }, add = TRUE)

  tryCatch({
    checkout_started <- proc.time()[["elapsed"]]
    conn <- pool::poolCheckout(pg_pool)
    checkout_ms <- (proc.time()[["elapsed"]] - checkout_started) * 1000
    on.exit(pool::poolReturn(conn), add = TRUE)

    sql_started <- proc.time()[["elapsed"]]
    out <- db_get_query(conn, statement, params = params)
    sql_ms <- (proc.time()[["elapsed"]] - sql_started) * 1000
    out
  }, error = function(e) {
    status <<- "error"
    stop(e)
  })
}

hub_fetch_team_ratings <- function(gy, ver) {
  cached_season_df(
    list("team_ppp_ratings_mv", as.integer(gy), ver),
    function() {
      tryCatch(
        db_get_query(
          pg_pool,
          "SELECT game_year, team_id, team_name, off_ppp, def_ppp, net_rtg,
                  games_played, wins, losses, off_poss, def_poss,
                  rank_net_rtg, rank_off_ppp, rank_def_ppp,
                  off_fga, off_layup_att, off_dunk_att, off_fg3_att,
                  off_c3_att, off_c3_known_att,
                  def_fga, def_layup_att, def_dunk_att, def_fg3_att,
                  def_c3_att, def_c3_known_att
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

# Shared by Team Hub and matching Compare presets. NULL means the persisted
# table is unavailable and callers should use the dynamic fallback.
hub_fetch_team_ratings_presets <- function(gy, ver, session = NULL) {
  gy <- as.integer(gy)
  cached_season_df(
    list("team_ratings_preset_cache", gy, ver),
    function() {
      out <- tryCatch(
        hub_db_get_query_timed(
          "SELECT preset_variant AS hub_variant,
                  game_year, team_id, team_name, off_ppp, def_ppp, net_rtg,
                  games_played, wins, losses, off_poss, def_poss,
                  rank_net_rtg, rank_off_ppp, rank_def_ppp,
                  off_fga, off_layup_att, off_dunk_att, off_fg3_att,
                  off_c3_att, off_c3_known_att,
                  def_fga, def_layup_att, def_dunk_att, def_fg3_att,
                  def_c3_att, def_c3_known_att,
                  (SELECT value
                     FROM basketball_test.app_meta
                    WHERE key = 'etl_full_last_success'
                    LIMIT 1) AS data_version
             FROM basketball_test.team_ratings_preset_cache
            WHERE game_year = $1::int4
            ORDER BY preset_variant, rank_net_rtg",
          params = list(gy),
          label = sprintf("preset_cache season=%d", gy),
          session = session
        ),
        error = function(e) NULL
      )
      if (is.null(out)) return(NULL)

      data_version <- if ("data_version" %in% names(out) && nrow(out)) {
        trimws(as.character(out$data_version[[1]] %||% ""))
      } else {
        ""
      }
      if (length(data_version) != 1L || is.na(data_version)) data_version <- ""
      out$data_version <- NULL
      if (nzchar(data_version)) {
        attr(out, "data_version") <- data_version

        # The first Storylines request starts with an unknown cache version.
        # Seed the resolved key before publishing the version so its immediate
        # reactive rerender does not make a second database request.
        if (!identical(data_version, as.character(ver %||% ""))) {
          GL_DATA_CACHE$set(
            rlang::hash(list("team_ratings_preset_cache", gy, data_version)),
            out
          )
        }
      }
      out
    }
  )
}

# One app round trip for every dataset rendered by the Home team analysis.
# The sources remain the existing ETL-refreshed facts. The default lineup
# branch reads sub_lineups_stats directly because that is the exact fast path
# used by fetch_lineups_csv_v2() for Home's full-season, filter-free request.
hub_dashboard_query_sql <- function() {
  paste(
    "/* home_dashboard_combined */ SELECT",
    "(SELECT value FROM basketball_test.app_meta",
    "  WHERE key = 'etl_full_last_success' LIMIT 1) AS data_version,",
    "(SELECT COALESCE(jsonb_agg(to_jsonb(p)",
    "  ORDER BY p.hub_variant, p.rank_net_rtg), '[]'::jsonb)::text",
    " FROM (SELECT preset_variant AS hub_variant, game_year, team_id, team_name,",
    "              off_ppp, def_ppp, net_rtg, games_played, wins, losses,",
    "              off_poss, def_poss, rank_net_rtg, rank_off_ppp, rank_def_ppp,",
    "              off_fga, off_layup_att, off_dunk_att, off_fg3_att,",
    "              off_c3_att, off_c3_known_att, def_fga, def_layup_att,",
    "              def_dunk_att, def_fg3_att, def_c3_att, def_c3_known_att",
    "         FROM basketball_test.team_ratings_preset_cache",
    "        WHERE game_year = $1::int4) p) AS storylines_json,",
    "(SELECT COALESCE(jsonb_agg(to_jsonb(r) ORDER BY r.rank_net_rtg),",
    "                       '[]'::jsonb)::text",
    " FROM (SELECT game_year, team_id, team_name, off_ppp, def_ppp, net_rtg,",
    "              games_played, wins, losses, off_poss, def_poss,",
    "              rank_net_rtg, rank_off_ppp, rank_def_ppp, off_fga,",
    "              off_layup_att, off_dunk_att, off_fg3_att, off_c3_att,",
    "              off_c3_known_att, def_fga, def_layup_att, def_dunk_att,",
    "              def_fg3_att, def_c3_att, def_c3_known_att",
    "         FROM basketball_test.team_ppp_ratings_mv",
    "        WHERE game_year = $1::int4) r) AS ratings_json,",
    "(SELECT COALESCE(jsonb_agg(to_jsonb(f) ORDER BY f.team_id),",
    "                       '[]'::jsonb)::text",
    " FROM (SELECT * FROM basketball_test.team_four_factors_mv",
    "        WHERE game_year = $1::int4) f) AS four_factors_json,",
    "(SELECT COALESCE(jsonb_agg(to_jsonb(o)",
    "  ORDER BY o.\"Net RTG Diff\" DESC, o.\"Team\", o.\"Last Name\", o.\"First Name\"),",
    "  '[]'::jsonb)::text",
    " FROM (SELECT * FROM basketball_test.onoff_default_mv",
    "        WHERE \"Year\" = $1::int4 AND team_id = $2::int4",
    "          AND COALESCE(\"ON Poss\", 0) >= 100",
    "          AND \"Net RTG Diff\" IS NOT NULL",
    "          AND \"Net RTG Diff\"::text NOT IN ('NaN', 'Infinity', '-Infinity')",
    "        ORDER BY \"Net RTG Diff\" DESC, \"Team\", \"Last Name\", \"First Name\"",
    "        LIMIT 5) o) AS onoff_json,",
    "(SELECT COALESCE(jsonb_agg(to_jsonb(t)), '[]'::jsonb)::text",
    " FROM (SELECT * FROM basketball_test.player_traditional_stats_mv",
    "        WHERE game_year = $1::int4 AND team_id = $2::int4) t) AS traditional_json,",
    "(SELECT COALESCE(jsonb_agg(to_jsonb(l)), '[]'::jsonb)::text",
    " FROM (SELECT player_names_str, ROUND(off_ppp - def_ppp, 1) AS net_rtg,",
    "              off_poss, def_poss",
    "         FROM basketball_test.sub_lineups_stats",
    "        WHERE game_year = $1::int4 AND team_id = $2::int4",
    "          AND num_lineup = 5",
    "          AND COALESCE(off_poss, 0) + COALESCE(def_poss, 0) >= 100) l)",
    " AS lineups_json"
  )
}

hub_parse_json_df <- function(value) {
  if (is.null(value) || length(value) != 1L || is.na(value) || !nzchar(value)) {
    return(data.frame())
  }
  out <- jsonlite::fromJSON(value, simplifyDataFrame = TRUE)
  if (is.data.frame(out)) return(out)
  if (is.list(out) && !length(out)) return(data.frame())
  as.data.frame(out, stringsAsFactors = FALSE, check.names = FALSE)
}

hub_dashboard_validation_error <- function(dashboard) {
  contracts <- list(
    storylines = c("hub_variant", "team_id", "net_rtg"),
    ratings = c("team_id", "team_name", "off_ppp", "def_ppp", "net_rtg"),
    four_factors = c(
      "team_id", "off_efg", "off_tov", "off_oreb", "off_ftr",
      "def_efg", "def_tov", "def_oreb", "def_ftr"
    ),
    onoff = c("team_id", "ON Poss", "Net RTG Diff", "First Name", "Last Name"),
    traditional = c("team_id", "pts", "gp"),
    lineups = c("player_names_str", "net_rtg", "off_poss", "def_poss")
  )
  required_nonempty <- c("storylines", "ratings", "four_factors")

  if (!is.list(dashboard)) return("response is not a dashboard list")
  for (section in names(contracts)) {
    value <- dashboard[[section]]
    if (!is.data.frame(value)) {
      return(sprintf("section %s is not a data frame", section))
    }
    if (section %in% required_nonempty && !nrow(value)) {
      return(sprintf("required section %s is empty", section))
    }
    # Empty selected-team sections are valid (for example, no lineup reaches
    # the possession threshold). JSON [] cannot preserve column names.
    if (nrow(value)) {
      missing <- setdiff(contracts[[section]], names(value))
      if (length(missing)) {
        return(sprintf(
          "section %s is missing columns: %s",
          section,
          paste(missing, collapse = ", ")
        ))
      }
    }
  }
  ""
}

hub_log_dashboard_fallback <- function(reason, gy, team_id, session = NULL) {
  if (exists("app_log", mode = "function")) {
    app_log(
      "hub_home_fallback",
      sprintf(
        "combined reader unavailable season=%d team=%d reason=%s; using legacy readers",
        gy,
        team_id,
        reason
      ),
      level = "WARN",
      session = session
    )
  }
}

hub_fetch_dashboard <- function(gy, team_id, ver, session = NULL) {
  gy <- suppressWarnings(as.integer(gy))
  team_id <- suppressWarnings(as.integer(team_id))
  if (!is.finite(gy) || !is.finite(team_id)) return(NULL)

  cached_season_df(
    list("home_dashboard", gy, team_id, ver),
    function() {
      query_error <- NULL
      out <- tryCatch(
        hub_db_get_query_timed(
          hub_dashboard_query_sql(),
          params = list(gy, team_id),
          label = sprintf("combined season=%d team=%d", gy, team_id),
          session = session
        ),
        error = function(e) {
          query_error <<- conditionMessage(e)
          NULL
        }
      )
      if (is.null(out) || nrow(out) != 1L) {
        received <- if (is.null(out)) 0L else nrow(out)
        reason <- query_error %||% sprintf("expected one row, received %d", received)
        hub_log_dashboard_fallback(reason, gy, team_id, session)
        return(NULL)
      }

      parse_error <- NULL
      dashboard <- tryCatch(
        list(
          storylines = hub_parse_json_df(out$storylines_json[[1]]),
          ratings = hub_parse_json_df(out$ratings_json[[1]]),
          four_factors = hub_parse_json_df(out$four_factors_json[[1]]),
          onoff = hub_parse_json_df(out$onoff_json[[1]]),
          traditional = hub_parse_json_df(out$traditional_json[[1]]),
          lineups = hub_parse_json_df(out$lineups_json[[1]])
        ),
        error = function(e) {
          parse_error <<- conditionMessage(e)
          NULL
        }
      )
      if (is.null(dashboard)) {
        hub_log_dashboard_fallback(parse_error %||% "JSON parsing failed", gy, team_id, session)
        return(NULL)
      }
      validation_error <- hub_dashboard_validation_error(dashboard)
      if (nzchar(validation_error)) {
        hub_log_dashboard_fallback(validation_error, gy, team_id, session)
        return(NULL)
      }

      version_value <- if ("data_version" %in% names(out)) {
        out$data_version[[1]]
      } else {
        ""
      }
      version <- trimws(as.character(version_value %||% ""))
      if (length(version) != 1L || is.na(version)) version <- ""
      if (nzchar(version)) {
        attr(dashboard, "data_version") <- version
        attr(dashboard$storylines, "data_version") <- version

        # Seed the canonical full-season caches returned in this same call.
        # This prevents the lower-priority prewarm and matching Compare routes
        # from immediately fetching the same rows again.
        GL_DATA_CACHE$set(
          rlang::hash(list("home_dashboard", gy, team_id, version)),
          dashboard
        )
        GL_DATA_CACHE$set(
          rlang::hash(list("team_ratings_preset_cache", gy, version)),
          dashboard$storylines
        )
        GL_DATA_CACHE$set(
          rlang::hash(list("team_ppp_ratings_mv", gy, version)),
          dashboard$ratings
        )
        GL_DATA_CACHE$set(
          rlang::hash(list("team_four_factors_mv", gy, version)),
          dashboard$four_factors
        )
      }
      dashboard
    }
  )
}

# Safety fallback for deployments where the persisted table has not been
# created yet. It remains batched into one database round trip.
hub_storyline_variants_sql <- function() {
  paste(
    c(
      paste0(
        "SELECT 'starters_hi'::text AS hub_variant, r.* ",
        "FROM basketball_test.get_team_ratings_dynamic(",
        "$1::int4, p_num_starters_off_min := 3, ",
        "p_num_starters_off_max := 5) r"
      ),
      paste0(
        "SELECT 'starters_lo'::text AS hub_variant, r.* ",
        "FROM basketball_test.get_team_ratings_dynamic(",
        "$1::int4, p_num_starters_off_min := 0, ",
        "p_num_starters_off_max := 2) r"
      ),
      paste0(
        "SELECT 'clutch'::text AS hub_variant, r.* ",
        "FROM basketball_test.get_team_ratings_dynamic(",
        "$1::int4, p_max_margin := 5, ",
        "p_max_time_remaining := 300) r"
      ),
      paste0(
        "SELECT 'last10'::text AS hub_variant, r.* ",
        "FROM basketball_test.get_team_ratings_dynamic(",
        "$1::int4, p_last_n_games := 10) r"
      ),
      paste0(
        "SELECT 'top4'::text AS hub_variant, r.* ",
        "FROM basketball_test.get_team_ratings_dynamic(",
        "$1::int4, p_opp_rank_side := 'top', ",
        "p_opp_rank_n := 4, p_opp_rank_metric := 'net') r"
      ),
      paste0(
        "SELECT 'bottom4'::text AS hub_variant, r.* ",
        "FROM basketball_test.get_team_ratings_dynamic(",
        "$1::int4, p_opp_rank_side := 'bottom', ",
        "p_opp_rank_n := 4, p_opp_rank_metric := 'net') r"
      )
    ),
    collapse = "\nUNION ALL\n"
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
    div(
      class = "hub-storylines-shell",
      uiOutput("hub_storylines", class = "hub-storylines-output"),
      div(
        class = "card bg-dark border-secondary mb-4 hub-card-static hub-storylines-loading",
        role = "status",
        `aria-live` = "polite",
        div(
          class = "card-body",
          tags$h6(class = "hub-block-title", "Storylines"),
          div(
            class = "hub-storylines-loading-body",
            tags$span(
              class = "spinner-border spinner-border-sm",
              `aria-hidden` = "true"
            ),
            tags$span("Analyzing team splits…")
          )
        )
      )
    )
  )
}

server_team_hub <- function(input, output, session, shared) {
  hub_auto_selected <- reactiveVal(DEFAULT_HOME_TEAM_ID)
  hub_resolved_team_id <- reactiveVal(DEFAULT_HOME_TEAM_ID)
  hub_remembered_seen <- reactiveVal(FALSE)
  hub_saved_default_team <- reactiveVal("")
  hub_default_set_this_session <- reactiveVal(FALSE)
  hub_checkbox_sync_pending <- reactiveVal(NULL)

  sync_home_default_checkbox <- function(checked) {
    checked <- isTRUE(checked)
    if (identical(isTRUE(isolate(input$home_set_default)), checked)) {
      return(invisible(NULL))
    }
    hub_checkbox_sync_pending(checked)
    updateCheckboxInput(
      session,
      "home_set_default",
      value = checked
    )
    invisible(NULL)
  }

  # Populate the selector and choose remembered team when it first arrives.
  # If localStorage arrives just after the random fallback, it may replace that
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
    resolved <- as.character(hub_resolved_team_id() %||% "")
    resolved_valid <- length(resolved) == 1L &&
      nzchar(resolved) &&
      resolved %in% team_ids
    remembered_received <- !is.null(input$hub_remembered_team)
    remembered <- as.character(input$hub_remembered_team %||% "")
    remembered_valid <- length(remembered) == 1L &&
      nzchar(remembered) &&
      remembered %in% team_ids
    first_remembered <- remembered_received &&
      !isTRUE(hub_remembered_seen())
    may_apply_remembered <- remembered_received &&
      !isTRUE(hub_remembered_seen()) &&
      (!current_valid || identical(current, hub_auto_selected()))

    # The initial named choice is already valid, so avoid delaying selector
    # synchronization on a ratings query. Ratings are only needed when the
    # current/default/remembered team cannot be used.
    needs_ratings <- !remembered_valid && !current_valid && !resolved_valid
    ratings <- if (needs_ratings) {
      tryCatch(
        hub_fetch_team_ratings(
          as.integer(shared$selected_game_year()),
          shared_data_version(shared)
        ),
        error = function(e) NULL
      )
    } else {
      NULL
    }
    selected <- if (may_apply_remembered && remembered_valid) {
      remembered
    } else if (current_valid) {
      current
    } else if (resolved_valid) {
      resolved
    } else {
      hub_default_team("", teams, ratings)
    }

    # Release Hub outputs immediately. Waiting for updateSelectizeInput() to
    # make a browser round trip adds avoidable startup latency.
    hub_resolved_team_id(selected)
    updateSelectizeInput(
      session,
      "home_team",
      choices = choices,
      selected = selected,
      server = TRUE
    )
    if (first_remembered) {
      hub_saved_default_team(
        if (remembered_valid) remembered else ""
      )
    }
    saved_default <- as.character(hub_saved_default_team() %||% "")
    sync_home_default_checkbox(
      isTRUE(hub_default_set_this_session()) &&
      length(saved_default) == 1L &&
        nzchar(saved_default) &&
        identical(selected, saved_default)
    )
    if (!identical(selected, current)) hub_auto_selected(selected)
    if (first_remembered) hub_remembered_seen(TRUE)
  }) |>
    bindEvent(
      shared$teams_for_year_df(),
      input$hub_remembered_team,
      ignoreNULL = FALSE
    )

  observeEvent(input$home_team, {
    tid <- as.character(input$home_team %||% "")
    teams <- shared$teams_for_year_df()
    team_ids <- if (!is.null(teams) && nrow(teams)) {
      as.character(teams$team_id)
    } else {
      character(0)
    }
    if (length(tid) == 1L && (!nzchar(tid) || tid %in% team_ids)) {
      hub_resolved_team_id(tid)
    }
    saved_default <- as.character(hub_saved_default_team() %||% "")
    sync_home_default_checkbox(
      isTRUE(hub_default_set_this_session()) &&
      length(tid) == 1L &&
        nzchar(tid) &&
        length(saved_default) == 1L &&
        identical(tid, saved_default)
    )
  }, ignoreInit = TRUE)

  observeEvent(input$home_set_default, {
    checked <- isTRUE(input$home_set_default)
    pending <- isolate(hub_checkbox_sync_pending())
    if (!is.null(pending) && identical(checked, pending)) {
      hub_checkbox_sync_pending(NULL)
      return(invisible(NULL))
    }

    tid <- as.character(input$home_team %||% "")
    enabled <- checked &&
      length(tid) == 1L &&
      nzchar(tid)
    if (enabled) {
      hub_default_set_this_session(TRUE)
      hub_saved_default_team(tid)
      session$sendCustomMessage(
        "ibpl-store-hub-team",
        list(enabled = TRUE, teamId = tid)
      )
      return(invisible(NULL))
    }

    saved_default <- as.character(hub_saved_default_team() %||% "")
    if (length(tid) == 1L &&
        length(saved_default) == 1L &&
        nzchar(saved_default) &&
        identical(tid, saved_default)) {
      hub_default_set_this_session(FALSE)
      hub_saved_default_team("")
      session$sendCustomMessage(
        "ibpl-store-hub-team",
        list(enabled = FALSE, teamId = NULL)
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
    tid <- as.character(hub_resolved_team_id() %||% "")
    req(nzchar(tid))
    tid
  })
  hub_storylines_ready <- reactive({
    identical(
      suppressWarnings(as.integer(shared$hub_storylines_ready_year())),
      hub_gy()
    )
  })

  hub_dashboard_df <- reactive({
    dashboard <- hub_fetch_dashboard(
      hub_gy(),
      hub_team_id(),
      hub_ver(),
      session = session
    )
    version <- attr(dashboard, "data_version", exact = TRUE)
    accept_data_version <- shared$accept_data_version
    if (is.function(accept_data_version) && !is.null(version)) {
      accept_data_version(version)
    }
    dashboard
  })

  hub_ratings_df <- reactive({
    dashboard <- hub_dashboard_df()
    if (!is.null(dashboard)) return(dashboard$ratings)
    hub_fetch_team_ratings(hub_gy(), hub_ver())
  })
  hub_ff_df <- reactive({
    dashboard <- hub_dashboard_df()
    if (!is.null(dashboard)) return(dashboard$four_factors)
    hub_fetch_team_ff(hub_gy(), hub_ver())
  })

  hub_onoff_df <- reactive({
    dashboard <- hub_dashboard_df()
    if (!is.null(dashboard)) return(dashboard$onoff)
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
    dashboard <- hub_dashboard_df()
    if (!is.null(dashboard)) {
      return(normalize_ts_result_cols(dashboard$traditional))
    }
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
    dashboard <- hub_dashboard_df()
    if (!is.null(dashboard)) return(dashboard$lineups)
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

  # Prefer the ETL-refreshed preset table. During a rolling deployment, fall
  # back to the previous batched dynamic query if that table is unavailable.
  hub_dyn_all_df <- reactive({
    dashboard <- hub_dashboard_df()
    if (!is.null(dashboard)) return(dashboard$storylines)
    gy <- hub_gy()
    persisted <- hub_fetch_team_ratings_presets(gy, hub_ver(), session = session)
    if (!is.null(persisted)) {
      accept_data_version <- shared$accept_data_version
      persisted_version <- attr(persisted, "data_version", exact = TRUE)
      if (is.function(accept_data_version) && !is.null(persisted_version)) {
        accept_data_version(persisted_version)
      }
      return(persisted)
    }

    cached_season_df(
      list("hub_team_dyn_all_fallback", gy, hub_ver()),
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
            hub_storyline_variants_sql(),
            params = list(gy)
          ),
          error = function(e) NULL
        )
      }
    )
  })

  hub_dyn_df <- function(variant) {
    df <- hub_dyn_all_df()
    if (is.null(df) || !nrow(df) || !("hub_variant" %in% names(df))) {
      return(NULL)
    }
    out <- df[as.character(df$hub_variant) == variant, , drop = FALSE]
    if (!nrow(out)) return(NULL)
    out$hub_variant <- NULL
    out
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
    req(hub_storylines_ready())
    info <- hub_identity_data(hub_ratings_df(), hub_ff_df(), hub_team_id())
    if (is.null(info)) return(NULL)
    row <- info$row
    n_teams <- info$n_teams
    mini_off <- hub_ff_mini(hub_ff_df(), hub_team_id(), "offense")
    mini_def <- hub_ff_mini(hub_ff_df(), hub_team_id(), "defense")
    ff_row <- function(label, mini) {
      if (is.null(mini)) return(NULL)
      div(
        class = "hub-ff-row",
        tags$span(class = "hub-ff-label", label),
        div(
          class = "hub-ff-chips",
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
      )
    }
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
        if (!is.null(mini_off) || !is.null(mini_def)) {
          div(
            class = "hub-ff-groups",
            ff_row("Offense", mini_off),
            ff_row("Defense", mini_def)
          )
        }
      )
    )
  })

  # ---- Key players ----
  output$hub_players <- renderUI({
    req(hub_storylines_ready())
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
    req(hub_storylines_ready())
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
    gy <- hub_gy()
    hub_team_id()
    mark_storylines_ready <- shared$hub_storylines_ready_year
    if (is.function(mark_storylines_ready)) {
      on.exit(
        mark_storylines_ready(gy),
        add = TRUE
      )
    }

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
    if (!length(lines)) {
      return(
        div(
          class = "card bg-dark border-secondary mb-4 hub-card-static",
          div(
            class = "card-body",
            tags$h6(class = "hub-block-title", "Storylines"),
            tags$p(
              class = "hub-storylines-empty mb-0",
              "No qualified storylines are available for this team."
            )
          )
        )
      )
    }
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
  outputOptions(
    output,
    "hub_storylines",
    priority = 100,
    suspendWhenHidden = TRUE
  )

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

  invisible(list(
    saved_default_team = hub_saved_default_team
  ))
}
