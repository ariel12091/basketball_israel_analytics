# app.R - Main entry point
# Sources modular files and assembles the Shiny app


# Source all modules
source("R/helpers.R", local = TRUE)
source("R/global.R", local = TRUE)
source("R/logger.R", local = TRUE)
source("R/mod_lineup_player_filter.R", local = TRUE)
source("R/mod_team_hub.R", local = TRUE)
source("R/ui_tab0_home.R", local = TRUE)
source("R/ui_tab1_onoff.R", local = TRUE)
source("R/ui_tab2_lineup.R", local = TRUE)
source("R/ui_tab3_team.R", local = TRUE)
source("R/server_tab1.R", local = TRUE)
source("R/server_tab2.R", local = TRUE)
source("R/server_tab3.R", local = TRUE)
source("R/ui_tab4_gamelogs.R", local = TRUE)
source("R/server_tab4.R", local = TRUE)
source("R/ui_tab5_traditional.R", local = TRUE)
source("R/server_tab5_traditional.R", local = TRUE)
source("R/ui_tab7_compare.R", local = TRUE)
source("R/server_tab7_compare.R", local = TRUE)

# ---------------- UI ----------------
ui <- navbarPage(
  id = "main_tabs",
  title = tags$span(
    tags$i(class = "bi bi-activity", style = "margin-right: 6px;"),
    "IBPL Analytics"
  ),
  theme = bslib::bs_theme(
    version = 5,
    bg = "#0d1117",
    fg = "#e6edf3",
    primary = "#e8a435",
    secondary = "#21262d",
    success = "#34d399",
    danger = "#f87171",
    info = "#60a5fa",
    base_font = "DM Sans, Inter, -apple-system, sans-serif",
    code_font = "JetBrains Mono, monospace",
    "navbar-bg" = "#0d1117"
  ),
  header = tagList(
    includeCSS("www/app.css"),
    tags$script(HTML(sprintf(
      paste0(
        "window.IBPL_IDLE_CONFIG = {",
        "timeoutSec:%d,warningSec:%d,stateTtlHours:%s,stateVersion:10",
        "};"
      ),
      APP_IDLE_TIMEOUT_SEC,
      APP_IDLE_WARNING_SEC,
      format(APP_IDLE_STATE_TTL_HOURS, scientific = FALSE, trim = TRUE)
    ))),
    includeScript("www/app.js"),
    tags$div(
      style = "position: fixed; right: 10px; top: 8px; font-size: 0.8rem; color: #8b949e; z-index: 9999; display: flex; align-items: center; gap: 6px; max-width: calc(100vw - 20px); white-space: nowrap;",
      tags$div(
        class = "navbar-season-select",
        selectInput("game_year", NULL,
                    choices = c("25-26" = "2026", "24-25" = "2025"),
                    selected = DEFAULT_GAME_YEAR)
      ),
      actionButton("open_glossary",
                   tags$span(tags$i(class = "bi bi-book"), " Glossary"),
                   class = "btn btn-sm btn-outline-secondary nav-help-btn"),
      tags$span(
        style = "display: inline-flex; align-items: center; gap: 4px; min-width: 0;",
        tags$span(style = "width: 6px; height: 6px; background: #34d399; border-radius: 50%; display: inline-block;"),
        tags$span(style = "display: inline-block; max-width: 210px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap;",
                  textOutput("last_updated", inline = TRUE))
      )
    )
  ),
  ui_tab0_home,
  ui_tab1_onoff,
  ui_tab2_lineup,
  ui_tab3_team,
  ui_tab4_gamelogs,
  ui_tab5_traditional,
  ui_tab7_compare
)

# ---------------- Server ----------------
server <- function(input, output, session) {
  startup_t0 <- proc.time()[["elapsed"]]
  init_session_request_guard(session)
  if (is.function(session$allowReconnect)) session$allowReconnect(FALSE)
  last_activity_at <- reactiveVal(as.numeric(Sys.time()))
  pending_ld_lineup_restore <- reactiveVal(NULL)
  idle_timeout_sec <- APP_IDLE_TIMEOUT_SEC
  idle_check_sec <- APP_IDLE_CHECK_SEC
  idle_close_session <- isTRUE(APP_IDLE_CLOSE_SESSION)
  log_startup <- function(step) {
    elapsed <- proc.time()[["elapsed"]] - startup_t0
    app_log("startup", sprintf("%s (%.3fs)", step, elapsed), session = session)
  }

  observeEvent(input$idle_activity_ts, {
    last_activity_at(as.numeric(Sys.time()))
  }, ignoreInit = TRUE)

  if (isTRUE(idle_close_session)) {
    observe({
      invalidateLater(idle_check_sec * 1000L, session)
      idle_for <- as.numeric(Sys.time()) - last_activity_at()
      if (is.finite(idle_for) && idle_for >= idle_timeout_sec) {
        session$close()
      }
    })
  }

  restore_selectize_ids <- c(
    "teams", "on_game_type", "on_opponents", "on_gn_min", "on_gn_max", "on_last_n",
    "ld_lineup_filter-team", "ld_lineup_filter-players_on", "ld_lineup_filter-players_off",
    "ld_game_type", "ld_opponents", "ld_gn_min", "ld_gn_max", "ld_last_n",
    "tr_game_type", "tr_opponents", "tr_gn_min", "tr_gn_max", "tr_last_n",
    "gl_team", "gl_game_type", "gl_opponents", "gl_gn_min", "gl_gn_max", "gl_last_n",
    "ts_teams", "ts_players", "ts_game_type", "ts_opponents", "ts_gn_min", "ts_gn_max", "ts_last_n",
    "cmp_split_gn", "cmp_players_gn_min", "cmp_players_gn_max",
    "cmp_player_a_gn_min", "cmp_player_a_gn_max", "cmp_player_b_gn_min", "cmp_player_b_gn_max",
    "cmp_player_a_list_team_filter", "cmp_player_a", "cmp_player_b_list_team_filter", "cmp_player_b",
    "cmp_a_teams", "cmp_a_opponents", "cmp_a_game_type",
    "cmp_b_teams", "cmp_b_opponents", "cmp_b_game_type",
    "cmp_lu_filter-team", "cmp_lu_filter-players_on", "cmp_lu_filter-players_off"
  )
  restore_select_ids <- c(
    "game_year", "home_team",
    "on_num_starters_off_mode", "on_num_starters_off", "on_num_starters_def_mode", "on_num_starters_def",
    "on_home_away", "on_outcome", "on_opp_rank_side", "on_opp_rank_n", "on_opp_rank_metric",
    "ld_num_starters_off_mode", "ld_num_starters_off", "ld_num_starters_def_mode", "ld_num_starters_def",
    "ld_clutch_status", "ld_home_away", "ld_outcome", "ld_opp_rank_side", "ld_opp_rank_n", "ld_opp_rank_metric",
    "tr_trad_display_mode", "tr_clutch_status", "tr_num_starters_off_mode", "tr_num_starters_off",
    "tr_num_starters_def_mode", "tr_num_starters_def", "tr_home_away", "tr_outcome",
    "tr_opp_rank_side", "tr_opp_rank_n", "tr_opp_rank_metric",
    "gl_num_starters_off_mode", "gl_num_starters_off", "gl_num_starters_def_mode", "gl_num_starters_def",
    "gl_home_away", "gl_outcome",
    "ts_display_mode", "ts_clutch_status", "ts_home_away", "ts_outcome",
    "ts_opp_rank_side", "ts_opp_rank_n", "ts_opp_rank_metric",
    "cmp_preset", "cmp_player_a_team", "cmp_player_b_team",
    "cmp_a_starters_mode", "cmp_a_starters_val", "cmp_a_opp_starters_mode", "cmp_a_opp_starters_val",
    "cmp_a_home_away", "cmp_a_outcome", "cmp_a_opp_rank_side", "cmp_a_opp_rank_n", "cmp_a_opp_rank_metric",
    "cmp_b_starters_mode", "cmp_b_starters_val", "cmp_b_opp_starters_mode", "cmp_b_opp_starters_val",
    "cmp_b_home_away", "cmp_b_outcome", "cmp_b_opp_rank_side", "cmp_b_opp_rank_n", "cmp_b_opp_rank_metric"
  )
  restore_radio_ids <- c(
    "onoff_view_mode", "ld_view_mode", "ld_num", "tr_view_mode", "gl_view_mode",
    "cmp_mode", "cmp_player_compare_mode", "cmp_lu_num", "cmp_team_player_rate_mode", "cmp_rate_mode"
  )
  restore_slider_ids <- c(
    "min_all_poss", "min_on_poss",
    "ld_minposs", "ld_clutch_margin", "ld_clutch_minutes",
    "tr_clutch_margin", "tr_clutch_minutes",
    "ts_min_gp_slider", "ts_clutch_margin", "ts_clutch_minutes",
    "cmp_min_poss", "cmp_a_clutch_margin", "cmp_a_clutch_minutes",
    "cmp_b_clutch_margin", "cmp_b_clutch_minutes"
  )
  restore_numeric_ids <- c("ts_min_gp")
  restore_checkbox_ids <- c(
    "ld_clutch_enabled", "ld_clutch_ot_margin",
    "tr_trad_defense_mode", "tr_clutch_enabled", "tr_clutch_ot_margin",
    "ts_show_ineligible", "ts_clutch_enabled", "ts_clutch_ot_margin",
    "cmp_a_clutch", "cmp_b_clutch"
  )
  restore_date_range_ids <- c(
    "date_range", "ld_dates", "tr_dates", "gl_dates", "ts_dates",
    "cmp_players_dates", "cmp_player_a_dates", "cmp_player_b_dates"
  )
  restore_date_ids <- c("cmp_split_date")
  restore_tab_values <- c("home", "onoff", "lineup_data", "team_ratings", "game_logs", "traditional_stats", "compare")

  restore_id_allowed <- function(id, tab) {
    if (id %in% c("main_tabs", "game_year")) return(TRUE)
    switch(
      tab,
      home = id %in% c("home_team"),
      onoff = id %in% c("teams", "date_range", "min_all_poss", "min_on_poss", "onoff_view_mode") ||
        startsWith(id, "on_"),
      lineup_data = startsWith(id, "ld_"),
      team_ratings = startsWith(id, "tr_"),
      game_logs = startsWith(id, "gl_"),
      traditional_stats = startsWith(id, "ts_"),
      compare = startsWith(id, "cmp_"),
      FALSE
    )
  }

  restore_chr_vec <- function(x, max_len = 80L) {
    sanitize_persisted_choices(x, max_len = max_len)
  }

  restore_chr_one <- function(x) {
    sanitize_single_choice(x)
  }

  restore_bool <- function(x) {
    if (is.logical(x) && length(x)) return(isTRUE(x[[1]]))
    val <- tolower(restore_chr_one(x))
    val %in% c("true", "1", "yes", "on")
  }

  restore_num <- function(x) {
    val <- suppressWarnings(as.numeric(restore_chr_one(x)))
    if (is.finite(val)) val else NA_real_
  }

  restore_state_values <- function(values) {
    if (is.null(values) || !is.list(values)) return(invisible(FALSE))
    restore_target_tab <- restore_chr_one(values$main_tabs)
    if (!restore_target_tab %in% restore_tab_values) restore_target_tab <- "home"
    defer_ld_player_restore <- identical(restore_target_tab, "lineup_data")
    ld_player_restore_ids <- c("ld_lineup_filter-players_on", "ld_lineup_filter-players_off")
    if (defer_ld_player_restore) {
      pending_ld_lineup_restore(list(
        team = restore_chr_one(values[["ld_lineup_filter-team"]]),
        players_on = restore_chr_vec(values[["ld_lineup_filter-players_on"]]),
        players_off = restore_chr_vec(values[["ld_lineup_filter-players_off"]])
      ))
    }

    restore_if_present <- function(id, fn) {
      if (!hasName(values, id)) return(invisible(NULL))
      if (!isTRUE(restore_id_allowed(id, restore_target_tab))) return(invisible(NULL))
      tryCatch(fn(values[[id]]), error = function(e) {
        app_log("idle_restore", sprintf("failed to restore %s: %s", id, conditionMessage(e)), level = "WARN", session = session)
      })
    }

    restore_if_present("main_tabs", function(v) {
      tab <- restore_chr_one(v)
      if (tab %in% restore_tab_values) updateTabsetPanel(session, "main_tabs", selected = tab)
    })

    for (id in restore_select_ids) {
      restore_if_present(id, function(v) {
        freezeReactiveValue(input, id)
        updateSelectInput(session, id, selected = restore_chr_one(v))
      })
    }
    for (id in restore_selectize_ids) {
      if (defer_ld_player_restore && id %in% ld_player_restore_ids) next
      restore_if_present(id, function(v) {
        freezeReactiveValue(input, id)
        updateSelectizeInput(session, id, selected = restore_chr_vec(v))
      })
    }
    for (id in restore_radio_ids) {
      restore_if_present(id, function(v) {
        freezeReactiveValue(input, id)
        updateRadioButtons(session, id, selected = restore_chr_one(v))
      })
    }
    for (id in restore_slider_ids) {
      restore_if_present(id, function(v) {
        val <- restore_num(v)
        if (!is.na(val)) {
          freezeReactiveValue(input, id)
          updateSliderInput(session, id, value = val)
        }
      })
    }
    for (id in restore_numeric_ids) {
      restore_if_present(id, function(v) {
        val <- restore_num(v)
        if (!is.na(val)) {
          freezeReactiveValue(input, id)
          updateNumericInput(session, id, value = val)
        }
      })
    }
    for (id in restore_checkbox_ids) {
      restore_if_present(id, function(v) {
        freezeReactiveValue(input, id)
        updateCheckboxInput(session, id, value = restore_bool(v))
      })
    }
    for (id in restore_date_range_ids) {
      restore_if_present(id, function(v) {
        vals <- restore_chr_vec(v, max_len = 2L)
        if (length(vals) >= 2L) {
          start_d <- suppressWarnings(as.Date(vals[[1]]))
          end_d <- suppressWarnings(as.Date(vals[[2]]))
          if (!is.na(start_d) && !is.na(end_d)) {
            freezeReactiveValue(input, id)
            updateDateRangeInput(session, id, start = start_d, end = end_d)
          }
        }
      })
    }
    for (id in restore_date_ids) {
      restore_if_present(id, function(v) {
        val <- suppressWarnings(as.Date(restore_chr_one(v)))
        if (!is.na(val)) {
          freezeReactiveValue(input, id)
          updateDateInput(session, id, value = val)
        }
      })
    }

    invisible(TRUE)
  }

  observeEvent(input[["ld_lineup_filter-team"]], {
    pending <- pending_ld_lineup_restore()
    if (is.null(pending) || !is.list(pending)) return(invisible(NULL))

    expected_team <- restore_chr_one(pending$team)
    current_team <- restore_chr_one(input[["ld_lineup_filter-team"]])
    if (nzchar(expected_team) && !identical(current_team, expected_team)) {
      return(invisible(NULL))
    }

    session$onFlushed(function() {
      players_on <- restore_chr_vec(pending$players_on)
      players_off <- restore_chr_vec(pending$players_off)
      if (length(players_on)) {
        freezeReactiveValue(input, "ld_lineup_filter-players_on")
        updateSelectizeInput(session, "ld_lineup_filter-players_on", selected = players_on)
      }
      if (length(players_off)) {
        freezeReactiveValue(input, "ld_lineup_filter-players_off")
        updateSelectizeInput(session, "ld_lineup_filter-players_off", selected = players_off)
      }
      pending_ld_lineup_restore(NULL)
    }, once = TRUE)
  }, ignoreInit = TRUE, priority = -100)

  observeEvent(input$ibpl_restore_state, {
    payload <- input$ibpl_restore_state
    if (is.null(payload) || !is.list(payload)) return(invisible(NULL))
    values <- payload$values
    restored <- restore_state_values(values)
    if (isTRUE(restored)) {
      session$sendCustomMessage("ibpl_restore_applied", list(ts = as.numeric(Sys.time())))
    }
  }, ignoreInit = TRUE)

  # ---- Shared helpers & reactives ----
  season_date_bounds <- season_date_bounds_for_year
  last_updated_cache <- reactiveVal(NA_character_)
  data_version_cache <- reactiveVal(NA_character_)
  hub_storylines_ready_year <- reactiveVal(NA_integer_)

  selected_game_year <- reactive({
    input$game_year %||% DEFAULT_GAME_YEAR
  })

  # ===== Teams dropdown choices =====
  teams_for_year_df <- reactive({
    gy_int <- as.integer(selected_game_year())
    req(gy_int)
    if (is.null(static_team_roster(gy_int))) {
      req(identical(hub_storylines_ready_year(), gy_int))
    }
    fetch_teams_distinct(gy_int)
  })

  # Warm the four canonical per-season lookups every tab reads from.
  prewarm_for_year <- function(gy_chr) {
    gy_int <- suppressWarnings(as.integer(gy_chr))
    if (!is.finite(gy_int) || is.na(gy_int)) return(invisible(NULL))
    fetch_teams_distinct(gy_int)
    fetch_teams_min(gy_int)
    fetch_gn_values(gy_int)
    fetch_players_basic(gy_int)
    ver <- tryCatch(
      shared_data_version(list(data_version = data_version_cache)),
      error = function(e) "unknown"
    )
    hub_fetch_team_ratings(gy_int, ver)
    hub_fetch_team_ff(gy_int, ver)
    invisible(NULL)
  }

  observeEvent(selected_game_year(), {
    td <- teams_for_year_df()
    team_choices <- stats::setNames(as.character(td$team_id), as.character(td$team_name))
    updateSelectizeInput(session, "teams", choices = team_choices, selected = character(0), server = TRUE)
    updateSelectizeInput(session, "on_opponents", choices = team_choices, selected = character(0), server = TRUE)
    updateSelectizeInput(session, "ld_opponents", choices = team_choices, selected = character(0), server = TRUE)
  }, ignoreInit = FALSE)

  selected_opp_ids_on <- reactive({
    ids <- suppressWarnings(as.integer(input$on_opponents))
    ids <- ids[is.finite(ids)]
    if (length(ids)) ids else NULL
  })

  selected_opp_ids_ld <- reactive({
    ids <- suppressWarnings(as.integer(input$ld_opponents))
    ids <- ids[is.finite(ids)]
    if (length(ids)) ids else NULL
  })

  last_success_path <- function() {
    candidates <- c(
      file.path(getwd(), "etl", "logs", "last_success.txt"),
      file.path(getwd(), "..", "etl", "logs", "last_success.txt")
    )
    existing <- candidates[file.exists(candidates)]
    if (length(existing)) existing[[1]] else NA_character_
  }

  last_success_db <- function() {
    tryCatch({
      # Cached process-wide (TTL just under the 60s poll) so concurrent
      # sessions share a single app_meta query per minute.
      q <- cached_ref_query(
        key = "app_meta_last_success",
        ttl_sec = 55,
        query_fun = function() db_get_query(
          pg_pool,
          "SELECT value FROM basketball_test.app_meta WHERE key = 'etl_full_last_success' LIMIT 1"
        )
      )
      if (nrow(q) && nzchar(q$value[1])) q$value[1] else NA_character_
    }, error = function(e) NA_character_)
  }

  accept_data_version <- function(version) {
    version <- trimws(as.character(version %||% ""))
    if (!length(version) || is.na(version[[1]]) || !nzchar(version[[1]])) {
      return(invisible(FALSE))
    }
    version <- version[[1]]
    if (!identical(isolate(data_version_cache()), version)) {
      data_version_cache(version)
    }
    last_updated_cache(paste("Last updated:", version))
    invisible(TRUE)
  }

  refresh_last_updated <- function() {
    ts <- last_success_db()
    if (is.na(ts)) {
      p <- last_success_path()
      if (is.na(p)) {
        last_updated_cache("Last updated: unavailable")
        return(invisible(NULL))
      }
      lines <- tryCatch(readLines(p, warn = FALSE), error = function(e) character(0))
      ts <- if (length(lines)) trimws(lines[[1]]) else ""
    }
    has_ts <- length(ts) > 0 && !is.na(ts[[1]]) && nzchar(trimws(ts[[1]]))
    txt <- if (!has_ts) "Last updated: unavailable" else paste("Last updated:", ts[[1]])
    if (has_ts) {
      accept_data_version(ts[[1]])
    } else {
      last_updated_cache(txt)
    }
    invisible(NULL)
  }

  last_updated_poll <- new.env(parent = emptyenv())
  last_updated_poll$released <- FALSE
  observe({
    gy <- suppressWarnings(as.integer(selected_game_year()))
    req(is.finite(gy))
    req(identical(hub_storylines_ready_year(), gy))
    invalidateLater(60000, session)

    # Storylines normally supplies this timestamp in its first useful query.
    # Avoid immediately querying app_meta for the same value again.
    if (!isTRUE(last_updated_poll$released)) {
      last_updated_poll$released <- TRUE
      current <- isolate(data_version_cache())
      if (length(current) && !is.na(current[[1]]) && nzchar(current[[1]])) {
        return(invisible(NULL))
      }
    }
    refresh_last_updated()
  })

  output$last_updated <- renderText({
    last_updated_cache() %||% "Last updated: unavailable"
  })

  # Storylines own the first expensive startup query batch. Reset the handoff
  # whenever the season changes; the Home module releases prewarm after its
  # Storylines output finishes for that season.
  observeEvent(selected_game_year(), {
    hub_storylines_ready_year(NA_integer_)
  }, ignoreInit = FALSE, priority = 200)

  observe({
    gy <- suppressWarnings(as.integer(selected_game_year()))
    req(is.finite(gy))
    req(identical(hub_storylines_ready_year(), gy))
    tryCatch(
      {
        prewarm_for_year(gy)
        log_startup(sprintf("prewarm complete for season %s", gy))
      },
      error = function(e) {
        app_log(
          "startup",
          sprintf("prewarm failed for season %s: %s", gy, conditionMessage(e)),
          level = "ERROR",
          session = session
        )
      }
    )
  }, priority = -100)

  observeEvent(input$open_glossary, {
    showModal(
      modalDialog(
        title = "Glossary",
        size = "l",
        # --- Efficiency ---
        tags$h5(style = "margin-top: 0; color: #e8a435;", "Efficiency"),
        tags$ul(
          tags$li(tags$b("PPP"), ": Points per 100 possessions (points per possession \u00d7 100)."),
          tags$li(tags$b("Net Rating"), ": Offensive PPP minus Defensive PPP. Positive = outscoring opponents."),
          tags$li(tags$b("Possessions"), ": Estimated offensive or defensive trips. More possessions = more reliable stats.")
        ),
        # --- Four Factors ---
        tags$h5(style = "color: #e8a435;", "Four Factors"),
        tags$ul(
          tags$li(tags$b("TS%"), ": True Shooting \u2014 scoring efficiency accounting for 2PT, 3PT, and free throws. Formula: pts / (2 \u00d7 (FGA + FT trips))."),
          tags$li(tags$b("OREB%"), ": Offensive rebound rate \u2014 share of available misses grabbed. On defense, it measures opponent offensive rebounds allowed."),
          tags$li(tags$b("TOV%"), ": Turnover rate \u2014 turnovers per possession. Lower is better on offense; higher is better on defense."),
          tags$li(tags$b("FTR"), ": Free throw rate \u2014 FTA / FGA. Measures how often a team or player gets to the line relative to shot attempts.")
        ),
        # --- Shot Splits ---
        tags$h5(style = "color: #e8a435;", "Shot Splits"),
        tags$ul(
          tags$li(tags$b("Off Shot / Def Shot"), ": Each cell shows 2PT and 3PT frequency (how often that shot type is taken) and accuracy (FG%)."),
          tags$li("The ", tags$span(style = "color: #5b8abd; font-weight: 600;", "blue"), " bar is 2PT frequency, the ",
                  tags$span(style = "color: #d4843e; font-weight: 600;", "orange"), " bar is 3PT frequency."),
          tags$li("Accuracy is shown as FG% text, colored from ", tags$span(style = "color: #f87171;", "red"),
                  " (below league average) to ", tags$span(style = "color: #34d399;", "green"), " (above league average).")
        ),
        # --- Colors & Ranking ---
        tags$h5(style = "color: #e8a435;", "Colors & Ranking"),
        tags$ul(
          tags$li(tags$b("Heat colors"), ": ", tags$span(style = "color: #34d399;", "Green"), " = good, ",
                  tags$span(style = "color: #f87171;", "red"), " = bad. ",
                  tags$b("Polarity flips for defense"), " \u2014 lower Def PPP is better, so green means fewer points allowed."),
          tags$li(tags$b("TOV% exception"), ": On offense, lower TOV% is green (fewer turnovers). On defense, higher TOV% is green (more opponent turnovers)."),
          tags$li(tags$b("Gray / no color"), ": The player, lineup, or team has too few possessions to rank reliably (below the minimum threshold)."),
          tags$li(tags$b("Percentile rank bars"), " (Four Factors view): The slider shows where a player ranks from 0% to 100% among all players with enough possessions. 50% = league median. Only players above the minimum possession threshold are included in rankings.")
        ),
        # --- Filters ---
        tags$h5(style = "color: #e8a435;", "Filters"),
        tags$ul(
          tags$li(tags$b("Game Number (GN)"), ": Each team's sequential game number in the season. Useful for filtering to a stretch of games."),
          tags$li(tags$b("Last N"), ": Only include the most recent N games. Mutually exclusive with GN range."),
          tags$li(tags$b("Opponent Strength"), ": Filter games by the opponent's league ranking over the selected sample."),
          tags$li(tags$b("Clutch"), " (Tabs 2, 3): Limit to close-game possessions based on score margin, time remaining, and lead/trail status. Overtime qualifies by default."),
          tags$li(tags$b("Min Possessions"), ": Minimum possessions to appear in the table. Higher = more stable data but fewer rows.")
        ),
        easyClose = TRUE,
        footer = modalButton("Close")
      )
    )
  }, ignoreInit = TRUE)

  # Create shared context for tab servers
  shared <- list(
    season_date_bounds = season_date_bounds,
    selected_game_year = selected_game_year,
    teams_for_year_df = teams_for_year_df,
    selected_opp_ids_on = selected_opp_ids_on,
    selected_opp_ids_ld = selected_opp_ids_ld,
    data_version = data_version_cache,
    accept_data_version = accept_data_version,
    hub_storylines_ready_year = hub_storylines_ready_year,
    pending_ld_team = reactiveVal(NULL),
    pending_gl_team = reactiveVal(NULL),
    pending_compare_preset = reactiveVal(NULL)
  )

  # Call tab server modules
  server_tab1(input, output, session, shared)
  server_tab2(input, output, session, shared)
  server_tab3(input, output, session, shared)
  server_tab4(input, output, session, shared)
  server_tab5_traditional(input, output, session, shared)
  server_tab7_compare(input, output, session, shared)
  server_team_hub(input, output, session, shared)

  # Card navigation: Who is helping my team? -> Tab 1
  observeEvent(input$go_onoff, {
    teams_df <- shared$teams_for_year_df()
    team_choices <- stats::setNames(as.character(teams_df$team_id), as.character(teams_df$team_name))
    if (!is.null(input$home_team) && input$home_team != "") {
      team_id <- as.character(input$home_team)
      if (team_id %in% unname(team_choices)) {
        updateSelectizeInput(session, "teams", choices = team_choices,
                             selected = team_id, server = TRUE)
      }
    } else {
      updateSelectizeInput(session, "teams", choices = team_choices,
                           selected = character(0), server = TRUE)
    }
    updateTabsetPanel(session, "main_tabs", selected = "onoff")
  })

  # Card navigation: Which lineups are working? -> Tab 2
  observeEvent(input$go_lineups, {
    if (!is.null(input$home_team) && input$home_team != "") {
      shared$pending_ld_team(input$home_team)
    }
    updateRadioButtons(session, "ld_num", selected = "5")
    updateTabsetPanel(session, "main_tabs", selected = "lineup_data")
  })

  # Card navigation: How is my team performing? -> Tab 3
  observeEvent(input$go_team, {
    updateTabsetPanel(session, "main_tabs", selected = "team_ratings")
  })

  # Card navigation: What happened in last night's game? -> Tab 4
  observeEvent(input$go_gamelogs, {
    if (!is.null(input$home_team) && input$home_team != "") {
      shared$pending_gl_team(input$home_team)
    }
    updateTabsetPanel(session, "main_tabs", selected = "game_logs")
  })

  # Card navigation: How are individual players performing? -> Tab 5
  observeEvent(input$go_playerstats, {
    updateTabsetPanel(session, "main_tabs", selected = "traditional_stats")
  })

  # Card navigation: How do starters compare to the bench? -> Tab 7
  observeEvent(input$go_compare, {
    shared$pending_compare_preset("starters_bench")
    updateTabsetPanel(session, "main_tabs", selected = "compare")
  })

  log_startup("server modules initialized")
}

shinyApp(ui, server)

