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
source("R/global_euro.R", local = TRUE)
source("R/ui_tab8_euro.R", local = TRUE)
source("R/server_tab8_euro.R", local = TRUE)

enableBookmarking(store = "url")
# 13: bookmarks captured before the .clientdata_url_search fix hold blank
# server-populated choices; discard them instead of restoring known-bad state.
IBPL_RESTORE_STATE_VERSION <- 13L

# Print the resolved idle settings once per worker. These can be overridden by
# a gitignored .Renviron, so without this line a stale value (a leftover test
# timeout, say) is invisible in both the repo and the running app.
app_log("startup", sprintf(
  "idle config: close_session=%s timeout=%ds warning=%ds check=%ds state_ttl=%sh",
  APP_IDLE_CLOSE_SESSION, APP_IDLE_TIMEOUT_SEC, APP_IDLE_WARNING_SEC,
  APP_IDLE_CHECK_SEC, format(APP_IDLE_STATE_TTL_HOURS, trim = TRUE)
))

# ---------------- UI ----------------
ui <- function(request) {
  navbarPage(
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
        "timeoutSec:%d,warningSec:%d,stateTtlHours:%s,stateVersion:%d",
        "};"
      ),
      APP_IDLE_TIMEOUT_SEC,
      APP_IDLE_WARNING_SEC,
      format(APP_IDLE_STATE_TTL_HOURS, scientific = FALSE, trim = TRUE),
      IBPL_RESTORE_STATE_VERSION
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
      # League switch. Only one league's tabs are visible at a time; the
      # filtering itself lives in app.js so switching needs no round-trip.
      tags$div(
        class = "league-switch",
        tags$button(type = "button", `data-league-btn` = "il", "IL",
                    title = "Israel Basketball Premier League"),
        tags$button(type = "button", `data-league-btn` = "el", "EL",
                    title = "EuroLeague")
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
  ui_tab0_home(),
  ui_tab1_onoff(),
  ui_tab2_lineup(),
  ui_tab3_team(),
  ui_tab4_gamelogs(),
  ui_tab5_traditional(),
  ui_tab7_compare(),
  ui_tab8_euro()
  )
}

# ---------------- Server ----------------
server <- function(input, output, session) {
  startup_t0 <- proc.time()[["elapsed"]]
  init_session_request_guard(session)
  if (is.function(session$allowReconnect)) session$allowReconnect(FALSE)
  last_activity_at <- reactiveVal(as.numeric(Sys.time()))
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

  # ---- Bookmark capture ----
  # Snapshot every non-excluded input; re-bookmark only when that snapshot
  # actually changes, so the idle heartbeat cannot cause bookmark churn.
  bookmark_snapshot <- debounce(reactive({
    vals <- reactiveValuesToList(input)
    ids <- setdiff(names(vals), bookmark_excluded_ids(names(vals)))
    vals[sort(ids)]
  }), 1500)

  last_bookmark_snapshot <- reactiveVal(NULL)

  observe({
    snap <- bookmark_snapshot()
    if (identical(snap, isolate(last_bookmark_snapshot()))) return(invisible(NULL))
    last_bookmark_snapshot(snap)
    setBookmarkExclude(bookmark_excluded_ids(names(reactiveValuesToList(input))), session)
    tryCatch(session$doBookmark(), error = function(e) {
      app_log("bookmark", sprintf("doBookmark failed: %s", conditionMessage(e)),
              level = "WARN", session = session)
    })
  }, priority = -200)

  onBookmarked(function(url) {
    session$sendCustomMessage("ibpl_bookmark_url", list(
      url = url,
      v = IBPL_RESTORE_STATE_VERSION
    ))
  })

  # ---- Shared helpers & reactives ----
  season_date_bounds <- season_date_bounds_for_year
  last_updated_cache <- reactiveVal(NA_character_)
  data_version_cache <- reactiveVal(NA_character_)
  hub_storylines_ready_year <- reactiveVal(NA_integer_)

  selected_game_year <- reactive({
    input$game_year %||% DEFAULT_GAME_YEAR
  })

  restored_tab <- sanitize_single_choice(restored_input_value(session, "main_tabs"))
  startup_restore_pending <- reactiveVal(
    nzchar(restored_tab) && !identical(restored_tab, "home")
  )

  # One line per session saying whether this session can restore at all. An
  # inactive context means the bookmark never reached the server, which looks
  # identical to "the filters did not come back".
  local({
    ctx <- tryCatch(session$restoreContext, error = function(e) NULL)
    active <- !is.null(ctx) && isTRUE(ctx$active)
    ids <- if (active) tryCatch(names(ctx$input$asList()), error = function(e) NULL) else NULL
    search <- tryCatch(isolate(session$clientData$url_search), error = function(e) NA_character_)
    app_log("bookmark", sprintf(
      "restore context active=%s values=%d tab=%s lineup_team=%s url_search_len=%s has_inputs=%s",
      active, length(ids), if (nzchar(restored_tab)) restored_tab else "-",
      paste(restored_input_value(session, "ld_lineup_filter-team"), collapse = ","),
      if (is.null(search)) "NULL" else nchar(search %||% ""),
      grepl("_inputs_", search %||% "", fixed = TRUE)
    ), session = session)
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
    for (id in c("teams", "on_opponents", "ld_opponents")) {
      updateSelectizeInput(
        session, id,
        choices = team_choices,
        selected = restore_aware_selection(session, id, isolate(input[[id]]), team_choices),
        server = TRUE
      )
    }
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

  # Storylines own the first expensive startup query batch. A restored session
  # can start on a hidden tab, so release that handoff without waiting for Home.
  observeEvent(selected_game_year(), {
    gy <- suppressWarnings(as.integer(selected_game_year()))
    if (isTRUE(startup_restore_pending())) {
      startup_restore_pending(FALSE)
      hub_storylines_ready_year(gy)
    } else if (nzchar(restored_tab)) {
      hub_storylines_ready_year(gy)
    } else {
      hub_storylines_ready_year(NA_integer_)
    }
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
  server_tab8_euro(input, output, session, shared)
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

  # Card navigation: EuroLeague on/off -> Tab 8. The league switch itself is
  # client-side; this only moves the tab once the card is clicked.
  observeEvent(input$go_euro_onoff, {
    updateTabsetPanel(session, "main_tabs", selected = "euro")
  })

  log_startup("server modules initialized")
}

shinyApp(ui, server)

