# app.R - Main entry point
# Sources modular files and assembles the Shiny app


# Source all modules
source("R/global.R", local = TRUE)
source("R/mod_lineup_player_filter.R", local = TRUE)
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
    tags$script(HTML(
      "(function() {
         if (!window.console || typeof window.console.warn !== 'function') return;
         var origWarn = window.console.warn.bind(window.console);
         var blocked = [
           'DEPRECATED: This filename',
           'The language code \"kh\" is deprecated',
           'The language code \"kr\" is deprecated',
           'This language code \"rs-latin\" is deprecated',
           'This language code \"rs\" is deprecated'
         ];
         window.console.warn = function() {
           var msg = arguments.length ? String(arguments[0]) : '';
           for (var i = 0; i < blocked.length; i++) {
             if (msg.indexOf(blocked[i]) !== -1) return;
           }
           return origWarn.apply(window.console, arguments);
         };
       })();"
    )),
    tags$script(HTML(
      "(function() {
         var lastSent = 0;
         var minIntervalMs = 15000;
         function sendActivity() {
           var now = Date.now();
           if ((now - lastSent) < minIntervalMs) return;
           lastSent = now;
           if (!window.Shiny || typeof window.Shiny.setInputValue !== 'function') return;
           window.Shiny.setInputValue('idle_activity_ts', now, {priority: 'event'});
         }
         function bindActivity() {
           var events = ['mousemove', 'mousedown', 'keydown', 'scroll', 'touchstart', 'click'];
           for (var i = 0; i < events.length; i++) {
             document.addEventListener(events[i], sendActivity, {passive: true});
           }
           document.addEventListener('visibilitychange', function() {
             if (document.visibilityState === 'visible') sendActivity();
           });
           sendActivity();
         }
         if (document.readyState === 'loading') {
           document.addEventListener('DOMContentLoaded', bindActivity);
         } else {
           bindActivity();
         }
       })();"
    )),
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
    ),
    # ── Tab hover dropdown: mode selector on navbar tabs ──
    tags$style(HTML("
      /* Hide sidebar radio buttons — they're still in the DOM for conditionalPanel */
      .view-mode-container { display: none !important; }
      #cmp_mode.shiny-input-radiogroup { display: none !important; }

      /* Dropdown on navbar tabs */
      .nav-item.tab-has-dropdown { position: relative; }
      .tab-hover-menu {
        display: none; position: absolute; top: 100%; left: 50%; transform: translateX(-50%);
        background: #161b22; border: 1px solid #30363d; border-radius: 8px;
        padding: 4px 0; z-index: 10000; min-width: 150px;
        box-shadow: 0 8px 24px rgba(0,0,0,.4);
      }
      .nav-item.tab-has-dropdown:hover .tab-hover-menu { display: block; }
      .tab-hover-menu .thm-item {
        display: flex; align-items: center; gap: 8px;
        padding: 7px 14px; font-size: .78rem; color: #c9d1d9;
        cursor: pointer; white-space: nowrap;
      }
      .tab-hover-menu .thm-item:hover { background: #1c2333; color: #e6edf3; }
      .tab-hover-menu .thm-item.active { color: #e8a435; }
      .tab-hover-menu .thm-item .thm-check { width: 14px; text-align: center; font-size: .68rem; }
    ")),
    tags$script(HTML("
      (function() {
        var CFG = [
          {tab: 'onoff',             inputId: 'onoff_view_mode', items: ['Summary', 'Four Factors'], def: 'Summary'},
          {tab: 'lineup_data',       inputId: 'ld_view_mode',    items: ['Summary', 'Four Factors'], def: 'Summary'},
          {tab: 'team_ratings',      inputId: 'tr_view_mode',    items: ['Summary', 'Four Factors', 'Traditional'], def: 'Summary'},
          {tab: 'game_logs',         inputId: 'gl_view_mode',    items: ['Summary', 'Four Factors'], def: 'Summary'},
          {tab: 'traditional_stats', inputId: 'ts_display_mode', items: ['Totals', 'Per Game', 'Per 60 Possessions', 'Per 30 Minutes'], def: 'Per Game', type: 'select'},
          {tab: 'compare',           inputId: 'cmp_mode',        items: ['Teams', 'Lineups', 'Players'], def: 'Teams'}
        ];

        function init() {
          CFG.forEach(function(c) {
            var link = document.querySelector('.nav-link[data-value=\"' + c.tab + '\"]');
            if (!link) return;
            var li = link.closest('.nav-item');
            li.classList.add('tab-has-dropdown');

            // Build dropdown menu
            var menu = document.createElement('div');
            menu.className = 'tab-hover-menu';
            c.items.forEach(function(item) {
              var row = document.createElement('div');
              row.className = 'thm-item' + (item === c.def ? ' active' : '');
              row.innerHTML = '<span class=\"thm-check\">' + (item === c.def ? '\\u2713' : '') + '</span>' + item;
              row.dataset.value = item;
              row.dataset.inputId = c.inputId;
              row.addEventListener('click', function(e) {
                e.stopPropagation();
                e.preventDefault();
                var val = this.dataset.value;
                var iid = this.dataset.inputId;
                // Navigate to this tab
                link.click();
                // Update hidden input: radio button or select
                if (c.type === 'select') {
                  setTimeout(function() {
                    var sel = document.getElementById(iid);
                    if (sel) {
                      sel.value = val;
                      sel.dispatchEvent(new Event('change', { bubbles: true }));
                    }
                    if (window.Shiny && typeof window.Shiny.setInputValue === 'function') {
                      window.Shiny.setInputValue(iid, val, { priority: 'event' });
                    }
                  }, 0);
                } else {
                  setTimeout(function() {
                    var radio = document.querySelector('input[name=\"' + iid + '\"][value=\"' + val + '\"]');
                    if (radio) { radio.click(); }
                    if (window.Shiny && typeof window.Shiny.setInputValue === 'function') {
                      window.Shiny.setInputValue(iid, val, { priority: 'event' });
                    }
                  }, 0);
                }
                // Update dropdown UI
                menu.querySelectorAll('.thm-item').forEach(function(r) {
                  var isActive = r.dataset.value === val;
                  r.className = 'thm-item' + (isActive ? ' active' : '');
                  r.querySelector('.thm-check').textContent = isActive ? '\\u2713' : '';
                });
                // Close menu
                menu.style.display = 'none';
                setTimeout(function() { menu.style.display = ''; }, 50);
              });
              menu.appendChild(row);
            });
            li.appendChild(menu);

            // Sync dropdown when hidden input changes (e.g. server-side reset)
            function syncMenu() {
              var current;
              if (c.type === 'select') {
                var sel = document.getElementById(c.inputId);
                current = sel ? sel.value : c.def;
              } else {
                var checked = document.querySelector('input[name=\"' + c.inputId + '\"]:checked');
                current = checked ? checked.value : c.def;
              }
              menu.querySelectorAll('.thm-item').forEach(function(r) {
                var isActive = r.dataset.value === current;
                r.className = 'thm-item' + (isActive ? ' active' : '');
                r.querySelector('.thm-check').textContent = isActive ? '\\u2713' : '';
              });
            }
            // Poll on tab activation (lightweight — only runs when user clicks a tab)
            link.addEventListener('shown.bs.tab', syncMenu);
            link.addEventListener('click', function() { setTimeout(syncMenu, 100); });
          });
        }

        if (document.readyState === 'loading') {
          document.addEventListener('DOMContentLoaded', init);
        } else {
          init();
        }
      })();
    "))
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
  last_activity_at <- reactiveVal(as.numeric(Sys.time()))
  idle_timeout_sec <- APP_IDLE_TIMEOUT_SEC
  idle_check_sec <- APP_IDLE_CHECK_SEC
  log_startup <- function(step) {
    elapsed <- proc.time()[["elapsed"]] - startup_t0
    message(sprintf("[startup] %s (%.3fs)", step, elapsed))
  }

  observeEvent(input$idle_activity_ts, {
    last_activity_at(as.numeric(Sys.time()))
  }, ignoreInit = TRUE)

  observe({
    invalidateLater(idle_check_sec * 1000L, session)
    idle_for <- as.numeric(Sys.time()) - last_activity_at()
    if (is.finite(idle_for) && idle_for >= idle_timeout_sec) {
      session$close()
    }
  })

  # ---- Shared helpers & reactives ----
  season_date_bounds <- function(gy) {
    if (identical(gy, "2026")) {
      list(start = as.Date("2025-10-01"), end = as.Date("2026-07-01"))
    } else {
      list(start = DEFAULT_START, end = DEFAULT_END)
    }
  }

  selected_game_year <- reactive({
    input$game_year %||% DEFAULT_GAME_YEAR
  })

  # ===== Teams dropdown choices =====
  teams_for_year_df <- reactive({
    gy_int <- as.integer(selected_game_year())
    req(gy_int)
    cached_ref_query(
      key = sprintf("teams_for_year_%d", gy_int),
      query_fun = function() {
        db_get_query(
          pg_pool,
          sprintf("SELECT DISTINCT team_id, team_name FROM basketball_test.full_rosters WHERE game_year = %d ORDER BY team_name", gy_int)
        )
      }
    )
  })

  prewarm_for_year <- function(gy_chr) {
    gy_int <- suppressWarnings(as.integer(gy_chr))
    if (!is.finite(gy_int) || is.na(gy_int)) return(invisible(NULL))
    cache_now <- as.numeric(Sys.time())
    cache_alias <- function(key, val) {
      assign(key, list(ts = cache_now, val = val), envir = .ref_cache_env)
      invisible(val)
    }

    # Shared teams query (DISTINCT pattern used by tabs 1, 3, 5).
    teams_distinct_q <- function() db_get_query(
      pg_pool,
      sprintf("SELECT DISTINCT team_id, team_name FROM basketball_test.full_rosters WHERE game_year = %d ORDER BY team_name", gy_int)
    )
    # Teams query with MIN/GROUP BY (used by tabs 2, 4).
    teams_min_q <- function() db_get_query(
      pg_pool,
      sprintf("SELECT DISTINCT team_id, MIN(team_name) AS team_name FROM basketball_test.full_rosters WHERE game_year = %d GROUP BY team_id ORDER BY MIN(team_name)", gy_int)
    )
    # GN query - shared across all tabs.
    gn_query <- function() db_get_query(
      pg_pool,
      sprintf("SELECT DISTINCT gn FROM basketball_test.final_schedule_mv WHERE game_year = %d ORDER BY gn", gy_int)
    )

    teams_distinct <- cached_ref_query(key = sprintf("teams_for_year_%d", gy_int), query_fun = teams_distinct_q)
    gn_df <- cached_ref_query(key = sprintf("on_gn_%d", gy_int), query_fun = gn_query)

    # Tab 2 (Lineups)
    teams_min <- cached_ref_query(key = sprintf("ld_teams_%d", gy_int), query_fun = teams_min_q)
    cache_alias(sprintf("ld_gn_%d", gy_int), gn_df)
    players_ld <- cached_ref_query(
      key = sprintf("ld_players_%d", gy_int),
      query_fun = function() db_get_query(
        pg_pool,
        sprintf("SELECT team_id, player_id, MIN(btrim(firstname)||' '||btrim(lastname)) AS name FROM basketball_test.full_rosters WHERE game_year = %d GROUP BY team_id, player_id ORDER BY MIN(btrim(firstname)||' '||btrim(lastname))", gy_int)
      )
    )

    # Tab 3 (Team Ratings)
    cache_alias(sprintf("tr_teams_%d", gy_int), teams_distinct)
    cache_alias(sprintf("tr_gn_%d", gy_int), gn_df)

    # Tab 4 (Game Logs)
    cache_alias(sprintf("gl_teams_%d", gy_int), teams_min)
    cache_alias(sprintf("gl_gn_%d", gy_int), gn_df)

    # Tab 5 (Player Stats)
    cache_alias(sprintf("ts_teams_%d", gy_int), teams_distinct)
    cache_alias(sprintf("ts_gn_%d", gy_int), gn_df)

    # Compare tab aliases (same refs)
    cache_alias(sprintf("cmp_teams_%d", gy_int), teams_distinct)
    cache_alias(sprintf("cmp_players_%d", gy_int), players_ld)
    cache_alias(sprintf("cmp_gn_%d", gy_int), gn_df)
  }

  observeEvent(selected_game_year(), {
    td <- teams_for_year_df()
    updateSelectizeInput(session, "teams", choices = td$team_name, selected = character(0), server = TRUE)
    updateSelectizeInput(session, "on_opponents", choices = td$team_name, selected = character(0), server = TRUE)
    updateSelectizeInput(session, "ld_opponents", choices = td$team_name, selected = character(0), server = TRUE)
  }, ignoreInit = FALSE)

  selected_opp_ids_on <- reactive({
    td <- teams_for_year_df()
    sel <- input$on_opponents
    if (is.null(sel) || !length(sel)) return(NULL)
    td %>% filter(team_name %in% sel) %>% pull(team_id)
  })

  selected_opp_ids_ld <- reactive({
    td <- teams_for_year_df()
    sel <- input$ld_opponents
    if (is.null(sel) || !length(sel)) return(NULL)
    td %>% filter(team_name %in% sel) %>% pull(team_id)
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
      q <- db_get_query(
        pg_pool,
        "SELECT value FROM basketball_test.app_meta WHERE key = 'etl_full_last_success' LIMIT 1"
      )
      if (nrow(q) && nzchar(q$value[1])) q$value[1] else NA_character_
    }, error = function(e) NA_character_)
  }

  last_updated_cache <- reactiveVal(NA_character_)

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
    txt <- if (!nzchar(ts)) "Last updated: unavailable" else paste("Last updated:", ts)
    last_updated_cache(txt)
    invisible(NULL)
  }

  observe({
    invalidateLater(60000, session)
    refresh_last_updated()
  })

  output$last_updated <- renderText({
    last_updated_cache() %||% "Last updated: unavailable"
  })

  observeEvent(selected_game_year(), {
    tryCatch(
      {
        prewarm_for_year(selected_game_year())
        log_startup(sprintf("prewarm complete for season %s", selected_game_year()))
      },
      error = function(e) {
        message(sprintf("[startup] prewarm failed for season %s: %s", selected_game_year(), conditionMessage(e)))
      }
    )
  }, ignoreInit = FALSE)

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

  observe({
    teams <- shared$teams_for_year_df()
    req(nrow(teams) > 0)
    choices <- c("", setNames(as.character(teams$team_id), teams$team_name))
    updateSelectizeInput(session, "home_team", choices = choices, selected = "", server = TRUE)
  }) |> bindEvent(shared$teams_for_year_df(), ignoreNULL = TRUE)

  # Card navigation: Who is helping my team? -> Tab 1
  observeEvent(input$go_onoff, {
    teams_df <- shared$teams_for_year_df()
    if (!is.null(input$home_team) && input$home_team != "") {
      team_name <- teams_df$team_name[teams_df$team_id == as.integer(input$home_team)]
      if (length(team_name) > 0) {
        updateSelectizeInput(session, "teams", choices = teams_df$team_name,
                             selected = team_name, server = TRUE)
      }
    } else {
      updateSelectizeInput(session, "teams", choices = teams_df$team_name,
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

