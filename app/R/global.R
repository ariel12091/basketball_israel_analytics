# global.R - Libraries, constants, DB pool, helper functions

library(shiny)
library(DBI)
library(dplyr)
library(pool)
library(RPostgres)
library(DT)
library(purrr)
library(bslib)
library(htmltools)

# Fail-closed HTML escaping policy for DT tables.
dt_escape_except <- function(data, html_cols = character()) {
  data_cols <- names(data)
  html_cols <- intersect(as.character(html_cols), data_cols)

  if (!length(html_cols)) {
    return(TRUE)
  }

  # DT interprets numeric values as the columns that must be escaped.
  # Positions remain stable when replacement display headers are supplied.
  # All callers use rownames = FALSE, so no row-name offset is required.
  which(!data_cols %in% html_cols)
}

# ---------------- Defaults ----------------
# Default season shown on load. To roll to a new season, bump this and add the
# matching label to the navbar selectInput choices in app.R — nothing else.
DEFAULT_GAME_YEAR <- "2026"   # 25-26
DEFAULT_MIN_ALL <- 100L
DEFAULT_MIN_ON  <- 300L
DEFAULT_MIN_NET <- -1e9
LD_DEFAULT_MIN_POSS <- 20L
LD_DEFAULT_NUM      <- "5"

# Players with fewer possessions than this won't get a color/rank bar
RANKING_BASELINE <- 100
RANKING_MIN_PCT  <- 0.25   # at least 25% of rows should be ranked

# Season window for a given game_year: Oct 1 (Y-1) through Jul 1 (Y).
season_date_bounds_for_year <- function(gy = DEFAULT_GAME_YEAR) {
  y <- suppressWarnings(as.integer(gy))
  if (length(y) != 1L || is.na(y)) y <- as.integer(DEFAULT_GAME_YEAR)
  list(start = as.Date(sprintf("%04d-10-01", y - 1L)),
       end   = as.Date(sprintf("%04d-07-01", y)))
}

# Static UI date literals + fast-path sentinels track the default season
# automatically, so they never go stale on a season rollover.
DEFAULT_START <- season_date_bounds_for_year(DEFAULT_GAME_YEAR)$start
DEFAULT_END   <- season_date_bounds_for_year(DEFAULT_GAME_YEAR)$end

# Color scale constants (shared across all renderDT calls)
CUTS      <- seq(0.05, 0.95, by = 0.05)
COLS_GRAD <- colorRampPalette(c("#8b2020", "#6b5a20", "#1a6b38"))(20)
COLS_REV  <- rev(COLS_GRAD)

# ---- Tooltip definitions ----
COLUMN_TOOLTIPS <- c(
  # Efficiency
  "PPP"         = "Points per 100 possessions",
  "Off PPP"     = "Offensive points per 100 possessions",
  "Def PPP"     = "Defensive points per 100 possessions",
  "Net"         = "Offensive PPP minus Defensive PPP",
  "Net Rtg"     = "Offensive PPP minus Defensive PPP",
  "Net RTG"     = "Offensive PPP minus Defensive PPP",
  # On/Off diffs (Tab 1 Summary Net Impact)
  "Off"         = "Offensive PPP diff: On-court minus Off-court",
  "Def"         = "Defensive PPP diff: On-court minus Off-court",
  # On/Off FF total diff
  "Diff"        = "Net PPP impact: On-court minus Off-court",
  # Four Factors
  "TS%"         = "True Shooting: pts / (2 \u00d7 (FGA + FT trips))",
  "USG%"        = "Usage rate: share of team shot, free-throw, and turnover possessions while on court",
  "OREB%"       = "Offense: offensive rebounds / available misses. Defense: opponent offensive rebounds allowed / available misses",
  "TOV%"        = "Turnover rate: turnovers / possessions",
  "FTR"         = "Free throw rate: FTA / FGA",
  # Shooting
  "Off Shot"    = "Offensive 2PT/3PT frequency and accuracy split",
  "Def Shot"    = "Defensive 2PT/3PT frequency and accuracy split",
  "FG%"         = "Field goal percentage",
  "2P%"         = "Two-point percentage",
  "3P%"         = "Three-point percentage",
  "FT%"         = "Free throw percentage",
  "eFG%"        = "Effective FG%: (FGM + 0.5 x 3PM) / FGA",
  # Usage / Volume
  "On Poss"     = "On-court possessions for this player or split",
  "ON Poss"     = "On-court possessions for this player or split",
  "Off Poss"    = "Offensive possessions",
  "OFF Poss"    = "Off-court possessions for this player or split",
  "Poss"        = "Number of possessions in this row or split",
  "Total Poss"  = "Total offensive plus defensive possessions",
  "Def Poss"    = "Defensive possessions",
  "Min"         = "Minutes played",
  "GP"          = "Games played",
  "Poss On Floor" = "Total possessions while this player was on the floor",
  "# Starters"  = "Number of starters in this lineup",
  # Game context
  "GN"          = "Team's sequential game number this season",
  "W/L"         = "Win or Loss",
  "+/-"         = "Point differential while lineup was on court",
  "Off Pace"    = "Offensive possessions per 40 minutes",
  "Def Pace"    = "Defensive possessions per 40 minutes",
  # Traditional
  "PTS" = "Points", "REB" = "Rebounds", "OREB" = "Offensive rebounds",
  "DREB" = "Defensive rebounds", "AST" = "Assists", "STL" = "Steals",
  "BLK" = "Blocks", "TOV" = "Turnovers",
  "FGM" = "Field goals made", "FGA" = "Field goal attempts",
  "2PM" = "Two-pointers made", "2PA" = "Two-point attempts",
  "3PM" = "Three-pointers made", "3PA" = "Three-point attempts",
  "FTM" = "Free throws made", "FTA" = "Free throw attempts"
)

OFF_OREB_TOOLTIP <- "Offensive rebound rate: offensive rebounds / available misses"
DEF_OREB_TOOLTIP <- "Opponent offensive rebound rate allowed: opponent offensive rebounds / available misses"

FILTER_TOOLTIPS <- c(
  "min_poss_side"     = "Minimum OFF + DEF possessions to appear in table",
  "min_on_poss"       = "Minimum ON-court possessions for percentile ranking",
  "own_starters"      = "Filter by number of starters in the team's lineup",
  "opp_starters"      = "Filter by number of starters in the opposing lineup",
  "gn"                = "Sequential game number within a team's season; player and team views both use the team's GN",
  "last_n"            = "Only include the most recent N games in the selected team context",
  "opp_strength"      = "Filter games by the opponent's league ranking over the selected sample",
  "clutch"            = "Limit results to close-game situations based on margin, time remaining, and score status",
  "group_size"        = "Number of players in each lineup combination (2-5)",
  "quick_preset"      = "Apply a prebuilt compare split like starters vs bench, clutch vs non-clutch, or date/GN split",
  "players_on"        = "Lineups must include all selected players",
  "players_off"       = "Lineups must exclude all selected players",
  "min_poss_lineup"   = "Minimum total possessions required for the lineup, team, or compare side to appear",
  "view_summary"      = "PPP ratings and shooting splits",
  "view_ff"           = "eFG%, OREB%, TOV%, FTR breakdown",
  "view_traditional"  = "Box-score counting stats"
)

# Tooltip-wrapped label for sidebar inputs
tt <- function(label, key) {
  tip <- FILTER_TOOLTIPS[[key]]
  if (is.null(tip)) return(label)
  tags$span(label, `data-tooltip` = tip)
}

# Shared JS headerCallback for DT tables - injects data-tooltip on th elements
HEADER_TOOLTIP_JS <- DT::JS(paste0(
  "function(thead, data, start, end, display) {",
  "  var tips = ", jsonlite::toJSON(as.list(COLUMN_TOOLTIPS), auto_unbox = TRUE), ";",
  "  var api = this.api();",
  "  var container = $(api.table().container());",
  "  var cells = container.find('thead th, .dataTables_scrollHead th');",
  "  cells.each(function() {",
  "    var cell = $(this);",
  "    var txt = cell.text().trim();",
  "    var existingTitle = cell.attr('title');",
  "    if (existingTitle) {",
  "      cell.css('cursor', 'help');",
  "      return;",
  "    }",
  "    if (tips[txt]) {",
  "      cell.attr('title', tips[txt]);",
  "      cell.css('cursor', 'help');",
  "    } else {",
  "      cell.removeAttr('title');",
  "    }",
  "  });",
  "}"
))

# Adaptive baseline: use RANKING_BASELINE when enough data qualifies,
# otherwise lower to the 75th-percentile so ~25% still get colored.
adaptive_baseline <- function(poss_vec) {
  n <- sum(!is.na(poss_vec))
  if (n == 0) return(0)
  pct_above <- sum(poss_vec >= RANKING_BASELINE, na.rm = TRUE) / n
  if (pct_above >= RANKING_MIN_PCT) return(RANKING_BASELINE)
  unname(quantile(poss_vec, 1 - RANKING_MIN_PCT, na.rm = TRUE))
}

# Null coalescing operator
`%||%` <- function(a, b) if (!is.null(a)) a else b

apply_visible_col_order <- function(df, visible_order, hidden_cols = character()) {
  if (is.null(df) || !length(visible_order)) return(df)

  all_cols <- names(df)
  hidden_cols <- intersect(hidden_cols, all_cols)
  visible_cols <- setdiff(all_cols, hidden_cols)
  saved_visible <- intersect(as.character(visible_order), visible_cols)
  if (!length(saved_visible)) return(df)

  df[, c(saved_visible, setdiff(visible_cols, saved_visible), hidden_cols), drop = FALSE]
}

dt_col_order_init_callback <- function(input_id, storage_key) {
  input_id_json <- jsonlite::toJSON(input_id, auto_unbox = TRUE)
  restore_id_json <- jsonlite::toJSON(paste0(input_id, "_restore"), auto_unbox = TRUE)
  storage_key_json <- jsonlite::toJSON(storage_key, auto_unbox = TRUE)

  DT::JS(sprintf(
    "function(settings, json) {
      var api = this.api();
      var inputId = %s;
      var restoreId = %s;
      var storageKey = %s;
      var maxColumns = 80;

      var cleanOrder = function(order) {
        if (!Array.isArray(order)) return [];
        return order.filter(function(name) {
          return typeof name === 'string' && name.length > 0 && name.length <= 80;
        }).slice(0, maxColumns);
      };

      var visibleColumnNames = function() {
        return cleanOrder(api.columns(':visible').header().toArray().map(function(header) {
          return $(header).text().replace(/\\s+/g, ' ').trim();
        }));
      };

      var setShinyOrder = function(order) {
        if (window.Shiny) {
          window.Shiny.setInputValue(inputId, cleanOrder(order), {priority: 'event'});
        }
      };

      var loadOrder = function() {
        try {
          var raw = window.localStorage.getItem(storageKey);
          return cleanOrder(raw ? JSON.parse(raw) : []);
        } catch (e) {
          return [];
        }
      };

      var saveOrder = function() {
        var order = visibleColumnNames();
        try {
          window.localStorage.setItem(storageKey, JSON.stringify(order));
        } catch (e) {}
        setShinyOrder(order);
      };

      api.on('column-reorder.dt', function() {
        window.setTimeout(saveOrder, 0);
      });

      var savedOrder = loadOrder();
      if (!savedOrder.length || !window.Shiny) return;

      window.__onoffColumnOrderSeeded = window.__onoffColumnOrderSeeded || {};
      if (window.__onoffColumnOrderSeeded[storageKey]) return;
      window.__onoffColumnOrderSeeded[storageKey] = true;

      setShinyOrder(savedOrder);
      window.setTimeout(function() {
        window.Shiny.setInputValue(restoreId, new Date().getTime(), {priority: 'event'});
      }, 0);
    }",
    input_id_json,
    restore_id_json,
    storage_key_json
  ))
}

csv_export_stamp <- function(now = Sys.time()) {
  format(now, "%Y%m%d_%H%M%S")
}

# ---------------- App-level cache & guardrails ----------------
REF_CACHE_TTL_SEC <- as.numeric(Sys.getenv("REF_CACHE_TTL_SEC", "300"))
if (!is.finite(REF_CACHE_TTL_SEC) || REF_CACHE_TTL_SEC < 0) REF_CACHE_TTL_SEC <- 60

GL_DATA_CACHE_MAX_MB <- as.numeric(Sys.getenv("GL_DATA_CACHE_MAX_MB", "64"))
if (!is.finite(GL_DATA_CACHE_MAX_MB) || GL_DATA_CACHE_MAX_MB <= 0) GL_DATA_CACHE_MAX_MB <- 64
GL_DATA_CACHE_MAX_AGE_SEC <- as.numeric(Sys.getenv("GL_DATA_CACHE_MAX_AGE_SEC", "3600"))
if (!is.finite(GL_DATA_CACHE_MAX_AGE_SEC) || GL_DATA_CACHE_MAX_AGE_SEC <= 0) GL_DATA_CACHE_MAX_AGE_SEC <- 3600
GL_DATA_CACHE <- cachem::cache_mem(
  max_size = GL_DATA_CACHE_MAX_MB * 1024^2,
  max_age = GL_DATA_CACHE_MAX_AGE_SEC
)

PG_STATEMENT_TIMEOUT_MS <- suppressWarnings(as.integer(Sys.getenv("PG_STATEMENT_TIMEOUT_MS", "20000")))
if (!is.finite(PG_STATEMENT_TIMEOUT_MS) || PG_STATEMENT_TIMEOUT_MS <= 0) PG_STATEMENT_TIMEOUT_MS <- 20000L
APP_IDLE_TIMEOUT_SEC <- suppressWarnings(as.integer(Sys.getenv("APP_IDLE_TIMEOUT_SEC", "360")))
if (!is.finite(APP_IDLE_TIMEOUT_SEC) || APP_IDLE_TIMEOUT_SEC <= 0) APP_IDLE_TIMEOUT_SEC <- 360L
APP_IDLE_TIMEOUT_MIN <- suppressWarnings(as.numeric(Sys.getenv("APP_IDLE_TIMEOUT_MIN", "")))
if (is.finite(APP_IDLE_TIMEOUT_MIN) && APP_IDLE_TIMEOUT_MIN > 0) {
  APP_IDLE_TIMEOUT_SEC <- as.integer(round(APP_IDLE_TIMEOUT_MIN * 60))
}
DEFAULT_IDLE_WARNING_SEC <- min(60L, max(10L, as.integer(floor(APP_IDLE_TIMEOUT_SEC / 4))))
APP_IDLE_WARNING_SEC <- suppressWarnings(as.integer(Sys.getenv("APP_IDLE_WARNING_SEC", DEFAULT_IDLE_WARNING_SEC)))
if (!is.finite(APP_IDLE_WARNING_SEC) || APP_IDLE_WARNING_SEC <= 0) APP_IDLE_WARNING_SEC <- DEFAULT_IDLE_WARNING_SEC
APP_IDLE_WARNING_SEC <- min(APP_IDLE_WARNING_SEC, max(1L, APP_IDLE_TIMEOUT_SEC - 1L))
APP_IDLE_CHECK_SEC <- suppressWarnings(as.integer(Sys.getenv("APP_IDLE_CHECK_SEC", "15")))
if (!is.finite(APP_IDLE_CHECK_SEC) || APP_IDLE_CHECK_SEC <= 0) APP_IDLE_CHECK_SEC <- 15L
APP_IDLE_STATE_TTL_HOURS <- suppressWarnings(as.numeric(Sys.getenv("APP_IDLE_STATE_TTL_HOURS", "24")))
if (!is.finite(APP_IDLE_STATE_TTL_HOURS) || APP_IDLE_STATE_TTL_HOURS <= 0) APP_IDLE_STATE_TTL_HOURS <- 24
APP_IDLE_CLOSE_SESSION <- tolower(trimws(Sys.getenv("APP_IDLE_CLOSE_SESSION", "false"))) %in% c("1", "true", "yes", "on")

.ref_cache_env <- new.env(parent = emptyenv())

cached_ref_query <- function(key, query_fun, ttl_sec = REF_CACHE_TTL_SEC) {
  now <- as.numeric(Sys.time())
  if (exists(key, envir = .ref_cache_env, inherits = FALSE)) {
    cached <- get(key, envir = .ref_cache_env, inherits = FALSE)
    if (!is.null(cached$ts) && !is.null(cached$val) && (now - cached$ts) <= ttl_sec) {
      return(cached$val)
    }
  }
  val <- query_fun()
  assign(key, list(ts = now, val = val), envir = .ref_cache_env)
  val
}

# Central query helper used across modules.
# Kept as a thin wrapper for pooler compatibility with parameterized queries.
db_get_query <- function(conn_or_pool, statement, params = NULL) {
  if (is.null(params)) {
    DBI::dbGetQuery(conn_or_pool, statement)
  } else {
    DBI::dbGetQuery(conn_or_pool, statement, params = params)
  }
}

# app_log() lives in R/logger.R (sourced from app.R after global.R).

# ---------------- Session safety guards ----------------
init_session_request_guard <- function(session) {
  if (is.null(session$userData$request_guard)) {
    env <- new.env(parent = emptyenv())
    env$last_notice_at <- 0
    session$userData$request_guard <- env
  }
  invisible(TRUE)
}

guard_query_window <- function(start_d = NA, end_d = NA, min_gn = NA_integer_, max_gn = NA_integer_,
                               last_n = NA_integer_, max_days = 430L, max_last_n = 80L, max_gn_span = 80L) {
  if (!is.na(last_n) && as.integer(last_n) > as.integer(max_last_n)) {
    return(list(ok = FALSE, reason = sprintf("Last N is capped at %d.", as.integer(max_last_n))))
  }
  if (!is.na(min_gn) && !is.na(max_gn)) {
    span <- as.integer(max_gn) - as.integer(min_gn) + 1L
    if (is.finite(span) && span > as.integer(max_gn_span)) {
      return(list(ok = FALSE, reason = sprintf("GN range is capped at %d games.", as.integer(max_gn_span))))
    }
  }
  s <- suppressWarnings(as.Date(start_d))
  e <- suppressWarnings(as.Date(end_d))
  if (!is.na(s) && !is.na(e) && e < s) {
    return(list(ok = FALSE, reason = "End date must be on or after start date."))
  }
  if (!is.na(s) && !is.na(e)) {
    span_days <- as.integer(e - s) + 1L
    if (is.finite(span_days) && span_days > as.integer(max_days)) {
      return(list(ok = FALSE, reason = sprintf("Date window is capped at %d days.", as.integer(max_days))))
    }
  }
  list(ok = TRUE, reason = "")
}

guard_heavy_request <- function(session, key,
                                start_d = NA, end_d = NA,
                                min_gn = NA_integer_, max_gn = NA_integer_, last_n = NA_integer_,
                                max_calls = 30L, window_sec = 60L,
                                max_days = 430L, max_last_n = 80L, max_gn_span = 80L) {
  init_session_request_guard(session)
  env <- session$userData$request_guard
  now <- as.numeric(Sys.time())
  bucket <- paste0("hits_", key)
  hits <- env[[bucket]]
  if (is.null(hits)) hits <- numeric(0)
  hits <- hits[hits >= (now - as.numeric(window_sec))]
  if (length(hits) >= as.integer(max_calls)) {
    if ((now - env$last_notice_at) > 2) {
      showNotification("Too many requests. Please wait a few seconds and try again.", type = "warning", duration = 4)
      env$last_notice_at <- now
    }
    env[[bucket]] <- hits
    return(FALSE)
  }
  env[[bucket]] <- c(hits, now)

  chk <- guard_query_window(
    start_d = start_d, end_d = end_d,
    min_gn = min_gn, max_gn = max_gn, last_n = last_n,
    max_days = max_days, max_last_n = max_last_n, max_gn_span = max_gn_span
  )
  if (!isTRUE(chk$ok)) {
    if ((now - env$last_notice_at) > 2) {
      showNotification(chk$reason, type = "warning", duration = 4)
      env$last_notice_at <- now
    }
    return(FALSE)
  }
  TRUE
}

# ---------------- PostgreSQL pool ----------------
pg_pool <- dbPool(
  drv      = Postgres(),
  bigint   = "numeric",
  host     = Sys.getenv("PG_HOST"),
  port     = as.integer(Sys.getenv("PG_PORT", "6543")),
  dbname   = Sys.getenv("PG_DB"),
  user     = Sys.getenv("PG_USER"),
  password = Sys.getenv("PG_PASS"),
  sslmode  = Sys.getenv("PG_SSLMODE", "require"),
  options  = sprintf("-c statement_timeout=%d", PG_STATEMENT_TIMEOUT_MS),
  minSize  = 0,
  maxSize  = as.integer(Sys.getenv("POOL_MAX", "3")),
  idleTimeout = 15000
)
onStop(function() poolClose(pg_pool))

# Pre-warm the connection pool (force SSL handshake at source time)
tryCatch(db_get_query(pg_pool, "SELECT 1"), error = function(e) NULL)

# Shared head tags
shared_head_tags <- function() {
  tags$head(
    tags$meta(name = "viewport", content = "width=device-width, initial-scale=1, maximum-scale=1"),
    tags$link(rel = "stylesheet", href = "https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500&family=Inter:wght@400;500;600;700&display=swap"),
    tags$link(rel = "stylesheet", href = "https://cdn.jsdelivr.net/npm/bootstrap-icons@1.11.3/font/bootstrap-icons.min.css")
  )
}

tab_explainer <- function(id, title, intro, bullets) {
  body_id <- paste0(id, "_body")
  tags$div(
    class = "explainer-card",
    tags$div(
      class = "explainer-top",
      tags$h4(class = "explainer-title", title),
      tags$a(
        href = "#",
        class = "explainer-toggle js-explainer-toggle",
        `data-target-id` = body_id,
        "Show/Hide"
      )
    ),
    tags$div(
      id = body_id,
      class = "explainer-body collapse",
      tags$p(intro),
      tags$ul(lapply(bullets, tags$li)),
      tags$p(
        style = "margin-top: 8px; margin-bottom: 0;",
        tags$a(
          href = "#",
          class = "js-shiny-event",
          `data-input-id` = "open_glossary",
          "Open glossary"
        )
      )
    )
  )
}

# ---------------- Filter Chips Builder ----------------
GAME_TYPE_LABELS <- c("5" = "Regular season", "16" = "PO QF", "26" = "PO SF",
                       "17" = "PO Finals", "33" = "Play-in", "34" = "Winner Cup", "35" = "State Cup")
GAME_TYPE_CHOICES_UI <- c(
  "All" = "",
  "Regular season" = "5",
  "Playoffs - Quarterfinals" = "16",
  "Playoffs - Finals" = "17",
  "Playoffs - Semifinals" = "26",
  "Play-in" = "33",
  "Winner Cup" = "34",
  "State Cup" = "35"
)

accordion_toggle_link <- function() {
  tags$div(
    class = "text-end mb-2",
    tags$a(
      href = "#",
      class = "small text-muted fw-bold js-accordion-toggle-all",
      style = "text-decoration: none;",
      "Collapse/Expand All"
    )
  )
}

game_context_filters_ui <- function(prefix, include_opp_rank = TRUE, opp_rank_blank_label = "\u2014") {
  panels <- list(
    bslib::accordion_panel(
      "Game Filters",
      selectizeInput(
        paste0(prefix, "_game_type"), "Game type",
        choices = GAME_TYPE_CHOICES_UI,
        selected = "", multiple = TRUE,
        options = list(placeholder = "All game types")
      ),
      selectizeInput(
        paste0(prefix, "_opponents"), "Opponents",
        choices = NULL, selected = character(0), multiple = TRUE,
        options = list(placeholder = "All opponents")
      ),
      selectInput(
        paste0(prefix, "_home_away"), "Home/Away",
        choices = c("All" = "", "Home" = "home", "Away" = "away"),
        selected = ""
      ),
      selectInput(
        paste0(prefix, "_outcome"), "Outcome",
        choices = c("All" = "", "Win" = "win", "Loss" = "loss"),
        selected = ""
      ),
      tags$hr(),
      fluidRow(
        column(
          6,
          selectizeInput(
            paste0(prefix, "_gn_min"), tt("From Game Number (GN)", "gn"),
            choices = NULL, selected = "", multiple = FALSE,
            options = list(placeholder = "Any")
          )
        ),
        column(
          6,
          selectizeInput(
            paste0(prefix, "_gn_max"), tt("To Game Number (GN)", "gn"),
            choices = NULL, selected = "", multiple = FALSE,
            options = list(placeholder = "Any")
          )
        )
      ),
      selectizeInput(
        paste0(prefix, "_last_n"), tt("Last N Team Games", "last_n"),
        choices = NULL, selected = "", multiple = FALSE,
        options = list(placeholder = "Any")
      )
    )
  )

  if (isTRUE(include_opp_rank)) {
    blank_choice <- setNames("", opp_rank_blank_label)
    panels[[length(panels) + 1]] <- bslib::accordion_panel(
      tt("Opponent Strength", "opp_strength"), value = "Opponent Strength",
      selectInput(
        paste0(prefix, "_opp_rank_side"), "Top / Bottom",
        choices = c("Off" = "", "Top" = "top", "Bottom" = "bottom"),
        selected = ""
      ),
      selectInput(
        paste0(prefix, "_opp_rank_n"), "Rank N",
        choices = c(blank_choice, setNames(as.character(1:12), as.character(1:12))),
        selected = ""
      ),
      selectInput(
        paste0(prefix, "_opp_rank_metric"), "Metric",
        choices = c(blank_choice, "Offense" = "off", "Defense" = "def", "Net rating" = "net"),
        selected = ""
      )
    )
  }

  do.call(bslib::accordion, c(panels, list(open = TRUE)))
}

make_chip <- function(label, clear_id, css_class = "") {
  tags$span(
    class = paste("filter-chip", css_class),
    label,
    tags$button(
      class = "chip-x",
      type = "button",
      `data-shiny-event` = clear_id,
      HTML("&times;")
    )
  )
}

make_season_chip <- function(gy) {
  label <- if (identical(gy, "2026")) "2025-26" else if (identical(gy, "2025")) "2024-25" else gy
  tags$span(class = "filter-chip chip-season", label)
}

normalize_stat_filter_cols <- function(filterable_cols) {
  cols <- if (is.function(filterable_cols)) filterable_cols() else filterable_cols
  if (is.null(cols)) return(stats::setNames(character(0), character(0)))
  if (is.list(cols) && !is.atomic(cols)) cols <- unlist(cols, use.names = TRUE)
  labels <- names(cols)
  cols <- as.character(cols)
  if (is.null(labels)) labels <- rep("", length(cols))
  keep <- nzchar(labels) & nzchar(cols)
  stats::setNames(cols[keep], labels[keep])
}

make_stat_filter_state <- function() {
  list(
    filters = reactiveVal(list()),
    next_id = reactiveVal(1L)
  )
}

reset_stat_filters <- function(state) {
  state$filters(list())
  state$next_id(1L)
  invisible(NULL)
}

apply_stat_filters <- function(df, filters) {
  if (is.null(df) || !nrow(df) || !length(filters)) return(df)
  for (f in filters) {
    col <- f$col
    if (!col %in% names(df)) next
    v <- suppressWarnings(as.numeric(df[[col]]))
    threshold <- suppressWarnings(as.numeric(f$value))
    if (length(threshold) != 1L || !is.finite(threshold)) next
    keep <- !is.na(v) & (if (identical(f$op, "le")) v <= threshold else v >= threshold)
    df <- df[keep, , drop = FALSE]
    if (!nrow(df)) break
  }
  df
}

setup_stat_filter_handlers <- function(prefix, input, session, filterable_cols, state) {
  add_id <- paste0(prefix, "_add_stat_filter")
  remove_id <- paste0(prefix, "_remove_stat_filter")
  col_id <- paste0(prefix, "_stat_filter_col")
  op_id <- paste0(prefix, "_stat_filter_op")
  value_id <- paste0(prefix, "_stat_filter_value")

  observeEvent(input[[add_id]], {
    cols <- normalize_stat_filter_cols(filterable_cols)
    col_label <- input[[col_id]] %||% ""
    op <- input[[op_id]] %||% "ge"
    raw_val <- input[[value_id]]
    val <- suppressWarnings(as.numeric(raw_val))
    if (!nzchar(col_label) || !col_label %in% names(cols)) return()
    if (!op %in% c("ge", "le")) return()
    if (length(val) != 1L || !is.finite(val)) return()

    new_id <- state$next_id()
    state$next_id(new_id + 1L)

    current <- state$filters()
    current[[length(current) + 1]] <- list(
      id = new_id,
      label = col_label,
      col = unname(cols[[col_label]]),
      op = op,
      value = val
    )
    state$filters(current)

    updateSelectInput(session, col_id, selected = "")
    updateRadioButtons(session, op_id, selected = "ge")
    updateNumericInput(session, value_id, value = NA)
  })

  observeEvent(input[[remove_id]], {
    rm_id <- suppressWarnings(as.integer(input[[remove_id]]))
    if (is.na(rm_id)) return()
    current <- state$filters()
    keep <- vapply(current, function(f) !identical(as.integer(f$id), rm_id), logical(1))
    state$filters(current[keep])
  }, ignoreInit = TRUE)
}

stat_filter_chips_ui <- function(prefix, state, filterable_cols, percent_hint = NULL) {
  cols <- normalize_stat_filter_cols(filterable_cols)
  choices <- names(cols)
  remove_id <- paste0(prefix, "_remove_stat_filter")
  filter_chips <- lapply(state$filters(), function(f) {
    op_sym <- if (identical(f$op, "ge")) "\u2265" else "\u2264"
    val_txt <- format(f$value, big.mark = ",", trim = TRUE)
    tags$span(
      class = "filter-chip chip-stat",
      sprintf("%s %s %s", f$label, op_sym, val_txt), " ",
      tags$a(
        href = "#",
        class = "js-shiny-event",
        `data-input-id` = remove_id,
        `data-shiny-value` = as.character(as.integer(f$id)),
        style = "margin-left:4px;color:inherit;",
        HTML("&times;")
      )
    )
  })

  pct_msg <- percent_hint
  if (is.null(pct_msg) && any(grepl("%", choices, fixed = TRUE))) {
    pct_msg <- "Percent columns: enter as 0-100."
  }

  add_btn <- bslib::popover(
    trigger = tags$span(
      class = "filter-chip filter-chip-add",
      id = paste0(prefix, "_stat_filter_add_btn"),
      tags$i(class = "bi bi-plus"), " Filter"
    ),
    title = "Add stat filter",
    placement = "bottom",
    div(
      class = paste0(prefix, "-stat-popover"),
      style = "min-width: 220px;",
      selectInput(
        paste0(prefix, "_stat_filter_col"), "Column",
        choices = c("Choose..." = "", choices),
        selected = "",
        width = "100%"
      ),
      radioButtons(
        paste0(prefix, "_stat_filter_op"), "Operator",
        choices = c("\u2265" = "ge", "\u2264" = "le"),
        selected = "ge",
        inline = TRUE
      ),
      numericInput(
        paste0(prefix, "_stat_filter_value"), "Value",
        value = NA,
        width = "100%"
      ),
      if (!is.null(pct_msg) && nzchar(pct_msg)) {
        tags$div(class = "small text-muted mb-2", pct_msg)
      },
      actionButton(
        paste0(prefix, "_add_stat_filter"), "Add",
        class = "btn-sm btn-primary w-100"
      )
    )
  )

  c(filter_chips, list(add_btn))
}

build_filter_chips <- function(prefix, input, season_bounds_fn, reset_btn_id = NULL,
                               team_label_map = NULL, opponent_label_map = NULL,
                               player_label_map = NULL,
                               teams_value = NULL, players_on_value = NULL, players_off_value = NULL,
                               extra_children = NULL) {
  get_input <- function(suffix) input[[paste0(prefix, suffix)]]
  map_label <- function(x, label_map) {
    if (is.null(label_map) || is.null(x)) return(x)
    key <- as.character(x)
    out <- unname(label_map[key])
    out[is.na(out) | !nzchar(out)] <- key[is.na(out) | !nzchar(out)]
    out
  }
  same_date <- function(a, b) {
    if (is.null(a) || is.null(b)) return(FALSE)
    a <- tryCatch(as.Date(a), error = function(e) NA)
    b <- tryCatch(as.Date(b), error = function(e) NA)
    if (is.na(a) || is.na(b)) return(FALSE)
    identical(a, b)
  }
  safe_date_token <- function(x, idx = 1L) {
    if (is.null(x) || is.environment(x)) return(NA_character_)
    i <- suppressWarnings(as.integer(idx))
    if (is.na(i) || i < 1L) i <- 1L
    if (length(x) < i) return(NA_character_)
    val <- tryCatch(x[[i]], error = function(e) NA_character_)
    if (is.environment(val)) return(NA_character_)
    as.character(val %||% NA_character_)
  }
  chips <- list()

  # Season chip (always visible, not dismissable) - single global input
  gy <- input$game_year %||% DEFAULT_GAME_YEAR
  chips[[length(chips) + 1]] <- make_season_chip(gy)

  # Date range (non-default)
  date_input <- if (prefix == "on") input$date_range else input[[paste0(prefix, "_dates")]]
  if (!is.null(date_input) && !is.environment(date_input) && length(date_input) == 2) {
    raw_start <- safe_date_token(date_input, 1L)
    raw_end <- safe_date_token(date_input, 2L)
    has_raw_start <- !is.null(raw_start) && nzchar(as.character(raw_start)) && !identical(as.character(raw_start), "NA")
    has_raw_end <- !is.null(raw_end) && nzchar(as.character(raw_end)) && !identical(as.character(raw_end), "NA")
    has_any_raw <- has_raw_start || has_raw_end

    start_d <- tryCatch(as.Date(raw_start), error = function(e) NA)
    end_d <- tryCatch(as.Date(raw_end), error = function(e) NA)

    bounds <- season_bounds_fn(gy)
    if (is.na(start_d)) start_d <- bounds$start
    if (is.na(end_d)) end_d <- bounds$end

    resolved_set <- !is.na(start_d) && !is.na(end_d)
    if (resolved_set) {
      show_when_set <- prefix %in% c("ld", "tr", "gl")
      is_non_default <- !same_date(start_d, bounds$start) || !same_date(end_d, bounds$end)
      if ((show_when_set && has_any_raw) || (!show_when_set && is_non_default)) {
        lbl <- paste(format(start_d, "%b %d"), "\u2013", format(end_d, "%b %d"))
        chips[[length(chips) + 1]] <- make_chip(lbl, paste0(prefix, "_clear_dates"), "chip-game")
      }
    }
  }

  # Game type
  gt <- get_input("_game_type")
  if (prefix == "on") gt <- input$on_game_type
  if (!is.null(gt) && length(gt) && any(nzchar(gt))) {
    labels <- vapply(gt[nzchar(gt)], function(x) GAME_TYPE_LABELS[x] %||% x, "")
    chips[[length(chips) + 1]] <- make_chip(paste(labels, collapse = ", "), paste0(prefix, "_clear_game_type"), "chip-game")
  }

  # Teams
  if (prefix == "on") {
    teams_val <- input$teams
  } else if (prefix == "ts") {
    teams_val <- input$ts_teams
  } else if (prefix %in% c("ld", "gl")) {
    tv <- teams_value %||% input[[paste0(prefix, "_team")]]
    teams_val <- if (!is.null(tv) && nzchar(tv %||% "")) tv else NULL
  } else {
    teams_val <- NULL
  }
  if (!is.null(teams_val) && length(teams_val) && any(nzchar(teams_val))) {
    mapped_teams <- map_label(teams_val, team_label_map)
    lbl <- if (length(mapped_teams) == 1) mapped_teams[1] else paste0(length(mapped_teams), " teams")
    chips[[length(chips) + 1]] <- make_chip(lbl, paste0(prefix, "_clear_teams"), "chip-game")
  }

  # Opponents
  opp_val <- get_input("_opponents")
  if (prefix == "on") opp_val <- input$on_opponents
  if (!is.null(opp_val) && length(opp_val)) {
    mapped_opponents <- map_label(opp_val, opponent_label_map)
    lbl <- if (length(mapped_opponents) == 1) paste("vs", mapped_opponents[1]) else paste0("vs ", length(mapped_opponents), " opps")
    chips[[length(chips) + 1]] <- make_chip(lbl, paste0(prefix, "_clear_opponents"), "chip-game")
  }

  # Home/Away
  ha <- get_input("_home_away")
  if (prefix == "on") ha <- input$on_home_away
  if (!is.null(ha) && nzchar(ha)) {
    chips[[length(chips) + 1]] <- make_chip(if (ha == "home") "Home" else "Away", paste0(prefix, "_clear_home_away"), "chip-game")
  }

  # Outcome
  out_val <- get_input("_outcome")
  if (prefix == "on") out_val <- input$on_outcome
  if (!is.null(out_val) && nzchar(out_val)) {
    chips[[length(chips) + 1]] <- make_chip(if (out_val == "win") "Wins" else "Losses", paste0(prefix, "_clear_outcome"), "chip-game")
  }

  # GN range
  gn_min <- get_input("_gn_min")
  gn_max <- get_input("_gn_max")
  if (prefix == "on") { gn_min <- input$on_gn_min; gn_max <- input$on_gn_max }
  if ((!is.null(gn_min) && nzchar(gn_min)) || (!is.null(gn_max) && nzchar(gn_max))) {
    parts <- c()
    if (!is.null(gn_min) && nzchar(gn_min)) parts <- c(parts, paste0("GN\u2265", gn_min))
    if (!is.null(gn_max) && nzchar(gn_max)) parts <- c(parts, paste0("GN\u2264", gn_max))
    chips[[length(chips) + 1]] <- make_chip(paste(parts, collapse = " "), paste0(prefix, "_clear_gn"), "chip-game")
  }

  # Last N
  last_n <- get_input("_last_n")
  if (prefix == "on") last_n <- input$on_last_n
  if (!is.null(last_n) && nzchar(last_n)) {
    chips[[length(chips) + 1]] <- make_chip(paste("Last", last_n, "games"), paste0(prefix, "_clear_last_n"), "chip-game")
  }

  # Opponent strength
  opp_side <- get_input("_opp_rank_side")
  if (prefix == "on") opp_side <- input$on_opp_rank_side
  if (!is.null(opp_side) && nzchar(opp_side)) {
    rank_n <- get_input("_opp_rank_n")
    rank_m <- get_input("_opp_rank_metric")
    if (prefix == "on") { rank_n <- input$on_opp_rank_n; rank_m <- input$on_opp_rank_metric }
    parts <- paste0("vs ", opp_side)
    if (!is.null(rank_n) && nzchar(rank_n)) parts <- paste0(parts, " ", rank_n)
    if (!is.null(rank_m) && nzchar(rank_m)) parts <- paste0(parts, " ", rank_m)
    chips[[length(chips) + 1]] <- make_chip(parts, paste0(prefix, "_clear_opp_rank"), "chip-game")
  }

  # Clutch (Tab 2, 3, 5 only)
  clutch_enabled <- get_input("_clutch_enabled")
  if (isTRUE(clutch_enabled)) {
    margin <- get_input("_clutch_margin") %||% 5
    mins <- get_input("_clutch_minutes") %||% 5
    status <- get_input("_clutch_status") %||% "all"
    mins <- suppressWarnings(as.integer(mins))
    if (is.na(mins) || mins < 1L) mins <- 5L
    lbl <- paste0("Clutch \u2264", mins, "min margin\u2264", margin)
    if (!identical(status, "all")) lbl <- paste0(lbl, " ", status)
    chips[[length(chips) + 1]] <- make_chip(lbl, paste0(prefix, "_clear_clutch"), "chip-clutch")
  }

  # Starters filter
  off_mode <- input[[paste0(prefix, "_num_starters_off_mode")]]
  off_val <- input[[paste0(prefix, "_num_starters_off")]]
  def_mode <- input[[paste0(prefix, "_num_starters_def_mode")]]
  def_val <- input[[paste0(prefix, "_num_starters_def")]]
  starters_parts <- c()
  if (!is.null(off_mode) && nzchar(off_mode) && !is.null(off_val) && nzchar(off_val)) {
    sym <- if (off_mode == "gte") "\u2265" else "\u2264"
    starters_parts <- c(starters_parts, paste0("Own ", sym, off_val))
  }
  if (!is.null(def_mode) && nzchar(def_mode) && !is.null(def_val) && nzchar(def_val)) {
    sym <- if (def_mode == "gte") "\u2265" else "\u2264"
    starters_parts <- c(starters_parts, paste0("Opp ", sym, def_val))
  }
  if (length(starters_parts)) {
    chips[[length(chips) + 1]] <- make_chip(
      paste("Starters:", paste(starters_parts, collapse = ", ")),
      paste0(prefix, "_clear_starters"), "chip-game")
  }

  # Players on/off (Tab 2)
  if (prefix == "ld") {
    pon <- players_on_value %||% input$ld_players_on
    if (!is.null(pon) && length(pon)) {
      mapped_on <- map_label(pon, player_label_map)
      lbl <- if (length(mapped_on) == 1) paste("On:", mapped_on[1]) else paste0("On: ", length(mapped_on), " players")
      chips[[length(chips) + 1]] <- make_chip(lbl, "ld_clear_players_on", "chip-game")
    }
    poff <- players_off_value %||% input$ld_players_off
    if (!is.null(poff) && length(poff)) {
      mapped_off <- map_label(poff, player_label_map)
      lbl <- if (length(mapped_off) == 1) paste("Off:", mapped_off[1]) else paste0("Off: ", length(mapped_off), " players")
      chips[[length(chips) + 1]] <- make_chip(lbl, "ld_clear_players_off", "chip-game")
    }
  }

  # Only show "Clear all" if there are removable chips (more than just season)
  has_active <- length(chips) > 1

  chip_children <- c(chips, extra_children %||% list())

  tags$div(
    class = "filter-chips",
    chip_children,
    if (has_active) {
      if (!is.null(reset_btn_id)) {
        tags$button(
          class = "chip-clear-all",
          type = "button",
          `data-click-target` = reset_btn_id,
          "Clear all"
        )
      } else {
        tags$button(
          class = "chip-clear-all",
          type = "button",
          `data-shiny-event` = paste0(prefix, "_reset_all_chips"),
          "Clear all"
        )
      }
    }
  )
}

setup_chip_clears <- function(prefix, session, input, shared,
                              game_type_id, opponents_id, home_away_id, outcome_id,
                              gn_min_id, gn_max_id, last_n_id, opp_rank_ids,
                              date_id, gy_input_id,
                              teams_ids = NULL, starters_ids = NULL,
                              clutch_enabled_id = NULL) {
  observeEvent(input[[paste0(prefix, "_clear_game_type")]], {
    updateSelectizeInput(session, game_type_id, selected = character(0))
  }, ignoreInit = TRUE)

  if (!is.null(teams_ids)) {
    observeEvent(input[[paste0(prefix, "_clear_teams")]], {
      for (tid in teams_ids) {
        if (tid %in% c("teams", "ts_teams")) {
          updateSelectizeInput(session, tid, selected = character(0))
        } else {
          updateSelectizeInput(session, tid, selected = "")
        }
      }
    }, ignoreInit = TRUE)
  }

  observeEvent(input[[paste0(prefix, "_clear_opponents")]], {
    updateSelectizeInput(session, opponents_id, selected = character(0))
  }, ignoreInit = TRUE)

  observeEvent(input[[paste0(prefix, "_clear_home_away")]], {
    updateSelectInput(session, home_away_id, selected = "")
  }, ignoreInit = TRUE)

  observeEvent(input[[paste0(prefix, "_clear_outcome")]], {
    updateSelectInput(session, outcome_id, selected = "")
  }, ignoreInit = TRUE)

  observeEvent(input[[paste0(prefix, "_clear_gn")]], {
    updateSelectizeInput(session, gn_min_id, selected = "")
    updateSelectizeInput(session, gn_max_id, selected = "")
  }, ignoreInit = TRUE)

  observeEvent(input[[paste0(prefix, "_clear_last_n")]], {
    updateSelectizeInput(session, last_n_id, selected = "")
  }, ignoreInit = TRUE)

  observeEvent(input[[paste0(prefix, "_clear_opp_rank")]], {
    for (rid in opp_rank_ids) updateSelectInput(session, rid, selected = "")
  }, ignoreInit = TRUE)

  observeEvent(input[[paste0(prefix, "_clear_dates")]], {
    gy <- input[[gy_input_id]] %||% DEFAULT_GAME_YEAR
    bounds <- shared$season_date_bounds(gy)
    updateDateRangeInput(session, date_id, start = bounds$start, end = bounds$end)
  }, ignoreInit = TRUE)

  if (!is.null(starters_ids) && length(starters_ids) == 4) {
    observeEvent(input[[paste0(prefix, "_clear_starters")]], {
      for (sid in starters_ids) updateSelectInput(session, sid, selected = "")
    }, ignoreInit = TRUE)
  }

  if (!is.null(clutch_enabled_id)) {
    observeEvent(input[[paste0(prefix, "_clear_clutch")]], {
      updateCheckboxInput(session, clutch_enabled_id, value = FALSE)
    }, ignoreInit = TRUE)
  }
}

update_gn_last_n_choices <- function(session, prefix, gn_vals) {
  gn_vals <- suppressWarnings(as.integer(gn_vals))
  gn_vals <- gn_vals[is.finite(gn_vals)]
  gn_choices <- c("", as.character(gn_vals))
  last_choices <- if (length(gn_vals)) c("", as.character(seq_len(max(gn_vals, na.rm = TRUE)))) else ""
  updateSelectizeInput(session, paste0(prefix, "_gn_min"), choices = gn_choices, selected = "")
  updateSelectizeInput(session, paste0(prefix, "_gn_max"), choices = gn_choices, selected = "")
  updateSelectizeInput(session, paste0(prefix, "_last_n"), choices = last_choices, selected = "")
}

resolve_gn_last_n_params <- function(input, prefix) {
  min_gn <- input[[paste0(prefix, "_gn_min")]] %||% ""
  max_gn <- input[[paste0(prefix, "_gn_max")]] %||% ""
  last_n <- input[[paste0(prefix, "_last_n")]] %||% ""

  min_gn <- if (nzchar(min_gn)) as.integer(min_gn) else NA_integer_
  max_gn <- if (nzchar(max_gn)) as.integer(max_gn) else NA_integer_
  last_n <- if (nzchar(last_n)) as.integer(last_n) else NA_integer_

  if (!is.na(last_n)) {
    min_gn <- NA_integer_
    max_gn <- NA_integer_
  }
  if (!is.na(min_gn) || !is.na(max_gn)) {
    last_n <- NA_integer_
  }
  if (!is.na(min_gn) && !is.na(max_gn) && min_gn > max_gn) {
    tmp <- min_gn
    min_gn <- max_gn
    max_gn <- tmp
  }

  list(min_gn = min_gn, max_gn = max_gn, last_n = last_n)
}

setup_gn_last_n_sync <- function(session, input, prefix) {
  observeEvent(input[[paste0(prefix, "_last_n")]], {
    last_n <- input[[paste0(prefix, "_last_n")]]
    if (!is.null(last_n) && nzchar(last_n)) {
      updateSelectizeInput(session, paste0(prefix, "_gn_min"), selected = "")
      updateSelectizeInput(session, paste0(prefix, "_gn_max"), selected = "")
    }
  }, ignoreInit = TRUE)

  observeEvent(list(input[[paste0(prefix, "_gn_min")]], input[[paste0(prefix, "_gn_max")]]), {
    gn_min <- input[[paste0(prefix, "_gn_min")]] %||% ""
    gn_max <- input[[paste0(prefix, "_gn_max")]] %||% ""
    last_n <- input[[paste0(prefix, "_last_n")]] %||% ""
    if ((nzchar(gn_min) || nzchar(gn_max)) && nzchar(last_n)) {
      updateSelectizeInput(session, paste0(prefix, "_last_n"), selected = "")
    }
  }, ignoreInit = TRUE)
}

reset_gn_last_n_inputs <- function(session, prefix) {
  updateSelectizeInput(session, paste0(prefix, "_gn_min"), selected = "")
  updateSelectizeInput(session, paste0(prefix, "_gn_max"), selected = "")
  updateSelectizeInput(session, paste0(prefix, "_last_n"), selected = "")
}

reset_opp_rank_inputs <- function(session, prefix) {
  updateSelectInput(session, paste0(prefix, "_opp_rank_side"), selected = "")
  updateSelectInput(session, paste0(prefix, "_opp_rank_n"), selected = "")
  updateSelectInput(session, paste0(prefix, "_opp_rank_metric"), selected = "")
}

reset_starters_inputs <- function(session, prefix, own_prefix = "num_starters_off", opp_prefix = "num_starters_def") {
  updateSelectInput(session, paste0(prefix, "_", own_prefix, "_mode"), selected = "")
  updateSelectInput(session, paste0(prefix, "_", own_prefix), selected = "")
  updateSelectInput(session, paste0(prefix, "_", opp_prefix, "_mode"), selected = "")
  updateSelectInput(session, paste0(prefix, "_", opp_prefix), selected = "")
}

reset_clutch_inputs <- function(session, prefix, status_default = "all", margin_default = 5, minutes_default = 5) {
  updateCheckboxInput(session, paste0(prefix, "_clutch_enabled"), value = FALSE)
  updateSliderInput(session, paste0(prefix, "_clutch_margin"), value = margin_default)
  updateSelectInput(session, paste0(prefix, "_clutch_status"), selected = status_default)
  updateSliderInput(session, paste0(prefix, "_clutch_minutes"), value = minutes_default)
  updateCheckboxInput(session, paste0(prefix, "_clutch_ot_margin"), value = FALSE)
}

blank_to_na_character <- function(x) {
  val <- x %||% ""
  if (!nzchar(val)) NA_character_ else as.character(val)
}

blank_to_na_integer <- function(x) {
  val <- x %||% ""
  if (!nzchar(val)) {
    NA_integer_
  } else {
    suppressWarnings(as.integer(val))
  }
}

is_invalid_persisted_token <- function(x) {
  if (is.null(x)) return(logical(0))
  val <- trimws(tolower(as.character(x)))
  is.na(val) | val %in% c("undefined", "null", "nan", "na")
}

sanitize_persisted_choices <- function(x, max_len = 80L, numeric_only = FALSE) {
  if (is.null(x)) return(character(0))
  vals <- if (is.list(x)) unlist(x, recursive = FALSE, use.names = FALSE) else x
  vals <- trimws(as.character(vals))
  vals <- vals[!is.na(vals) & nzchar(vals)]
  vals <- vals[!is_invalid_persisted_token(vals)]
  if (isTRUE(numeric_only) && length(vals)) {
    nums <- suppressWarnings(as.integer(vals))
    vals <- vals[!is.na(nums)]
  }
  vals <- substr(vals, 1L, 200L)
  vals[seq_len(min(length(vals), max_len))]
}

sanitize_single_choice <- function(x, numeric_only = FALSE) {
  vals <- sanitize_persisted_choices(x, max_len = 1L, numeric_only = numeric_only)
  if (length(vals)) vals[[1]] else ""
}

csv_if_any <- function(x, integerize = FALSE) {
  if (is.null(x) || !length(x)) return(NA_character_)
  vals <- as.character(x)
  vals <- vals[nzchar(vals)]
  if (!length(vals)) return(NA_character_)
  if (isTRUE(integerize)) {
    vals <- suppressWarnings(as.integer(vals))
    vals <- vals[!is.na(vals)]
    if (!length(vals)) return(NA_character_)
  }
  paste(vals, collapse = ",")
}

resolve_clutch_params <- function(enabled, margin, status, minutes, ot_margin) {
  clutch_enabled <- isTRUE(enabled)
  list(
    max_margin = if (clutch_enabled) suppressWarnings(as.integer(margin)) else NA_integer_,
    margin_status = if (clutch_enabled) (status %||% "all") else NA_character_,
    max_time_remaining = if (clutch_enabled) suppressWarnings(as.integer(minutes)) * 60L else NA_integer_,
    ot_margin_filter = if (clutch_enabled) isTRUE(ot_margin) else FALSE
  )
}

resolve_starters_bounds <- function(off_mode, off_val, def_mode, def_val) {
  off_mode <- off_mode %||% ""
  def_mode <- def_mode %||% ""
  off_val <- if (nzchar(off_mode)) suppressWarnings(as.integer(off_val)) else NA_integer_
  def_val <- if (nzchar(def_mode)) suppressWarnings(as.integer(def_val)) else NA_integer_
  list(
    num_starters_off_min = if (identical(off_mode, "gte")) off_val else NA_integer_,
    num_starters_off_max = if (identical(off_mode, "lte")) off_val else NA_integer_,
    num_starters_def_min = if (identical(def_mode, "gte")) def_val else NA_integer_,
    num_starters_def_max = if (identical(def_mode, "lte")) def_val else NA_integer_
  )
}

team_select_choices_with_all <- function(teams_df, all_label = "\u2014 All teams \u2014") {
  if (is.null(teams_df) || !nrow(teams_df)) {
    out <- ""
    names(out) <- all_label
    return(out)
  }
  out <- c("", as.character(teams_df$team_id))
  names(out) <- c(all_label, teams_df$team_name)
  out
}

update_single_team_selectize <- function(session, select_id, teams_df, selected = "", all_label = "\u2014 All teams \u2014") {
  updateSelectizeInput(
    session,
    select_id,
    choices = team_select_choices_with_all(teams_df, all_label = all_label),
    selected = selected,
    server = TRUE
  )
}

app_image_src <- function(rel_path, mime = "image/png") {
  candidates <- c(
    file.path("www", rel_path),
    file.path(getwd(), "www", rel_path),
    file.path(getwd(), "app", "www", rel_path)
  )
  existing <- candidates[file.exists(candidates)]
  if (length(existing)) {
    return(base64enc::dataURI(file = existing[[1]], mime = mime))
  }
  rel_path
}

