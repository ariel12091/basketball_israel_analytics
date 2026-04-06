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

# ---------------- Defaults ----------------
DEFAULT_START <- as.Date("2024-10-01")
DEFAULT_END   <- as.Date("2025-07-01")
DEFAULT_GAME_YEAR <- "2026"
DEFAULT_MIN_ALL <- 100L
DEFAULT_MIN_ON  <- 300L
DEFAULT_MIN_NET <- -1e9
LD_DEFAULT_MIN_POSS <- 20L
LD_DEFAULT_NUM      <- "5"

# Players with fewer possessions than this won't get a color/rank bar
RANKING_BASELINE <- 100
RANKING_MIN_PCT  <- 0.25   # at least 25% of rows should be ranked

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
  "OREB%"       = "Offense: offensive rebounds / available misses. Defense: opponent offensive rebounds allowed / available misses",
  "TOV%"        = "Turnover rate: turnovers / possessions",
  "FTR"         = "Free throw rate: FTA / FGA",
  # Shooting
  "Off Shot"    = "Offensive 2PT/3PT frequency and accuracy split",
  "Def Shot"    = "Defensive 2PT/3PT frequency and accuracy split",
  "FG%"         = "Field goal percentage",
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
  "view_ff"           = "TS%, OREB%, TOV%, FTR breakdown",
  "view_traditional"  = "Box-score counting stats"
)

# Tooltip-wrapped label for sidebar inputs
tt <- function(label, key) {
  tip <- FILTER_TOOLTIPS[[key]]
  if (is.null(tip)) return(label)
  tags$span(label, `data-tooltip` = tip)
}

# Shared JS headerCallback for DT tables — injects data-tooltip on th elements
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

# ---------------- App-level cache & guardrails ----------------
REF_CACHE_TTL_SEC <- as.numeric(Sys.getenv("REF_CACHE_TTL_SEC", "300"))
if (!is.finite(REF_CACHE_TTL_SEC) || REF_CACHE_TTL_SEC < 0) REF_CACHE_TTL_SEC <- 60

PG_STATEMENT_TIMEOUT_MS <- suppressWarnings(as.integer(Sys.getenv("PG_STATEMENT_TIMEOUT_MS", "20000")))
if (!is.finite(PG_STATEMENT_TIMEOUT_MS) || PG_STATEMENT_TIMEOUT_MS <= 0) PG_STATEMENT_TIMEOUT_MS <- 20000L
APP_IDLE_TIMEOUT_SEC <- suppressWarnings(as.integer(Sys.getenv("APP_IDLE_TIMEOUT_SEC", "180")))
if (!is.finite(APP_IDLE_TIMEOUT_SEC) || APP_IDLE_TIMEOUT_SEC <= 0) APP_IDLE_TIMEOUT_SEC <- 180L
APP_IDLE_TIMEOUT_MIN <- suppressWarnings(as.numeric(Sys.getenv("APP_IDLE_TIMEOUT_MIN", "")))
if (is.finite(APP_IDLE_TIMEOUT_MIN) && APP_IDLE_TIMEOUT_MIN > 0) {
  APP_IDLE_TIMEOUT_SEC <- as.integer(round(APP_IDLE_TIMEOUT_MIN * 60))
}
APP_IDLE_CHECK_SEC <- suppressWarnings(as.integer(Sys.getenv("APP_IDLE_CHECK_SEC", "15")))
if (!is.finite(APP_IDLE_CHECK_SEC) || APP_IDLE_CHECK_SEC <= 0) APP_IDLE_CHECK_SEC <- 15L

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

# ---------------- Shared CSS ----------------
shared_css <- HTML("
  /* ============ DARK EDITORIAL THEME ============ */

  /* Global Font & Base Colors */
  body, .container-fluid, .form-control, .btn {
    font-family: 'DM Sans', 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
  }
  table.dataTable {
    font-family: 'DM Sans', 'Inter', -apple-system, BlinkMacSystemFont, sans-serif !important;
  }
  body {
    background: #0d1117;
    color: #e6edf3;
  }

  /* ---- Navbar ---- */
  .navbar {
    background: linear-gradient(135deg, #0d1117 0%, #161b22 100%);
    border-bottom: 1px solid #21262d;
    box-shadow: 0 1px 8px rgba(0,0,0,0.4);
    padding-top: 6px;
    padding-bottom: 6px;
  }
  .navbar-brand {
    font-weight: 700;
    color: #e8a435;
    letter-spacing: 0.5px;
    font-size: 1.1rem;
  }
  .navbar .navbar-nav .nav-link {
    color: #8b949e;
    font-weight: 600;
    font-size: 0.88rem;
    padding: 8px 14px;
    border-bottom: 2px solid transparent;
    transition: color 0.2s, border-color 0.3s;
  }
  .navbar .navbar-nav .nav-link:hover {
    color: #e6edf3;
  }
  .navbar .navbar-nav .nav-link.active,
  .navbar .navbar-nav .nav-item.active > .nav-link {
    color: #e8a435;
    border-bottom: 2px solid #e8a435;
  }
  .nav-tabs { border-bottom: none; }
  .nav-tabs .nav-link { border: none; }
  .tab-content { background: transparent; }

  /* ---- Sidebar ---- */
  .well, .sidebar, div[class*='col-sm-3'] > div, .card {
    background: #161b22;
    border: 1px solid #21262d;
    border-radius: 10px;
    color: #c9d1d9;
  }

  /* ---- Form Controls ---- */
  .form-control, .form-select {
    background: #0d1117;
    color: #e6edf3;
    border-color: #30363d;
  }
  .selectize-input, .selectize-dropdown {
    background: #0d1117 !important;
    color: #e6edf3 !important;
    border-color: #30363d !important;
  }
  .selectize-input { border-radius: 6px !important; }
  .selectize-input.focus { border-color: #e8a435 !important; box-shadow: 0 0 0 2px rgba(232,164,53,0.15) !important; }
  .selectize-dropdown { border-radius: 6px !important; border-top: none !important; }
  .selectize-dropdown .option { color: #c9d1d9 !important; }
  .selectize-dropdown .option.active { background: #1c2333 !important; color: #e8a435 !important; }
  .selectize-input .item { background: #1c2333 !important; color: #e6edf3 !important; border: 1px solid #30363d !important; border-radius: 4px !important; }

  .shiny-date-input .input-daterange .input-sm { background: #0d1117 !important; color: #e6edf3 !important; border-color: #30363d !important; }

  /* IRS sliders */
  .irs--shiny .irs-bar { background: #e8a435 !important; border: none !important; }
  .irs--shiny .irs-handle { background: #e8a435 !important; border: 2px solid #0d1117 !important; }
  .irs--shiny .irs-line { background: #21262d !important; }
  .irs--shiny .irs-min, .irs--shiny .irs-max { color: #8b949e !important; background: transparent !important; }
  .irs--shiny .irs-from, .irs--shiny .irs-to, .irs--shiny .irs-single { background: #e8a435 !important; color: #0d1117 !important; }
  .irs--shiny .irs-grid-text { color: #6e7681 !important; }

  /* Buttons */
  .btn-outline-secondary {
    color: #8b949e;
    border-color: #30363d;
  }
  .btn-outline-secondary:hover {
    background: #21262d;
    color: #e6edf3;
    border-color: #e8a435;
  }
  .btn-default, .action-button {
    background: #21262d;
    color: #c9d1d9;
    border-color: #30363d;
  }
  .action-button:hover { background: #30363d; color: #e8a435; }

  /* Checkboxes */
  .checkbox label, .radio label, .shiny-input-container > label, .control-label, label {
    color: #c9d1d9;
  }
  .help-block { color: #6e7681; }

  /* ---- Tables (DataTables) ---- */
  table.dataTable {
    background: #161b22 !important;
    border-collapse: collapse !important;
  }
  table.dataTable thead th {
    text-transform: uppercase;
    font-size: 0.82rem;
    letter-spacing: 0.5px;
    color: #8b949e !important;
    background: #0d1117 !important;
    padding-top: 11px !important;
    padding-bottom: 11px !important;
    border-bottom: 2px solid #21262d !important;
    border-top: none !important;
  }
  table.dataTable tbody td {
    vertical-align: middle;
    font-size: 0.93rem;
    padding: 7px 10px !important;
    color: #e6edf3 !important;
    border-bottom: 1px solid #21262d !important;
  }
  table.dataTable tbody tr { background: #161b22 !important; }
  table.dataTable tbody tr:nth-child(even) { background: #1c2333 !important; }
  table.dataTable tbody tr:hover { background: #242d3d !important; }
  table.dataTable tbody tr.selected { background: #2a1f0a !important; }

  /* Heat-colored cells: ensure text is light */
  table.dataTable tbody td[style*='background-color'] { color: #e6edf3 !important; }

  /* Section Dividers */
  table.dataTable thead th.section-left-border,
  table.dataTable tbody td.section-left-border {
    border-left: 3px solid #30363d !important;
    padding-left: 25px !important;
  }

  /* Grouped headers */
  th.group-head {
    background: #1c2333 !important;
    color: #e8a435 !important;
    font-weight: 800;
    text-align: center;
    border-bottom: 1px solid #30363d !important;
  }
  th.sub-head { background: #141920 !important; font-weight: 700; color: #c9d1d9 !important; }

  /* DT controls (search, pagination, info) */
  .dataTables_wrapper .dataTables_filter input {
    background: #0d1117 !important;
    color: #e6edf3 !important;
    border: 1px solid #30363d !important;
    border-radius: 6px;
  }
  .dataTables_wrapper .dataTables_length select {
    background: #0d1117 !important;
    color: #e6edf3 !important;
    border: 1px solid #30363d !important;
  }
  .dataTables_wrapper .dataTables_info { color: #8b949e !important; }
  .dataTables_wrapper .dataTables_paginate .paginate_button {
    color: #8b949e !important;
    background: transparent !important;
    border: 1px solid #30363d !important;
  }
  .dataTables_wrapper .dataTables_paginate .paginate_button.current {
    color: #e8a435 !important;
    background: #21262d !important;
    border-color: #e8a435 !important;
  }
  .dataTables_wrapper .dataTables_paginate .paginate_button:hover {
    color: #e6edf3 !important;
    background: #21262d !important;
    border-color: #30363d !important;
  }
  .dataTables_filter label { color: #8b949e !important; }
  .dataTables_length label { color: #8b949e !important; }

  /* DT filter row inputs */
  table.dataTable thead .dt-filter-row input,
  table.dataTable thead input[type='text'],
  table.dataTable thead input[type='search'] {
    background: #0d1117 !important;
    color: #e6edf3 !important;
    border: 1px solid #30363d !important;
    border-radius: 4px;
  }
  table.dataTable thead select {
    background: #0d1117 !important;
    color: #e6edf3 !important;
    border: 1px solid #30363d !important;
  }

  /* ---- Accordion ---- */
  .accordion-button {
    padding: 0.5rem 1rem;
    font-weight: 600;
    background-color: #1c2333;
    color: #c9d1d9;
    border: none;
  }
  .accordion-button:not(.collapsed) {
    background-color: #1c2333;
    color: #e8a435;
    box-shadow: none;
  }
  .accordion-button::after {
    filter: invert(0.7);
  }
  .accordion-item {
    background: #161b22;
    border-color: #21262d;
  }
  .accordion-body {
    background: #161b22;
    color: #c9d1d9;
  }

  /* ---- Explainer Card ---- */
  .explainer-card {
    background: #1c2333;
    border: 1px solid #30363d;
    border-radius: 10px;
    padding: 12px 14px;
    margin-bottom: 14px;
  }
  .explainer-top {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 10px;
  }
  .explainer-title {
    font-size: 0.95rem;
    font-weight: 700;
    color: #e8a435;
    margin: 0;
  }
  .explainer-body {
    margin-top: 8px;
    color: #c9d1d9;
    font-size: 0.9rem;
  }
  .explainer-body.collapse:not(.show) {
    display: none;
  }
  .explainer-body p {
    margin-bottom: 6px;
  }
  .explainer-body ul {
    margin: 0 0 0 18px;
    padding: 0;
  }
  .explainer-toggle {
    font-size: 0.8rem;
    font-weight: 600;
    color: #e8a435;
    text-decoration: none;
  }
  .explainer-toggle:hover {
    color: #f0c060;
    text-decoration: underline;
  }
  .nav-help-btn {
    margin-right: 8px;
    color: #8b949e;
    border-color: #30363d;
  }

  /* ---- Example Wrapper (card-like border around the example section) ---- */
  .example-wrapper {
    background: #1c2333;
    border: 1px solid #30363d;
    border-radius: 10px;
    padding: 12px 14px;
    margin-bottom: 14px;
  }

  /* ---- Example Card ---- */
  .example-card {
    background: #1a1f2b;
    border: 1px solid #30363d;
    border-radius: 10px;
    padding: 10px 12px;
    margin-bottom: 14px;
    color: #c9d1d9;
    font-size: 0.9rem;
  }
  .example-card-title {
    font-weight: 700;
    color: #e8a435;
    margin-bottom: 6px;
  }
  .example-grid {
    display: grid;
    grid-template-columns: 1fr;
    gap: 10px;
    align-items: stretch;
    margin-bottom: 14px;
  }
  .example-snippet {
    background: #0d1117;
    border: 1px solid #30363d;
    border-radius: 8px;
    padding: 4px;
  }
  .example-snippet img {
    width: 100%;
    height: auto;
    display: block;
    border-radius: 4px;
  }
  .example-snippet-caption {
    margin-top: 6px;
    font-size: 0.8rem;
    color: #6e7681;
    text-align: center;
  }

  /* ---- Visual Range Plot (Four Factors) ---- */
  .diff-val {
    font-size: 1.15em; font-weight: 700; line-height: 1; margin-bottom: 5px; letter-spacing: -0.5px;
    color: #e6edf3;
  }
  .diff-val.unranked { color: #6e7681; font-weight: 500; }

  .rank-bar-container {
    position: relative; width: 90px; height: 12px; margin: 0 auto; background: #30363d; border-radius: 6px;
  }
  .rank-bar-container.hidden { display: none; }
  .rank-track { display: none; }
  .range-connect {
    position: absolute; top: 50%; height: 4px; background: #6e7681; z-index: 1; transform: translateY(-50%); border-radius: 2px;
  }
  .dot-off {
    position: absolute; top: 50%; width: 8px; height: 8px; background: #0d1117; border: 2px solid #8b949e; border-radius: 50%; transform: translate(-50%, -50%); z-index: 2;
  }
  .dot-on {
    position: absolute; top: 50%; width: 10px; height: 10px; background: #e8a435; border: 1px solid #0d1117; border-radius: 50%; transform: translate(-50%, -50%); z-index: 3;
  }
  .sub-text {
    font-size: 0.75em; color: #6e7681; margin-top: 4px; white-space: nowrap; font-family: 'JetBrains Mono', 'Inter', monospace;
  }

  /* ---- View Mode Toggle ---- */
  .view-mode-container .shiny-options-group { display: flex; width: 100%; justify-content: center; gap: 10px; }
  .view-mode-container .radio label {
    font-weight: 600; background: #0d1117; padding: 8px 15px;
    border: 1px solid #30363d; border-radius: 6px; cursor: pointer;
    transition: all 0.2s; color: #8b949e;
  }
  .view-mode-container .radio label:hover { background: #1c2333; color: #e6edf3; }
  .view-mode-container .radio input[type='radio']:checked + span { color: #e8a435; }

  /* ---- Legend ---- */
  .legend-box {
    display: flex; align-items: center; justify-content: center; gap: 20px;
    background: #1c2333; border: 1px solid #30363d; border-radius: 8px;
    padding: 10px 20px; margin-bottom: 15px; font-size: 0.85rem; color: #c9d1d9;
  }
  .legend-item { display: flex; align-items: center; gap: 6px; }
  .legend-icon-on { width: 10px; height: 10px; background: #e8a435; border: 1px solid #0d1117; border-radius: 50%; }
  .legend-icon-off { width: 8px; height: 8px; background: #0d1117; border: 2px solid #8b949e; border-radius: 50%; }
  .legend-bar { position: relative; width: 60px; height: 6px; background: #30363d; border-radius: 3px; }
  .legend-tick { position: absolute; top: -2px; bottom: -2px; width: 1px; background: #6e7681; }

  /* ---- Shot Split Stacked Bars ---- */
  .shot-acc-label { font-size:0.85em; text-align:center; margin-bottom:1px; letter-spacing:-0.3px; color: #e6edf3; }
  .shot-bar-container { display:flex; width:110px; height:16px; border-radius:3px; overflow:hidden; margin:2px auto 0; background:#21262d; }
  .shot-bar-2pt { background:#5b8abd; display:flex; align-items:center; justify-content:center; color:#fff; font-size:0.65em; font-weight:600; }
  .shot-bar-3pt { background:#d4843e; display:flex; align-items:center; justify-content:center; color:#fff; font-size:0.65em; font-weight:600; }

  /* ---- Modal ---- */
  .modal-content {
    background: #161b22;
    border: 1px solid #30363d;
    color: #e6edf3;
  }
  .modal-header { border-bottom-color: #21262d; }
  .modal-header .modal-title { color: #e8a435; }
  .modal-header .btn-close { filter: invert(0.8); }
  .modal-footer { border-top-color: #21262d; }
  .modal-footer .btn { background: #21262d; color: #c9d1d9; border-color: #30363d; }

  /* ---- Misc ---- */
  hr { border-color: #21262d; }
  a { color: #e8a435; }
  a:hover { color: #f0c060; }
  .container-fluid { background: #0d1117; }
  .tab-pane { background: #0d1117; }
  .help-text, .shiny-text-output { color: #8b949e; }

  /* Download button */
  .btn-default.shiny-download-link {
    background: #1c2333;
    color: #e8a435;
    border: 1px solid #30363d;
    font-weight: 600;
  }
  .btn-default.shiny-download-link:hover {
    background: #242d3d;
    border-color: #e8a435;
  }

  /* ---- Global Season Selector (navbar) ---- */
  .navbar-season-select {
    display: inline-flex;
    align-items: center;
    width: 90px;
    min-width: 90px;
    max-width: 90px;
  }
  .navbar-season-select .form-group { margin: 0; }
  .navbar-season-select .form-select,
  .navbar-season-select select {
    height: 30px; min-height: 30px;
    width: 90px;
    min-width: 90px;
    max-width: 90px;
    padding: 2px 34px 2px 10px;
    text-align: left;
    text-align-last: left;
    font-size: 0.82rem; font-weight: 700;
    background: #161b22; color: #e8a435;
    border: 1px solid #e8a435; border-radius: 6px;
    cursor: pointer;
  }
  .navbar-season-select .form-select:focus,
  .navbar-season-select select:focus {
    box-shadow: 0 0 0 2px rgba(232,164,53,0.2);
  }
  .navbar-season-select label { display: none; }

  /* ---- Tab Icon styling ---- */
  .nav-link .bi { margin-right: 5px; font-size: 0.9em; opacity: 0.7; }
  .nav-link.active .bi, .nav-item.active .nav-link .bi { opacity: 1; }

  /* ---- Mobile Responsive ---- */
  @media (max-width: 768px) {
    table.dataTable tbody td { font-size: 0.8rem; padding: 4px 6px !important; }
    table.dataTable thead th { font-size: 0.75rem; padding-top: 8px !important; padding-bottom: 8px !important; }
    .navbar-brand { font-size: 0.9rem; }
    .dataTables_wrapper { width: 100% !important; overflow-x: auto; }
    .irs-handle { width: 32px !important; height: 32px !important; top: -8px !important; }
    .irs-bar, .irs-line { height: 8px !important; }
    .legend-box { flex-wrap: wrap; gap: 10px; padding: 8px 12px; font-size: 0.75rem; }
    .example-grid { grid-template-columns: 1fr; }
  }

  /* ---- Loading Skeleton & Transitions ---- */
  .dataTables_processing {
    background: linear-gradient(90deg, transparent, rgba(232,164,53,0.08), transparent) !important;
    color: #e8a435 !important;
    border: none !important;
    font-weight: 600;
    font-size: 0.9rem;
    z-index: 10;
  }

  /* Fade-in for tables */
  .dataTables_wrapper { animation: fadeInTable 0.3s ease-in; }
  @keyframes fadeInTable {
    from { opacity: 0; transform: translateY(4px); }
    to { opacity: 1; transform: translateY(0); }
  }

  /* Progress bar under navbar */
  .shiny-busy ~ .navbar::after {
    content: '';
    position: absolute;
    bottom: 0; left: 0;
    height: 2px;
    width: 100%;
    background: linear-gradient(90deg, transparent, #e8a435, transparent);
    animation: progressSlide 1.5s ease-in-out infinite;
  }
  @keyframes progressSlide {
    0% { transform: translateX(-100%); }
    100% { transform: translateX(100%); }
  }

  /* Recalculating overlay */
  .recalculating { opacity: 0.4 !important; transition: opacity 0.2s ease; }

  /* ---- Filter Chips ---- */
  .filter-chips {
    display: flex; flex-wrap: wrap; align-items: center; gap: 6px;
    padding: 8px 0; margin-bottom: 10px;
  }
  .filter-chip {
    display: inline-flex; align-items: center; gap: 4px;
    padding: 4px 10px; border-radius: 16px;
    font-size: 0.78rem; font-weight: 600; line-height: 1.3;
    border: 1px solid #30363d; background: #1c2333; color: #c9d1d9;
    white-space: nowrap;
  }
  .filter-chip.chip-season {
    background: #21262d; color: #e8a435; border-color: #e8a435;
  }
  .filter-chip.chip-game { border-color: #3a6fa0; color: #7db8e8; }
  .filter-chip.chip-clutch { border-color: #e8a435; color: #e8a435; }
  .filter-chip .chip-x {
    cursor: pointer; margin-left: 2px; font-size: 0.9em; opacity: 0.6;
    color: inherit; background: none; border: none; padding: 0; line-height: 1;
  }
  .filter-chip .chip-x:hover { opacity: 1; }
  .chip-clear-all {
    cursor: pointer; font-size: 0.75rem; font-weight: 600; color: #f87171;
    background: none; border: 1px solid #f87171; border-radius: 16px;
    padding: 4px 10px; white-space: nowrap; transition: all 0.2s;
  }
  .chip-clear-all:hover { background: rgba(248,113,113,0.1); }
  .filter-chip-add {
    border: 1px dashed #6e7681 !important;
    background: transparent !important;
    color: #c9d1d9 !important;
    cursor: pointer;
  }
  .filter-chip-add:hover {
    border-color: #e8a435 !important;
    color: #e8a435 !important;
  }
  .ts-stat-popover .form-control,
  .ts-stat-popover .form-select {
    margin-bottom: 6px;
  }

  /* ---- Tooltips ---- */
  [data-tooltip] { position: relative; display: inline-block; cursor: help; }
  [data-tooltip]::after {
    content: attr(data-tooltip);
    position: absolute; bottom: 100%; left: 50%;
    transform: translateX(-50%); margin-bottom: 6px;
    background: #1c2333; color: #e6edf3; border: 1px solid #30363d;
    font-size: 0.72rem; font-weight: 400; line-height: 1.4;
    padding: 5px 9px; border-radius: 6px; white-space: normal;
    max-width: 260px; width: max-content;
    z-index: 9999; pointer-events: none;
    opacity: 0; transition: opacity 0.15s 0.4s;
  }
  [data-tooltip]:hover::after { opacity: 1; }
  /* Sidebar labels sit against the viewport edge, so anchor tooltips from the left. */
  .well [data-tooltip]::after,
  .sidebar [data-tooltip]::after,
  div[class*='col-sm-3'] > div [data-tooltip]::after {
    left: 0;
    transform: none;
  }
  /* DT headers: use native title (set by JS), hide CSS tooltip */
  th[data-tooltip]::after { display: none; }
")

# Shared head tags
shared_head_tags <- function() {
  tags$head(
    tags$meta(name = "viewport", content = "width=device-width, initial-scale=1, maximum-scale=1"),
    tags$link(rel = "stylesheet", href = "https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500&family=Inter:wght@400;500;600;700&display=swap"),
    tags$link(rel = "stylesheet", href = "https://cdn.jsdelivr.net/npm/bootstrap-icons@1.11.3/font/bootstrap-icons.min.css"),
    tags$style(shared_css),
    tags$script(HTML("
      window.applyViewModeTooltips = function() {
        var viewTips = {
          'Summary': 'PPP ratings and shooting splits',
          'Four Factors': 'TS%, OREB%, TOV%, FTR breakdown',
          'Traditional': 'Box-score counting stats'
        };
        $('.view-mode-container .radio label, .view-mode-container .shiny-options-group label').each(function() {
          var txt = $(this).text().trim();
          if (viewTips[txt]) $(this).attr('data-tooltip', viewTips[txt]);
        });
      };
      $(function() {
        window.applyViewModeTooltips();
        $(document).on('shiny:connected shiny:value', function() {
          window.applyViewModeTooltips();
        });
      });
    "))
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
        class = "explainer-toggle",
        onclick = paste0(
          "var body=document.getElementById('", body_id, "');",
          "var bsBody=bootstrap.Collapse.getOrCreateInstance(body);",
          "bsBody.toggle();",
          "var card=this.closest('.explainer-card');",
          "var sib=card.nextElementSibling;",
          "while(sib){",
          "  if(sib.classList.contains('collapse')){",
          "    bootstrap.Collapse.getOrCreateInstance(sib).toggle();break;",
          "  }",
          "  if(sib.querySelector&&sib.querySelector('.collapse')){",
          "    bootstrap.Collapse.getOrCreateInstance(sib.querySelector('.collapse')).toggle();break;",
          "  }",
          "  sib=sib.nextElementSibling;",
          "}",
          "return false;"
        ),
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
          onclick = "Shiny.setInputValue('open_glossary', Math.random(), {priority:'event'}); return false;",
          "Open glossary"
        )
      )
    )
  )
}

# ---------------- Filter Chips Builder ----------------
GAME_TYPE_LABELS <- c("5" = "Regular season", "16" = "PO QF", "26" = "PO SF",
                       "17" = "PO Finals", "33" = "Play-in", "34" = "Winner Cup", "35" = "State Cup")

make_chip <- function(label, clear_id, css_class = "") {
  tags$span(
    class = paste("filter-chip", css_class),
    label,
    tags$button(
      class = "chip-x",
      onclick = sprintf("Shiny.setInputValue('%s', Math.random(), {priority:'event'}); return false;", clear_id),
      HTML("&times;")
    )
  )
}

make_season_chip <- function(gy) {
  label <- if (identical(gy, "2026")) "2025-26" else if (identical(gy, "2025")) "2024-25" else gy
  tags$span(class = "filter-chip chip-season", label)
}

build_filter_chips <- function(prefix, input, season_bounds_fn, reset_btn_id = NULL,
                               team_label_map = NULL, player_label_map = NULL) {
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
    tv <- input[[paste0(prefix, "_team")]]
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
    lbl <- if (length(opp_val) == 1) paste("vs", opp_val[1]) else paste0("vs ", length(opp_val), " opps")
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
    pon <- input$ld_players_on
    if (!is.null(pon) && length(pon)) {
      mapped_on <- map_label(pon, player_label_map)
      lbl <- if (length(mapped_on) == 1) paste("On:", mapped_on[1]) else paste0("On: ", length(mapped_on), " players")
      chips[[length(chips) + 1]] <- make_chip(lbl, "ld_clear_players_on", "chip-game")
    }
    poff <- input$ld_players_off
    if (!is.null(poff) && length(poff)) {
      mapped_off <- map_label(poff, player_label_map)
      lbl <- if (length(mapped_off) == 1) paste("Off:", mapped_off[1]) else paste0("Off: ", length(mapped_off), " players")
      chips[[length(chips) + 1]] <- make_chip(lbl, "ld_clear_players_off", "chip-game")
    }
  }

  # Only show "Clear all" if there are removable chips (more than just season)
  has_active <- length(chips) > 1

  tags$div(
    class = "filter-chips",
    chips,
    if (has_active) {
      clear_js <- if (!is.null(reset_btn_id)) {
        sprintf("document.getElementById('%s').click(); return false;", reset_btn_id)
      } else {
        sprintf("Shiny.setInputValue('%s_reset_all_chips', Math.random(), {priority:'event'}); return false;", prefix)
      }
      tags$button(class = "chip-clear-all", onclick = clear_js, "Clear all")
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

