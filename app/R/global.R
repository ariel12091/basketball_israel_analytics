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
REF_CACHE_TTL_SEC <- as.numeric(Sys.getenv("REF_CACHE_TTL_SEC", "60"))
if (!is.finite(REF_CACHE_TTL_SEC) || REF_CACHE_TTL_SEC < 0) REF_CACHE_TTL_SEC <- 60

PG_STATEMENT_TIMEOUT_MS <- suppressWarnings(as.integer(Sys.getenv("PG_STATEMENT_TIMEOUT_MS", "8000")))
if (!is.finite(PG_STATEMENT_TIMEOUT_MS) || PG_STATEMENT_TIMEOUT_MS <= 0) PG_STATEMENT_TIMEOUT_MS <- 8000L

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
tryCatch(DBI::dbGetQuery(pg_pool, "SELECT 1"), error = function(e) NULL)

# ---------------- Shared CSS ----------------
shared_css <- HTML("
  /* ============ DARK EDITORIAL THEME ============ */

  /* Global Font & Base Colors */
  body, .container-fluid, .form-control, .btn, table.dataTable {
    font-family: 'DM Sans', 'Inter', -apple-system, BlinkMacSystemFont, sans-serif !important;
  }
  body {
    background: #0d1117 !important;
    color: #e6edf3 !important;
  }

  /* ---- Navbar ---- */
  .navbar {
    background: linear-gradient(135deg, #0d1117 0%, #161b22 100%) !important;
    border-bottom: 1px solid #21262d !important;
    box-shadow: 0 1px 8px rgba(0,0,0,0.4);
    padding-top: 6px !important;
    padding-bottom: 6px !important;
  }
  .navbar-brand {
    font-weight: 700 !important;
    color: #e8a435 !important;
    letter-spacing: 0.5px;
    font-size: 1.1rem !important;
  }
  .navbar-nav .nav-link {
    color: #8b949e !important;
    font-weight: 600;
    font-size: 0.88rem;
    padding: 8px 14px !important;
    border-bottom: 2px solid transparent;
    transition: color 0.2s, border-color 0.3s;
  }
  .navbar-nav .nav-link:hover {
    color: #e6edf3 !important;
  }
  .navbar-nav .nav-link.active,
  .navbar-nav .nav-item.active > .nav-link {
    color: #e8a435 !important;
    border-bottom: 2px solid #e8a435;
  }
  .nav-tabs { border-bottom: none !important; }
  .nav-tabs .nav-link { border: none !important; }
  .tab-content { background: transparent !important; }

  /* ---- Sidebar ---- */
  .well, .sidebar, div[class*='col-sm-3'] > div, .card {
    background: #161b22 !important;
    border: 1px solid #21262d !important;
    border-radius: 10px;
    color: #c9d1d9 !important;
  }

  /* ---- Form Controls ---- */
  .form-control, .form-select, .selectize-input, .selectize-dropdown {
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
    color: #8b949e !important;
    border-color: #30363d !important;
  }
  .btn-outline-secondary:hover {
    background: #21262d !important;
    color: #e6edf3 !important;
    border-color: #e8a435 !important;
  }
  .btn-default, .action-button {
    background: #21262d !important;
    color: #c9d1d9 !important;
    border-color: #30363d !important;
  }
  .action-button:hover { background: #30363d !important; color: #e8a435 !important; }

  /* Checkboxes */
  .checkbox label, .radio label, .shiny-input-container > label, .control-label, label {
    color: #c9d1d9 !important;
  }
  .help-block { color: #6e7681 !important; }

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
    background-color: #1c2333 !important;
    color: #c9d1d9 !important;
    border: none;
  }
  .accordion-button:not(.collapsed) {
    background-color: #1c2333 !important;
    color: #e8a435 !important;
    box-shadow: none !important;
  }
  .accordion-button::after {
    filter: invert(0.7);
  }
  .accordion-item {
    background: #161b22 !important;
    border-color: #21262d !important;
  }
  .accordion-body {
    background: #161b22 !important;
    color: #c9d1d9 !important;
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
    display: none !important;
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
    color: #8b949e !important;
    border-color: #30363d !important;
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
  .view-mode-container .radio input[type='radio']:checked + span { color: #e8a435 !important; }

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
    background: #161b22 !important;
    border: 1px solid #30363d !important;
    color: #e6edf3 !important;
  }
  .modal-header { border-bottom-color: #21262d !important; }
  .modal-header .modal-title { color: #e8a435 !important; }
  .modal-header .btn-close { filter: invert(0.8); }
  .modal-footer { border-top-color: #21262d !important; }
  .modal-footer .btn { background: #21262d !important; color: #c9d1d9 !important; border-color: #30363d !important; }

  /* ---- Misc ---- */
  hr { border-color: #21262d !important; }
  a { color: #e8a435; }
  a:hover { color: #f0c060; }
  .container-fluid { background: #0d1117 !important; }
  .tab-pane { background: #0d1117 !important; }
  .help-text, .shiny-text-output { color: #8b949e !important; }

  /* Download button */
  .btn-default.shiny-download-link {
    background: #1c2333 !important;
    color: #e8a435 !important;
    border: 1px solid #30363d !important;
    font-weight: 600;
  }
  .btn-default.shiny-download-link:hover {
    background: #242d3d !important;
    border-color: #e8a435 !important;
  }

  /* ---- Tab Icon styling ---- */
  .nav-link .bi { margin-right: 5px; font-size: 0.9em; opacity: 0.7; }
  .nav-link.active .bi, .nav-item.active .nav-link .bi { opacity: 1; }

  /* ---- Mobile Responsive ---- */
  @media (max-width: 768px) {
    table.dataTable tbody td { font-size: 0.8rem; padding: 4px 6px !important; }
    table.dataTable thead th { font-size: 0.75rem; padding-top: 8px !important; padding-bottom: 8px !important; }
    .navbar-brand { font-size: 0.9rem !important; }
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
")

# Shared head tags
shared_head_tags <- function() {
  tags$head(
    tags$meta(name = "viewport", content = "width=device-width, initial-scale=1, maximum-scale=1"),
    tags$link(rel = "stylesheet", href = "https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500&family=Inter:wght@400;500;600;700&display=swap"),
    tags$link(rel = "stylesheet", href = "https://cdn.jsdelivr.net/npm/bootstrap-icons@1.11.3/font/bootstrap-icons.min.css"),
    tags$style(shared_css)
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
        onclick = "return false;",
        `data-bs-toggle` = "collapse",
        `data-bs-target` = paste0("#", body_id),
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
