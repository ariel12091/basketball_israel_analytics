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
COLS_GRAD <- colorRampPalette(c("#d73027", "#fee08b", "#1a9850"))(20)
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
  minSize  = 0,
  maxSize  = as.integer(Sys.getenv("POOL_MAX", "3")),
  idleTimeout = 15000
)
onStop(function() poolClose(pg_pool))

# Pre-warm the connection pool (force SSL handshake at source time)
tryCatch(DBI::dbGetQuery(pg_pool, "SELECT 1"), error = function(e) NULL)

# ---------------- Shared CSS ----------------
shared_css <- HTML("
  /* Global Font */
  body, .container-fluid, .form-control, .btn, table.dataTable {
    font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif !important;
  }

  /* Table Headers */
  table.dataTable thead th {
    text-transform: uppercase;
    font-size: 0.85rem;
    letter-spacing: 0.5px;
    color: #555;
    padding-top: 12px !important;
    padding-bottom: 12px !important;
    border-bottom: 1px solid #ddd !important;
  }

  /* Table Body */
  table.dataTable tbody td {
    vertical-align: middle;
    font-size: 0.95rem;
    padding: 8px 10px !important;
  }

  /* Section Dividers - Thick Border */
  table.dataTable thead th.section-left-border,
  table.dataTable tbody td.section-left-border {
    border-left: 3px solid #e0e0e0 !important;
    padding-left: 25px !important;
  }

  th.group-head {
    background:#f7efe5 !important;
    font-weight:800;
    text-align:center;
    border-bottom: 1px solid #ddd !important;
  }
  th.sub-head { background:#fafafa !important; font-weight:700; }

  .accordion-button { padding: 0.5rem 1rem; font-weight: 600; background-color: #f8f9fa; }

  /* Visual Range Plot Styles */
  .diff-val {
    font-size: 1.15em; font-weight: 700; line-height: 1; margin-bottom: 5px; letter-spacing: -0.5px;
  }
  .diff-val.unranked { color: #999; font-weight: 500; }

  .rank-bar-container {
    position: relative; width: 90px; height: 12px; margin: 0 auto; background: #e9ecef; border-radius: 6px;
  }
  .rank-bar-container.hidden { display: none; }
  .rank-track { display: none; }
  .range-connect {
    position: absolute; top: 50%; height: 4px; background: #adb5bd; z-index: 1; transform: translateY(-50%); border-radius: 2px;
  }
  .dot-off {
    position: absolute; top: 50%; width: 8px; height: 8px; background: #fff; border: 2px solid #6c757d; border-radius: 50%; transform: translate(-50%, -50%); z-index: 2;
  }
  .dot-on {
    position: absolute; top: 50%; width: 10px; height: 10px; background: #212529; border: 1px solid #fff; border-radius: 50%; transform: translate(-50%, -50%); z-index: 3;
  }
  .sub-text {
    font-size: 0.75em; color: #6c757d; margin-top: 4px; white-space: nowrap; font-family: 'Inter', monospace;
  }

  /* View Mode Toggle */
  .view-mode-container .shiny-options-group { display: flex; width: 100%; justify-content: center; gap: 10px; }
  .view-mode-container .radio label { font-weight: 600; background: #fff; padding: 8px 15px; border: 1px solid #dee2e6; border-radius: 6px; cursor: pointer; transition: all 0.2s; }
  .view-mode-container .radio label:hover { background: #f8f9fa; }
  .view-mode-container .radio input[type='radio']:checked + span { color: #0d6efd; }

  /* Legend */
  .legend-box {
    display: flex; align-items: center; justify-content: center; gap: 20px;
    background: #f8f9fa; border: 1px solid #e9ecef; border-radius: 8px;
    padding: 10px 20px; margin-bottom: 15px; font-size: 0.85rem; color: #495057;
  }
  .legend-item { display: flex; align-items: center; gap: 6px; }
  .legend-icon-on { width: 10px; height: 10px; background: #212529; border: 1px solid #fff; border-radius: 50%; }
  .legend-icon-off { width: 8px; height: 8px; background: #fff; border: 2px solid #6c757d; border-radius: 50%; }
  .legend-bar { position: relative; width: 60px; height: 6px; background: #e9ecef; border-radius: 3px; }
  .legend-tick { position: absolute; top: -2px; bottom: -2px; width: 1px; background: #999; }

  /* Shot Split Stacked Bars */
  .shot-acc-label { font-size:0.85em; text-align:center; margin-bottom:1px; letter-spacing:-0.3px; }
  .shot-bar-container { display:flex; width:110px; height:16px; border-radius:3px; overflow:hidden; margin:2px auto 0; background:#eee; }
  .shot-bar-2pt { background:#5b8abd; display:flex; align-items:center; justify-content:center; color:#fff; font-size:0.65em; font-weight:600; }
  .shot-bar-3pt { background:#d4843e; display:flex; align-items:center; justify-content:center; color:#fff; font-size:0.65em; font-weight:600; }

  /* ---- Mobile Responsive ---- */
  @media (max-width: 768px) {
    /* Smaller table fonts */
    table.dataTable tbody td { font-size: 0.8rem; padding: 4px 6px !important; }
    table.dataTable thead th { font-size: 0.75rem; padding-top: 8px !important; padding-bottom: 8px !important; }

    /* Navbar title */
    .navbar-brand { font-size: 0.9rem !important; }

    /* Full-width DT container */
    .dataTables_wrapper { width: 100% !important; overflow-x: auto; }

    /* Touchable slider handles */
    .irs-handle { width: 32px !important; height: 32px !important; top: -8px !important; }
    .irs-bar, .irs-line { height: 8px !important; }

    /* Legend compact */
    .legend-box { flex-wrap: wrap; gap: 10px; padding: 8px 12px; font-size: 0.75rem; }
  }
")

# Shared head tags
shared_head_tags <- function() {
  tags$head(
    tags$meta(name = "viewport", content = "width=device-width, initial-scale=1, maximum-scale=1"),
    tags$link(rel = "preload", href = "https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap", as = "style", onload = "this.rel='stylesheet'"),
    tags$noscript(tags$link(rel = "stylesheet", href = "https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap")),
    tags$style(shared_css)
  )
}
