# global.R - Libraries, constants, DB pool, helper functions

library(shiny)
library(DBI)
library(dplyr)
library(pool)
library(RPostgres)
library(DT)
library(bslib)
library(htmltools)

# ---------------- Defaults ----------------
# Default season shown on load. To roll to a new season, add its static roster,
# bump this value, and add the matching navbar label in app.R.
DEFAULT_GAME_YEAR <- "2026"   # 25-26

# Season-aware team rosters used before the first database connection. Team IDs
# are provider IDs and can be recycled between seasons, so each season needs an
# explicit mapping.
STATIC_TEAM_ROSTERS <- list(
  `2026` = data.frame(
    team_id = 2:15,
    team_name = c(
      "MACCABI TEL AVIV",
      "HAPOEL TEL AVIV",
      "HAPOEL JERUSALEM",
      "HAPOEL HOLON",
      "BNEI HERZLIYA",
      "MACCABI RAMAT GAN",
      "HAPOEL HAEMEK",
      "NESS ZIONA",
      "GALIL ELION",
      "BEER SHEVA",
      "KIRYAT ATA",
      "MACCABI RAANANA",
      "RISHON LEZION",
      "ELIZUR NETANYA"
    ),
    stringsAsFactors = FALSE
  )
)

static_team_roster <- function(gy) {
  roster <- STATIC_TEAM_ROSTERS[[as.character(as.integer(gy))]]
  if (is.null(roster)) return(NULL)
  roster <- roster[order(roster$team_name), , drop = FALSE]
  rownames(roster) <- NULL
  roster
}

.INITIAL_HOME_ROSTER <- static_team_roster(DEFAULT_GAME_YEAR)
if (is.null(.INITIAL_HOME_ROSTER) || !nrow(.INITIAL_HOME_ROSTER)) {
  stop("Static team roster is missing for DEFAULT_GAME_YEAR")
}
.INITIAL_HOME_TEAM <- .INITIAL_HOME_ROSTER[
  sample.int(nrow(.INITIAL_HOME_ROSTER), 1L),
  c("team_id", "team_name"),
  drop = FALSE
]
rm(.INITIAL_HOME_ROSTER)
DEFAULT_HOME_TEAM_ID <- as.character(.INITIAL_HOME_TEAM$team_id[[1]])
DEFAULT_HOME_TEAM_NAME <- as.character(.INITIAL_HOME_TEAM$team_name[[1]])
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
# Anchors chosen so WCAG relative luminance rises strictly across all twenty
# steps (0.0482 -> 0.1632, a 3.4x span, minimum adjacent step +0.00233). The
# previous anchors spanned only 1.7x and were not monotonic, so under red-green
# deficiency the top half of the scale collapsed into one indistinguishable
# band. Hue still reads red-bad / green-good for everyone else.
RAMP_ANCHORS <- c("#6e2622", "#615641", "#2f7f4d")  # worst -> best
COLS_GRAD <- colorRampPalette(RAMP_ANCHORS)(20)
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
  "BLK" = "Blocks", "DFL" = "Deflections", "TOV" = "Turnovers",
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
APP_IDLE_TIMEOUT_SEC <- suppressWarnings(as.integer(Sys.getenv("APP_IDLE_TIMEOUT_SEC", "600")))
if (!is.finite(APP_IDLE_TIMEOUT_SEC) || APP_IDLE_TIMEOUT_SEC <= 0) APP_IDLE_TIMEOUT_SEC <- 600L
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
APP_IDLE_CLOSE_SESSION <- tolower(trimws(Sys.getenv("APP_IDLE_CLOSE_SESSION", "true"))) %in% c("1", "true", "yes", "on")

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

# Process-wide cache for season-level MV pulls (GL_DATA_CACHE), so concurrent
# sessions share one copy instead of each holding a full-season data frame.
# NULL results (failed loads) are returned but never cached, so a transient DB
# error doesn't stick until cache expiry.
cached_season_df <- function(key_parts, query_fun) {
  key <- rlang::hash(key_parts)
  cached <- GL_DATA_CACHE$get(key)
  if (!cachem::is.key_missing(cached)) return(cached)
  val <- query_fun()
  if (!is.null(val)) GL_DATA_CACHE$set(key, val)
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

# ---------------- Canonical per-season reference lookups ----------------
# One cache key per dataset per season, shared by every tab and the prewarm in
# app.R — replaces per-tab keys (on_gn_/ld_teams_/tr_teams_/...) that aliased
# identical data under different names.

# Teams as stored (tabs 1, 3, 5, 6, 7 dropdowns).
fetch_teams_distinct <- function(gy) {
  gy <- as.integer(gy)
  static <- static_team_roster(gy)
  if (!is.null(static)) return(static)
  cached_ref_query(
    key = sprintf("teams_distinct_%d", gy),
    query_fun = function() db_get_query(
      pg_pool,
      "SELECT DISTINCT team_id, team_name
         FROM basketball_test.full_rosters
        WHERE game_year = $1::int4
        ORDER BY team_name",
      params = list(gy)
    )
  )
}

# One row per team_id with a canonical name (tabs 2, 4).
fetch_teams_min <- function(gy) {
  gy <- as.integer(gy)
  static <- static_team_roster(gy)
  if (!is.null(static)) return(static)
  cached_ref_query(
    key = sprintf("teams_min_%d", gy),
    query_fun = function() db_get_query(
      pg_pool,
      "SELECT team_id, MIN(team_name) AS team_name
         FROM basketball_test.full_rosters
        WHERE game_year = $1::int4
        GROUP BY team_id
        ORDER BY MIN(team_name)",
      params = list(gy)
    )
  )
}

# Distinct game numbers for GN/Last-N dropdowns (all tabs).
fetch_gn_values <- function(gy) {
  gy <- as.integer(gy)
  cached_ref_query(
    key = sprintf("gn_values_%d", gy),
    query_fun = function() db_get_query(
      pg_pool,
      "SELECT DISTINCT gn
         FROM basketball_test.final_schedule_mv
        WHERE game_year = $1::int4
        ORDER BY gn",
      params = list(gy)
    )
  )
}

# (team_id, player_id, name) picker pool (tabs 2, 7).
fetch_players_basic <- function(gy) {
  gy <- as.integer(gy)
  cached_ref_query(
    key = sprintf("players_basic_%d", gy),
    query_fun = function() db_get_query(
      pg_pool,
      "SELECT team_id,
              player_id,
              MIN(btrim(firstname)||' '||btrim(lastname)) AS name
         FROM basketball_test.full_rosters
        WHERE game_year = $1::int4
        GROUP BY team_id, player_id
        ORDER BY MIN(btrim(firstname)||' '||btrim(lastname))",
      params = list(gy)
    )
  )
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
  # The pooler discards the startup `options` string, so a timeout set there
  # never took effect -- `SHOW statement_timeout` returned the 2min server
  # default. Issue it as an ordinary statement once per connection instead;
  # verified 2026-08-19 to hold across checkouts and to cancel for real.
  onCreate = function(con) {
    DBI::dbExecute(con, sprintf("SET statement_timeout = %d", PG_STATEMENT_TIMEOUT_MS))
  },
  minSize  = 0,
  maxSize  = as.integer(Sys.getenv("POOL_MAX", "3")),
  idleTimeout = 15000
)
onStop(function() poolClose(pg_pool))

# Warm one pooled connection off the boot critical path.
#
# Measured 2026-09-01: a first checkout costs ~1,700-2,200ms (TCP + TLS +
# auth + the onCreate SET). With minSize = 0 that always lands on a user
# request -- it showed up inside the 9.2s cold Home prewarm. minSize = 1
# does move it to boot, but measured +2.7s to boot against -1.7s on the
# request, so it is a loss whenever the worker is booted by the request it
# then has to serve. This keeps minSize = 0 (the 2026-08-18 steady-state
# finding stands) and instead connects from the event loop once R goes
# idle, which is the gap while the browser parses the page and opens its
# websocket. Boot time is unchanged and the connection is ready before the
# first session queries. Best-effort: a failure here is retried by the
# normal checkout path. Set POOL_PREWARM=false to disable.
if (!tolower(trimws(Sys.getenv("POOL_PREWARM", "true"))) %in%
      c("0", "false", "no", "off")) {
  later::later(function() {
    tryCatch({
      con <- pool::poolCheckout(pg_pool)
      pool::poolReturn(con)
    }, error = function(e) NULL)
  }, delay = 0)
}

# Shared head tags
shared_head_tags <- function() {
  tags$head(
    tags$meta(name = "viewport", content = "width=device-width, initial-scale=1, maximum-scale=1"),
    # Archivo is the display face: a variable grotesque with a real width axis,
    # so headers and big numbers can be set condensed the way a scoreboard or a
    # jersey number is, without a second family. DM Sans stays the body face and
    # JetBrains Mono stays for dense inline data.
    tags$link(rel = "stylesheet", href = "https://fonts.googleapis.com/css2?family=Archivo:wdth,wght@75..112,500..800&family=DM+Sans:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500&family=Inter:wght@400;500;600;700&display=swap"),
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

# ---------------- League selector ----------------
# The ONE place a league is chosen. Lives in global.R rather than global_euro.R
# because it owns both leagues: "il" is the Israeli league, every other value is
# a EuroLeague-side competition code fed to euro_selected_competition().
#
# The list is deliberately STATIC rather than read from the schedule. Deriving
# it from what happens to be loaded made a league vanish from the navbar
# entirely when its data was missing, which reads as a bug rather than as an
# empty season. app.js mirrors these values; keep the two in step.
LEAGUE_SELECT_CHOICES <- c(
  "EuroLeague"     = "E",
  "EuroCup"        = "U",
  "Israeli League" = "il"
)
LEAGUE_SELECT_DEFAULT <- "il"

navbar_league_select_ui <- function() {
  tags$div(
    class = "navbar-league-select",
    selectInput("league_select", NULL,
                choices = LEAGUE_SELECT_CHOICES,
                selected = LEAGUE_SELECT_DEFAULT)
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

# Clutch-time filter controls, shared by Tab 2 (lineups) and Tab 3 (team
# ratings), which held byte-identical copies under the "ld" and "tr" prefixes.
# Extracted while there were still only two: the EuroLeague counterparts get
# clutch next, and four copies of a control group whose defaults must agree is
# how the margin bound on one tab quietly stops matching the other.
#
# The five inputs map onto the four SQL clutch params through
# resolve_clutch_params() in helpers.R, which owns the minutes-to-seconds
# conversion and the NA-when-disabled semantics. reset_clutch_inputs() resets
# them. The defaults here must stay in step with both.
#
# OT wording is deliberate: overtime bypasses the margin and status filters by
# default (p_ot_margin_filter = FALSE), and the time filter always bypasses OT.
clutch_filter_ui <- function(prefix, margin_default = 5, minutes_default = 5) {
  tagList(
    checkboxInput(paste0(prefix, "_clutch_enabled"), tt("Clutch", "clutch"), value = FALSE),
    conditionalPanel(
      condition = sprintf("input.%s_clutch_enabled == true", prefix),
      sliderInput(paste0(prefix, "_clutch_margin"), "Max point margin", min = 0, max = 10, value = margin_default, step = 1),
      selectInput(
        paste0(prefix, "_clutch_status"),
        "Score status",
        choices = c("All" = "all", "Leading" = "leading", "Trailing" = "trailing", "Tied" = "tied"),
        selected = "all"
      ),
      sliderInput(paste0(prefix, "_clutch_minutes"), "Max minutes remaining", min = 1, max = 5, value = minutes_default, step = 1),
      checkboxInput(paste0(prefix, "_clutch_ot_margin"), "Exclude OT if margin exceeded", value = FALSE),
      helpText("By default, overtime always qualifies. Check above to apply margin filter to OT.")
    )
  )
}

game_context_filters_ui <- function(prefix, include_opp_rank = TRUE,
                                    opp_rank_blank_label = "\u2014",
                                    game_type_id = paste0(prefix, "_game_type"),
                                    game_type_label = "Game type",
                                    game_type_choices = GAME_TYPE_CHOICES_UI,
                                    game_type_selected = "",
                                    game_type_placeholder = "All game types",
                                    gn_min_label = tt("From Game Number (GN)", "gn"),
                                    gn_max_label = tt("To Game Number (GN)", "gn"),
                                    opp_rank_max = 12L,
                                    opp_rank_metric_selected = "") {
  panels <- list(
    bslib::accordion_panel(
      "Game Filters",
      selectizeInput(
        game_type_id, game_type_label,
        choices = game_type_choices,
        selected = game_type_selected, multiple = TRUE,
        options = list(placeholder = game_type_placeholder)
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
            paste0(prefix, "_gn_min"), gn_min_label,
            choices = NULL, selected = "", multiple = FALSE,
            options = list(placeholder = "Any")
          )
        ),
        column(
          6,
          selectizeInput(
            paste0(prefix, "_gn_max"), gn_max_label,
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
        choices = c(blank_choice, setNames(as.character(seq_len(opp_rank_max)),
                                           as.character(seq_len(opp_rank_max)))),
        selected = ""
      ),
      selectInput(
        paste0(prefix, "_opp_rank_metric"), "Metric",
        choices = c(blank_choice, "Offense" = "off", "Defense" = "def", "Net rating" = "net"),
        selected = opp_rank_metric_selected
      )
    )
  }

  do.call(bslib::accordion, c(panels, list(open = TRUE)))
}

game_context_descriptor <- function(prefix, league = c("israel", "euroleague"),
                                    opp_rank_metric_selected = "") {
  league <- match.arg(league)
  if (identical(league, "israel")) {
    return(list(
      prefix = prefix,
      game_type_id = paste0(prefix, "_game_type"),
      game_type_label = "Game type",
      game_type_choices = GAME_TYPE_CHOICES_UI,
      game_type_selected = "",
      game_type_placeholder = "All game types",
      gn_min_label = tt("From Game Number (GN)", "gn"),
      gn_max_label = tt("To Game Number (GN)", "gn"),
      opp_rank_max = 12L,
      opp_rank_metric_selected = opp_rank_metric_selected
    ))
  }
  list(
    prefix = prefix,
    game_type_id = paste0(prefix, "_phase"),
    game_type_label = "Phase",
    game_type_choices = NULL,
    game_type_selected = character(0),
    game_type_placeholder = "All phases",
    gn_min_label = "From Round",
    gn_max_label = "To Round",
    opp_rank_max = 20L,
    opp_rank_metric_selected = opp_rank_metric_selected
  )
}

game_context_filters_from_descriptor <- function(descriptor) {
  game_context_filters_ui(
    descriptor$prefix,
    game_type_id = descriptor$game_type_id,
    game_type_label = descriptor$game_type_label,
    game_type_choices = descriptor$game_type_choices,
    game_type_selected = descriptor$game_type_selected,
    game_type_placeholder = descriptor$game_type_placeholder,
    gn_min_label = descriptor$gn_min_label,
    gn_max_label = descriptor$gn_max_label,
    opp_rank_max = descriptor$opp_rank_max,
    opp_rank_metric_selected = descriptor$opp_rank_metric_selected
  )
}

# The on/off pair has the same input structure. Keep the league-specific IDs,
# labels and feature gates in one place so the two tab files only supply their
# own explanatory copy and data access.
onoff_tab_descriptor <- function(league = c("israel", "euroleague")) {
  league <- match.arg(league)
  if (identical(league, "israel")) {
    return(list(
      league = league, prefix = "on", view_id = "onoff_view_mode",
      filters_id = "onoff-filters", reset_id = "reset_defaults",
      date_id = "date_range", teams_id = "teams", game_type_id = "on_game_type",
      table_id = "onoff_dt", chips_id = "on_filter_chips",
      min_all_id = "min_all_poss", min_on_id = "min_on_poss",
      view_choices = c("Summary", "Four Factors", "Shot Profile"),
      game_type_label = "Game type", game_type_choices = GAME_TYPE_CHOICES_UI,
      game_type_selected = "",
      game_type_placeholder = "All game types",
      gn_min_label = tt("From Game Number (GN)", "gn"),
      gn_max_label = tt("To Game Number (GN)", "gn"),
      # Where a row pivot goes. A league's rows must reach its own tabs: the
      # two leagues' team ids collide numerically, so an Israeli target on a
      # EuroLeague row silently selects a different team.
      pivot_lineups = "lineup_data", pivot_game_logs = "game_logs",
      opp_rank_max = 12L, show_shot_profile = TRUE, show_impact = TRUE,
      show_download = TRUE, initial_min_all = DEFAULT_MIN_ALL,
      initial_min_on = DEFAULT_MIN_ON
    ))
  }

  list(
    league = league, prefix = "euro", view_id = "euro_view_mode",
    filters_id = "euro-filters", reset_id = "euro_reset_defaults",
    date_id = "euro_date_range", teams_id = "euro_teams", game_type_id = "euro_phase",
    table_id = "euro_dt", chips_id = "euro_filter_chips",
    min_all_id = "euro_min_all_poss", min_on_id = "euro_min_on_poss",
    view_choices = c("Summary", "Four Factors"),
    game_type_label = "Phase", game_type_choices = NULL,
    game_type_selected = character(0),
    game_type_placeholder = "All phases",
    gn_min_label = "From Round", gn_max_label = "To Round",
    opp_rank_metric_selected = "",
    pivot_lineups = "euro_lineups", pivot_game_logs = "euro_game_logs",
    opp_rank_max = 20L, show_shot_profile = FALSE, show_impact = FALSE,
    show_download = FALSE, initial_min_all = DEFAULT_MIN_ALL,
    initial_min_on = DEFAULT_MIN_ON
  )
}

starter_context_filters_ui <- function(prefix) {
  tagList(
    fluidRow(
      column(6, selectInput(paste0(prefix, "_num_starters_off_mode"),
                            tt("Own lineup starters", "own_starters"),
                            choices = c("ALL" = "", "At least (>=)" = "gte", "At most (<=)" = "lte"), selected = "")),
      column(6, selectInput(paste0(prefix, "_num_starters_off"), "Own value",
                            choices = c("\u2014" = "", as.character(0:5)), selected = ""))
    ),
    fluidRow(
      column(6, selectInput(paste0(prefix, "_num_starters_def_mode"),
                            tt("Opponent lineup starters", "opp_starters"),
                            choices = c("ALL" = "", "At least (>=)" = "gte", "At most (<=)" = "lte"), selected = "")),
      column(6, selectInput(paste0(prefix, "_num_starters_def"), "Opp value",
                            choices = c("\u2014" = "", as.character(0:5)), selected = ""))
    )
  )
}

onoff_starter_filters_ui <- starter_context_filters_ui

onoff_game_context_filters_ui <- game_context_filters_from_descriptor

shot_splits_legend_ui <- function(view_id) {
  conditionalPanel(
    condition = sprintf("input.%s == 'Summary'", view_id),
    div(
      class = "legend-box",
      span(style = "font-weight:700; margin-right:10px;", "Shot Splits:"),
      div(class = "legend-item",
          div(style = "display:flex; flex-direction:column; align-items:center; gap:2px;",
              span(style = "font-size:0.75em; color:var(--ibpl-text-dim); text-transform:uppercase; letter-spacing:0.5px;", "Frequency"),
              div(style = "display:flex; align-items:center; gap:8px;",
                  div(style = "width:14px; height:14px; background:var(--ibpl-fg2); border-radius:3px;"), span("2PT"),
                  div(style = "width:14px; height:14px; background:var(--ibpl-fg3); border-radius:3px; margin-left:6px;"), span("3PT")))),
      span(style = "margin:0 12px; color:var(--ibpl-border);", "|"),
      div(class = "legend-item",
          div(style = "display:flex; flex-direction:column; align-items:center; gap:2px;",
              span(style = "font-size:0.75em; color:var(--ibpl-text-dim); text-transform:uppercase; letter-spacing:0.5px;", "Accuracy"),
              div(style = "display:flex; align-items:center; gap:6px;",
                  span(style = "color:var(--ibpl-neg); font-weight:600;", "FG%"),
                  span(style = "color:var(--ibpl-text-dim); margin:0 2px;", "\u2192"),
                  span(style = "color:var(--ibpl-pos); font-weight:600;", "FG%"))))
    )
  )
}

onoff_summary_legend_ui <- shot_splits_legend_ui

onoff_rank_legend_ui <- function(view_id, mode = "Four Factors", note = NULL) {
  conditionalPanel(
    condition = sprintf("input.%s == '%s'", view_id, mode),
    div(
      class = "legend-box",
      span(style = "font-weight:700; margin-right:5px;", "Legend:"),
      div(class = "legend-item", div(class = "legend-icon-on"), span("On-Court")),
      div(class = "legend-item", div(class = "legend-icon-off"), span("Off-Court")),
      div(class = "legend-item", span("0%"),
          div(class = "legend-bar", div(class = "legend-tick", style = "left:0;"),
              div(class = "legend-tick", style = "left:50%; height:12px; top:-2px; background:var(--ibpl-text-dim);"),
              div(class = "legend-tick", style = "right:0;")), span("100% Rank")),
      span(style = "margin-left: 15px; font-size: 0.8em; color: var(--ibpl-text-dim);",
           note %||% paste0("(Ranked Players: > ", RANKING_BASELINE, " poss)"))
    )
  )
}

# A chip reports a filter's value and offers to clear it. `focus_id` makes it
# the way in as well: clicking the chip body opens the filter panel and moves
# focus to the control that owns the value. It cannot host a live copy of that
# control -- Shiny input ids are unique per session -- so it reveals it.
make_chip <- function(label, clear_id, css_class = "", focus_id = NULL) {
  focusable <- !is.null(focus_id) && nzchar(as.character(focus_id))
  tags$span(
    class = paste("filter-chip", css_class, if (focusable) "chip-focusable" else ""),
    `data-chip-focus` = if (focusable) as.character(focus_id) else NULL,
    tabindex = if (focusable) "0" else NULL,
    role = if (focusable) "button" else NULL,
    label,
    tags$button(
      class = "chip-x",
      type = "button",
      `data-shiny-event` = clear_id,
      `aria-label` = paste("Clear", label),
      HTML("&times;")
    )
  )
}

make_season_chip <- function(gy, label = NULL) {
  if (is.null(label)) {
    label <- if (identical(gy, "2026")) "2025-26" else if (identical(gy, "2025")) "2024-25" else gy
  }
  tags$span(class = "filter-chip chip-season", label)
}

# The arguments after `extra_children` exist for the league dimension: which
# season value/label the chip bar names, which input holds the dates, whether a
# set date range is worth a chip on its own, where the game-type filter lives
# and how its codes read, and what a schedule position is called (Israeli game
# number vs EuroLeague round). Every one defaults to the Israeli behaviour, so
# the seven Israeli call sites pass none of them.
build_filter_chips <- function(prefix, input, season_bounds_fn, reset_btn_id = NULL,
                               team_label_map = NULL, opponent_label_map = NULL,
                               player_label_map = NULL,
                               teams_value = NULL, players_on_value = NULL, players_off_value = NULL,
                               extra_children = NULL,
                               season_value = NULL, season_label = NULL,
                               date_input_id = NULL, dates_show_when_set = NULL,
                               game_type_input_id = NULL, game_type_labeller = NULL,
                               gn_label = "GN", input_ids = NULL) {
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

  # Which input owns each chip. Defaults to the <prefix>_<thing> convention
  # every Israeli tab follows; a tab whose id differs passes an override, the
  # same way setup_chip_clears() already takes its ids.
  owner <- function(thing, default = paste0(prefix, "_", thing)) {
    if (is.null(input_ids)) return(default)
    val <- input_ids[[thing]]
    if (is.null(val) || !nzchar(as.character(val))) default else as.character(val)
  }

  # Season chip (always visible, not dismissable) - single global input
  gy <- season_value %||% input$game_year %||% DEFAULT_GAME_YEAR
  chips[[length(chips) + 1]] <- make_season_chip(gy, label = season_label)

  # Date range (non-default)
  date_id <- date_input_id %||% (if (prefix == "on") "date_range" else paste0(prefix, "_dates"))
  date_input <- input[[date_id]]
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
      show_when_set <- dates_show_when_set %||% (prefix %in% c("ld", "tr", "gl"))
      is_non_default <- !same_date(start_d, bounds$start) || !same_date(end_d, bounds$end)
      if ((show_when_set && has_any_raw) || (!show_when_set && is_non_default)) {
        lbl <- paste(format(start_d, "%b %d"), "\u2013", format(end_d, "%b %d"))
        chips[[length(chips) + 1]] <- make_chip(lbl, paste0(prefix, "_clear_dates"), "chip-game", owner("dates", date_id))
      }
    }
  }

  # Game type
  gt <- input[[game_type_input_id %||% paste0(prefix, "_game_type")]]
  if (!is.null(gt) && length(gt) && any(nzchar(gt))) {
    labeller <- game_type_labeller %||%
      function(x) vapply(x, function(v) GAME_TYPE_LABELS[v] %||% v, "")
    labels <- labeller(gt[nzchar(gt)])
    chips[[length(chips) + 1]] <- make_chip(paste(labels, collapse = ", "), paste0(prefix, "_clear_game_type"), "chip-game", owner("game_type", game_type_input_id %||% paste0(prefix, "_game_type")))
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
    teams_val <- teams_value %||% get_input("_teams")
  }
  if (!is.null(teams_val) && length(teams_val) && any(nzchar(teams_val))) {
    mapped_teams <- map_label(teams_val, team_label_map)
    lbl <- if (length(mapped_teams) == 1) mapped_teams[1] else paste0(length(mapped_teams), " teams")
    chips[[length(chips) + 1]] <- make_chip(lbl, paste0(prefix, "_clear_teams"), "chip-game", owner("teams", if (prefix == "on") "teams" else paste0(prefix, "_teams")))
  }

  # Opponents
  opp_val <- get_input("_opponents")
  if (prefix == "on") opp_val <- input$on_opponents
  if (!is.null(opp_val) && length(opp_val)) {
    mapped_opponents <- map_label(opp_val, opponent_label_map)
    lbl <- if (length(mapped_opponents) == 1) paste("vs", mapped_opponents[1]) else paste0("vs ", length(mapped_opponents), " opps")
    chips[[length(chips) + 1]] <- make_chip(lbl, paste0(prefix, "_clear_opponents"), "chip-game", owner("opponents"))
  }

  # Home/Away
  ha <- get_input("_home_away")
  if (prefix == "on") ha <- input$on_home_away
  if (!is.null(ha) && nzchar(ha)) {
    chips[[length(chips) + 1]] <- make_chip(if (ha == "home") "Home" else "Away", paste0(prefix, "_clear_home_away"), "chip-game", owner("home_away"))
  }

  # Outcome
  out_val <- get_input("_outcome")
  if (prefix == "on") out_val <- input$on_outcome
  if (!is.null(out_val) && nzchar(out_val)) {
    chips[[length(chips) + 1]] <- make_chip(if (out_val == "win") "Wins" else "Losses", paste0(prefix, "_clear_outcome"), "chip-game", owner("outcome"))
  }

  # GN range
  gn_min <- get_input("_gn_min")
  gn_max <- get_input("_gn_max")
  if (prefix == "on") { gn_min <- input$on_gn_min; gn_max <- input$on_gn_max }
  if ((!is.null(gn_min) && nzchar(gn_min)) || (!is.null(gn_max) && nzchar(gn_max))) {
    parts <- c()
    if (!is.null(gn_min) && nzchar(gn_min)) parts <- c(parts, paste0(gn_label, "\u2265", gn_min))
    if (!is.null(gn_max) && nzchar(gn_max)) parts <- c(parts, paste0(gn_label, "\u2264", gn_max))
    chips[[length(chips) + 1]] <- make_chip(paste(parts, collapse = " "), paste0(prefix, "_clear_gn"), "chip-game", owner("gn", paste0(prefix, "_gn_min")))
  }

  # Last N
  last_n <- get_input("_last_n")
  if (prefix == "on") last_n <- input$on_last_n
  if (!is.null(last_n) && nzchar(last_n)) {
    chips[[length(chips) + 1]] <- make_chip(paste("Last", last_n, "games"), paste0(prefix, "_clear_last_n"), "chip-game", owner("last_n"))
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
    chips[[length(chips) + 1]] <- make_chip(parts, paste0(prefix, "_clear_opp_rank"), "chip-game", owner("opp_rank", paste0(prefix, "_opp_rank_side")))
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
    chips[[length(chips) + 1]] <- make_chip(lbl, paste0(prefix, "_clear_clutch"), "chip-clutch", owner("clutch", paste0(prefix, "_clutch_margin")))
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

  # Players on/off (the lineup tabs; no other prefix has these inputs)
  pon <- players_on_value %||% get_input("_players_on")
  if (!is.null(pon) && length(pon)) {
    mapped_on <- map_label(pon, player_label_map)
    lbl <- if (length(mapped_on) == 1) paste("On:", mapped_on[1]) else paste0("On: ", length(mapped_on), " players")
    chips[[length(chips) + 1]] <- make_chip(lbl, paste0(prefix, "_clear_players_on"), "chip-game", owner("players_on"))
  }
  poff <- players_off_value %||% get_input("_players_off")
  if (!is.null(poff) && length(poff)) {
    mapped_off <- map_label(poff, player_label_map)
    lbl <- if (length(mapped_off) == 1) paste("Off:", mapped_off[1]) else paste0("Off: ", length(mapped_off), " players")
    chips[[length(chips) + 1]] <- make_chip(lbl, paste0(prefix, "_clear_players_off"), "chip-game", owner("players_off"))
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

# Chips bar plus the min-possession controls, laid out as one row: chips grow
# from the left, the controls sit against the right edge. The on/off and lineup
# tabs in both leagues call this, so the four sites cannot drift apart.
# The chips row is where filters are read, so it is also where they are
# reached. The toggle is pure client state -- it collapses a layout column and
# nothing else -- so it is a plain button rather than a Shiny input: no
# round trip, and no filter state to keep in step.
filter_chips_row <- function(chips_output_id, ...) {
  controls <- list(...)
  tags$div(
    class = "chips-row",
    tags$button(
      type = "button",
      class = "chips-filters-toggle js-filters-toggle",
      `aria-expanded` = "true",
      `aria-label` = "Hide the filter panel",
      tags$i(class = "bi bi-sliders", `aria-hidden` = "true"),
      tags$span(class = "chips-filters-toggle-label", "Filters")
    ),
    tags$div(class = "chips-row-chips", uiOutput(chips_output_id)),
    if (length(controls)) tags$div(class = "chips-row-controls", controls)
  )
}

# Show or hide the range track, the on/off values and the points estimate
# inside the Four Factors cells. Display-only and client-side: app.js toggles a
# body class and remembers it in localStorage, so nothing round-trips to the
# server and the state survives a DataTables redraw -- which per-cell state
# would not, because DT re-renders every cell on sort, page and filter.
ff_ranges_toggle <- function(view_mode_input_id) {
  conditionalPanel(
    condition = sprintf("input.%s == 'Four Factors'", view_mode_input_id),
    tags$button(
      type = "button",
      class = "chips-ranges-toggle js-ranges-toggle",
      `aria-pressed` = "true",
      title = paste(
        "Adds three things to every Four Factors cell: where the player ranks",
        "across the league, his on-court and off-court rate, and the estimated",
        "points that gap is worth."
      ),
      tags$i(class = "bi bi-bar-chart-line", `aria-hidden` = "true"),
      tags$span(class = "js-ranges-toggle-label", "Hide on/off detail")
    )
  )
}

# A min-possession slider sized for the chips row rather than the sidebar: no
# tick grid, a fixed narrow track, and a small caption label. The long wording
# lives in the tooltip, keyed the same way as the sidebar version was.
minposs_slider <- function(input_id, label, tooltip_key, max, value,
                           step = 10, width = "150px") {
  tags$div(
    class = "minposs-compact",
    sliderInput(input_id, tt(label, tooltip_key),
                min = 0, max = max, value = value, step = step,
                width = width, ticks = FALSE)
  )
}

setup_chip_clears <- function(prefix, session, input, shared,
                              game_type_id, opponents_id, home_away_id, outcome_id,
                              gn_min_id, gn_max_id, last_n_id, opp_rank_ids,
                              date_id, gy_input_id,
                              teams_ids = NULL, starters_ids = NULL,
                              clutch_enabled_id = NULL, bounds_fn = NULL,
                              teams_multiple = NULL, season_value_fn = NULL) {
  # Which season the date chip resets to. Tabs 8-10 read a EuroLeague season
  # from gy_input_id, so resolving it with the Israeli bounds gave them the
  # wrong window (season 2025 -> Oct 2024-Jul 2025 instead of Sep 2025-Jul 2026).
  bounds_fn <- bounds_fn %||% shared$season_date_bounds
  observeEvent(input[[paste0(prefix, "_clear_game_type")]], {
    resolved_game_type_id <- if (is.function(game_type_id)) game_type_id() else game_type_id
    updateSelectizeInput(session, resolved_game_type_id, selected = character(0))
  }, ignoreInit = TRUE)

  if (!is.null(teams_ids)) {
    # Preserve the established Israeli defaults for existing callers, while
    # allowing league descriptors to state the selector cardinality directly.
    if (is.null(teams_multiple)) teams_multiple <- teams_ids %in% c("teams", "ts_teams")
    observeEvent(input[[paste0(prefix, "_clear_teams")]], {
      for (i in seq_along(teams_ids)) {
        tid <- teams_ids[[i]]
        is_multiple <- if (length(teams_multiple) == 1L) {
          isTRUE(teams_multiple)
        } else {
          isTRUE(teams_multiple[[i]])
        }
        if (is_multiple) {
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
    gy <- if (is.function(season_value_fn)) season_value_fn() else input[[gy_input_id]]
    gy <- gy %||% DEFAULT_GAME_YEAR
    bounds <- bounds_fn(gy)
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

