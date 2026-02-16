# Plumber API for frontend-v2
# Wraps existing PL/pgSQL functions with a thin REST layer.
# Uses the same pool/dbGetQuery pattern as the Shiny app.
#
# Run: Rscript server/run.R
# Default port: 3001

library(plumber)
library(DBI)
library(pool)
library(RPostgres)
library(jsonlite)

# Load credentials from app/.Renviron (read-only user — same as Shiny)
# Walk up from working directory to find the repo root (contains app/.Renviron)
.find_repo_root <- function() {
  d <- getwd()
  for (i in 1:5) {
    if (file.exists(file.path(d, "app", ".Renviron"))) return(d)
    d <- dirname(d)
  }
  stop("Could not find app/.Renviron in any parent directory")
}
.repo_root <- .find_repo_root()
readRenviron(file.path(.repo_root, "app", ".Renviron"))

SCHEMA <- "basketball_test"
DEFAULT_MIN_NET <- -1e9
ALLOWED_ORIGINS <- trimws(strsplit(
  Sys.getenv("FRONTEND_ALLOWED_ORIGINS", "http://localhost:5173,http://127.0.0.1:5173"),
  ",", fixed = TRUE
)[[1]])
ALLOWED_ORIGINS <- ALLOWED_ORIGINS[nzchar(ALLOWED_ORIGINS)]
API_KEY <- Sys.getenv("FRONTEND_API_KEY", "")
RATE_LIMIT_WINDOW_SEC <- max(1L, as.integer(Sys.getenv("FRONTEND_RATE_WINDOW_SEC", "60")))
RATE_LIMIT_MAX_REQUESTS <- max(1L, as.integer(Sys.getenv("FRONTEND_RATE_MAX_REQUESTS", "180")))
REQ_HITS <- new.env(parent = emptyenv())

# ── Pool setup (mirrors app/R/global.R) ──────────────────────
pg_pool <- dbPool(
  drv      = Postgres(),
  bigint   = "numeric",
  host     = Sys.getenv("PG_HOST"),
  port     = as.integer(Sys.getenv("PG_PORT", "6543")),
  dbname   = Sys.getenv("PG_DB"),
  user     = Sys.getenv("PG_USER"),
  password = Sys.getenv("PG_PASS"),
  sslmode  = Sys.getenv("PG_SSLMODE", "require"),
  options  = "-c statement_timeout=8000",
  minSize  = 0,
  maxSize  = as.integer(Sys.getenv("POOL_MAX", "3")),
  idleTimeout = 15000
)
tryCatch(DBI::dbGetQuery(pg_pool, "SELECT 1"), error = function(e) message("Pool warm-up failed: ", e$message))

# ── Helpers ───────────────────────────────────────────────────
parse_int_csv <- function(val) {
  if (is.null(val) || val == "") return(NULL)
  as.integer(strsplit(val, ",")[[1]])
}

# Only game-level filters trigger SQL path. Team + min_poss are applied client-side on MV data.
season_date_bounds <- function(game_year) {
  gy <- as.integer(game_year)
  list(start = paste0(gy - 1, "-10-01"), end = paste0(gy, "-07-01"))
}

needs_filtered <- function(opp_ids, game_type, home_away, outcome,
                           gn_min, gn_max, last_n, start_date, end_date,
                           game_year = 2026, opp_rank_side = "") {
  bounds <- season_date_bounds(game_year)
  nzchar(opp_ids) || nzchar(game_type) ||
  nzchar(home_away) || nzchar(outcome) ||
  nzchar(gn_min) || nzchar(gn_max) || nzchar(last_n) ||
  nzchar(opp_rank_side) ||
  start_date != bounds$start || end_date != bounds$end
}

# Mirrors run_onoff_compute_14 from server_tab1.R — 17 params with explicit casts
run_onoff_compute <- function(pool, start_d, end_d, team_csv, min_all, min_on,
                               game_year, game_type_csv, opp_ids_csv,
                               home_away, outcome,
                               opp_rank_side = NA_character_,
                               opp_rank_n = NA_integer_,
                               opp_rank_metric = NA_character_,
                               min_gn = NA_integer_, max_gn = NA_integer_, last_n = NA_integer_) {
  DBI::dbGetQuery(pool, paste0(
    "SELECT * FROM ", SCHEMA, ".onoff_compute(",
    "$1::date,$2::date,$3::text,$4::int4,$5::int4,$6::numeric,$7::text,",
    "$8::text,$9::text,$10::text,$11::text,$12::text,$13::int4,$14::text,",
    "$15::int4,$16::int4,$17::int4", ")"
  ), params = list(
    as.Date(start_d), as.Date(end_d), team_csv,
    as.integer(min_all), as.integer(min_on), DEFAULT_MIN_NET, as.character(game_year),
    game_type_csv, opp_ids_csv, home_away, outcome,
    opp_rank_side, opp_rank_n, opp_rank_metric,
    min_gn, max_gn, last_n
  ))
}

# Mirrors run_four_factors_compute from server_tab1.R — 14 params with explicit casts
run_ff_compute <- function(pool, game_year, start_d, end_d, team_csv,
                            game_type_csv, opp_ids_csv, home_away, outcome,
                            opp_rank_side = NA_character_,
                            opp_rank_n = NA_integer_,
                            opp_rank_metric = NA_character_,
                            min_gn = NA_integer_, max_gn = NA_integer_, last_n = NA_integer_) {
  DBI::dbGetQuery(pool, paste0(
    "SELECT * FROM ", SCHEMA, ".four_factors_compute(",
    "$1::int4,$2::date,$3::date,$4::text,$5::text,$6::text,",
    "$7::text,$8::text,$9::text,$10::int4,$11::text,",
    "$12::int4,$13::int4,$14::int4", ")"
  ), params = list(
    as.integer(game_year), as.Date(start_d), as.Date(end_d), team_csv,
    game_type_csv, opp_ids_csv, home_away, outcome,
    opp_rank_side, opp_rank_n, opp_rank_metric,
    min_gn, max_gn, last_n
  ))
}

# Convert SQL quoted-column names to camelCase for the frontend
rename_onoff <- function(df) {
  if (nrow(df) == 0) return(df)
  nms <- c(
    "Team" = "team", "First Name" = "firstName", "Last Name" = "lastName",
    "Net RTG Diff" = "netDiff", "Off ON Diff" = "offOnDiff", "Def ON Diff" = "defOnDiff",
    "Off ON PPP" = "offOnPpp", "Def ON PPP" = "defOnPpp", "On Net RTG" = "onNetRtg",
    "Off OFF PPP" = "offOffPpp", "Def OFF PPP" = "defOffPpp", "Off Net RTG" = "offNetRtg",
    "ON Poss" = "onPoss", "OFF Poss" = "offPoss",
    "pr_net" = "prNet", "pr_off_on" = "prOffOn", "pr_off_off" = "prOffOff",
    "pr_def_on_inv" = "prDefOnInv", "pr_def_off_inv" = "prDefOffInv",
    "pr_off_on_d" = "prOffOnD", "pr_def_on_d" = "prDefOnD",
    "pr_on_net" = "prOnNet", "pr_off_net" = "prOffNet",
    "off_on_fg2_made" = "offOnFg2Made", "off_on_fg2_att" = "offOnFg2Att",
    "off_on_fg3_made" = "offOnFg3Made", "off_on_fg3_att" = "offOnFg3Att",
    "off_off_fg2_made" = "offOffFg2Made", "off_off_fg2_att" = "offOffFg2Att",
    "off_off_fg3_made" = "offOffFg3Made", "off_off_fg3_att" = "offOffFg3Att",
    "def_on_fg2_made" = "defOnFg2Made", "def_on_fg2_att" = "defOnFg2Att",
    "def_on_fg3_made" = "defOnFg3Made", "def_on_fg3_att" = "defOnFg3Att",
    "def_off_fg2_made" = "defOffFg2Made", "def_off_fg2_att" = "defOffFg2Att",
    "def_off_fg3_made" = "defOffFg3Made", "def_off_fg3_att" = "defOffFg3Att",
    "player_id" = "playerId", "team_id" = "teamId"
  )
  for (old in names(nms)) {
    if (old %in% names(df)) names(df)[names(df) == old] <- nms[[old]]
  }
  # Drop columns not in the mapping (Year, extra pr_* fields)
  df[, names(df) %in% unname(nms), drop = FALSE]
}

rename_ff <- function(df) {
  if (nrow(df) == 0) return(df)
  nms <- c(
    "player_id" = "playerId", "team_id" = "teamId",
    "firstname" = "firstName", "lastname" = "lastName", "team_name" = "teamName",
    "off_on_ts" = "offOnTs", "off_off_ts" = "offOffTs",
    "def_on_ts" = "defOnTs", "def_off_ts" = "defOffTs",
    "off_on_oreb" = "offOnOreb", "off_off_oreb" = "offOffOreb",
    "def_on_oreb" = "defOnOreb", "def_off_oreb" = "defOffOreb",
    "off_on_tov" = "offOnTov", "off_off_tov" = "offOffTov",
    "def_on_tov" = "defOnTov", "def_off_tov" = "defOffTov",
    "off_on_ftr" = "offOnFtr", "off_off_ftr" = "offOffFtr",
    "def_on_ftr" = "defOnFtr", "def_off_ftr" = "defOffFtr",
    "off_on_poss" = "offOnPoss", "off_off_poss" = "offOffPoss",
    "def_on_poss" = "defOnPoss", "def_off_poss" = "defOffPoss",
    "Off TS% Diff" = "offTsDiff", "Off OREB% Diff" = "offOrebDiff",
    "Off TOV% Diff" = "offTovDiff", "Off FTR Diff" = "offFtrDiff",
    "Def TS% Diff" = "defTsDiff", "Def OREB% Diff" = "defOrebDiff",
    "Def TOV% Diff" = "defTovDiff", "Def FTR Diff" = "defFtrDiff"
  )
  for (old in names(nms)) {
    if (old %in% names(df)) names(df)[names(df) == old] <- nms[[old]]
  }
  df
}

na_int <- function(x) if (is.null(x) || x == "") NA_integer_ else as.integer(x)
na_chr <- function(x) if (is.null(x) || x == "") NA_character_ else as.character(x)
normalize_origin <- function(origin) {
  if (is.null(origin) || !nzchar(origin)) return("")
  gsub("/+$", "", tolower(trimws(origin)))
}
is_allowed_origin <- function(origin) {
  if (!length(ALLOWED_ORIGINS)) return(FALSE)
  normalize_origin(origin) %in% normalize_origin(ALLOWED_ORIGINS)
}
client_ip <- function(req) {
  xff <- req$HTTP_X_FORWARDED_FOR
  if (!is.null(xff) && nzchar(xff)) {
    return(trimws(strsplit(xff, ",", fixed = TRUE)[[1]][1]))
  }
  if (!is.null(req$REMOTE_ADDR) && nzchar(req$REMOTE_ADDR)) return(req$REMOTE_ADDR)
  "unknown"
}
check_rate_limit <- function(ip) {
  now <- as.numeric(Sys.time())
  prev <- REQ_HITS[[ip]]
  if (is.null(prev) || !is.list(prev) || is.null(prev$start) || is.null(prev$count)) {
    REQ_HITS[[ip]] <- list(start = now, count = 1L)
    return(TRUE)
  }
  if ((now - prev$start) > RATE_LIMIT_WINDOW_SEC) {
    REQ_HITS[[ip]] <- list(start = now, count = 1L)
    return(TRUE)
  }
  next_count <- as.integer(prev$count) + 1L
  REQ_HITS[[ip]] <- list(start = prev$start, count = next_count)
  next_count <= RATE_LIMIT_MAX_REQUESTS
}

#* @apiTitle IBPL Court Impact API
#* @apiDescription Thin REST layer over existing PL/pgSQL functions

#* Enable CORS
#* @filter cors
function(req, res) {
  origin <- req$HTTP_ORIGIN
  if (!is.null(origin) && nzchar(origin)) {
    if (!is_allowed_origin(origin)) {
      res$status <- 403
      return(list(error = "Origin not allowed"))
    }
    res$setHeader("Access-Control-Allow-Origin", origin)
    res$setHeader("Vary", "Origin")
  }
  res$setHeader("Access-Control-Allow-Methods", "GET, OPTIONS")
  res$setHeader("Access-Control-Allow-Headers", "Content-Type, X-API-Key, Authorization")
  if (req$REQUEST_METHOD == "OPTIONS") {
    res$status <- 200
    return(list())
  }
  plumber::forward()
}

#* Optional API key auth (keeps anonymous UX when key is unset)
#* @filter auth
function(req, res) {
  if (!nzchar(API_KEY)) return(plumber::forward())
  key <- req$HTTP_X_API_KEY
  if (is.null(key) || !nzchar(key)) {
    authz <- req$HTTP_AUTHORIZATION
    if (!is.null(authz) && nzchar(authz)) {
      key <- sub("^Bearer\\s+", "", authz, perl = TRUE)
    }
  }
  if (!identical(key, API_KEY)) {
    res$status <- 401
    return(list(error = "Unauthorized"))
  }
  plumber::forward()
}

#* Basic in-memory IP rate limit guard
#* @filter rate_limit
function(req, res) {
  if (!check_rate_limit(client_ip(req))) {
    res$status <- 429
    return(list(error = "Too many requests"))
  }
  plumber::forward()
}

#* Route alias/normalization for backward-compatible clients
#* @filter route_aliases
function(req, res) {
  path <- req$PATH_INFO
  if (!is.null(path) && nzchar(path)) {
    normalized <- path
    if (startsWith(normalized, "/api/") && nchar(normalized) > 5L) {
      normalized <- sub("/+$", "", normalized)
    }

    target <- NULL
    if (!identical(normalized, path)) target <- normalized
    if (identical(normalized, "/api/lineups")) target <- "/api/lineups/summary"
    if (identical(normalized, "/api/lineups/ff")) target <- "/api/lineups/four-factors"
    if (identical(normalized, "/api/lineups/four_factors")) target <- "/api/lineups/four-factors"
    if (identical(normalized, "/api/lineups/game_log")) target <- "/api/lineups/game-log"
    if (identical(normalized, "/api/lineups/gamelog")) target <- "/api/lineups/game-log"

    if (!is.null(target) && !identical(target, path)) {
      qs <- req$QUERY_STRING
      loc <- if (!is.null(qs) && nzchar(qs)) paste0(target, "?", qs) else target
      res$status <- 307
      res$setHeader("Location", loc)
      return(list(error = "Redirecting to canonical route"))
    }
  }
  plumber::forward()
}

# ── GET /api/meta/teams ──────────────────────────────────────
#* @get /api/meta/teams
#* @param game_year:int Season year (default 2026)
#* @serializer json
function(game_year = "2026") {
  gy <- as.integer(game_year)
  df <- DBI::dbGetQuery(pg_pool, sprintf(
    "SELECT DISTINCT team_id, team_name FROM %s.full_rosters WHERE game_year = $1 ORDER BY team_name",
    SCHEMA
  ), params = list(gy))
  names(df) <- c("teamId", "teamName")
  df
}

# ── GET /api/meta/game-numbers ─────────────────────────────
#* @get /api/meta/game-numbers
#* @param game_year:int Season year (default 2026)
#* @serializer json
function(game_year = "2026") {
  gy <- as.integer(game_year)
  df <- DBI::dbGetQuery(pg_pool, sprintf(
    "SELECT DISTINCT gn FROM %s.final_schedule_mv WHERE game_year = $1 ORDER BY gn",
    SCHEMA
  ), params = list(gy))
  as.integer(df$gn)
}

# ── GET /api/meta/last-updated ────────────────────────────────
#* @get /api/meta/last-updated
#* @serializer json
function() {
  ts <- tryCatch({
    df <- DBI::dbGetQuery(pg_pool, sprintf(
      "SELECT value FROM %s.app_meta WHERE key = 'etl_full_last_success' LIMIT 1", SCHEMA
    ))
    if (nrow(df) && nzchar(df$value[1])) df$value[1] else NULL
  }, error = function(e) NULL)

  # Fallback to file
  if (is.null(ts)) {
    candidates <- c(
      file.path(.repo_root, "etl", "logs", "last_success.txt"),
      file.path(.repo_root, "app", "etl", "logs", "last_success.txt")
    )
    for (p in candidates) {
      if (file.exists(p)) {
        lines <- tryCatch(readLines(p, warn = FALSE), error = function(e) character(0))
        if (length(lines) && nzchar(trimws(lines[[1]]))) { ts <- trimws(lines[[1]]); break }
      }
    }
  }

  list(lastUpdated = if (!is.null(ts)) ts else NA)
}

# ── GET /api/onoff/summary ───────────────────────────────────
#* @get /api/onoff/summary
#* @serializer json
function(req, res,
         game_year = "2026", start_date = "2025-10-01", end_date = "2026-06-30",
         team_ids = "", min_on = "0", min_all = "0",
         game_type = "", opp_ids = "",
         home_away = "", outcome = "",
         gn_min = "", gn_max = "", last_n = "",
         opp_rank_side = "", opp_rank_n = "", opp_rank_metric = "") {

  gy <- as.integer(game_year)

  if (!needs_filtered(opp_ids, game_type, home_away, outcome,
                      gn_min, gn_max, last_n, start_date, end_date,
                      game_year = gy, opp_rank_side = opp_rank_side)) {
    # Fast path: MV
    df <- DBI::dbGetQuery(pg_pool, sprintf(
      'SELECT * FROM %s.onoff_default_mv WHERE "Year" = $1', SCHEMA
    ), params = list(gy))
  } else {
    # Filtered path: call onoff_compute() with exact Shiny-app signature
    team_csv <- if (nzchar(team_ids)) team_ids else NA_character_
    df <- run_onoff_compute(
      pg_pool,
      start_d = start_date, end_d = end_date,
      team_csv = team_csv,
      min_all = as.integer(min_all), min_on = as.integer(min_on),
      game_year = gy,
      game_type_csv = if (nzchar(game_type)) game_type else NA_character_,
      opp_ids_csv = if (nzchar(opp_ids)) opp_ids else NA_character_,
      home_away = if (nzchar(home_away)) home_away else NA_character_,
      outcome = if (nzchar(outcome)) outcome else NA_character_,
      opp_rank_side = na_chr(opp_rank_side),
      opp_rank_n = na_int(opp_rank_n),
      opp_rank_metric = na_chr(opp_rank_metric),
      min_gn = na_int(gn_min), max_gn = na_int(gn_max), last_n = na_int(last_n)
    )
  }

  # Replace NAs with 0 in shot columns
  shot_cols <- grep("fg[23]", names(df), value = TRUE)
  for (col in shot_cols) df[[col]][is.na(df[[col]])] <- 0

  rename_onoff(df)
}

# ── GET /api/onoff/four-factors ──────────────────────────────
#* @get /api/onoff/four-factors
#* @serializer json
function(req, res,
         game_year = "2026", start_date = "2025-10-01", end_date = "2026-06-30",
         team_ids = "", game_type = "", opp_ids = "",
         home_away = "", outcome = "",
         gn_min = "", gn_max = "", last_n = "",
         opp_rank_side = "", opp_rank_n = "", opp_rank_metric = "") {

  gy <- as.integer(game_year)
  team_csv <- if (nzchar(team_ids)) team_ids else NA_character_
  gt_csv   <- if (nzchar(game_type)) game_type else NA_character_
  opp_csv  <- if (nzchar(opp_ids)) opp_ids else NA_character_
  ha       <- if (nzchar(home_away)) home_away else NA_character_
  oc       <- if (nzchar(outcome)) outcome else NA_character_

  if (!needs_filtered(opp_ids, game_type, home_away, outcome,
                      gn_min, gn_max, last_n, start_date, end_date,
                      game_year = gy, opp_rank_side = opp_rank_side)) {
    # Fast path: join MV + onoff MV for net diffs
    df <- DBI::dbGetQuery(pg_pool, sprintf('
      SELECT ff.*, o."Net RTG Diff", o."Off ON Diff", o."Def ON Diff"
      FROM %s.player_advanced_stats_mv ff
      LEFT JOIN %s.onoff_default_mv o
        ON ff.player_id = o.player_id AND ff.game_year = o."Year"
      WHERE ff.game_year = $1
    ', SCHEMA, SCHEMA), params = list(gy))
  } else {
    # Filtered: call both functions with exact Shiny-app signatures, join in R
    ff <- run_ff_compute(
      pg_pool, game_year = gy, start_d = start_date, end_d = end_date,
      team_csv = team_csv, game_type_csv = gt_csv, opp_ids_csv = opp_csv,
      home_away = ha, outcome = oc,
      opp_rank_side = na_chr(opp_rank_side),
      opp_rank_n = na_int(opp_rank_n),
      opp_rank_metric = na_chr(opp_rank_metric),
      min_gn = na_int(gn_min), max_gn = na_int(gn_max), last_n = na_int(last_n)
    )

    onoff <- run_onoff_compute(
      pg_pool,
      start_d = start_date, end_d = end_date,
      team_csv = team_csv,
      min_all = 0L, min_on = 0L, game_year = gy,
      game_type_csv = gt_csv, opp_ids_csv = opp_csv,
      home_away = ha, outcome = oc,
      opp_rank_side = na_chr(opp_rank_side),
      opp_rank_n = na_int(opp_rank_n),
      opp_rank_metric = na_chr(opp_rank_metric),
      min_gn = na_int(gn_min), max_gn = na_int(gn_max), last_n = na_int(last_n)
    )

    # Join diffs from onoff
    onoff_lookup <- onoff[, c("player_id", "Net RTG Diff", "Off ON Diff", "Def ON Diff"), drop = FALSE]
    df <- merge(ff, onoff_lookup, by = "player_id", all.x = TRUE)
    df[["Net RTG Diff"]][is.na(df[["Net RTG Diff"]])] <- 0
    df[["Off ON Diff"]][is.na(df[["Off ON Diff"]])] <- 0
    df[["Def ON Diff"]][is.na(df[["Def ON Diff"]])] <- 0
  }

  # Add net diffs
  out <- rename_ff(df)
  out$netRtgDiff <- ifelse(is.na(df[["Net RTG Diff"]]), 0, df[["Net RTG Diff"]])
  out$offDiff    <- ifelse(is.na(df[["Off ON Diff"]]),   0, df[["Off ON Diff"]])
  out$defDiff    <- ifelse(is.na(df[["Def ON Diff"]]),   0, df[["Def ON Diff"]])

  # Replace NAs in numeric columns with 0
  for (col in names(out)) {
    if (is.numeric(out[[col]])) out[[col]][is.na(out[[col]])] <- 0
  }

  out
}

# ── Lineup helpers ─────────────────────────────────────────────
run_fetch_lineups <- function(pool, num, team_csv, player_csv, player_off_csv,
                               exact, start_date, end_date, min_poss, game_year,
                               game_type_csv, opp_ids_csv, home_away, outcome,
                               opp_rank_side, opp_rank_n, opp_rank_metric,
                               max_margin, margin_status, max_time_remaining,
                               ot_margin_filter, min_gn, max_gn, last_n) {
  DBI::dbGetQuery(pool, paste0(
    "SELECT * FROM ", SCHEMA, ".fetch_lineups_csv_v2(",
    "$1::int4,$2::text,$3::text,$4::text,$5::bool,$6::date,$7::date,$8::int4,$9::int4,",
    "$10::text,$11::text,$12::text,$13::text,$14::text,$15::int4,$16::text,",
    "$17::int4,$18::text,$19::int4,$20::bool,$21::int4,$22::int4,$23::int4", ")"
  ), params = list(
    as.integer(num), team_csv, player_csv, player_off_csv,
    as.logical(exact), as.Date(start_date), as.Date(end_date),
    as.integer(min_poss), as.integer(game_year),
    game_type_csv, opp_ids_csv, home_away, outcome,
    opp_rank_side, opp_rank_n, opp_rank_metric,
    max_margin, margin_status, max_time_remaining, ot_margin_filter,
    min_gn, max_gn, last_n
  ))
}

run_fetch_lineups_ff <- function(pool, num, team_csv, player_csv, player_off_csv,
                                  exact, start_date, end_date, min_poss, game_year,
                                  game_type_csv, opp_ids_csv, home_away, outcome,
                                  opp_rank_side, opp_rank_n, opp_rank_metric,
                                  max_margin, margin_status, max_time_remaining,
                                  ot_margin_filter, min_gn, max_gn, last_n) {
  DBI::dbGetQuery(pool, paste0(
    "SELECT * FROM ", SCHEMA, ".fetch_lineups_four_factors_csv(",
    "$1::int4,$2::text,$3::text,$4::text,$5::bool,$6::date,$7::date,$8::int4,$9::int4,",
    "$10::text,$11::text,$12::text,$13::text,$14::text,$15::int4,$16::text,",
    "$17::int4,$18::text,$19::int4,$20::bool,$21::int4,$22::int4,$23::int4", ")"
  ), params = list(
    as.integer(num), team_csv, player_csv, player_off_csv,
    as.logical(exact), as.Date(start_date), as.Date(end_date),
    as.integer(min_poss), as.integer(game_year),
    game_type_csv, opp_ids_csv, home_away, outcome,
    opp_rank_side, opp_rank_n, opp_rank_metric,
    max_margin, margin_status, max_time_remaining, ot_margin_filter,
    min_gn, max_gn, last_n
  ))
}

rename_lineup_summary <- function(df) {
  if (nrow(df) == 0) return(df)
  nms <- c(
    "team_id" = "teamId", "sub_lineup_hash" = "subLineupHash",
    "num_lineup" = "numLineup", "player_ids" = "playerIds",
    "player_names_str" = "playerNamesStr",
    "off_poss" = "offPoss", "off_pts" = "offPts", "off_ppp" = "offPpp",
    "def_poss" = "defPoss", "def_pts" = "defPts", "def_ppp" = "defPpp",
    "net_rtg" = "netRtg", "minutes" = "minutes",
    "total_poss" = "totalPoss", "plus_minus" = "plusMinus",
    "off_fg2_made" = "offFg2Made", "off_fg2_att" = "offFg2Att",
    "off_fg3_made" = "offFg3Made", "off_fg3_att" = "offFg3Att",
    "def_fg2_made" = "defFg2Made", "def_fg2_att" = "defFg2Att",
    "def_fg3_made" = "defFg3Made", "def_fg3_att" = "defFg3Att"
  )
  for (old in names(nms)) {
    if (old %in% names(df)) names(df)[names(df) == old] <- nms[[old]]
  }
  # Convert player_ids from PG array string to JSON array
  if ("playerIds" %in% names(df)) {
    df$playerIds <- lapply(df$playerIds, function(s) {
      as.integer(strsplit(gsub("[{}]", "", as.character(s)), ",")[[1]])
    })
  }
  df
}

rename_lineup_ff <- function(df) {
  if (nrow(df) == 0) return(df)
  nms <- c(
    "team_id" = "teamId", "sub_lineup_hash" = "subLineupHash",
    "num_lineup" = "numLineup", "player_ids" = "playerIds",
    "player_names_str" = "playerNamesStr",
    "off_ts" = "offTs", "off_oreb" = "offOreb", "off_tov" = "offTov", "off_ftr" = "offFtr",
    "off_poss" = "offPoss", "off_pts" = "offPts", "off_ppp" = "offPpp",
    "def_ts" = "defTs", "def_oreb" = "defOreb", "def_tov" = "defTov", "def_ftr" = "defFtr",
    "def_poss" = "defPoss", "def_pts" = "defPts", "def_ppp" = "defPpp",
    "net_rtg" = "netRtg", "minutes" = "minutes", "total_poss" = "totalPoss",
    "off_ts_poss" = "offTsPoss", "off_oreb_cnt" = "offOrebCnt",
    "off_oreb_opps" = "offOrebOpps", "off_tov_cnt" = "offTovCnt",
    "off_fta" = "offFta", "off_fga_cnt" = "offFgaCnt",
    "def_ts_poss" = "defTsPoss", "def_oreb_cnt" = "defOrebCnt",
    "def_oreb_opps" = "defOrebOpps", "def_tov_cnt" = "defTovCnt",
    "def_fta" = "defFta", "def_fga_cnt" = "defFgaCnt"
  )
  for (old in names(nms)) {
    if (old %in% names(df)) names(df)[names(df) == old] <- nms[[old]]
  }
  if ("playerIds" %in% names(df)) {
    df$playerIds <- lapply(df$playerIds, function(s) {
      as.integer(strsplit(gsub("[{}]", "", as.character(s)), ",")[[1]])
    })
  }
  df
}

# ── GET /api/lineups/summary ──────────────────────────────────
#* @get /api/lineups/summary
#* @serializer json
function(req, res,
         game_year = "2026", start_date = "2025-10-01", end_date = "2026-06-30",
         num = "5", game_type = "", opp_ids = "",
         home_away = "", outcome = "",
         gn_min = "", gn_max = "", last_n = "",
         opp_rank_side = "", opp_rank_n = "", opp_rank_metric = "",
         clutch_margin = "", clutch_status = "", clutch_minutes = "",
         clutch_ot_margin = "false") {

  gy <- as.integer(game_year)
  bounds <- season_date_bounds(gy)

  # Clutch params
  max_margin <- na_int(clutch_margin)
  margin_status <- na_chr(clutch_status)
  max_time_remaining <- if (nzchar(clutch_minutes)) as.integer(clutch_minutes) * 60L else NA_integer_
  ot_margin_filter <- identical(clutch_ot_margin, "true")

  df <- run_fetch_lineups(
    pg_pool,
    num = as.integer(num),
    team_csv = NA_character_, player_csv = NA_character_,
    player_off_csv = NA_character_, exact = TRUE,
    start_date = if (nzchar(start_date)) start_date else bounds$start,
    end_date = if (nzchar(end_date)) end_date else bounds$end,
    min_poss = 0L, game_year = gy,
    game_type_csv = na_chr(game_type), opp_ids_csv = na_chr(opp_ids),
    home_away = na_chr(home_away), outcome = na_chr(outcome),
    opp_rank_side = na_chr(opp_rank_side),
    opp_rank_n = na_int(opp_rank_n),
    opp_rank_metric = na_chr(opp_rank_metric),
    max_margin = max_margin, margin_status = margin_status,
    max_time_remaining = max_time_remaining,
    ot_margin_filter = ot_margin_filter,
    min_gn = na_int(gn_min), max_gn = na_int(gn_max), last_n = na_int(last_n)
  )

  if (is.null(df) || nrow(df) == 0) return(list())

  # Replace NAs in shot/numeric columns with 0
  shot_cols <- grep("fg[23]", names(df), value = TRUE)
  for (col in shot_cols) df[[col]][is.na(df[[col]])] <- 0
  for (col in c("off_poss", "off_pts", "def_poss", "def_pts", "minutes")) {
    if (col %in% names(df)) df[[col]][is.na(df[[col]])] <- 0
  }

  # Ensure PPP/net columns exist for the frontend contract
  if (!("off_ppp" %in% names(df)) && all(c("off_pts", "off_poss") %in% names(df))) {
    df$off_ppp <- ifelse(df$off_poss > 0, round(df$off_pts / df$off_poss * 100, 1), NA_real_)
  }
  if (!("def_ppp" %in% names(df)) && all(c("def_pts", "def_poss") %in% names(df))) {
    df$def_ppp <- ifelse(df$def_poss > 0, round(df$def_pts / df$def_poss * 100, 1), NA_real_)
  }
  if (!("net_rtg" %in% names(df)) && all(c("off_ppp", "def_ppp") %in% names(df))) {
    df$net_rtg <- ifelse(!is.na(df$off_ppp) & !is.na(df$def_ppp), round(df$off_ppp - df$def_ppp, 1), NA_real_)
  }

  df$total_poss <- df$off_poss + df$def_poss
  df$plus_minus <- df$off_pts - df$def_pts

  # Drop game_year, player_names (array) columns before rename
  df$game_year <- NULL
  df$player_names <- NULL

  out <- rename_lineup_summary(df)
  if (!("offPpp" %in% names(out)) && all(c("offPts", "offPoss") %in% names(out))) {
    out$offPpp <- ifelse(out$offPoss > 0, round(out$offPts / out$offPoss * 100, 1), 0)
  }
  if (!("defPpp" %in% names(out)) && all(c("defPts", "defPoss") %in% names(out))) {
    out$defPpp <- ifelse(out$defPoss > 0, round(out$defPts / out$defPoss * 100, 1), 0)
  }
  if (!("netRtg" %in% names(out)) && all(c("offPpp", "defPpp") %in% names(out))) {
    out$netRtg <- round(out$offPpp - out$defPpp, 1)
  }
  for (col in c("offPpp", "defPpp", "netRtg")) {
    if (col %in% names(out)) out[[col]][is.na(out[[col]])] <- 0
  }
  out
}

# ── GET /api/lineups/four-factors ─────────────────────────────
#* @get /api/lineups/four-factors
#* @serializer json
function(req, res,
         game_year = "2026", start_date = "2025-10-01", end_date = "2026-06-30",
         num = "5", game_type = "", opp_ids = "",
         home_away = "", outcome = "",
         gn_min = "", gn_max = "", last_n = "",
         opp_rank_side = "", opp_rank_n = "", opp_rank_metric = "",
         clutch_margin = "", clutch_status = "", clutch_minutes = "",
         clutch_ot_margin = "false") {

  gy <- as.integer(game_year)
  bounds <- season_date_bounds(gy)

  max_margin <- na_int(clutch_margin)
  margin_status <- na_chr(clutch_status)
  max_time_remaining <- if (nzchar(clutch_minutes)) as.integer(clutch_minutes) * 60L else NA_integer_
  ot_margin_filter <- identical(clutch_ot_margin, "true")

  df <- run_fetch_lineups_ff(
    pg_pool,
    num = as.integer(num),
    team_csv = NA_character_, player_csv = NA_character_,
    player_off_csv = NA_character_, exact = TRUE,
    start_date = if (nzchar(start_date)) start_date else bounds$start,
    end_date = if (nzchar(end_date)) end_date else bounds$end,
    min_poss = 0L, game_year = gy,
    game_type_csv = na_chr(game_type), opp_ids_csv = na_chr(opp_ids),
    home_away = na_chr(home_away), outcome = na_chr(outcome),
    opp_rank_side = na_chr(opp_rank_side),
    opp_rank_n = na_int(opp_rank_n),
    opp_rank_metric = na_chr(opp_rank_metric),
    max_margin = max_margin, margin_status = margin_status,
    max_time_remaining = max_time_remaining,
    ot_margin_filter = ot_margin_filter,
    min_gn = na_int(gn_min), max_gn = na_int(gn_max), last_n = na_int(last_n)
  )

  if (is.null(df) || nrow(df) == 0) return(list())

  for (col in names(df)) {
    if (is.numeric(df[[col]])) df[[col]][is.na(df[[col]])] <- 0
  }

  df$total_poss <- df$off_poss + df$def_poss

  df$game_year <- NULL
  df$player_names <- NULL

  rename_lineup_ff(df)
}

# ── GET /api/lineups/game-log ─────────────────────────────────
#* @get /api/lineups/game-log
#* @serializer unboxedJSON
function(req, res,
         sub_hash = "", team_id = "", game_year = "2026", view_mode = "summary") {

  req_hash <- as.character(sub_hash)
  req_tid  <- as.integer(team_id)
  gy       <- as.integer(game_year)

  if (!nzchar(req_hash) || is.na(req_tid)) {
    res$status <- 400
    return(list(error = "sub_hash and team_id are required"))
  }

  # Resolve sub_lineup_hash → lineup_hash(es)
  lineup_hashes <- DBI::dbGetQuery(pg_pool,
    sprintf("SELECT DISTINCT lineup_hash FROM %s.sub_lineups WHERE sub_lineup_hash = $1 AND team_id = $2 AND game_year = $3", SCHEMA),
    params = list(req_hash, req_tid, gy))$lineup_hash

  if (length(lineup_hashes) == 0) lineup_hashes <- req_hash
  lineup_hashes <- unique(as.character(lineup_hashes))
  lineup_hashes <- lineup_hashes[!is.na(lineup_hashes) & nzchar(lineup_hashes)]
  if (length(lineup_hashes) == 0) return(list(lineupName = req_hash, games = list()))

  hash_placeholders <- paste(sprintf("$%d", seq_along(lineup_hashes)), collapse = ",")
  tid_idx <- length(lineup_hashes) + 1
  gy_idx  <- length(lineup_hashes) + 2
  qparams <- c(as.list(lineup_hashes), list(req_tid, gy))

  # Get schedule
  sched <- DBI::dbGetQuery(pg_pool, sprintf(
    "SELECT game_id, gn, game_date, opp_team_name, team_score, opp_score,
            team_score > opp_score AS has_won
     FROM %s.final_schedule_mv WHERE team_id = $1 AND game_year = $2", SCHEMA
  ), params = list(req_tid, gy))
  sched$result <- ifelse(sched$has_won, "W", "L")
  sched$score <- paste0(sched$team_score, "-", sched$opp_score)

  # Get lineup name
  lineup_name <- DBI::dbGetQuery(pg_pool, sprintf(
    "SELECT player_names_str FROM %s.sub_lineups_stats WHERE sub_lineup_hash = $1 AND team_id = $2 AND game_year = $3 LIMIT 1", SCHEMA
  ), params = list(req_hash, req_tid, gy))$player_names_str
  if (length(lineup_name) == 0 || is.na(lineup_name)) lineup_name <- req_hash

  if (identical(view_mode, "ff")) {
    # Four Factors path
    ff_query <- sprintf(
      "SELECT game_id, type_lineup,
              SUM(total_points) AS total_points, SUM(total_poss) AS total_poss,
              SUM(ts_poss_count) AS ts_poss_count, SUM(oreb_count) AS oreb_count,
              SUM(oreb_opportunities) AS oreb_opportunities, SUM(tov_count) AS tov_count,
              SUM(total_ft_attempts) AS total_ft_attempts, SUM(total_fga) AS total_fga,
              SUM(minutes) AS mins
       FROM %s.lineup_four_factors_by_game
       WHERE lineup_hash IN (%s) AND team_id = $%d AND game_year = $%d
       GROUP BY game_id, type_lineup", SCHEMA, hash_placeholders, tid_idx, gy_idx)
    ff_data <- DBI::dbGetQuery(pg_pool, ff_query, params = qparams)
    if (nrow(ff_data) == 0) return(list(lineupName = lineup_name, games = list()))

    off <- ff_data[ff_data$type_lineup == "offense", ]
    def <- ff_data[ff_data$type_lineup == "defense", ]
    combined <- merge(off, def, by = "game_id", all = TRUE, suffixes = c("_off", "_def"))
    safe <- function(x) ifelse(is.na(x), 0, x)

    games <- lapply(seq_len(nrow(combined)), function(i) {
      r <- combined[i, ]
      off_poss <- safe(r$total_poss_off); def_poss <- safe(r$total_poss_def)
      off_pts <- safe(r$total_points_off); def_pts <- safe(r$total_points_def)
      off_ppp <- if (off_poss > 0) round(off_pts / off_poss * 100, 1) else NA
      def_ppp <- if (def_poss > 0) round(def_pts / def_poss * 100, 1) else NA

      s <- sched[sched$game_id == r$game_id, ]
      list(
        gn = if (nrow(s)) s$gn[1] else NA,
        gameDate = if (nrow(s)) as.character(s$game_date[1]) else "",
        opponent = if (nrow(s)) s$opp_team_name[1] else "",
        result = if (nrow(s)) s$result[1] else "",
        score = if (nrow(s)) s$score[1] else "",
        offPpp = off_ppp, defPpp = def_ppp,
        netRtg = round(safe(off_ppp) - safe(def_ppp), 1),
        offPoss = off_poss, defPoss = def_poss,
        minutes = round(safe(r$mins_off), 1),
        offTs = if (safe(r$ts_poss_count_off) > 0) round(off_pts / (2 * r$ts_poss_count_off) * 100, 1) else NA,
        offOreb = if (safe(r$oreb_opportunities_off) > 0) round(safe(r$oreb_count_off) / r$oreb_opportunities_off * 100, 1) else NA,
        offTov = if (off_poss > 0) round(safe(r$tov_count_off) / off_poss * 100, 1) else NA,
        offFtr = if (safe(r$total_fga_off) > 0) round(safe(r$total_ft_attempts_off) / r$total_fga_off * 100, 1) else NA,
        defTs = if (safe(r$ts_poss_count_def) > 0) round(def_pts / (2 * r$ts_poss_count_def) * 100, 1) else NA,
        defOreb = if (safe(r$oreb_opportunities_def) > 0) round(safe(r$oreb_count_def) / r$oreb_opportunities_def * 100, 1) else NA,
        defTov = if (def_poss > 0) round(safe(r$tov_count_def) / def_poss * 100, 1) else NA,
        defFtr = if (safe(r$total_fga_def) > 0) round(safe(r$total_ft_attempts_def) / r$total_fga_def * 100, 1) else NA
      )
    })
    # Sort by GN
    gn_order <- order(sapply(games, function(g) ifelse(is.na(g$gn), 999, g$gn)))
    games <- games[gn_order]

  } else {
    # Summary path
    game_query <- sprintf(
      "SELECT game_id, type_lineup,
              SUM(total_poss) AS poss, SUM(total_pts) AS pts,
              SUM(fg2_made) AS fg2m, SUM(fg2_att) AS fg2a,
              SUM(fg3_made) AS fg3m, SUM(fg3_att) AS fg3a,
              SUM(minutes) AS mins
       FROM %s.mv_lineup_totals_by_day
       WHERE lineup_hash IN (%s) AND team_id = $%d AND game_year = $%d
       GROUP BY game_id, type_lineup", SCHEMA, hash_placeholders, tid_idx, gy_idx)
    game_data <- DBI::dbGetQuery(pg_pool, game_query, params = qparams)
    if (nrow(game_data) == 0) return(list(lineupName = lineup_name, games = list()))

    off <- game_data[game_data$type_lineup == "offense", ]
    def <- game_data[game_data$type_lineup == "defense", ]
    combined <- merge(off, def, by = "game_id", all = TRUE, suffixes = c("_off", "_def"))
    safe <- function(x) ifelse(is.na(x), 0, x)

    games <- lapply(seq_len(nrow(combined)), function(i) {
      r <- combined[i, ]
      off_poss <- safe(r$poss_off); def_poss <- safe(r$poss_def)
      off_pts <- safe(r$pts_off); def_pts <- safe(r$pts_def)
      off_ppp <- if (off_poss > 0) round(off_pts / off_poss * 100, 1) else NA
      def_ppp <- if (def_poss > 0) round(def_pts / def_poss * 100, 1) else NA

      s <- sched[sched$game_id == r$game_id, ]
      list(
        gn = if (nrow(s)) s$gn[1] else NA,
        gameDate = if (nrow(s)) as.character(s$game_date[1]) else "",
        opponent = if (nrow(s)) s$opp_team_name[1] else "",
        result = if (nrow(s)) s$result[1] else "",
        score = if (nrow(s)) s$score[1] else "",
        offPpp = off_ppp, defPpp = def_ppp,
        netRtg = round(safe(off_ppp) - safe(def_ppp), 1),
        offPoss = off_poss, defPoss = def_poss,
        minutes = round(safe(r$mins_off), 1),
        offFg2Made = safe(r$fg2m_off), offFg2Att = safe(r$fg2a_off),
        offFg3Made = safe(r$fg3m_off), offFg3Att = safe(r$fg3a_off),
        defFg2Made = safe(r$fg2m_def), defFg2Att = safe(r$fg2a_def),
        defFg3Made = safe(r$fg3m_def), defFg3Att = safe(r$fg3a_def)
      )
    })
    gn_order <- order(sapply(games, function(g) ifelse(is.na(g$gn), 999, g$gn)))
    games <- games[gn_order]
  }

  list(lineupName = lineup_name, games = games)
}

# ── GET /api/meta/players ─────────────────────────────────────
#* @get /api/meta/players
#* @param game_year:int Season year (default 2026)
#* @serializer json
function(game_year = "2026") {
  gy <- as.integer(game_year)
  df <- DBI::dbGetQuery(pg_pool, sprintf(
    "SELECT team_id, player_id, MIN(btrim(firstname)||' '||btrim(lastname)) AS name
     FROM %s.full_rosters WHERE game_year = $1
     GROUP BY team_id, player_id ORDER BY MIN(btrim(firstname)||' '||btrim(lastname))",
    SCHEMA
  ), params = list(gy))
  names(df) <- c("teamId", "playerId", "name")
  df
}
