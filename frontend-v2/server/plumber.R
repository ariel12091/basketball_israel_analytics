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

`%||%` <- function(a, b) if (!is.null(a)) a else b

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
SHINY_INTERNAL_API_KEY <- Sys.getenv("SHINY_INTERNAL_API_KEY", "")
RATE_LIMIT_WINDOW_SEC <- max(1L, as.integer(Sys.getenv("FRONTEND_RATE_WINDOW_SEC", "60")))
RATE_LIMIT_MAX_REQUESTS <- max(1L, as.integer(Sys.getenv("FRONTEND_RATE_MAX_REQUESTS", "180")))
REQ_HITS <- new.env(parent = emptyenv())
PROFILE_TIMING <- identical(tolower(Sys.getenv("FRONTEND_PROFILE_TIMING", "0")), "1")
CACHE_TTL_SEC <- max(1L, as.integer(Sys.getenv("FRONTEND_CACHE_TTL_SEC", "60")))
RESP_CACHE <- new.env(parent = emptyenv())

.ms_now <- function() as.numeric(proc.time()[3]) * 1000
.ms_elapsed <- function(start_ms) .ms_now() - start_ms
.timed <- function(fn) {
  t0 <- .ms_now()
  out <- fn()
  list(value = out, ms = .ms_elapsed(t0))
}
perf_log <- function(req, route, total_ms, db_ms, transform_ms, rows = NA_integer_) {
  if (!PROFILE_TIMING) return(invisible(NULL))
  ip <- tryCatch(client_ip(req), error = function(e) "unknown")
  message(sprintf(
    "[perf] route=%s total_ms=%.1f db_ms=%.1f transform_ms=%.1f rows=%s ip=%s",
    route, total_ms, db_ms, transform_ms,
    ifelse(is.na(rows), "NA", as.character(rows)), ip
  ))
  invisible(NULL)
}

cache_key <- function(route, req = NULL, suffix = "") {
  qs <- ""
  if (!is.null(req) && !is.null(req$QUERY_STRING) && nzchar(req$QUERY_STRING)) qs <- req$QUERY_STRING
  paste(route, qs, suffix, sep = "|")
}

cache_get <- function(key) {
  v <- RESP_CACHE[[key]]
  if (is.null(v)) return(NULL)
  if (!is.list(v) || is.null(v$exp) || is.null(v$val) || as.numeric(Sys.time()) > v$exp) {
    rm(list = key, envir = RESP_CACHE, inherits = FALSE)
    return(NULL)
  }
  v$val
}

cache_set <- function(key, value, ttl_sec = CACHE_TTL_SEC) {
  RESP_CACHE[[key]] <- list(exp = as.numeric(Sys.time()) + ttl_sec, val = value)
  invisible(value)
}

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

# Parse PostgreSQL int-array text (e.g. "{1,2,3}") into list-of-vectors
# using a single vectorized JSON decode.
parse_pg_int_array_json <- function(x) {
  if (is.null(x) || !length(x)) return(list())
  s <- as.character(x)
  s[is.na(s) | s == "" | s == "{}"] <- "[]"
  s <- chartr("{}", "[]", s)
  payload <- paste0("[", paste(s, collapse = ","), "]")
  out <- tryCatch(jsonlite::fromJSON(payload, simplifyVector = FALSE), error = function(e) NULL)
  if (!is.null(out) && length(out) == length(s)) return(out)

  # Fail-safe: preserve shape without per-row parsing
  rep(list(integer(0)), length(s))
}

# Only game-level filters trigger SQL path. Team + min_poss are applied client-side on MV data.
season_date_bounds <- function(game_year) {
  gy <- as.integer(game_year)
  list(start = paste0(gy - 1, "-10-01"), end = paste0(gy, "-07-01"))
}

needs_filtered <- function(opp_ids, game_type, home_away, outcome,
                           gn_min, gn_max, last_n, start_date, end_date,
                           game_year = 2026, opp_rank_side = "",
                           num_starters_off_mode = "", num_starters_off = "",
                           num_starters_def_mode = "", num_starters_def = "") {
  bounds <- season_date_bounds(game_year)
  has_starters <- (nzchar(num_starters_off_mode) && nzchar(num_starters_off)) ||
    (nzchar(num_starters_def_mode) && nzchar(num_starters_def))
  nzchar(opp_ids) || nzchar(game_type) ||
  nzchar(home_away) || nzchar(outcome) ||
  nzchar(gn_min) || nzchar(gn_max) || nzchar(last_n) ||
  nzchar(opp_rank_side) ||
  has_starters ||
  start_date != bounds$start || end_date != bounds$end
}

# ── Ranking helpers (mirrors frontend-v2/src/utils/ranking.ts) ─
RANKING_BASELINE <- 100
RANKING_MIN_PCT <- 0.25
AUTO_TARGET_ROWS <- 150L

adaptive_baseline_r <- function(poss_vec) {
  n <- length(poss_vec)
  if (n == 0L) return(0)
  pct_above <- sum(poss_vec >= RANKING_BASELINE, na.rm = TRUE) / n
  if (pct_above >= RANKING_MIN_PCT) return(RANKING_BASELINE)
  sorted <- sort(poss_vec)
  idx <- floor(n * (1 - RANKING_MIN_PCT)) + 1L
  sorted[min(idx, n)]
}

pr_rank <- function(vals) {
  valid <- !is.na(vals)
  n <- sum(valid)
  result <- rep(NA_real_, length(vals))
  if (n == 0L) return(result)
  if (n == 1L) { result[valid] <- 0.5; return(result) }
  r <- rank(vals[valid], ties.method = "average")
  result[valid] <- (r - 1) / (n - 1)
  result
}

auto_minposs_target_r <- function(poss_vec, step = 10L, target_rows = AUTO_TARGET_ROWS) {
  vals <- poss_vec[is.finite(poss_vec)]
  if (!length(vals)) return(0L)
  vals <- sort(vals, decreasing = TRUE)
  if (length(vals) <= target_rows) return(0L)
  kth <- vals[target_rows]
  as.integer(ceiling(kth / step) * step)
}

# ── Ranked-data cache (full dataset, keyed by game-level filters only) ─
RANKED_CACHE <- new.env(parent = emptyenv())

ranked_key_from_qs <- function(route, qs) {
  # Strip local filter params from query string for cache key
  qs <- gsub("&?min_poss=[^&]*", "", qs)
  qs <- gsub("&?filter_team_ids=[^&]*", "", qs)
  qs <- gsub("&?players_on=[^&]*", "", qs)
  qs <- gsub("&?players_off=[^&]*", "", qs)
  qs <- gsub("^[&?]+", "", qs)
  paste(route, qs, sep = "|")
}

ranked_cache_get <- function(key) {
  v <- RANKED_CACHE[[key]]
  if (is.null(v)) return(NULL)
  if (as.numeric(Sys.time()) > v$exp) {
    rm(list = key, envir = RANKED_CACHE, inherits = FALSE)
    return(NULL)
  }
  v$val
}

ranked_cache_set <- function(key, value, ttl_sec = CACHE_TTL_SEC) {
  RANKED_CACHE[[key]] <- list(exp = as.numeric(Sys.time()) + ttl_sec, val = value)
  invisible(value)
}

# Apply local lineup filters on ranked data (team, players, min_poss)
apply_lineup_local_filters <- function(df, team_ids, players_on, players_off, min_poss) {
  if (length(team_ids) > 0) {
    df <- df[df$teamId %in% team_ids, , drop = FALSE]
  }
  if (length(players_on) > 0) {
    keep <- vapply(df$playerIds, function(ids) all(players_on %in% ids), logical(1))
    df <- df[keep, , drop = FALSE]
  }
  if (length(players_off) > 0) {
    keep <- vapply(df$playerIds, function(ids) !any(players_off %in% ids), logical(1))
    df <- df[keep, , drop = FALSE]
  }
  auto_val <- auto_minposs_target_r(df$totalPoss)
  if (!is.na(min_poss) && min_poss > 0L) {
    df <- df[df$totalPoss >= min_poss, , drop = FALSE]
  }
  list(rows = df, autoMinPoss = auto_val)
}

# Mirrors run_onoff_compute_14 from server_tab1.R — 23 params with explicit casts
run_onoff_compute <- function(pool, start_d, end_d, team_csv, min_all, min_on,
                               game_year, game_type_csv, opp_ids_csv,
                               home_away, outcome,
                               opp_rank_side = NA_character_,
                               opp_rank_n = NA_integer_,
                               opp_rank_metric = NA_character_,
                               min_gn = NA_integer_, max_gn = NA_integer_, last_n = NA_integer_,
                               num_starters_off = NA_integer_, num_starters_def = NA_integer_,
                               num_starters_off_min = NA_integer_, num_starters_off_max = NA_integer_,
                               num_starters_def_min = NA_integer_, num_starters_def_max = NA_integer_) {
  DBI::dbGetQuery(pool, paste0(
    "SELECT * FROM ", SCHEMA, ".onoff_compute(",
    "$1::date,$2::date,$3::text,$4::int4,$5::int4,$6::numeric,$7::text,",
    "$8::text,$9::text,$10::text,$11::text,$12::text,$13::int4,$14::text,",
    "$15::int4,$16::int4,$17::int4,$18::int4,$19::int4,$20::int4,$21::int4,$22::int4,$23::int4", ")"
  ), params = list(
    as.Date(start_d), as.Date(end_d), team_csv,
    as.integer(min_all), as.integer(min_on), DEFAULT_MIN_NET, as.character(game_year),
    game_type_csv, opp_ids_csv, home_away, outcome,
    opp_rank_side, opp_rank_n, opp_rank_metric,
    min_gn, max_gn, last_n,
    num_starters_off, num_starters_def,
    num_starters_off_min, num_starters_off_max,
    num_starters_def_min, num_starters_def_max
  ))
}

# Mirrors run_four_factors_compute from server_tab1.R — 20 params with explicit casts
run_ff_compute <- function(pool, game_year, start_d, end_d, team_csv,
                            game_type_csv, opp_ids_csv, home_away, outcome,
                            opp_rank_side = NA_character_,
                            opp_rank_n = NA_integer_,
                            opp_rank_metric = NA_character_,
                            min_gn = NA_integer_, max_gn = NA_integer_, last_n = NA_integer_,
                            num_starters_off = NA_integer_, num_starters_def = NA_integer_,
                            num_starters_off_min = NA_integer_, num_starters_off_max = NA_integer_,
                            num_starters_def_min = NA_integer_, num_starters_def_max = NA_integer_) {
  DBI::dbGetQuery(pool, paste0(
    "SELECT * FROM ", SCHEMA, ".four_factors_compute(",
    "$1::int4,$2::date,$3::date,$4::text,$5::text,$6::text,",
    "$7::text,$8::text,$9::text,$10::int4,$11::text,",
    "$12::int4,$13::int4,$14::int4,$15::int4,$16::int4,$17::int4,$18::int4,$19::int4,$20::int4", ")"
  ), params = list(
    as.integer(game_year), as.Date(start_d), as.Date(end_d), team_csv,
    game_type_csv, opp_ids_csv, home_away, outcome,
    opp_rank_side, opp_rank_n, opp_rank_metric,
    min_gn, max_gn, last_n,
    num_starters_off, num_starters_def,
    num_starters_off_min, num_starters_off_max,
    num_starters_def_min, num_starters_def_max
  ))
}

# Filtered FF path optimized: perform FF+OnOff join in PostgreSQL (single roundtrip)
run_ff_with_diffs_compute <- function(pool, game_year, start_d, end_d, team_csv,
                                      game_type_csv, opp_ids_csv, home_away, outcome,
                                      opp_rank_side = NA_character_,
                                      opp_rank_n = NA_integer_,
                                      opp_rank_metric = NA_character_,
                                      min_gn = NA_integer_, max_gn = NA_integer_, last_n = NA_integer_,
                                      num_starters_off = NA_integer_, num_starters_def = NA_integer_,
                                      num_starters_off_min = NA_integer_, num_starters_off_max = NA_integer_,
                                      num_starters_def_min = NA_integer_, num_starters_def_max = NA_integer_) {
  DBI::dbGetQuery(pool, paste0(
    "SELECT ff.*, oo.\"Net RTG Diff\", oo.\"Off ON Diff\", oo.\"Def ON Diff\" ",
    "FROM ", SCHEMA, ".four_factors_compute(",
    "$1::int4,$2::date,$3::date,$4::text,$5::text,$6::text,",
    "$7::text,$8::text,$9::text,$10::int4,$11::text,",
    "$12::int4,$13::int4,$14::int4,$15::int4,$16::int4,$17::int4,$18::int4,$19::int4,$20::int4",
    ") ff ",
    "LEFT JOIN ", SCHEMA, ".onoff_compute(",
    "$2::date,$3::date,$4::text,$21::int4,$22::int4,$23::numeric,$24::text,",
    "$5::text,$6::text,$7::text,$8::text,$9::text,$10::int4,$11::text,",
    "$12::int4,$13::int4,$14::int4,$15::int4,$16::int4,$17::int4,$18::int4,$19::int4,$20::int4",
    ") oo ",
    "ON ff.player_id = oo.player_id AND ff.team_id = oo.team_id"
  ), params = list(
    as.integer(game_year), as.Date(start_d), as.Date(end_d), team_csv,
    game_type_csv, opp_ids_csv, home_away, outcome,
    opp_rank_side, opp_rank_n, opp_rank_metric,
    min_gn, max_gn, last_n,
    num_starters_off, num_starters_def,
    num_starters_off_min, num_starters_off_max,
    num_starters_def_min, num_starters_def_max,
    0L, 0L, DEFAULT_MIN_NET, as.character(game_year)
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
request_api_key <- function(req) {
  key <- req$HTTP_X_API_KEY
  if (is.null(key) || !nzchar(key)) key <- req$HTTP_X_SHINY_API_KEY
  if (is.null(key) || !nzchar(key)) {
    authz <- req$HTTP_AUTHORIZATION
    if (!is.null(authz) && nzchar(authz)) {
      key <- sub("^Bearer\\s+", "", authz, perl = TRUE)
    }
  }
  if (is.null(key)) "" else key
}
is_internal_query_path <- function(req) {
  path <- req$PATH_INFO
  if (is.null(path) || !nzchar(path)) return(FALSE)
  normalized <- sub("/+$", "", path)
  identical(normalized, "/api/internal/query") || startsWith(normalized, "/api/shiny/")
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
  res$setHeader("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
  res$setHeader("Access-Control-Allow-Headers", "Content-Type, X-API-Key, X-Shiny-API-Key, Authorization")
  if (req$REQUEST_METHOD == "OPTIONS") {
    res$status <- 200
    return(list())
  }
  plumber::forward()
}

#* Optional API key auth (keeps anonymous UX when key is unset)
#* @filter auth
function(req, res) {
  if (is_internal_query_path(req)) return(plumber::forward())
  if (!nzchar(API_KEY)) return(plumber::forward())
  key <- request_api_key(req)
  if (!identical(key, API_KEY)) {
    res$status <- 401
    return(list(error = "Unauthorized"))
  }
  plumber::forward()
}

#* Basic in-memory IP rate limit guard
#* @filter rate_limit
function(req, res) {
  if (is_internal_query_path(req)) return(plumber::forward())
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

strip_sql_comments <- function(statement) {
  no_block <- gsub("/\\*.*?\\*/", " ", statement, perl = TRUE)
  gsub("--[^\\r\\n]*(\\r?\\n|$)", " ", no_block, perl = TRUE)
}

is_read_only_statement <- function(statement) {
  if (!is.character(statement) || length(statement) != 1L || !nzchar(trimws(statement))) return(FALSE)
  if (nchar(statement, type = "bytes") > 200000L) return(FALSE)
  stripped <- strip_sql_comments(statement)
  normalized <- tolower(trimws(gsub("\\s+", " ", stripped)))
  if (!grepl("^(select|with)\\b", normalized, perl = TRUE)) return(FALSE)
  if (grepl(";", normalized, fixed = TRUE)) return(FALSE)
  denied <- paste(
    c("insert", "update", "delete", "merge", "drop", "alter", "create",
      "truncate", "grant", "revoke", "copy", "do", "call", "execute",
      "refresh", "vacuum", "analyze", "set", "reset"),
    collapse = "|"
  )
  !grepl(paste0("\\b(", denied, ")\\b"), normalized, perl = TRUE)
}

query_payload_to_response <- function(df) {
  rows <- if (nrow(df)) {
    lapply(seq_len(nrow(df)), function(i) as.list(df[i, , drop = FALSE]))
  } else {
    list()
  }
  list(
    columns = names(df),
    classes = lapply(df, class),
    rows = rows
  )
}

# Shiny-specific API routes return DB-style data-frame payloads because the
# current Shiny modules already do their own R-side post-processing.
shiny_auth_error <- function(req, res) {
  if (!nzchar(SHINY_INTERNAL_API_KEY)) {
    res$status <- 403
    return(list(error = "Shiny API routes are disabled"))
  }
  if (!identical(request_api_key(req), SHINY_INTERNAL_API_KEY)) {
    res$status <- 401
    return(list(error = "Unauthorized"))
  }
  NULL
}

shiny_body_params <- function(req, res, expected = NULL) {
  body <- tryCatch(
    jsonlite::fromJSON(req$postBody, simplifyVector = FALSE),
    error = function(e) NULL
  )
  params <- body$params
  if (is.null(params)) params <- list()
  if (!is.list(params)) {
    res$status <- 400
    return(list(error = "params must be a JSON array"))
  }
  params <- lapply(params, function(x) if (is.null(x)) NA else x)
  if (!is.null(expected) && length(params) != expected) {
    res$status <- 400
    return(list(error = sprintf("Expected %d params, got %d", expected, length(params))))
  }
  params
}

sql_placeholders <- function(casts) {
  paste(sprintf("$%d::%s", seq_along(casts), casts), collapse = ",")
}

sql_function_call <- function(fn, casts) {
  sprintf("SELECT * FROM %s.%s(%s)", SCHEMA, fn, sql_placeholders(casts))
}

shiny_query_response <- function(res, statement, params = list()) {
  out <- tryCatch({
    if (length(params)) {
      DBI::dbGetQuery(pg_pool, statement, params = params)
    } else {
      DBI::dbGetQuery(pg_pool, statement)
    }
  }, error = function(e) {
    res$status <- 500
    list(error = e$message)
  })
  if (is.list(out) && !is.data.frame(out) && !is.null(out$error)) return(out)
  query_payload_to_response(out)
}

shiny_function_route <- function(req, res, fn, casts) {
  auth_error <- shiny_auth_error(req, res)
  if (!is.null(auth_error)) return(auth_error)
  params <- shiny_body_params(req, res, length(casts))
  if (is.list(params) && !is.null(params$error)) return(params)
  shiny_query_response(res, sql_function_call(fn, casts), params)
}

shiny_sql_route <- function(req, res, statement, expected = NULL) {
  auth_error <- shiny_auth_error(req, res)
  if (!is.null(auth_error)) return(auth_error)
  params <- shiny_body_params(req, res, expected)
  if (is.list(params) && !is.null(params$error)) return(params)
  shiny_query_response(res, statement, params)
}

#* @post /api/shiny/meta/teams-distinct
#* @serializer unboxedJSON
function(req, res) {
  shiny_sql_route(req, res, sprintf(
    "SELECT DISTINCT team_id, team_name
       FROM %s.full_rosters
      WHERE game_year = $1::int4
      ORDER BY team_name",
    SCHEMA
  ), expected = 1L)
}

#* @post /api/shiny/meta/teams-min
#* @serializer unboxedJSON
function(req, res) {
  shiny_sql_route(req, res, sprintf(
    "SELECT DISTINCT team_id, MIN(team_name) AS team_name
       FROM %s.full_rosters
      WHERE game_year = $1::int4
      GROUP BY team_id
      ORDER BY MIN(team_name)",
    SCHEMA
  ), expected = 1L)
}

#* @post /api/shiny/meta/players
#* @serializer unboxedJSON
function(req, res) {
  shiny_sql_route(req, res, sprintf(
    "SELECT team_id,
            player_id,
            MIN(btrim(firstname)||' '||btrim(lastname)) AS name
       FROM %s.full_rosters
      WHERE game_year = $1::int4
      GROUP BY team_id, player_id
      ORDER BY MIN(btrim(firstname)||' '||btrim(lastname))",
    SCHEMA
  ), expected = 1L)
}

#* @post /api/shiny/meta/game-numbers
#* @serializer unboxedJSON
function(req, res) {
  shiny_sql_route(req, res, sprintf(
    "SELECT DISTINCT gn
       FROM %s.final_schedule_mv
      WHERE game_year = $1::int4
      ORDER BY gn",
    SCHEMA
  ), expected = 1L)
}

#* @post /api/shiny/meta/last-success
#* @serializer unboxedJSON
function(req, res) {
  shiny_sql_route(req, res, sprintf(
    "SELECT value FROM %s.app_meta WHERE key = 'etl_full_last_success' LIMIT 1",
    SCHEMA
  ), expected = 0L)
}

#* @post /api/shiny/onoff/default
#* @serializer unboxedJSON
function(req, res) {
  shiny_sql_route(req, res, sprintf(
    "SELECT *
       FROM %s.onoff_default_mv
      WHERE \"Year\" = $1::int4
      ORDER BY \"Net RTG Diff\" DESC, \"Team\", \"Last Name\", \"First Name\"",
    SCHEMA
  ), expected = 1L)
}

#* @post /api/shiny/onoff/player-advanced
#* @serializer unboxedJSON
function(req, res) {
  shiny_sql_route(req, res, sprintf(
    "SELECT *
       FROM %s.player_advanced_stats_mv
      WHERE game_year = $1::int4",
    SCHEMA
  ), expected = 1L)
}

#* @post /api/shiny/teams/ratings-default
#* @serializer unboxedJSON
function(req, res) {
  shiny_sql_route(req, res, sprintf(
    "SELECT game_year, team_id, team_name, off_ppp, def_ppp, net_rtg,
            games_played, wins, losses, off_poss, def_poss,
            rank_net_rtg, rank_off_ppp, rank_def_ppp
       FROM %s.team_ppp_ratings_mv
      WHERE game_year = $1::int4
      ORDER BY rank_net_rtg",
    SCHEMA
  ), expected = 1L)
}

#* @post /api/shiny/teams/four-factors-default
#* @serializer unboxedJSON
function(req, res) {
  shiny_sql_route(req, res, sprintf(
    "SELECT *
       FROM %s.team_four_factors_mv
      WHERE game_year = $1::int4",
    SCHEMA
  ), expected = 1L)
}

#* @post /api/shiny/players/traditional-default
#* @serializer unboxedJSON
function(req, res) {
  shiny_sql_route(req, res, sprintf(
    "SELECT player_id, team_id, team_name, player_name AS \"Player\",
            gp, poss_on_floor, minutes,
            pts, reb, ast, stl, blk, tov, fgm, fga, \"3pm\", \"3pa\", ftm, fta,
            fg_pct, tp_pct, ft_pct, efg, ts
       FROM %s.player_traditional_stats_mv
      WHERE game_year = $1::int4",
    SCHEMA
  ), expected = 1L)
}

#* @post /api/shiny/gamelogs/schedule
#* @serializer unboxedJSON
function(req, res) {
  shiny_sql_route(req, res, sprintf(
    "SELECT * FROM %s.final_schedule_mv WHERE game_year = $1::int4",
    SCHEMA
  ), expected = 1L)
}

#* @post /api/shiny/gamelogs/lineup-totals
#* @serializer unboxedJSON
function(req, res) {
  shiny_sql_route(req, res, sprintf(
    "SELECT team_id, lineup_hash, type_lineup, g_date, game_id, game_year,
            total_poss, total_pts, fg2_made, fg2_att, fg3_made, fg3_att, minutes, num_starters
       FROM %s.mv_lineup_totals_by_day
      WHERE game_year = $1::int4",
    SCHEMA
  ), expected = 1L)
}

#* @post /api/shiny/gamelogs/lineup-four-factors
#* @serializer unboxedJSON
function(req, res) {
  shiny_sql_route(req, res, sprintf(
    "SELECT lineup_hash, team_id, game_id, game_year, type_lineup,
            total_points, total_poss, ts_poss_count, oreb_count,
            oreb_opportunities, tov_count, total_ft_attempts, total_fga,
            total_fgm, total_fg3_made, minutes, num_starters
       FROM %s.lineup_four_factors_by_game
      WHERE game_year = $1::int4",
    SCHEMA
  ), expected = 1L)
}

#* @post /api/shiny/onoff/summary
#* @serializer unboxedJSON
function(req, res) {
  shiny_function_route(
    req, res, "onoff_compute",
    c("date", "date", "text", "int4", "int4", "numeric", "text",
      "text", "text", "text", "text", "text", "int4", "text",
      "int4", "int4", "int4", "int4", "int4", "int4", "int4", "int4", "int4")
  )
}

#* @post /api/shiny/onoff/four-factors
#* @serializer unboxedJSON
function(req, res) {
  shiny_function_route(
    req, res, "four_factors_compute",
    c("int4", "date", "date", "text", "text", "text", "text", "text", "text", "int4",
      "text", "int4", "int4", "int4", "int4", "int4", "int4", "int4", "int4", "int4")
  )
}

#* @post /api/shiny/lineups/summary
#* @serializer unboxedJSON
function(req, res) {
  shiny_function_route(
    req, res, "fetch_lineups_csv_v2",
    c("int4", "text", "text", "text", "bool", "date", "date", "int4", "int4",
      "text", "text", "text", "text", "text", "int4", "text", "int4", "text", "int4", "bool",
      "int4", "int4", "int4", "int4", "int4", "int4", "int4", "int4", "int4")
  )
}

#* @post /api/shiny/lineups/four-factors
#* @serializer unboxedJSON
function(req, res) {
  shiny_function_route(
    req, res, "fetch_lineups_four_factors_csv",
    c("int4", "text", "text", "text", "bool", "date", "date", "int4", "int4",
      "text", "text", "text", "text", "text", "int4", "text", "int4", "text", "int4", "bool",
      "int4", "int4", "int4", "int4", "int4", "int4", "int4", "int4", "int4")
  )
}

#* @post /api/shiny/teams/ratings
#* @serializer unboxedJSON
function(req, res) {
  shiny_function_route(
    req, res, "get_team_ratings_dynamic",
    c("int4", "date", "date", "text", "text", "text", "text", "text", "int4", "text",
      "int4", "text", "int4", "bool", "int4", "int4", "int4", "int4", "int4", "int4", "int4", "int4", "int4")
  )
}

#* @post /api/shiny/teams/four-factors
#* @serializer unboxedJSON
function(req, res) {
  shiny_function_route(
    req, res, "get_team_four_factors_dynamic",
    c("int4", "date", "date", "text", "text", "text", "text", "text", "int4", "text",
      "int4", "text", "int4", "bool", "int4", "int4", "int4", "int4", "int4", "int4", "int4", "int4", "int4")
  )
}

#* @post /api/shiny/players/traditional
#* @serializer unboxedJSON
function(req, res) {
  shiny_function_route(
    req, res, "get_player_traditional_dynamic",
    c("int4", "date", "date", "text", "text", "text", "text", "text", "text", "int4",
      "text", "int4", "text", "int4", "bool", "int4", "int4", "int4")
  )
}

# Private Shiny compatibility endpoint. It preserves legacy query paths while
# the remaining Shiny reads move to explicit /api/shiny routes.
#* @post /api/internal/query
#* @serializer unboxedJSON
function(req, res) {
  if (!nzchar(SHINY_INTERNAL_API_KEY)) {
    res$status <- 403
    return(list(error = "Internal Shiny query endpoint is disabled"))
  }
  if (!identical(request_api_key(req), SHINY_INTERNAL_API_KEY)) {
    res$status <- 401
    return(list(error = "Unauthorized"))
  }

  body <- tryCatch(
    jsonlite::fromJSON(req$postBody, simplifyVector = FALSE),
    error = function(e) NULL
  )
  statement <- body$statement
  params <- body$params
  if (is.null(params)) params <- list()
  if (!is.list(params)) {
    res$status <- 400
    return(list(error = "params must be a JSON array"))
  }
  params <- lapply(params, function(x) if (is.null(x)) NA else x)

  if (!is_read_only_statement(statement)) {
    res$status <- 400
    return(list(error = "Only single read-only SELECT/WITH statements are allowed"))
  }

  out <- tryCatch({
    if (length(params)) {
      DBI::dbGetQuery(pg_pool, statement, params = params)
    } else {
      DBI::dbGetQuery(pg_pool, statement)
    }
  }, error = function(e) {
    res$status <- 500
    list(error = e$message)
  })
  if (is.list(out) && !is.data.frame(out) && !is.null(out$error)) return(out)
  query_payload_to_response(out)
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
         game_year = "2026", start_date = "2025-10-01", end_date = "2026-07-01",
         team_ids = "", min_on = "0", min_all = "0",
         game_type = "", opp_ids = "",
         home_away = "", outcome = "",
         gn_min = "", gn_max = "", last_n = "",
         opp_rank_side = "", opp_rank_n = "", opp_rank_metric = "",
         num_starters_off_mode = "", num_starters_off = "",
         num_starters_def_mode = "", num_starters_def = "") {
  key <- cache_key("/api/onoff/summary", req)
  hit <- cache_get(key)
  if (!is.null(hit)) return(hit)

  req_t0 <- .ms_now()
  db_ms <- 0
  transform_t0 <- NA_real_
  gy <- as.integer(game_year)

  if (!needs_filtered(opp_ids, game_type, home_away, outcome,
                      gn_min, gn_max, last_n, start_date, end_date,
                      game_year = gy, opp_rank_side = opp_rank_side,
                      num_starters_off_mode = num_starters_off_mode,
                      num_starters_off = num_starters_off,
                      num_starters_def_mode = num_starters_def_mode,
                      num_starters_def = num_starters_def)) {
    # Fast path: MV
    q <- .timed(function() DBI::dbGetQuery(pg_pool, sprintf(
      'SELECT * FROM %s.onoff_default_mv WHERE "Year" = $1', SCHEMA
    ), params = list(gy)))
    df <- q$value
    db_ms <- db_ms + q$ms
  } else {
    # Filtered path: call onoff_compute() with exact Shiny-app signature
    team_csv <- if (nzchar(team_ids)) team_ids else NA_character_
    off_val <- if (nzchar(num_starters_off_mode) && nzchar(num_starters_off)) as.integer(num_starters_off) else NA_integer_
    def_val <- if (nzchar(num_starters_def_mode) && nzchar(num_starters_def)) as.integer(num_starters_def) else NA_integer_
    q <- .timed(function() run_onoff_compute(
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
      min_gn = na_int(gn_min), max_gn = na_int(gn_max), last_n = na_int(last_n),
      num_starters_off = NA_integer_, num_starters_def = NA_integer_,
      num_starters_off_min = if (identical(num_starters_off_mode, "gte")) off_val else NA_integer_,
      num_starters_off_max = if (identical(num_starters_off_mode, "lte")) off_val else NA_integer_,
      num_starters_def_min = if (identical(num_starters_def_mode, "gte")) def_val else NA_integer_,
      num_starters_def_max = if (identical(num_starters_def_mode, "lte")) def_val else NA_integer_
    ))
    df <- q$value
    db_ms <- db_ms + q$ms
  }
  transform_t0 <- .ms_now()

  # Replace NAs in numeric columns with 0 to avoid frontend render crashes
  for (col in names(df)) {
    if (is.numeric(df[[col]])) df[[col]][is.na(df[[col]])] <- 0
  }

  out <- rename_onoff(df)
  perf_log(
    req, "/api/onoff/summary",
    total_ms = .ms_elapsed(req_t0),
    db_ms = db_ms,
    transform_ms = .ms_elapsed(transform_t0),
    rows = nrow(out)
  )
  cache_set(key, out)
}

# ── GET /api/onoff/four-factors ──────────────────────────────
#* @get /api/onoff/four-factors
#* @serializer json
function(req, res,
         game_year = "2026", start_date = "2025-10-01", end_date = "2026-07-01",
         team_ids = "", game_type = "", opp_ids = "",
         home_away = "", outcome = "",
         gn_min = "", gn_max = "", last_n = "",
         opp_rank_side = "", opp_rank_n = "", opp_rank_metric = "",
         num_starters_off_mode = "", num_starters_off = "",
         num_starters_def_mode = "", num_starters_def = "") {
  key <- cache_key("/api/onoff/four-factors", req)
  hit <- cache_get(key)
  if (!is.null(hit)) return(hit)

  req_t0 <- .ms_now()
  db_ms <- 0
  transform_t0 <- NA_real_
  gy <- as.integer(game_year)
  team_csv <- if (nzchar(team_ids)) team_ids else NA_character_
  gt_csv   <- if (nzchar(game_type)) game_type else NA_character_
  opp_csv  <- if (nzchar(opp_ids)) opp_ids else NA_character_
  ha       <- if (nzchar(home_away)) home_away else NA_character_
  oc       <- if (nzchar(outcome)) outcome else NA_character_

  if (!needs_filtered(opp_ids, game_type, home_away, outcome,
                      gn_min, gn_max, last_n, start_date, end_date,
                      game_year = gy, opp_rank_side = opp_rank_side,
                      num_starters_off_mode = num_starters_off_mode,
                      num_starters_off = num_starters_off,
                      num_starters_def_mode = num_starters_def_mode,
                      num_starters_def = num_starters_def)) {
    # Fast path: join MV + onoff MV for net diffs
    q <- .timed(function() DBI::dbGetQuery(pg_pool, sprintf('
      SELECT ff.*, o."Net RTG Diff", o."Off ON Diff", o."Def ON Diff"
      FROM %s.player_advanced_stats_mv ff
      LEFT JOIN %s.onoff_default_mv o
        ON ff.player_id = o.player_id
       AND ff.team_id = o.team_id
       AND ff.game_year = o."Year"
      WHERE ff.game_year = $1
    ', SCHEMA, SCHEMA), params = list(gy)))
    df <- q$value
    db_ms <- db_ms + q$ms
  } else {
    # Filtered path: single SQL call (DB-side join of FF + OnOff diffs)
    off_val <- if (nzchar(num_starters_off_mode) && nzchar(num_starters_off)) as.integer(num_starters_off) else NA_integer_
    def_val <- if (nzchar(num_starters_def_mode) && nzchar(num_starters_def)) as.integer(num_starters_def) else NA_integer_
    q <- .timed(function() run_ff_with_diffs_compute(
      pg_pool, game_year = gy, start_d = start_date, end_d = end_date,
      team_csv = team_csv, game_type_csv = gt_csv, opp_ids_csv = opp_csv,
      home_away = ha, outcome = oc,
      opp_rank_side = na_chr(opp_rank_side),
      opp_rank_n = na_int(opp_rank_n),
      opp_rank_metric = na_chr(opp_rank_metric),
      min_gn = na_int(gn_min), max_gn = na_int(gn_max), last_n = na_int(last_n),
      num_starters_off = NA_integer_, num_starters_def = NA_integer_,
      num_starters_off_min = if (identical(num_starters_off_mode, "gte")) off_val else NA_integer_,
      num_starters_off_max = if (identical(num_starters_off_mode, "lte")) off_val else NA_integer_,
      num_starters_def_min = if (identical(num_starters_def_mode, "gte")) def_val else NA_integer_,
      num_starters_def_max = if (identical(num_starters_def_mode, "lte")) def_val else NA_integer_
    ))
    df <- q$value
    db_ms <- db_ms + q$ms
    df[["Net RTG Diff"]][is.na(df[["Net RTG Diff"]])] <- 0
    df[["Off ON Diff"]][is.na(df[["Off ON Diff"]])] <- 0
    df[["Def ON Diff"]][is.na(df[["Def ON Diff"]])] <- 0
  }
  transform_t0 <- .ms_now()

  # Add net diffs
  out <- rename_ff(df)
  out$netRtgDiff <- ifelse(is.na(df[["Net RTG Diff"]]), 0, df[["Net RTG Diff"]])
  out$offDiff    <- ifelse(is.na(df[["Off ON Diff"]]),   0, df[["Off ON Diff"]])
  out$defDiff    <- ifelse(is.na(df[["Def ON Diff"]]),   0, df[["Def ON Diff"]])

  # Replace NAs in numeric columns with 0
  for (col in names(out)) {
    if (is.numeric(out[[col]])) out[[col]][is.na(out[[col]])] <- 0
  }

  perf_log(
    req, "/api/onoff/four-factors",
    total_ms = .ms_elapsed(req_t0),
    db_ms = db_ms,
    transform_ms = .ms_elapsed(transform_t0),
    rows = nrow(out)
  )
  cache_set(key, out)
}

# ── Lineup helpers ─────────────────────────────────────────────
run_fetch_lineups <- function(pool, num, team_csv, player_csv, player_off_csv,
                               exact, start_date, end_date, min_poss, game_year,
                               game_type_csv, opp_ids_csv, home_away, outcome,
                               opp_rank_side, opp_rank_n, opp_rank_metric,
                               max_margin, margin_status, max_time_remaining,
                               ot_margin_filter, min_gn, max_gn, last_n,
                               num_starters_off = NA_integer_, num_starters_def = NA_integer_,
                               num_starters_off_min = NA_integer_, num_starters_off_max = NA_integer_,
                               num_starters_def_min = NA_integer_, num_starters_def_max = NA_integer_) {
  DBI::dbGetQuery(pool, paste0(
    "SELECT * FROM ", SCHEMA, ".fetch_lineups_csv_v2(",
    "$1::int4,$2::text,$3::text,$4::text,$5::bool,$6::date,$7::date,$8::int4,$9::int4,",
    "$10::text,$11::text,$12::text,$13::text,$14::text,$15::int4,$16::text,",
    "$17::int4,$18::text,$19::int4,$20::bool,$21::int4,$22::int4,$23::int4,",
    "$24::int4,$25::int4,$26::int4,$27::int4,$28::int4,$29::int4", ")"
  ), params = list(
    as.integer(num), team_csv, player_csv, player_off_csv,
    as.logical(exact), as.Date(start_date), as.Date(end_date),
    as.integer(min_poss), as.integer(game_year),
    game_type_csv, opp_ids_csv, home_away, outcome,
    opp_rank_side, opp_rank_n, opp_rank_metric,
    max_margin, margin_status, max_time_remaining, ot_margin_filter,
    min_gn, max_gn, last_n,
    num_starters_off, num_starters_def,
    num_starters_off_min, num_starters_off_max,
    num_starters_def_min, num_starters_def_max
  ))
}

run_fetch_lineups_ff <- function(pool, num, team_csv, player_csv, player_off_csv,
                                  exact, start_date, end_date, min_poss, game_year,
                                  game_type_csv, opp_ids_csv, home_away, outcome,
                                  opp_rank_side, opp_rank_n, opp_rank_metric,
                                  max_margin, margin_status, max_time_remaining,
                                  ot_margin_filter, min_gn, max_gn, last_n,
                                  num_starters_off = NA_integer_, num_starters_def = NA_integer_,
                                  num_starters_off_min = NA_integer_, num_starters_off_max = NA_integer_,
                                  num_starters_def_min = NA_integer_, num_starters_def_max = NA_integer_) {
  DBI::dbGetQuery(pool, paste0(
    "SELECT * FROM ", SCHEMA, ".fetch_lineups_four_factors_csv(",
    "$1::int4,$2::text,$3::text,$4::text,$5::bool,$6::date,$7::date,$8::int4,$9::int4,",
    "$10::text,$11::text,$12::text,$13::text,$14::text,$15::int4,$16::text,",
    "$17::int4,$18::text,$19::int4,$20::bool,$21::int4,$22::int4,$23::int4,",
    "$24::int4,$25::int4,$26::int4,$27::int4,$28::int4,$29::int4", ")"
  ), params = list(
    as.integer(num), team_csv, player_csv, player_off_csv,
    as.logical(exact), as.Date(start_date), as.Date(end_date),
    as.integer(min_poss), as.integer(game_year),
    game_type_csv, opp_ids_csv, home_away, outcome,
    opp_rank_side, opp_rank_n, opp_rank_metric,
    max_margin, margin_status, max_time_remaining, ot_margin_filter,
    min_gn, max_gn, last_n,
    num_starters_off, num_starters_def,
    num_starters_off_min, num_starters_off_max,
    num_starters_def_min, num_starters_def_max
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
  # Parse player_ids from PG array string to integer vector list column
  # (serializes as JSON array; unlist ensures proper %in% matching in apply_lineup_local_filters)
  if ("playerIds" %in% names(df)) {
    df$playerIds <- lapply(parse_pg_int_array_json(df$playerIds), function(x) as.integer(unlist(x)))
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
  # Parse player_ids from PG array string to integer vector list column
  if ("playerIds" %in% names(df)) {
    df$playerIds <- lapply(parse_pg_int_array_json(df$playerIds), function(x) as.integer(unlist(x)))
  }
  df
}

# ── GET /api/lineups/summary ──────────────────────────────────
#* @get /api/lineups/summary
#* @serializer unboxedJSON
function(req, res,
         game_year = "2026", start_date = "2025-10-01", end_date = "2026-07-01",
         num = "5", game_type = "", opp_ids = "",
         min_poss = "20",
         filter_team_ids = "", players_on = "", players_off = "",
         home_away = "", outcome = "",
         gn_min = "", gn_max = "", last_n = "",
         opp_rank_side = "", opp_rank_n = "", opp_rank_metric = "",
         clutch_margin = "", clutch_status = "", clutch_minutes = "",
         clutch_ot_margin = "false",
         num_starters_off_mode = "", num_starters_off = "",
         num_starters_def_mode = "", num_starters_def = "") {
  # Response cache (full query string key, including local filters)
  resp_key <- cache_key("/api/lineups/summary", req)
  resp_hit <- cache_get(resp_key)
  if (!is.null(resp_hit)) return(resp_hit)

  req_t0 <- .ms_now()
  db_ms <- 0
  gy <- as.integer(game_year)
  bounds <- season_date_bounds(gy)

  # Ranked-data cache (game-level filters only, excludes local filters)
  qs <- req$QUERY_STRING %||% ""
  rk_key <- ranked_key_from_qs("/api/lineups/summary", qs)
  ranked <- ranked_cache_get(rk_key)

  if (is.null(ranked)) {
    # Clutch params
    max_margin <- na_int(clutch_margin)
    margin_status <- na_chr(clutch_status)
    max_time_remaining <- if (nzchar(clutch_minutes)) as.integer(clutch_minutes) * 60L else NA_integer_
    ot_margin_filter <- identical(clutch_ot_margin, "true")
    off_val <- if (nzchar(num_starters_off_mode) && nzchar(num_starters_off)) as.integer(num_starters_off) else NA_integer_
    def_val <- if (nzchar(num_starters_def_mode) && nzchar(num_starters_def)) as.integer(num_starters_def) else NA_integer_

    # Fetch full dataset from DB with min_poss=0 for correct ranking
    q <- .timed(function() run_fetch_lineups(
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
      min_gn = na_int(gn_min), max_gn = na_int(gn_max), last_n = na_int(last_n),
      num_starters_off = NA_integer_, num_starters_def = NA_integer_,
      num_starters_off_min = if (identical(num_starters_off_mode, "gte")) off_val else NA_integer_,
      num_starters_off_max = if (identical(num_starters_off_mode, "lte")) off_val else NA_integer_,
      num_starters_def_min = if (identical(num_starters_def_mode, "gte")) def_val else NA_integer_,
      num_starters_def_max = if (identical(num_starters_def_mode, "lte")) def_val else NA_integer_
    ))
    df <- q$value
    db_ms <- db_ms + q$ms

    if (is.null(df) || nrow(df) == 0) return(list(rows = list(), meta = list(autoMinPoss = 0L)))

    # Replace NAs in shot/numeric columns with 0
    shot_cols <- grep("fg[23]", names(df), value = TRUE)
    for (col in shot_cols) df[[col]][is.na(df[[col]])] <- 0
    for (col in c("off_poss", "off_pts", "def_poss", "def_pts", "minutes")) {
      if (col %in% names(df)) df[[col]][is.na(df[[col]])] <- 0
    }

    # Ensure PPP/net columns exist
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

    # Compute percentile ranks on full population (two-tier ranking)
    thresh <- adaptive_baseline_r(df$total_poss)
    qualify <- df$total_poss >= thresh
    df$pr_net     <- pr_rank(ifelse(qualify, df$net_rtg, NA_real_))
    df$pr_off_ppp <- pr_rank(ifelse(qualify, df$off_ppp, NA_real_))
    pr_def_raw    <- pr_rank(ifelse(qualify, df$def_ppp, NA_real_))
    df$pr_def_ppp_inv <- ifelse(is.na(pr_def_raw), NA_real_, 1 - pr_def_raw)

    df$game_year <- NULL
    df$player_names <- NULL

    out <- rename_lineup_summary(df)
    # Map rank columns to camelCase
    names(out)[names(out) == "pr_net"]         <- "prNet"
    names(out)[names(out) == "pr_off_ppp"]     <- "prOffPpp"
    names(out)[names(out) == "pr_def_ppp_inv"] <- "prDefPppInv"

    for (col in c("offPpp", "defPpp", "netRtg")) {
      if (col %in% names(out)) out[[col]][is.na(out[[col]])] <- 0
    }

    ranked <- out
    ranked_cache_set(rk_key, ranked)
  }

  # Apply local filters on cached ranked data
  team_ids_vec  <- parse_int_csv(filter_team_ids)
  players_on_vec  <- parse_int_csv(players_on)
  players_off_vec <- parse_int_csv(players_off)
  min_p <- as.integer(min_poss)

  result <- apply_lineup_local_filters(ranked, team_ids_vec, players_on_vec, players_off_vec, min_p)

  perf_log(
    req, "/api/lineups/summary",
    total_ms = .ms_elapsed(req_t0),
    db_ms = db_ms,
    transform_ms = .ms_elapsed(req_t0) - db_ms,
    rows = nrow(result$rows)
  )
  resp <- list(rows = result$rows, meta = list(autoMinPoss = result$autoMinPoss))
  cache_set(resp_key, resp)
}

# ── GET /api/lineups/four-factors ─────────────────────────────
#* @get /api/lineups/four-factors
#* @serializer unboxedJSON
function(req, res,
         game_year = "2026", start_date = "2025-10-01", end_date = "2026-07-01",
         num = "5", game_type = "", opp_ids = "",
         min_poss = "20",
         filter_team_ids = "", players_on = "", players_off = "",
         home_away = "", outcome = "",
         gn_min = "", gn_max = "", last_n = "",
         opp_rank_side = "", opp_rank_n = "", opp_rank_metric = "",
         clutch_margin = "", clutch_status = "", clutch_minutes = "",
         clutch_ot_margin = "false",
         num_starters_off_mode = "", num_starters_off = "",
         num_starters_def_mode = "", num_starters_def = "") {
  # Response cache (full query string key, including local filters)
  resp_key <- cache_key("/api/lineups/four-factors", req)
  resp_hit <- cache_get(resp_key)
  if (!is.null(resp_hit)) return(resp_hit)

  req_t0 <- .ms_now()
  db_ms <- 0
  gy <- as.integer(game_year)
  bounds <- season_date_bounds(gy)

  # Ranked-data cache (game-level filters only)
  qs <- req$QUERY_STRING %||% ""
  rk_key <- ranked_key_from_qs("/api/lineups/four-factors", qs)
  ranked <- ranked_cache_get(rk_key)

  if (is.null(ranked)) {
    max_margin <- na_int(clutch_margin)
    margin_status <- na_chr(clutch_status)
    max_time_remaining <- if (nzchar(clutch_minutes)) as.integer(clutch_minutes) * 60L else NA_integer_
    ot_margin_filter <- identical(clutch_ot_margin, "true")
    off_val <- if (nzchar(num_starters_off_mode) && nzchar(num_starters_off)) as.integer(num_starters_off) else NA_integer_
    def_val <- if (nzchar(num_starters_def_mode) && nzchar(num_starters_def)) as.integer(num_starters_def) else NA_integer_

    # Fetch full dataset from DB with min_poss=0 for correct ranking
    q <- .timed(function() run_fetch_lineups_ff(
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
      min_gn = na_int(gn_min), max_gn = na_int(gn_max), last_n = na_int(last_n),
      num_starters_off = NA_integer_, num_starters_def = NA_integer_,
      num_starters_off_min = if (identical(num_starters_off_mode, "gte")) off_val else NA_integer_,
      num_starters_off_max = if (identical(num_starters_off_mode, "lte")) off_val else NA_integer_,
      num_starters_def_min = if (identical(num_starters_def_mode, "gte")) def_val else NA_integer_,
      num_starters_def_max = if (identical(num_starters_def_mode, "lte")) def_val else NA_integer_
    ))
    df <- q$value
    db_ms <- db_ms + q$ms

    if (is.null(df) || nrow(df) == 0) return(list(rows = list(), meta = list(autoMinPoss = 0L)))

    for (col in names(df)) {
      if (is.numeric(df[[col]])) df[[col]][is.na(df[[col]])] <- 0
    }

    df$total_poss <- df$off_poss + df$def_poss

    # Compute 11 percentile ranks on full population (two-tier ranking)
    thresh <- adaptive_baseline_r(df$total_poss)
    qualify <- df$total_poss >= thresh
    qval <- function(v) ifelse(qualify, v, NA_real_)

    df$pr_net      <- pr_rank(qval(df$net_rtg))
    df$pr_off_ppp  <- pr_rank(qval(df$off_ppp))
    df$pr_off_ts   <- pr_rank(qval(df$off_ts))
    df$pr_off_oreb <- pr_rank(qval(df$off_oreb))
    pr_off_tov_raw <- pr_rank(qval(df$off_tov))
    df$pr_off_tov  <- ifelse(is.na(pr_off_tov_raw), NA_real_, 1 - pr_off_tov_raw)  # inverted
    df$pr_off_ftr  <- pr_rank(qval(df$off_ftr))
    pr_def_ppp_raw <- pr_rank(qval(df$def_ppp))
    df$pr_def_ppp  <- ifelse(is.na(pr_def_ppp_raw), NA_real_, 1 - pr_def_ppp_raw)  # inverted
    pr_def_ts_raw  <- pr_rank(qval(df$def_ts))
    df$pr_def_ts   <- ifelse(is.na(pr_def_ts_raw), NA_real_, 1 - pr_def_ts_raw)    # inverted
    pr_def_oreb_raw <- pr_rank(qval(df$def_oreb))
    df$pr_def_oreb <- ifelse(is.na(pr_def_oreb_raw), NA_real_, 1 - pr_def_oreb_raw) # inverted
    df$pr_def_tov  <- pr_rank(qval(df$def_tov))  # NOT inverted (opponent TOV = good)
    pr_def_ftr_raw <- pr_rank(qval(df$def_ftr))
    df$pr_def_ftr  <- ifelse(is.na(pr_def_ftr_raw), NA_real_, 1 - pr_def_ftr_raw)  # inverted

    df$game_year <- NULL
    df$player_names <- NULL

    out <- rename_lineup_ff(df)
    # Map rank columns to camelCase
    rank_map <- c(
      "pr_net" = "prNet", "pr_off_ppp" = "prOffPpp",
      "pr_off_ts" = "prOffTs", "pr_off_oreb" = "prOffOreb",
      "pr_off_tov" = "prOffTov", "pr_off_ftr" = "prOffFtr",
      "pr_def_ppp" = "prDefPpp", "pr_def_ts" = "prDefTs",
      "pr_def_oreb" = "prDefOreb", "pr_def_tov" = "prDefTov",
      "pr_def_ftr" = "prDefFtr"
    )
    for (old in names(rank_map)) {
      if (old %in% names(out)) names(out)[names(out) == old] <- rank_map[[old]]
    }

    ranked <- out
    ranked_cache_set(rk_key, ranked)
  }

  # Apply local filters on cached ranked data
  team_ids_vec  <- parse_int_csv(filter_team_ids)
  players_on_vec  <- parse_int_csv(players_on)
  players_off_vec <- parse_int_csv(players_off)
  min_p <- as.integer(min_poss)

  result <- apply_lineup_local_filters(ranked, team_ids_vec, players_on_vec, players_off_vec, min_p)

  perf_log(
    req, "/api/lineups/four-factors",
    total_ms = .ms_elapsed(req_t0),
    db_ms = db_ms,
    transform_ms = .ms_elapsed(req_t0) - db_ms,
    rows = nrow(result$rows)
  )
  resp <- list(rows = result$rows, meta = list(autoMinPoss = result$autoMinPoss))
  cache_set(resp_key, resp)
}

# ── GET /api/lineups/game-log ─────────────────────────────────
#* @get /api/lineups/game-log
#* @serializer unboxedJSON
function(req, res,
         sub_hash = "", team_id = "", game_year = "2026", view_mode = "summary") {

  req_t0 <- .ms_now()
  db_ms <- 0
  req_hash <- as.character(sub_hash)
  req_tid  <- as.integer(team_id)
  gy       <- as.integer(game_year)

  if (!nzchar(req_hash) || is.na(req_tid)) {
    res$status <- 400
    return(list(error = "sub_hash and team_id are required"))
  }

  # Resolve sub_lineup_hash → lineup_hash(es)
  q_hash <- .timed(function() DBI::dbGetQuery(pg_pool,
    sprintf("SELECT DISTINCT lineup_hash FROM %s.sub_lineups WHERE sub_lineup_hash = $1 AND team_id = $2 AND game_year = $3", SCHEMA),
    params = list(req_hash, req_tid, gy)))
  db_ms <- db_ms + q_hash$ms
  lineup_hashes <- q_hash$value$lineup_hash

  if (length(lineup_hashes) == 0) lineup_hashes <- req_hash
  lineup_hashes <- unique(as.character(lineup_hashes))
  lineup_hashes <- lineup_hashes[!is.na(lineup_hashes) & nzchar(lineup_hashes)]
  if (length(lineup_hashes) == 0) return(list(lineupName = req_hash, games = list()))

  hash_placeholders <- paste(sprintf("$%d", seq_along(lineup_hashes)), collapse = ",")
  tid_idx <- length(lineup_hashes) + 1
  gy_idx  <- length(lineup_hashes) + 2
  qparams <- c(as.list(lineup_hashes), list(req_tid, gy))

  # Get schedule
  q_sched <- .timed(function() DBI::dbGetQuery(pg_pool, sprintf(
    "SELECT game_id, gn, game_date, opp_team_name, team_score, opp_score,
            team_score > opp_score AS has_won
     FROM %s.final_schedule_mv WHERE team_id = $1 AND game_year = $2", SCHEMA
  ), params = list(req_tid, gy)))
  db_ms <- db_ms + q_sched$ms
  sched <- q_sched$value
  sched$result <- ifelse(sched$has_won, "W", "L")
  sched$score <- paste0(sched$team_score, "-", sched$opp_score)

  # Get lineup name
  q_name <- .timed(function() DBI::dbGetQuery(pg_pool, sprintf(
    "SELECT player_names_str FROM %s.sub_lineups_stats WHERE sub_lineup_hash = $1 AND team_id = $2 AND game_year = $3 LIMIT 1", SCHEMA
  ), params = list(req_hash, req_tid, gy)))
  db_ms <- db_ms + q_name$ms
  lineup_name <- q_name$value$player_names_str
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
    q_ff <- .timed(function() DBI::dbGetQuery(pg_pool, ff_query, params = qparams))
    db_ms <- db_ms + q_ff$ms
    ff_data <- q_ff$value
    if (nrow(ff_data) == 0) return(list(lineupName = lineup_name, games = list()))
    transform_t0 <- .ms_now()

    off <- ff_data[ff_data$type_lineup == "offense", ]
    def <- ff_data[ff_data$type_lineup == "defense", ]
    combined <- merge(off, def, by = "game_id", all = TRUE, suffixes = c("_off", "_def"))
    combined <- merge(
      combined,
      sched[, c("game_id", "gn", "game_date", "opp_team_name", "result", "score"), drop = FALSE],
      by = "game_id",
      all.x = TRUE
    )

    off_poss <- ifelse(is.na(combined$total_poss_off), 0, combined$total_poss_off)
    def_poss <- ifelse(is.na(combined$total_poss_def), 0, combined$total_poss_def)
    off_pts <- ifelse(is.na(combined$total_points_off), 0, combined$total_points_off)
    def_pts <- ifelse(is.na(combined$total_points_def), 0, combined$total_points_def)
    off_ppp <- ifelse(off_poss > 0, round(off_pts / off_poss * 100, 1), NA_real_)
    def_ppp <- ifelse(def_poss > 0, round(def_pts / def_poss * 100, 1), NA_real_)

    games <- data.frame(
      gn = combined$gn,
      gameDate = ifelse(is.na(combined$game_date), "", as.character(combined$game_date)),
      opponent = ifelse(is.na(combined$opp_team_name), "", combined$opp_team_name),
      result = ifelse(is.na(combined$result), "", combined$result),
      score = ifelse(is.na(combined$score), "", combined$score),
      offPpp = off_ppp,
      defPpp = def_ppp,
      netRtg = round(ifelse(is.na(off_ppp), 0, off_ppp) - ifelse(is.na(def_ppp), 0, def_ppp), 1),
      offPoss = off_poss,
      defPoss = def_poss,
      minutes = round(ifelse(is.na(combined$mins_off), 0, combined$mins_off), 1),
      offTs = ifelse(ifelse(is.na(combined$ts_poss_count_off), 0, combined$ts_poss_count_off) > 0,
                     round(off_pts / (2 * combined$ts_poss_count_off) * 100, 1), NA_real_),
      offOreb = ifelse(ifelse(is.na(combined$oreb_opportunities_off), 0, combined$oreb_opportunities_off) > 0,
                       round(ifelse(is.na(combined$oreb_count_off), 0, combined$oreb_count_off) / combined$oreb_opportunities_off * 100, 1), NA_real_),
      offTov = ifelse(off_poss > 0,
                      round(ifelse(is.na(combined$tov_count_off), 0, combined$tov_count_off) / off_poss * 100, 1), NA_real_),
      offFtr = ifelse(ifelse(is.na(combined$total_fga_off), 0, combined$total_fga_off) > 0,
                      round(ifelse(is.na(combined$total_ft_attempts_off), 0, combined$total_ft_attempts_off) / combined$total_fga_off * 100, 1), NA_real_),
      defTs = ifelse(ifelse(is.na(combined$ts_poss_count_def), 0, combined$ts_poss_count_def) > 0,
                     round(def_pts / (2 * combined$ts_poss_count_def) * 100, 1), NA_real_),
      defOreb = ifelse(ifelse(is.na(combined$oreb_opportunities_def), 0, combined$oreb_opportunities_def) > 0,
                       round(ifelse(is.na(combined$oreb_count_def), 0, combined$oreb_count_def) / combined$oreb_opportunities_def * 100, 1), NA_real_),
      defTov = ifelse(def_poss > 0,
                      round(ifelse(is.na(combined$tov_count_def), 0, combined$tov_count_def) / def_poss * 100, 1), NA_real_),
      defFtr = ifelse(ifelse(is.na(combined$total_fga_def), 0, combined$total_fga_def) > 0,
                      round(ifelse(is.na(combined$total_ft_attempts_def), 0, combined$total_ft_attempts_def) / combined$total_fga_def * 100, 1), NA_real_)
    )
    games <- games[order(ifelse(is.na(games$gn), 999, games$gn)), , drop = FALSE]

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
    q_game <- .timed(function() DBI::dbGetQuery(pg_pool, game_query, params = qparams))
    db_ms <- db_ms + q_game$ms
    game_data <- q_game$value
    if (nrow(game_data) == 0) return(list(lineupName = lineup_name, games = list()))
    transform_t0 <- .ms_now()

    off <- game_data[game_data$type_lineup == "offense", ]
    def <- game_data[game_data$type_lineup == "defense", ]
    combined <- merge(off, def, by = "game_id", all = TRUE, suffixes = c("_off", "_def"))
    combined <- merge(
      combined,
      sched[, c("game_id", "gn", "game_date", "opp_team_name", "result", "score"), drop = FALSE],
      by = "game_id",
      all.x = TRUE
    )

    off_poss <- ifelse(is.na(combined$poss_off), 0, combined$poss_off)
    def_poss <- ifelse(is.na(combined$poss_def), 0, combined$poss_def)
    off_pts <- ifelse(is.na(combined$pts_off), 0, combined$pts_off)
    def_pts <- ifelse(is.na(combined$pts_def), 0, combined$pts_def)
    off_ppp <- ifelse(off_poss > 0, round(off_pts / off_poss * 100, 1), NA_real_)
    def_ppp <- ifelse(def_poss > 0, round(def_pts / def_poss * 100, 1), NA_real_)

    games <- data.frame(
      gn = combined$gn,
      gameDate = ifelse(is.na(combined$game_date), "", as.character(combined$game_date)),
      opponent = ifelse(is.na(combined$opp_team_name), "", combined$opp_team_name),
      result = ifelse(is.na(combined$result), "", combined$result),
      score = ifelse(is.na(combined$score), "", combined$score),
      offPpp = off_ppp,
      defPpp = def_ppp,
      netRtg = round(ifelse(is.na(off_ppp), 0, off_ppp) - ifelse(is.na(def_ppp), 0, def_ppp), 1),
      offPoss = off_poss,
      defPoss = def_poss,
      minutes = round(ifelse(is.na(combined$mins_off), 0, combined$mins_off), 1),
      offFg2Made = ifelse(is.na(combined$fg2m_off), 0, combined$fg2m_off),
      offFg2Att = ifelse(is.na(combined$fg2a_off), 0, combined$fg2a_off),
      offFg3Made = ifelse(is.na(combined$fg3m_off), 0, combined$fg3m_off),
      offFg3Att = ifelse(is.na(combined$fg3a_off), 0, combined$fg3a_off),
      defFg2Made = ifelse(is.na(combined$fg2m_def), 0, combined$fg2m_def),
      defFg2Att = ifelse(is.na(combined$fg2a_def), 0, combined$fg2a_def),
      defFg3Made = ifelse(is.na(combined$fg3m_def), 0, combined$fg3m_def),
      defFg3Att = ifelse(is.na(combined$fg3a_def), 0, combined$fg3a_def)
    )
    games <- games[order(ifelse(is.na(games$gn), 999, games$gn)), , drop = FALSE]
  }

  out <- list(lineupName = lineup_name, games = games)
  perf_log(
    req, "/api/lineups/game-log",
    total_ms = .ms_elapsed(req_t0),
    db_ms = db_ms,
    transform_ms = .ms_elapsed(transform_t0),
    rows = nrow(games)
  )
  out
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

# ══════════════════════════════════════════════════════════════
# Tab 3: Team Ratings
# ══════════════════════════════════════════════════════════════

run_team_ratings_dynamic <- function(pool, game_year, start_d, end_d,
                                      game_type_csv, opp_ids_csv, home_away, outcome,
                                      opp_rank_side, opp_rank_n, opp_rank_metric,
                                      max_margin, margin_status, max_time_remaining, ot_margin_filter,
                                      min_gn = NA_integer_, max_gn = NA_integer_, last_n = NA_integer_,
                                      num_starters_off = NA_integer_, num_starters_def = NA_integer_,
                                      num_starters_off_min = NA_integer_, num_starters_off_max = NA_integer_,
                                      num_starters_def_min = NA_integer_, num_starters_def_max = NA_integer_) {
  DBI::dbGetQuery(pool, paste0(
    "SELECT * FROM ", SCHEMA, ".get_team_ratings_dynamic(",
    "$1::int4,$2::date,$3::date,$4::text,$5::text,$6::text,$7::text,",
    "$8::text,$9::int4,$10::text,$11::int4,$12::text,$13::int4,$14::bool,",
    "$15::int4,$16::int4,$17::int4,$18::int4,$19::int4,$20::int4,$21::int4,$22::int4,$23::int4", ")"
  ), params = list(
    as.integer(game_year),
    if (!is.na(start_d)) as.Date(start_d) else NA,
    if (!is.na(end_d)) as.Date(end_d) else NA,
    game_type_csv, opp_ids_csv, home_away, outcome,
    opp_rank_side, opp_rank_n, opp_rank_metric,
    max_margin, margin_status, max_time_remaining, ot_margin_filter,
    min_gn, max_gn, last_n,
    num_starters_off, num_starters_def,
    num_starters_off_min, num_starters_off_max,
    num_starters_def_min, num_starters_def_max
  ))
}

run_team_ff_dynamic <- function(pool, game_year, start_d, end_d,
                                 game_type_csv, opp_ids_csv, home_away, outcome,
                                 opp_rank_side, opp_rank_n, opp_rank_metric,
                                 max_margin, margin_status, max_time_remaining, ot_margin_filter,
                                 min_gn = NA_integer_, max_gn = NA_integer_, last_n = NA_integer_,
                                 num_starters_off = NA_integer_, num_starters_def = NA_integer_,
                                 num_starters_off_min = NA_integer_, num_starters_off_max = NA_integer_,
                                 num_starters_def_min = NA_integer_, num_starters_def_max = NA_integer_) {
  DBI::dbGetQuery(pool, paste0(
    "SELECT * FROM ", SCHEMA, ".get_team_four_factors_dynamic(",
    "$1::int4,$2::date,$3::date,$4::text,$5::text,$6::text,$7::text,",
    "$8::text,$9::int4,$10::text,$11::int4,$12::text,$13::int4,$14::bool,",
    "$15::int4,$16::int4,$17::int4,$18::int4,$19::int4,$20::int4,$21::int4,$22::int4,$23::int4", ")"
  ), params = list(
    as.integer(game_year),
    if (!is.na(start_d)) as.Date(start_d) else NA,
    if (!is.na(end_d)) as.Date(end_d) else NA,
    game_type_csv, opp_ids_csv, home_away, outcome,
    opp_rank_side, opp_rank_n, opp_rank_metric,
    max_margin, margin_status, max_time_remaining, ot_margin_filter,
    min_gn, max_gn, last_n,
    num_starters_off, num_starters_def,
    num_starters_off_min, num_starters_off_max,
    num_starters_def_min, num_starters_def_max
  ))
}

# Pace computation: fetch game minutes per team (mirrors server_tab3.R)
fetch_team_game_minutes <- function(pool, game_year, start_date, end_date,
                                     game_type_csv, opp_ids_csv, home_away, outcome,
                                     opp_rank_side, opp_rank_n, opp_rank_metric,
                                     min_gn, max_gn, last_n,
                                     clutch_active = FALSE,
                                     max_margin = NA_integer_, margin_status = NA_character_,
                                     max_time_remaining = NA_integer_, ot_margin_filter = FALSE,
                                     num_starters_off_min = NA_integer_, num_starters_off_max = NA_integer_,
                                     num_starters_def_min = NA_integer_, num_starters_def_max = NA_integer_) {
  # When clutch/starters are active, compute floor time from filtered segments
  poss_scope_active <- clutch_active ||
    any(!is.na(c(num_starters_off_min, num_starters_off_max, num_starters_def_min, num_starters_def_max)))

  if (!poss_scope_active) {
    DBI::dbGetQuery(pool,
      paste0("WITH params AS (
         SELECT
           CASE WHEN $4::text IS NULL OR btrim($4::text) = '' THEN NULL::int4[]
                ELSE string_to_array(regexp_replace($4::text, '\\s+', '', 'g'), ',')::int4[] END AS game_types,
           CASE WHEN $5::text IS NULL OR btrim($5::text) = '' THEN NULL::int4[]
                ELSE string_to_array(regexp_replace($5::text, '\\s+', '', 'g'), ',')::int4[] END AS opp_ids
       ),
       sched_base AS (
         SELECT
           fs.game_id, fs.team_id, fs.game_year, fs.opp_team_id, fs.game_date, fs.gn,
           fs.is_home, fs.has_won,
           ROW_NUMBER() OVER (
             PARTITION BY fs.team_id, fs.game_year
             ORDER BY fs.game_date DESC NULLS LAST, fs.game_id DESC
           ) AS rn_recent
         FROM ", SCHEMA, ".final_schedule_mv fs
         CROSS JOIN params p0
         WHERE fs.game_year = $1::int4
           AND ($2::date IS NULL OR fs.game_date >= $2::date)
           AND ($3::date IS NULL OR fs.game_date <= $3::date)
           AND (p0.game_types IS NULL OR fs.game_type = ANY(p0.game_types))
           AND (p0.opp_ids IS NULL OR fs.opp_team_id = ANY(p0.opp_ids))
           AND ($6::text IS NULL OR $6::text = '' OR ($6::text = 'home' AND fs.is_home) OR ($6::text = 'away' AND NOT fs.is_home))
           AND ($7::text IS NULL OR $7::text = '' OR ($7::text = 'win' AND fs.has_won IS TRUE) OR ($7::text = 'loss' AND fs.has_won IS FALSE))
           AND ($11::int4 IS NULL OR fs.gn >= $11::int4)
           AND ($12::int4 IS NULL OR fs.gn <= $12::int4)
       ),
       sched_last_n AS (
         SELECT * FROM sched_base
         WHERE ($13::int4 IS NULL OR rn_recent <= $13::int4)
       ),
       sched_ranked AS (
         SELECT sb.*,
           CASE WHEN $8::text IN ('top','bottom') THEN
             CASE COALESCE($10::text, 'net')
               WHEN 'off' THEN r.rank_off_ppp WHEN 'def' THEN r.rank_def_ppp ELSE r.rank_net_rtg END
           ELSE NULL END AS opp_rank,
           CASE WHEN $8::text = 'bottom' THEN
             MAX(CASE COALESCE($10::text, 'net')
               WHEN 'off' THEN r.rank_off_ppp WHEN 'def' THEN r.rank_def_ppp ELSE r.rank_net_rtg END
             ) OVER (PARTITION BY sb.game_year)
           ELSE NULL END AS max_rank
         FROM sched_last_n sb
         LEFT JOIN ", SCHEMA, ".team_ppp_ratings_mv r
           ON r.game_year::int4 = sb.game_year AND r.team_id::int4 = sb.opp_team_id
           AND $8::text IN ('top','bottom')
       ),
       sched_filtered AS (
         SELECT game_id, team_id FROM sched_ranked
         WHERE $8::text IS NULL OR $8::text = '' OR $9::int4 IS NULL
            OR ($8::text = 'top' AND opp_rank <= $9::int4)
            OR ($8::text = 'bottom' AND opp_rank >= (max_rank - $9::int4 + 1))
       ),
       game_quarters AS (
         SELECT sf.team_id, sf.game_id,
           GREATEST(MAX(COALESCE(d.quarter, 4)), 4) AS max_q
         FROM sched_filtered sf
         JOIN ", SCHEMA, ".df_pts_poss_lineups_longer_mv d
           ON d.game_id = sf.game_id AND d.team_id = sf.team_id
         GROUP BY sf.team_id, sf.game_id
       )
       SELECT team_id, SUM(40 + 5 * GREATEST(max_q - 4, 0))::numeric AS game_minutes
       FROM game_quarters GROUP BY team_id"),
      params = list(
        as.integer(game_year),
        if (!is.na(start_date) && nzchar(start_date)) as.Date(start_date) else NA,
        if (!is.na(end_date) && nzchar(end_date)) as.Date(end_date) else NA,
        game_type_csv, opp_ids_csv, home_away, outcome,
        opp_rank_side, opp_rank_n, opp_rank_metric,
        min_gn, max_gn, last_n
      )
    )
  } else {
    # Clutch/starters active: compute floor time from filtered segments
    DBI::dbGetQuery(pool,
      paste0("WITH params AS (
         SELECT
           CASE WHEN $4::text IS NULL OR btrim($4::text) = '' THEN NULL::int4[]
                ELSE string_to_array(regexp_replace($4::text, '\\s+', '', 'g'), ',')::int4[] END AS game_types,
           CASE WHEN $5::text IS NULL OR btrim($5::text) = '' THEN NULL::int4[]
                ELSE string_to_array(regexp_replace($5::text, '\\s+', '', 'g'), ',')::int4[] END AS opp_ids
       ),
       sched_base AS (
         SELECT
           fs.game_id, fs.team_id, fs.game_year, fs.opp_team_id, fs.game_date, fs.gn,
           fs.is_home, fs.has_won,
           ROW_NUMBER() OVER (
             PARTITION BY fs.team_id, fs.game_year
             ORDER BY fs.game_date DESC NULLS LAST, fs.game_id DESC
           ) AS rn_recent
         FROM ", SCHEMA, ".final_schedule_mv fs
         CROSS JOIN params p0
         WHERE fs.game_year = $1::int4
           AND ($2::date IS NULL OR fs.game_date >= $2::date)
           AND ($3::date IS NULL OR fs.game_date <= $3::date)
           AND (p0.game_types IS NULL OR fs.game_type = ANY(p0.game_types))
           AND (p0.opp_ids IS NULL OR fs.opp_team_id = ANY(p0.opp_ids))
           AND ($6::text IS NULL OR $6::text = '' OR ($6::text = 'home' AND fs.is_home) OR ($6::text = 'away' AND NOT fs.is_home))
           AND ($7::text IS NULL OR $7::text = '' OR ($7::text = 'win' AND fs.has_won IS TRUE) OR ($7::text = 'loss' AND fs.has_won IS FALSE))
           AND ($11::int4 IS NULL OR fs.gn >= $11::int4)
           AND ($12::int4 IS NULL OR fs.gn <= $12::int4)
       ),
       sched_last_n AS (
         SELECT * FROM sched_base WHERE ($13::int4 IS NULL OR rn_recent <= $13::int4)
       ),
       sched_ranked AS (
         SELECT sb.*,
           CASE WHEN $8::text IN ('top','bottom') THEN
             CASE COALESCE($10::text, 'net')
               WHEN 'off' THEN r.rank_off_ppp WHEN 'def' THEN r.rank_def_ppp ELSE r.rank_net_rtg END
           ELSE NULL END AS opp_rank,
           CASE WHEN $8::text = 'bottom' THEN
             MAX(CASE COALESCE($10::text, 'net')
               WHEN 'off' THEN r.rank_off_ppp WHEN 'def' THEN r.rank_def_ppp ELSE r.rank_net_rtg END
             ) OVER (PARTITION BY sb.game_year)
           ELSE NULL END AS max_rank
         FROM sched_last_n sb
         LEFT JOIN ", SCHEMA, ".team_ppp_ratings_mv r
           ON r.game_year::int4 = sb.game_year AND r.team_id::int4 = sb.opp_team_id
           AND $8::text IN ('top','bottom')
       ),
       sched_filtered AS (
         SELECT game_id, team_id FROM sched_ranked
         WHERE $8::text IS NULL OR $8::text = '' OR $9::int4 IS NULL
            OR ($8::text = 'top' AND opp_rank <= $9::int4)
            OR ($8::text = 'bottom' AND opp_rank >= (max_rank - $9::int4 + 1))
       ),
       filtered_rows AS (
         SELECT d.team_id, d.game_id, d.lineup_hash, d.segment_id, d.end_game_seconds_remaining
         FROM ", SCHEMA, ".df_pts_poss_lineups_longer_mv d
         JOIN sched_filtered sf ON sf.game_id = d.game_id AND sf.team_id = d.team_id
         WHERE (COALESCE($14::int4, NULL) IS NULL
                OR ABS(CASE WHEN d.type_lineup = 'offense'
                            THEN (d.own_team_score - COALESCE(d.team_score, 0)) - d.opp_team_score
                            ELSE d.own_team_score - (d.opp_team_score - COALESCE(d.team_score, 0))
                       END) <= $14::int4
                OR (d.quarter > 4 AND NOT COALESCE($17::bool, FALSE)))
           AND ($15::text IS NULL OR $15::text = '' OR $15::text = 'all'
                OR ($15::text = 'leading'  AND CASE WHEN d.type_lineup = 'offense'
                     THEN (d.own_team_score - COALESCE(d.team_score, 0)) > d.opp_team_score
                     ELSE d.own_team_score > (d.opp_team_score - COALESCE(d.team_score, 0)) END)
                OR ($15::text = 'trailing' AND CASE WHEN d.type_lineup = 'offense'
                     THEN (d.own_team_score - COALESCE(d.team_score, 0)) < d.opp_team_score
                     ELSE d.own_team_score < (d.opp_team_score - COALESCE(d.team_score, 0)) END)
                OR ($15::text = 'tied' AND CASE WHEN d.type_lineup = 'offense'
                     THEN (d.own_team_score - COALESCE(d.team_score, 0)) = d.opp_team_score
                     ELSE d.own_team_score = (d.opp_team_score - COALESCE(d.team_score, 0)) END)
                OR (d.quarter > 4 AND NOT COALESCE($17::bool, FALSE)))
           AND ($16::int4 IS NULL OR d.end_game_seconds_remaining <= $16::int4 OR d.quarter > 4)
           AND (COALESCE($18::int4, NULL) IS NULL OR d.own_starters >= $18::int4)
           AND (COALESCE($19::int4, NULL) IS NULL OR d.own_starters <= $19::int4)
           AND (COALESCE($20::int4, NULL) IS NULL OR d.opp_starters >= $20::int4)
           AND (COALESCE($21::int4, NULL) IS NULL OR d.opp_starters <= $21::int4)
           AND d.lineup_hash IS NOT NULL AND d.segment_id IS NOT NULL AND d.end_game_seconds_remaining IS NOT NULL
       ),
       filtered_segments AS (
         SELECT team_id, game_id, lineup_hash, segment_id,
           GREATEST(MAX(end_game_seconds_remaining) - MIN(end_game_seconds_remaining), 0)::numeric AS seg_seconds
         FROM filtered_rows GROUP BY team_id, game_id, lineup_hash, segment_id
       )
       SELECT team_id, ROUND(SUM(seg_seconds) / 60.0, 3)::numeric AS game_minutes
       FROM filtered_segments GROUP BY team_id"),
      params = list(
        as.integer(game_year),
        if (!is.na(start_date) && nzchar(start_date)) as.Date(start_date) else NA,
        if (!is.na(end_date) && nzchar(end_date)) as.Date(end_date) else NA,
        game_type_csv, opp_ids_csv, home_away, outcome,
        opp_rank_side, opp_rank_n, opp_rank_metric,
        min_gn, max_gn, last_n,
        max_margin, margin_status, max_time_remaining, ot_margin_filter,
        num_starters_off_min, num_starters_off_max,
        num_starters_def_min, num_starters_def_max
      )
    )
  }
}

# Add pace columns given poss + game minutes
add_team_pace_cols <- function(df, minutes_map = NULL) {
  if (is.null(df) || !nrow(df)) return(df)
  gp_col <- if ("games_played" %in% names(df)) "games_played" else if ("gp" %in% names(df)) "gp" else NA_character_
  if (is.na(gp_col)) { df$off_pace <- NA_real_; df$def_pace <- NA_real_; return(df) }
  gp <- suppressWarnings(as.numeric(df[[gp_col]]))
  gp[!is.finite(gp) | gp <= 0] <- NA_real_
  off_poss <- if ("off_poss" %in% names(df)) suppressWarnings(as.numeric(df$off_poss)) else rep(NA_real_, nrow(df))
  def_poss <- if ("def_poss" %in% names(df)) suppressWarnings(as.numeric(df$def_poss)) else rep(NA_real_, nrow(df))
  minutes_vec <- rep(NA_real_, nrow(df))
  if (!is.null(minutes_map) && "team_id" %in% names(df)) {
    mins <- suppressWarnings(as.numeric(minutes_map[as.character(df$team_id)]))
    mins[!is.finite(mins) | mins <= 0] <- NA_real_
    minutes_vec <- mins
  }
  miss <- is.na(minutes_vec)
  if (any(miss)) minutes_vec[miss] <- gp[miss] * 40
  df$off_pace <- ifelse(is.na(minutes_vec), NA_real_, (off_poss / minutes_vec) * 40)
  df$def_pace <- ifelse(is.na(minutes_vec), NA_real_, (def_poss / minutes_vec) * 40)
  df
}

rename_team_summary <- function(df) {
  if (nrow(df) == 0) return(df)
  nms <- c(
    "team_name" = "teamName", "off_ppp" = "offPpp", "def_ppp" = "defPpp",
    "net_rtg" = "netRtg", "games_played" = "gamesPlayed", "wins" = "wins",
    "losses" = "losses", "off_poss" = "offPoss", "def_poss" = "defPoss",
    "rank_net_rtg" = "rankNet", "rank_off_ppp" = "rankOff", "rank_def_ppp" = "rankDef",
    "off_pace" = "offPace", "def_pace" = "defPace", "team_id" = "teamId"
  )
  for (old in names(nms)) {
    if (old %in% names(df)) names(df)[names(df) == old] <- nms[[old]]
  }
  df[, names(df) %in% unname(nms), drop = FALSE]
}

rename_team_ff <- function(df) {
  if (nrow(df) == 0) return(df)
  nms <- c(
    "team_name" = "teamName", "team_id" = "teamId",
    "off_ppp" = "offPpp", "def_ppp" = "defPpp", "net_rtg" = "netRtg",
    "off_ts" = "offTs", "off_oreb" = "offOreb", "off_tov" = "offTov", "off_ftr" = "offFtr",
    "def_ts" = "defTs", "def_oreb" = "defOreb", "def_tov" = "defTov", "def_ftr" = "defFtr",
    "off_pace" = "offPace", "def_pace" = "defPace",
    "off_poss" = "offPoss", "def_poss" = "defPoss",
    "pr_off_ppp" = "prOffPpp", "pr_off_ts" = "prOffTs", "pr_off_oreb" = "prOffOreb",
    "pr_off_tov" = "prOffTov", "pr_off_ftr" = "prOffFtr",
    "pr_def_ppp" = "prDefPpp", "pr_def_ts" = "prDefTs", "pr_def_oreb" = "prDefOreb",
    "pr_def_tov" = "prDefTov", "pr_def_ftr" = "prDefFtr", "pr_net" = "prNet"
  )
  for (old in names(nms)) {
    if (old %in% names(df)) names(df)[names(df) == old] <- nms[[old]]
  }
  df[, names(df) %in% unname(nms), drop = FALSE]
}

# Helper: check if clutch params are active
has_clutch <- function(clutch_margin, clutch_status, clutch_minutes) {
  nzchar(clutch_margin) || (nzchar(clutch_status) && clutch_status != "all") || nzchar(clutch_minutes)
}

# ── GET /api/teams/summary ──────────────────────────────────
#* @get /api/teams/summary
#* @serializer json
function(req, res,
         game_year = "2026", start_date = "2025-10-01", end_date = "2026-07-01",
         game_type = "", opp_ids = "",
         home_away = "", outcome = "",
         gn_min = "", gn_max = "", last_n = "",
         opp_rank_side = "", opp_rank_n = "", opp_rank_metric = "",
         num_starters_off_mode = "", num_starters_off = "",
         num_starters_def_mode = "", num_starters_def = "",
         clutch_margin = "", clutch_status = "", clutch_minutes = "",
         clutch_ot_margin = "false") {
  key <- cache_key("/api/teams/summary", req)
  hit <- cache_get(key)
  if (!is.null(hit)) return(hit)

  req_t0 <- .ms_now()
  db_ms <- 0
  gy <- as.integer(game_year)

  off_val <- if (nzchar(num_starters_off_mode) && nzchar(num_starters_off)) as.integer(num_starters_off) else NA_integer_
  def_val <- if (nzchar(num_starters_def_mode) && nzchar(num_starters_def)) as.integer(num_starters_def) else NA_integer_
  ns_off_min <- if (identical(num_starters_off_mode, "gte")) off_val else NA_integer_
  ns_off_max <- if (identical(num_starters_off_mode, "lte")) off_val else NA_integer_
  ns_def_min <- if (identical(num_starters_def_mode, "gte")) def_val else NA_integer_
  ns_def_max <- if (identical(num_starters_def_mode, "lte")) def_val else NA_integer_

  clutch_active <- has_clutch(clutch_margin, clutch_status, clutch_minutes)
  cm <- if (clutch_active) na_int(clutch_margin) else NA_integer_
  cs <- if (clutch_active) na_chr(clutch_status) else NA_character_
  ct <- if (clutch_active && nzchar(clutch_minutes)) as.integer(clutch_minutes) * 60L else NA_integer_
  co <- if (clutch_active) identical(clutch_ot_margin, "true") else FALSE

  if (!needs_filtered(opp_ids, game_type, home_away, outcome, gn_min, gn_max, last_n,
                      start_date, end_date, game_year = gy, opp_rank_side = opp_rank_side,
                      num_starters_off_mode = num_starters_off_mode, num_starters_off = num_starters_off,
                      num_starters_def_mode = num_starters_def_mode, num_starters_def = num_starters_def) &&
      !clutch_active) {
    q <- .timed(function() DBI::dbGetQuery(pg_pool, sprintf(
      "SELECT game_year, team_id, team_name, off_ppp, def_ppp, net_rtg, games_played, wins, losses, off_poss, def_poss, rank_net_rtg, rank_off_ppp, rank_def_ppp FROM %s.team_ppp_ratings_mv WHERE game_year = $1 ORDER BY rank_net_rtg",
      SCHEMA
    ), params = list(gy)))
    df <- q$value
    db_ms <- db_ms + q$ms
  } else {
    q <- .timed(function() run_team_ratings_dynamic(
      pg_pool, game_year = gy,
      start_d = start_date, end_d = end_date,
      game_type_csv = na_chr(game_type), opp_ids_csv = na_chr(opp_ids),
      home_away = na_chr(home_away), outcome = na_chr(outcome),
      opp_rank_side = na_chr(opp_rank_side), opp_rank_n = na_int(opp_rank_n),
      opp_rank_metric = na_chr(opp_rank_metric),
      max_margin = cm, margin_status = cs, max_time_remaining = ct, ot_margin_filter = co,
      min_gn = na_int(gn_min), max_gn = na_int(gn_max), last_n = na_int(last_n),
      num_starters_off = NA_integer_, num_starters_def = NA_integer_,
      num_starters_off_min = ns_off_min, num_starters_off_max = ns_off_max,
      num_starters_def_min = ns_def_min, num_starters_def_max = ns_def_max
    ))
    df <- q$value
    db_ms <- db_ms + q$ms
  }

  if (is.null(df) || nrow(df) == 0) return(list())

  # Compute pace columns
  q_mins <- .timed(function() fetch_team_game_minutes(
    pg_pool, game_year = gy, start_date = start_date, end_date = end_date,
    game_type_csv = na_chr(game_type), opp_ids_csv = na_chr(opp_ids),
    home_away = na_chr(home_away), outcome = na_chr(outcome),
    opp_rank_side = na_chr(opp_rank_side), opp_rank_n = na_int(opp_rank_n),
    opp_rank_metric = na_chr(opp_rank_metric),
    min_gn = na_int(gn_min), max_gn = na_int(gn_max), last_n = na_int(last_n),
    clutch_active = clutch_active,
    max_margin = cm, margin_status = cs, max_time_remaining = ct, ot_margin_filter = co,
    num_starters_off_min = ns_off_min, num_starters_off_max = ns_off_max,
    num_starters_def_min = ns_def_min, num_starters_def_max = ns_def_max
  ))
  db_ms <- db_ms + q_mins$ms
  mins_df <- q_mins$value
  mins_map <- if (!is.null(mins_df) && nrow(mins_df)) setNames(mins_df$game_minutes, as.character(mins_df$team_id)) else NULL
  df <- add_team_pace_cols(df, minutes_map = mins_map)

  for (col in names(df)) {
    if (is.numeric(df[[col]])) df[[col]][is.na(df[[col]])] <- 0
  }

  out <- rename_team_summary(df)
  perf_log(req, "/api/teams/summary", total_ms = .ms_elapsed(req_t0), db_ms = db_ms,
           transform_ms = .ms_elapsed(req_t0) - db_ms, rows = nrow(out))
  cache_set(key, out)
}

# ── GET /api/teams/four-factors ─────────────────────────────
#* @get /api/teams/four-factors
#* @serializer json
function(req, res,
         game_year = "2026", start_date = "2025-10-01", end_date = "2026-07-01",
         game_type = "", opp_ids = "",
         home_away = "", outcome = "",
         gn_min = "", gn_max = "", last_n = "",
         opp_rank_side = "", opp_rank_n = "", opp_rank_metric = "",
         num_starters_off_mode = "", num_starters_off = "",
         num_starters_def_mode = "", num_starters_def = "",
         clutch_margin = "", clutch_status = "", clutch_minutes = "",
         clutch_ot_margin = "false") {
  key <- cache_key("/api/teams/four-factors", req)
  hit <- cache_get(key)
  if (!is.null(hit)) return(hit)

  req_t0 <- .ms_now()
  db_ms <- 0
  gy <- as.integer(game_year)

  off_val <- if (nzchar(num_starters_off_mode) && nzchar(num_starters_off)) as.integer(num_starters_off) else NA_integer_
  def_val <- if (nzchar(num_starters_def_mode) && nzchar(num_starters_def)) as.integer(num_starters_def) else NA_integer_
  ns_off_min <- if (identical(num_starters_off_mode, "gte")) off_val else NA_integer_
  ns_off_max <- if (identical(num_starters_off_mode, "lte")) off_val else NA_integer_
  ns_def_min <- if (identical(num_starters_def_mode, "gte")) def_val else NA_integer_
  ns_def_max <- if (identical(num_starters_def_mode, "lte")) def_val else NA_integer_

  clutch_active <- has_clutch(clutch_margin, clutch_status, clutch_minutes)
  cm <- if (clutch_active) na_int(clutch_margin) else NA_integer_
  cs <- if (clutch_active) na_chr(clutch_status) else NA_character_
  ct <- if (clutch_active && nzchar(clutch_minutes)) as.integer(clutch_minutes) * 60L else NA_integer_
  co <- if (clutch_active) identical(clutch_ot_margin, "true") else FALSE

  if (!needs_filtered(opp_ids, game_type, home_away, outcome, gn_min, gn_max, last_n,
                      start_date, end_date, game_year = gy, opp_rank_side = opp_rank_side,
                      num_starters_off_mode = num_starters_off_mode, num_starters_off = num_starters_off,
                      num_starters_def_mode = num_starters_def_mode, num_starters_def = num_starters_def) &&
      !clutch_active) {
    q <- .timed(function() DBI::dbGetQuery(pg_pool, sprintf(
      "SELECT * FROM %s.team_four_factors_mv WHERE game_year = $1", SCHEMA
    ), params = list(gy)))
    df <- q$value
    db_ms <- db_ms + q$ms
  } else {
    q <- .timed(function() run_team_ff_dynamic(
      pg_pool, game_year = gy,
      start_d = start_date, end_d = end_date,
      game_type_csv = na_chr(game_type), opp_ids_csv = na_chr(opp_ids),
      home_away = na_chr(home_away), outcome = na_chr(outcome),
      opp_rank_side = na_chr(opp_rank_side), opp_rank_n = na_int(opp_rank_n),
      opp_rank_metric = na_chr(opp_rank_metric),
      max_margin = cm, margin_status = cs, max_time_remaining = ct, ot_margin_filter = co,
      min_gn = na_int(gn_min), max_gn = na_int(gn_max), last_n = na_int(last_n),
      num_starters_off = NA_integer_, num_starters_def = NA_integer_,
      num_starters_off_min = ns_off_min, num_starters_off_max = ns_off_max,
      num_starters_def_min = ns_def_min, num_starters_def_max = ns_def_max
    ))
    df <- q$value
    db_ms <- db_ms + q$ms
  }

  if (is.null(df) || nrow(df) == 0) return(list())

  # Compute PR ranks (all teams qualify, no baseline filtering needed)
  pr_vec <- function(x, invert = FALSE) {
    n <- sum(!is.na(x))
    if (n <= 1) return(rep(NA_real_, length(x)))
    r <- rank(x, na.last = "keep", ties.method = "average")
    p <- (r - 1) / (n - 1)
    if (invert) p <- 1 - p
    as.numeric(p)
  }
  df$pr_off_ppp  <- pr_vec(df$off_ppp)
  df$pr_off_ts   <- pr_vec(df$off_ts)
  df$pr_off_oreb <- pr_vec(df$off_oreb)
  df$pr_off_tov  <- pr_vec(df$off_tov, invert = TRUE)
  df$pr_off_ftr  <- pr_vec(df$off_ftr)
  df$pr_def_ppp  <- pr_vec(df$def_ppp, invert = TRUE)
  df$pr_def_ts   <- pr_vec(df$def_ts, invert = TRUE)
  df$pr_def_oreb <- pr_vec(df$def_oreb, invert = TRUE)
  df$pr_def_tov  <- pr_vec(df$def_tov)
  df$pr_def_ftr  <- pr_vec(df$def_ftr, invert = TRUE)
  df$pr_net      <- pr_vec(df$net_rtg)

  # Compute pace columns
  q_mins <- .timed(function() fetch_team_game_minutes(
    pg_pool, game_year = gy, start_date = start_date, end_date = end_date,
    game_type_csv = na_chr(game_type), opp_ids_csv = na_chr(opp_ids),
    home_away = na_chr(home_away), outcome = na_chr(outcome),
    opp_rank_side = na_chr(opp_rank_side), opp_rank_n = na_int(opp_rank_n),
    opp_rank_metric = na_chr(opp_rank_metric),
    min_gn = na_int(gn_min), max_gn = na_int(gn_max), last_n = na_int(last_n),
    clutch_active = clutch_active,
    max_margin = cm, margin_status = cs, max_time_remaining = ct, ot_margin_filter = co,
    num_starters_off_min = ns_off_min, num_starters_off_max = ns_off_max,
    num_starters_def_min = ns_def_min, num_starters_def_max = ns_def_max
  ))
  db_ms <- db_ms + q_mins$ms
  mins_df <- q_mins$value
  mins_map <- if (!is.null(mins_df) && nrow(mins_df)) setNames(mins_df$game_minutes, as.character(mins_df$team_id)) else NULL
  df <- add_team_pace_cols(df, minutes_map = mins_map)

  for (col in names(df)) {
    if (is.numeric(df[[col]])) df[[col]][is.na(df[[col]])] <- 0
  }

  out <- rename_team_ff(df)
  perf_log(req, "/api/teams/four-factors", total_ms = .ms_elapsed(req_t0), db_ms = db_ms,
           transform_ms = .ms_elapsed(req_t0) - db_ms, rows = nrow(out))
  cache_set(key, out)
}

# ══════════════════════════════════════════════════════════════
# Tab 4: Game Logs
# ══════════════════════════════════════════════════════════════

rename_gamelog_summary <- function(df) {
  if (nrow(df) == 0) return(df)
  nms <- c(
    "gn" = "gn", "game_date" = "gameDate", "team_name" = "teamName",
    "opp_team_name" = "opponent", "result" = "result", "score_display" = "score",
    "off_ppp" = "offPpp", "def_ppp" = "defPpp", "net_rtg" = "netRtg",
    "off_poss" = "offPoss", "def_poss" = "defPoss",
    "off_fg2m" = "offFg2Made", "off_fg2a" = "offFg2Att",
    "off_fg3m" = "offFg3Made", "off_fg3a" = "offFg3Att",
    "def_fg2m" = "defFg2Made", "def_fg2a" = "defFg2Att",
    "def_fg3m" = "defFg3Made", "def_fg3a" = "defFg3Att",
    "game_id" = "gameId", "team_id" = "teamId"
  )
  for (old in names(nms)) {
    if (old %in% names(df)) names(df)[names(df) == old] <- nms[[old]]
  }
  df[, names(df) %in% unname(nms), drop = FALSE]
}

rename_gamelog_ff <- function(df) {
  if (nrow(df) == 0) return(df)
  nms <- c(
    "gn" = "gn", "game_date" = "gameDate", "team_name" = "teamName",
    "opp_team_name" = "opponent", "result" = "result", "score_display" = "score",
    "off_ppp" = "offPpp", "off_ts_pct" = "offTsPct", "off_oreb_pct" = "offOrebPct",
    "off_tov_pct" = "offTovPct", "off_ftr_pct" = "offFtrPct",
    "def_ppp" = "defPpp", "def_ts_pct" = "defTsPct", "def_oreb_pct" = "defOrebPct",
    "def_tov_pct" = "defTovPct", "def_ftr_pct" = "defFtrPct",
    "off_poss" = "offPoss", "def_poss" = "defPoss",
    "game_id" = "gameId", "team_id" = "teamId"
  )
  for (old in names(nms)) {
    if (old %in% names(df)) names(df)[names(df) == old] <- nms[[old]]
  }
  df[, names(df) %in% unname(nms), drop = FALSE]
}

# ── GET /api/gamelogs/summary ────────────────────────────────
#* @get /api/gamelogs/summary
#* @serializer json
function(req, res,
         game_year = "2026", start_date = "2025-10-01", end_date = "2026-07-01",
         game_type = "", opp_ids = "",
         home_away = "", outcome = "",
         gn_min = "", gn_max = "", last_n = "",
         filter_team_id = "",
         num_starters_off_mode = "", num_starters_off = "",
         num_starters_def_mode = "", num_starters_def = "") {
  key <- cache_key("/api/gamelogs/summary", req)
  hit <- cache_get(key)
  if (!is.null(hit)) return(hit)

  req_t0 <- .ms_now()
  db_ms <- 0
  gy <- as.integer(game_year)

  # Fetch schedule
  q_sched <- .timed(function() DBI::dbGetQuery(pg_pool, sprintf(
    "SELECT * FROM %s.final_schedule_mv WHERE game_year = $1", SCHEMA
  ), params = list(gy)))
  db_ms <- db_ms + q_sched$ms
  sched <- q_sched$value
  if (is.null(sched) || nrow(sched) == 0) return(list())

  # Apply schedule filters in R (mirrors server_tab4.R gl_filtered_schedule)
  if (nzchar(filter_team_id)) sched <- sched[sched$team_id == as.integer(filter_team_id), , drop = FALSE]
  if (nzchar(start_date)) sched <- sched[!is.na(sched$game_date) & sched$game_date >= as.Date(start_date), , drop = FALSE]
  if (nzchar(end_date)) sched <- sched[!is.na(sched$game_date) & sched$game_date <= as.Date(end_date), , drop = FALSE]
  if (nzchar(game_type)) {
    gt_vals <- as.integer(strsplit(game_type, ",")[[1]])
    sched <- sched[sched$game_type %in% gt_vals, , drop = FALSE]
  }
  if (nzchar(opp_ids)) {
    opp_vals <- as.integer(strsplit(opp_ids, ",")[[1]])
    sched <- sched[sched$opp_team_id %in% opp_vals, , drop = FALSE]
  }
  if (nzchar(home_away)) {
    if (home_away == "home") sched <- sched[sched$is_home == TRUE, , drop = FALSE]
    else sched <- sched[sched$is_home == FALSE, , drop = FALSE]
  }
  if (nzchar(outcome)) {
    if (outcome == "win") sched <- sched[sched$has_won == TRUE, , drop = FALSE]
    else sched <- sched[sched$has_won == FALSE, , drop = FALSE]
  }
  mg <- na_int(gn_min); xg <- na_int(gn_max); ln <- na_int(last_n)
  if (!is.na(ln)) { mg <- NA_integer_; xg <- NA_integer_ }
  if (!is.na(mg) || !is.na(xg)) ln <- NA_integer_
  if (!is.na(mg)) sched <- sched[!is.na(sched$gn) & sched$gn >= mg, , drop = FALSE]
  if (!is.na(xg)) sched <- sched[!is.na(sched$gn) & sched$gn <= xg, , drop = FALSE]
  if (!is.na(ln)) {
    sched <- sched[order(sched$game_date, sched$game_id, decreasing = TRUE), , drop = FALSE]
    sched <- do.call(rbind, lapply(split(sched, sched$team_id), function(x) head(x, ln)))
    rownames(sched) <- NULL
  }
  if (nrow(sched) == 0) return(list())

  # Fetch lineup totals
  q_lt <- .timed(function() DBI::dbGetQuery(pg_pool, sprintf(
    "SELECT team_id, type_lineup, game_id, total_poss, total_pts, fg2_made, fg2_att, fg3_made, fg3_att, num_starters
     FROM %s.mv_lineup_totals_by_day WHERE game_year = $1", SCHEMA
  ), params = list(gy)))
  db_ms <- db_ms + q_lt$ms
  lt <- q_lt$value

  # Inner join on schedule pairs
  sched_key <- paste(sched$game_id, sched$team_id)
  lt_key <- paste(lt$game_id, lt$team_id)
  lt <- lt[lt_key %in% sched_key, , drop = FALSE]

  # Apply starters filter
  off_mode <- num_starters_off_mode; def_mode <- num_starters_def_mode
  off_val <- if (nzchar(off_mode) && nzchar(num_starters_off)) as.integer(num_starters_off) else NA_integer_
  def_val <- if (nzchar(def_mode) && nzchar(num_starters_def)) as.integer(num_starters_def) else NA_integer_
  ns_off_min <- if (identical(off_mode, "gte")) off_val else NA_integer_
  ns_off_max <- if (identical(off_mode, "lte")) off_val else NA_integer_
  ns_def_min <- if (identical(def_mode, "gte")) def_val else NA_integer_
  ns_def_max <- if (identical(def_mode, "lte")) def_val else NA_integer_

  is_off <- lt$type_lineup == "offense"
  is_def <- lt$type_lineup == "defense"
  ns <- lt$num_starters
  keep_off <- is_off & (is.na(ns_off_min) | ns >= ns_off_min) & (is.na(ns_off_max) | ns <= ns_off_max)
  keep_def <- is_def & (is.na(ns_def_min) | ns >= ns_def_min) & (is.na(ns_def_max) | ns <= ns_def_max)
  lt <- lt[keep_off | keep_def, , drop = FALSE]
  if (nrow(lt) == 0) return(list())

  # Aggregate per (game_id, team_id, type_lineup)
  agg <- aggregate(cbind(total_poss, total_pts, fg2_made, fg2_att, fg3_made, fg3_att) ~
                     game_id + team_id + type_lineup, data = lt, FUN = sum, na.rm = TRUE)

  off <- agg[agg$type_lineup == "offense", , drop = FALSE]
  def <- agg[agg$type_lineup == "defense", , drop = FALSE]
  names(off)[names(off) == "total_poss"] <- "off_poss"
  names(off)[names(off) == "total_pts"] <- "off_pts"
  names(off) <- sub("^fg", "off_fg", names(off))
  names(def)[names(def) == "total_poss"] <- "def_poss"
  names(def)[names(def) == "total_pts"] <- "def_pts"
  names(def) <- sub("^fg", "def_fg", names(def))

  combined <- merge(
    off[, !names(off) %in% "type_lineup", drop = FALSE],
    def[, c("game_id", "team_id", "def_poss", "def_pts", "def_fg2_made", "def_fg2_att", "def_fg3_made", "def_fg3_att"), drop = FALSE],
    by = c("game_id", "team_id"), all.x = TRUE
  )
  combined$off_ppp <- ifelse(combined$off_poss > 0, round(combined$off_pts / combined$off_poss * 100, 1), NA_real_)
  combined$def_ppp <- ifelse(combined$def_poss > 0, round(combined$def_pts / combined$def_poss * 100, 1), NA_real_)
  combined$net_rtg <- round(ifelse(is.na(combined$off_ppp), 0, combined$off_ppp) - ifelse(is.na(combined$def_ppp), 0, combined$def_ppp), 1)

  # Join schedule info
  sched$result <- ifelse(sched$has_won, "W", "L")
  sched$score_display <- paste0(sched$team_score, "-", sched$opp_score)
  sinfo <- sched[, c("game_id", "team_id", "team_name", "gn", "game_date", "opp_team_name", "result", "score_display"), drop = FALSE]
  result <- merge(combined, sinfo, by = c("game_id", "team_id"))

  # Rename shot columns to match expected names
  names(result) <- sub("off_fg2_made", "off_fg2m", names(result))
  names(result) <- sub("off_fg2_att", "off_fg2a", names(result))
  names(result) <- sub("off_fg3_made", "off_fg3m", names(result))
  names(result) <- sub("off_fg3_att", "off_fg3a", names(result))
  names(result) <- sub("def_fg2_made", "def_fg2m", names(result))
  names(result) <- sub("def_fg2_att", "def_fg2a", names(result))
  names(result) <- sub("def_fg3_made", "def_fg3m", names(result))
  names(result) <- sub("def_fg3_att", "def_fg3a", names(result))

  result$game_date <- as.character(result$game_date)
  result <- result[order(result$game_date, result$gn, decreasing = TRUE), , drop = FALSE]

  for (col in names(result)) {
    if (is.numeric(result[[col]])) result[[col]][is.na(result[[col]])] <- 0
  }

  out <- rename_gamelog_summary(result)
  perf_log(req, "/api/gamelogs/summary", total_ms = .ms_elapsed(req_t0), db_ms = db_ms,
           transform_ms = .ms_elapsed(req_t0) - db_ms, rows = nrow(out))
  cache_set(key, out)
}

# ── GET /api/gamelogs/four-factors ───────────────────────────
#* @get /api/gamelogs/four-factors
#* @serializer json
function(req, res,
         game_year = "2026", start_date = "2025-10-01", end_date = "2026-07-01",
         game_type = "", opp_ids = "",
         home_away = "", outcome = "",
         gn_min = "", gn_max = "", last_n = "",
         filter_team_id = "",
         num_starters_off_mode = "", num_starters_off = "",
         num_starters_def_mode = "", num_starters_def = "") {
  key <- cache_key("/api/gamelogs/four-factors", req)
  hit <- cache_get(key)
  if (!is.null(hit)) return(hit)

  req_t0 <- .ms_now()
  db_ms <- 0
  gy <- as.integer(game_year)

  # Fetch schedule (same pattern as summary)
  q_sched <- .timed(function() DBI::dbGetQuery(pg_pool, sprintf(
    "SELECT * FROM %s.final_schedule_mv WHERE game_year = $1", SCHEMA
  ), params = list(gy)))
  db_ms <- db_ms + q_sched$ms
  sched <- q_sched$value
  if (is.null(sched) || nrow(sched) == 0) return(list())

  # Apply schedule filters in R
  if (nzchar(filter_team_id)) sched <- sched[sched$team_id == as.integer(filter_team_id), , drop = FALSE]
  if (nzchar(start_date)) sched <- sched[!is.na(sched$game_date) & sched$game_date >= as.Date(start_date), , drop = FALSE]
  if (nzchar(end_date)) sched <- sched[!is.na(sched$game_date) & sched$game_date <= as.Date(end_date), , drop = FALSE]
  if (nzchar(game_type)) {
    gt_vals <- as.integer(strsplit(game_type, ",")[[1]])
    sched <- sched[sched$game_type %in% gt_vals, , drop = FALSE]
  }
  if (nzchar(opp_ids)) {
    opp_vals <- as.integer(strsplit(opp_ids, ",")[[1]])
    sched <- sched[sched$opp_team_id %in% opp_vals, , drop = FALSE]
  }
  if (nzchar(home_away)) {
    if (home_away == "home") sched <- sched[sched$is_home == TRUE, , drop = FALSE]
    else sched <- sched[sched$is_home == FALSE, , drop = FALSE]
  }
  if (nzchar(outcome)) {
    if (outcome == "win") sched <- sched[sched$has_won == TRUE, , drop = FALSE]
    else sched <- sched[sched$has_won == FALSE, , drop = FALSE]
  }
  mg <- na_int(gn_min); xg <- na_int(gn_max); ln <- na_int(last_n)
  if (!is.na(ln)) { mg <- NA_integer_; xg <- NA_integer_ }
  if (!is.na(mg) || !is.na(xg)) ln <- NA_integer_
  if (!is.na(mg)) sched <- sched[!is.na(sched$gn) & sched$gn >= mg, , drop = FALSE]
  if (!is.na(xg)) sched <- sched[!is.na(sched$gn) & sched$gn <= xg, , drop = FALSE]
  if (!is.na(ln)) {
    sched <- sched[order(sched$game_date, sched$game_id, decreasing = TRUE), , drop = FALSE]
    sched <- do.call(rbind, lapply(split(sched, sched$team_id), function(x) head(x, ln)))
    rownames(sched) <- NULL
  }
  if (nrow(sched) == 0) return(list())

  # Fetch lineup four factors
  q_ff <- .timed(function() DBI::dbGetQuery(pg_pool, sprintf(
    "SELECT team_id, game_id, type_lineup, total_points, total_poss,
            ts_poss_count, oreb_count, oreb_opportunities, tov_count,
            total_ft_attempts, total_fga, num_starters
     FROM %s.lineup_four_factors_by_game WHERE game_year = $1", SCHEMA
  ), params = list(gy)))
  db_ms <- db_ms + q_ff$ms
  ff <- q_ff$value

  # Inner join on schedule pairs
  sched_key <- paste(sched$game_id, sched$team_id)
  ff_key <- paste(ff$game_id, ff$team_id)
  ff <- ff[ff_key %in% sched_key, , drop = FALSE]

  # Apply starters filter
  off_mode <- num_starters_off_mode; def_mode <- num_starters_def_mode
  off_v <- if (nzchar(off_mode) && nzchar(num_starters_off)) as.integer(num_starters_off) else NA_integer_
  def_v <- if (nzchar(def_mode) && nzchar(num_starters_def)) as.integer(num_starters_def) else NA_integer_
  ns_off_min <- if (identical(off_mode, "gte")) off_v else NA_integer_
  ns_off_max <- if (identical(off_mode, "lte")) off_v else NA_integer_
  ns_def_min <- if (identical(def_mode, "gte")) def_v else NA_integer_
  ns_def_max <- if (identical(def_mode, "lte")) def_v else NA_integer_

  is_off <- ff$type_lineup == "offense"
  is_def <- ff$type_lineup == "defense"
  ns <- ff$num_starters
  keep_off <- is_off & (is.na(ns_off_min) | ns >= ns_off_min) & (is.na(ns_off_max) | ns <= ns_off_max)
  keep_def <- is_def & (is.na(ns_def_min) | ns >= ns_def_min) & (is.na(ns_def_max) | ns <= ns_def_max)
  ff <- ff[keep_off | keep_def, , drop = FALSE]
  if (nrow(ff) == 0) return(list())

  # Aggregate per (game_id, team_id, type_lineup)
  agg <- aggregate(cbind(total_points, total_poss, ts_poss_count, oreb_count,
                          oreb_opportunities, tov_count, total_ft_attempts, total_fga) ~
                     game_id + team_id + type_lineup, data = ff, FUN = sum, na.rm = TRUE)

  off <- agg[agg$type_lineup == "offense", , drop = FALSE]
  def <- agg[agg$type_lineup == "defense", , drop = FALSE]

  # Rename for merge
  off_cols <- c("game_id", "team_id", "off_pts", "off_poss", "off_ts_poss", "off_oreb",
                "off_oreb_opp", "off_tov", "off_fta", "off_fga")
  names(off) <- c("game_id", "team_id", "type_lineup", "off_pts", "off_poss", "off_ts_poss",
                   "off_oreb", "off_oreb_opp", "off_tov", "off_fta", "off_fga")
  def_cols <- c("game_id", "team_id", "def_pts", "def_poss", "def_ts_poss", "def_oreb",
                "def_oreb_opp", "def_tov", "def_fta", "def_fga")
  names(def) <- c("game_id", "team_id", "type_lineup", "def_pts", "def_poss", "def_ts_poss",
                   "def_oreb", "def_oreb_opp", "def_tov", "def_fta", "def_fga")

  combined <- merge(
    off[, !names(off) %in% "type_lineup", drop = FALSE],
    def[, c("game_id", "team_id", "def_pts", "def_poss", "def_ts_poss", "def_oreb",
            "def_oreb_opp", "def_tov", "def_fta", "def_fga"), drop = FALSE],
    by = c("game_id", "team_id"), all.x = TRUE
  )

  combined$off_ppp <- ifelse(combined$off_poss > 0, round(combined$off_pts / combined$off_poss * 100, 1), NA_real_)
  combined$def_ppp <- ifelse(combined$def_poss > 0, round(combined$def_pts / combined$def_poss * 100, 1), NA_real_)
  combined$off_ts_pct <- ifelse(combined$off_ts_poss > 0, round(combined$off_pts / (2 * combined$off_ts_poss) * 100, 1), NA_real_)
  combined$off_oreb_pct <- ifelse(combined$off_oreb_opp > 0, round(combined$off_oreb / combined$off_oreb_opp * 100, 1), NA_real_)
  combined$off_tov_pct <- ifelse(combined$off_poss > 0, round(combined$off_tov / combined$off_poss * 100, 1), NA_real_)
  combined$off_ftr_pct <- ifelse(combined$off_fga > 0, round(combined$off_fta / combined$off_fga * 100, 1), NA_real_)
  combined$def_ts_pct <- ifelse(combined$def_ts_poss > 0, round(combined$def_pts / (2 * combined$def_ts_poss) * 100, 1), NA_real_)
  combined$def_oreb_pct <- ifelse(combined$def_oreb_opp > 0, round(combined$def_oreb / combined$def_oreb_opp * 100, 1), NA_real_)
  combined$def_tov_pct <- ifelse(combined$def_poss > 0, round(combined$def_tov / combined$def_poss * 100, 1), NA_real_)
  combined$def_ftr_pct <- ifelse(combined$def_fga > 0, round(combined$def_fta / combined$def_fga * 100, 1), NA_real_)

  # Join schedule info
  sched$result <- ifelse(sched$has_won, "W", "L")
  sched$score_display <- paste0(sched$team_score, "-", sched$opp_score)
  sinfo <- sched[, c("game_id", "team_id", "team_name", "gn", "game_date", "opp_team_name", "result", "score_display"), drop = FALSE]
  result <- merge(combined, sinfo, by = c("game_id", "team_id"))

  result$game_date <- as.character(result$game_date)
  result <- result[order(result$game_date, result$gn, decreasing = TRUE), , drop = FALSE]

  for (col in names(result)) {
    if (is.numeric(result[[col]])) result[[col]][is.na(result[[col]])] <- 0
  }

  out <- rename_gamelog_ff(result)
  perf_log(req, "/api/gamelogs/four-factors", total_ms = .ms_elapsed(req_t0), db_ms = db_ms,
           transform_ms = .ms_elapsed(req_t0) - db_ms, rows = nrow(out))
  cache_set(key, out)
}
