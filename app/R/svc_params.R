# svc_params.R - validation of untrusted parameters for the service layer.
#
# A service's *_params() function (svc_game_logs_params(), ...) turns a
# caller's query -- the API's, or any other non-Shiny caller's -- into the
# typed parameter list its service takes. Shiny never calls these: it builds
# the same list from its inputs. Every parser here returns a typed value or
# signals an api_error() naming the parameter; nothing is coerced, defaulted
# over or silently dropped (Security rule 4: fail closed).
#
# `query` is a named list of character vectors, one element per occurrence
# of a name (the API's api_query() builds it from the raw query string, so an
# empty value or a repeated name is visible here rather than lost).

# The condition every validation failure signals. `status` is the HTTP status
# the API answers with; `param` names the offending parameter (NULL when the
# failure is not about one).
api_error <- function(status = 400L, param = NULL, message = "") {
  structure(
    class = c("api_error", "error", "condition"),
    list(message = as.character(message), call = NULL,
         status = as.integer(status), param = param)
  )
}

svc_reject <- function(param, fmt, ..., status = 400L) {
  stop(api_error(status, param, sprintf(fmt, ...)))
}

# API league ids -> the services' league and EuroLeague competition code.
# EuroLeague and EuroCup are one league to the services (R7): the competition
# is a parameter.
SVC_LEAGUES <- list(
  il = list(league = "il", competition = NULL),
  euroleague = list(league = "euroleague", competition = "E"),
  eurocup = list(league = "euroleague", competition = "U")
)

svc_league <- function(league) {
  lg <- if (is.character(league) && length(league) == 1L) SVC_LEAGUES[[league]]
  if (is.null(lg)) {
    svc_reject("league", "Unknown league '%s'; expected one of: %s.",
               paste(league, collapse = ","), paste(names(SVC_LEAGUES), collapse = ", "),
               status = 404L)
  }
  lg
}

# ---------------- The query ----------------

# Any name outside `allowed` is rejected, naming the first one given.
svc_check_names <- function(query, allowed) {
  nms <- names(query) %||% rep("", length(query))
  bad <- nms[!nms %in% allowed]
  if (length(bad)) {
    svc_reject(bad[[1]], "Unknown parameter '%s'; accepted here: %s.",
               bad[[1]], paste(allowed, collapse = ", "))
  }
  invisible(TRUE)
}

# All values given for `name`, in order (character(0) when absent).
svc_values <- function(query, name) {
  vals <- unlist(query[names(query) %in% name], use.names = FALSE)
  as.character(vals %||% character(0))
}

# One value: NULL when absent; given twice or empty is an error.
svc_one <- function(query, name) {
  vals <- svc_values(query, name)
  if (!length(vals)) return(NULL)
  if (length(vals) > 1L) svc_reject(name, "'%s' was given more than once.", name)
  if (!nzchar(trimws(vals))) svc_reject(name, "'%s' must not be empty.", name)
  vals
}

# One comma-separated list: NULL when absent; an empty element is an error.
svc_list <- function(query, name) {
  val <- svc_one(query, name)
  if (is.null(val)) return(NULL)
  parts <- trimws(strsplit(val, ",", fixed = TRUE)[[1]])
  if (!length(parts) || any(!nzchar(parts)) || grepl(",\\s*$", val)) {
    svc_reject(name, "'%s' must be a comma-separated list without empty entries.", name)
  }
  parts
}

# ---------------- Typed values ----------------

svc_int <- function(x, param, min = -Inf, max = Inf) {
  if (is.null(x)) return(NULL)
  ok <- grepl("^-?[0-9]{1,9}$", x)
  if (!all(ok)) svc_reject(param, "'%s' must be a whole number, got '%s'.", param, x[!ok][[1]])
  v <- as.integer(x)
  out <- v < min | v > max
  if (any(out)) {
    range_txt <- if (is.finite(max)) sprintf("between %s and %s", min, max) else sprintf("at least %s", min)
    svc_reject(param, "'%s' must be %s, got %d.", param, range_txt, v[out][[1]])
  }
  v
}

svc_number <- function(x, param) {
  ok <- grepl("^-?([0-9]+([.][0-9]*)?|[.][0-9]+)$", x)
  if (!ok) svc_reject(param, "'%s' needs a number, got '%s'.", param, x)
  as.numeric(x)
}

svc_date <- function(x, param) {
  if (is.null(x)) return(NULL)
  d <- if (grepl("^[0-9]{4}-[0-9]{2}-[0-9]{2}$", x)) {
    suppressWarnings(as.Date(x, format = "%Y-%m-%d"))
  } else {
    as.Date(NA)
  }
  if (is.na(d) || !identical(format(d, "%Y-%m-%d"), x)) {
    svc_reject(param, "'%s' must be a date as YYYY-MM-DD, got '%s'.", param, x)
  }
  d
}

svc_choice <- function(x, param, choices) {
  if (is.null(x)) return(NULL)
  bad <- x[!x %in% choices]
  if (length(bad)) {
    svc_reject(param, "'%s' must be one of: %s; got '%s'.",
               param, paste(choices, collapse = ", "), bad[[1]])
  }
  x
}

# ---------------- Seasons ----------------

# The seasons a league has data for, newest first. Israeli: the seasons of the
# season_teams catalogue (its generated copy, STATIC_TEAM_ROSTERS -- the same
# set the navbar offers). EuroLeague / EuroCup: euro_fetch_seasons(), Shiny's
# own lookup. Never a list typed here.
svc_seasons <- function(league) {
  lg <- svc_league(league)
  if (identical(lg$league, "il")) {
    sort(as.integer(names(STATIC_TEAM_ROSTERS)), decreasing = TRUE)
  } else {
    s <- euro_fetch_seasons(lg$competition)
    sort(as.integer(s$game_year), decreasing = TRUE)
  }
}

svc_default_season <- function(league) {
  if (identical(svc_league(league)$league, "il")) as.integer(DEFAULT_GAME_YEAR) else as.integer(EURO_DEFAULT_SEASON)
}

# `season` from the query, or the league's default season (Shiny's selector
# default); either way it must be a season with data.
svc_season <- function(query, league) {
  season <- svc_int(svc_one(query, "season"), "season") %||% svc_default_season(league)
  have <- svc_seasons(league)
  if (!season %in% have) {
    svc_reject("season", "No data for season %d in %s; seasons with data: %s.",
               season, league, paste(have, collapse = ", "))
  }
  season
}

svc_season_bounds <- function(league, season) {
  if (identical(svc_league(league)$league, "il")) {
    season_date_bounds_for_year(season)
  } else {
    euro_season_date_bounds(season)
  }
}

# date_from / date_to, each defaulting to the season's own bound (where
# Shiny's date input starts), each inside the season window, in order.
svc_dates <- function(query, bounds) {
  from <- svc_date(svc_one(query, "date_from"), "date_from") %||% bounds$start
  to <- svc_date(svc_one(query, "date_to"), "date_to") %||% bounds$end
  for (d in list(list("date_from", from), list("date_to", to))) {
    if (d[[2]] < bounds$start || d[[2]] > bounds$end) {
      svc_reject(d[[1]], "'%s' must lie in the season window %s to %s.",
                 d[[1]], format(bounds$start), format(bounds$end))
    }
  }
  if (from > to) svc_reject("date_from", "'date_from' (%s) is after 'date_to' (%s).", format(from), format(to))
  c(from, to)
}

# ---------------- Response controls ----------------

svc_format <- function(query) {
  svc_choice(svc_one(query, "format"), "format", c("json", "csv")) %||% "json"
}

# sort=<column>:<asc|desc>[,...] -- syntax only here; the endpoint checks
# the columns against its own.
svc_sort <- function(query) {
  parts <- svc_list(query, "sort")
  if (is.null(parts)) return(NULL)
  ok <- grepl("^[A-Za-z0-9_%. ]+:(asc|desc)$", parts)
  if (!all(ok)) svc_reject("sort", "'sort' entries must be <column>:asc or <column>:desc, got '%s'.", parts[!ok][[1]])
  lapply(strsplit(parts, ":", fixed = TRUE), function(x) list(column = x[[1]], direction = x[[2]]))
}

# ---------------- Data version ----------------

# The league's ETL data version, read where Shiny reads it: Israeli
# etl_last_success_db() normalised by shared_data_version(), EuroLeague and
# EuroCup euro_data_version(). It rides in every season-pull cache key, so a
# new ETL run is a new key.
svc_data_version <- function(league) {
  if (identical(svc_league(league)$league, "il")) {
    version <- etl_last_success_db()
    shared_data_version(list(data_version = function() version))
  } else {
    euro_data_version()
  }
}
