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

# A caller's value as echoed in an error message: at most 64 characters, and
# never bytes that are not UTF-8 text.
SVC_ECHO_MAX <- 64L
svc_show <- function(x) {
  x <- as.character(x %||% "")[1]
  if (is.na(x)) return("NA")
  if (!validUTF8(x)) return("<not UTF-8 text>")
  if (nchar(x) > SVC_ECHO_MAX) paste0(substr(x, 1L, SVC_ECHO_MAX), "...") else x
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
               svc_show(paste(league, collapse = ",")), paste(names(SVC_LEAGUES), collapse = ", "),
               status = 404L)
  }
  lg
}

# ---------------- The query ----------------

# Any name outside `allowed` is rejected, naming the first one given. A name
# that is empty or not UTF-8 text cannot be named, so it is reported as
# param "query".
svc_check_names <- function(query, allowed) {
  nms <- names(query) %||% rep("", length(query))
  unnamable <- !nzchar(nms) | !validUTF8(nms)
  if (any(unnamable)) {
    svc_reject("query", "The query holds a parameter with an empty or undecodable name.")
  }
  bad <- nms[!nms %in% allowed]
  if (length(bad)) {
    if (!length(allowed)) {
      svc_reject(bad[[1]], "This endpoint accepts no parameters; got '%s'.", svc_show(bad[[1]]))
    }
    svc_reject(bad[[1]], "Unknown parameter '%s'; accepted here: %s.",
               svc_show(bad[[1]]), paste(allowed, collapse = ", "))
  }
  invisible(TRUE)
}

# All values given for `name`, in order (character(0) when absent).
svc_values <- function(query, name) {
  vals <- as.character(unlist(query[names(query) %in% name], use.names = FALSE) %||% character(0))
  if (!all(validUTF8(vals))) svc_reject(name, "'%s' must be UTF-8 text.", name)
  vals
}

# One value: NULL when absent; given twice or empty is an error.
svc_one <- function(query, name) {
  vals <- svc_values(query, name)
  if (!length(vals)) return(NULL)
  if (length(vals) > 1L) svc_reject(name, "'%s' was given more than once.", name)
  if (!nzchar(trimws(vals))) svc_reject(name, "'%s' must not be empty.", name)
  vals
}

# Caps on how many entries one request may carry (security audit M-1: each
# entry costs R time and is echoed back in meta). Any list of ids -- teams,
# opponents, game types, phases -- holds at most SVC_MAX_IDS (a season has
# about 20 teams); sort at most SVC_MAX_SORT keys.
SVC_MAX_IDS <- 50L
SVC_MAX_SORT <- 5L

# Rejects more than `max` entries for `param`, before any entry is parsed.
svc_cap <- function(x, param, max) {
  if (length(x) > max) svc_reject(param, "'%s' takes at most %d entries; got %d.", param, max, length(x))
  invisible(x)
}

# One comma-separated list: NULL when absent; an empty element is an error;
# more than `max` entries is an error.
svc_list <- function(query, name, max = SVC_MAX_IDS) {
  val <- svc_one(query, name)
  if (is.null(val)) return(NULL)
  parts <- trimws(strsplit(val, ",", fixed = TRUE)[[1]])
  svc_cap(parts, name, max)
  if (!length(parts) || any(!nzchar(parts)) || grepl(",\\s*$", val)) {
    svc_reject(name, "'%s' must be a comma-separated list without empty entries.", name)
  }
  parts
}

# ---------------- Typed values ----------------

svc_int <- function(x, param, min = -Inf, max = Inf) {
  if (is.null(x)) return(NULL)
  ok <- grepl("^-?[0-9]{1,9}$", x)
  if (!all(ok)) svc_reject(param, "'%s' must be a whole number, got '%s'.", param, svc_show(x[!ok][[1]]))
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
  v <- if (ok) as.numeric(x) else NA_real_
  if (!is.finite(v)) svc_reject(param, "'%s' needs a finite number, got '%s'.", param, svc_show(x))
  v
}

svc_date <- function(x, param) {
  if (is.null(x)) return(NULL)
  d <- if (grepl("^[0-9]{4}-[0-9]{2}-[0-9]{2}$", x)) {
    suppressWarnings(as.Date(x, format = "%Y-%m-%d"))
  } else {
    as.Date(NA)
  }
  if (is.na(d) || !identical(format(d, "%Y-%m-%d"), x)) {
    svc_reject(param, "'%s' must be a date as YYYY-MM-DD, got '%s'.", param, svc_show(x))
  }
  d
}

svc_choice <- function(x, param, choices) {
  if (is.null(x)) return(NULL)
  bad <- x[!x %in% choices]
  if (length(bad)) {
    svc_reject(param, "'%s' must be one of: %s; got '%s'.",
               param, paste(choices, collapse = ", "), svc_show(bad[[1]]))
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
  parts <- svc_list(query, "sort", max = SVC_MAX_SORT)
  if (is.null(parts)) return(NULL)
  ok <- grepl("^[A-Za-z0-9_%. ]+:(asc|desc)$", parts)
  if (!all(ok)) svc_reject("sort", "'sort' entries must be <column>:asc or <column>:desc, got '%s'.", svc_show(parts[!ok][[1]]))
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
