# params.R - the API's view of a request's parameters.
#
# Validation itself belongs to the service layer (app/R/svc_params.R and each
# svc_*_params()); this file only turns a request into the query those take,
# and checks the reference endpoints' few parameters with the same parsers.

# The query string as a named list, one character element per occurrence, in
# order. Read from the raw string rather than Plumber's parsed args, which
# drop a name with an empty value (`team=`) and merge repeated names -- both
# must reach the validators so that they can reject or collect them.
#
# Every name and value must decode to UTF-8 text without NUL (`%00`, `%FF`
# are rejected here, never echoed): a bad value is a 400 naming its
# parameter, a bad name a 400 on param "query".
api_query <- function(req) {
  qs <- req$QUERY_STRING %||% ""
  qs <- sub("^[?]", "", qs)
  if (!nzchar(qs)) return(list())
  pieces <- strsplit(qs, "&", fixed = TRUE)[[1]]
  pieces <- pieces[nzchar(pieces)]
  decode <- function(x) {
    out <- tryCatch(httpuv::decodeURIComponent(chartr("+", " ", x)), error = function(e) NA_character_)
    if (is.na(out) || !validUTF8(out)) NA_character_ else enc2utf8(out)
  }
  out <- lapply(pieces, function(p) {
    has_value <- grepl("=", p, fixed = TRUE)
    key <- decode(sub("=.*$", "", p))
    if (is.na(key)) {
      stop(api_error(400L, "query", "A parameter name could not be decoded as UTF-8 text."))
    }
    val <- if (has_value) decode(sub("^[^=]*=", "", p)) else ""
    if (is.na(val)) {
      stop(api_error(400L, key, sprintf("'%s' could not be decoded as UTF-8 text.", svc_show(key))))
    }
    stats::setNames(list(val), key)
  })
  if (!length(out)) return(list())
  do.call(c, out)
}

# Plumber filter used in place of Plumber's default ones (run.R). Plumber's own
# queryString filter decodes the query before any route runs and answers a
# 500 on bytes such as %00; the handlers read req$QUERY_STRING through
# api_query() instead, so Plumber is given no parsed query arguments at all.
# The API has only GET routes, so the default body parsers are not needed.
api_query_filter <- function(req) {
  req$argsQuery <- list()
  req$.internal$queryStringHandled <- TRUE
  plumber::forward()
}

# Reference endpoints: `allowed` names only; a season (validated against the
# league's seasons) when `season` is allowed; the response format.
api_reference_params <- function(query, league, allowed = c("season", "format")) {
  lg <- svc_league(league)
  svc_check_names(query, allowed)
  list(
    league = league,
    competition = lg$competition,
    season = if ("season" %in% allowed) svc_season(query, league),
    format = svc_format(query)
  )
}

# 404 for an endpoint the league does not have (an Israeli-only list asked of
# EuroLeague, or the reverse).
api_require_league <- function(league, leagues) {
  svc_league(league)
  if (!league %in% leagues) {
    stop(api_error(404L, NULL, sprintf("This endpoint is not available for league '%s' (only: %s).",
                                       league, paste(leagues, collapse = ", "))))
  }
  invisible(TRUE)
}
