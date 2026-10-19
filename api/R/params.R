# params.R - the API's view of a request's parameters.
#
# Validation itself belongs to the service layer (app/R/svc_params.R and each
# svc_*_params()); this file only turns a request into the query those take,
# and checks the reference endpoints' few parameters with the same parsers.

# The query string as a named list, one character element per occurrence, in
# order. Read from the raw string rather than Plumber's parsed args, which
# drop a name with an empty value (`team=`) and merge repeated names -- both
# must reach the validators so that they can reject or collect them.
api_query <- function(req) {
  qs <- req$QUERY_STRING %||% ""
  qs <- sub("^[?]", "", qs)
  if (!nzchar(qs)) return(list())
  pieces <- strsplit(qs, "&", fixed = TRUE)[[1]]
  pieces <- pieces[nzchar(pieces)]
  decode <- function(x) httpuv::decodeURIComponent(chartr("+", " ", x))
  keys <- vapply(pieces, function(p) decode(sub("=.*$", "", p)), character(1), USE.NAMES = FALSE)
  vals <- vapply(pieces, function(p) if (grepl("=", p, fixed = TRUE)) decode(sub("^[^=]*=", "", p)) else "",
                 character(1), USE.NAMES = FALSE)
  stats::setNames(as.list(vals), keys)
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
