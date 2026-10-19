# handlers_reference.R - reference endpoints: the lists Shiny's dropdowns are
# filled from, each read through the same canonical lookup Shiny calls (so
# the same cache key and the same answer), plus the data versions.
#
# Every handler is handler(req, res, league): the query is read from `req`
# (api_query()), so tests call the handlers directly with a stub request.

API_IL <- "il"
API_EURO <- c("euroleague", "eurocup")

api_reference_meta <- function(rp) {
  list(league = rp$league, competition = rp$competition %||% NA, season = rp$season %||% NA)
}

# One reference list: validate, look up, respond.
api_reference <- function(req, res, league, slug, lookup, allowed = c("season", "format"),
                          leagues = c(API_IL, API_EURO)) {
  api_guard(res, {
    api_require_league(league, leagues)
    rp <- api_reference_params(api_query(req), league, allowed)
    rows <- lookup(rp)
    api_respond(res, list(rows = as.data.frame(rows %||% data.frame()), meta = api_reference_meta(rp)),
                rp$format, slug = paste(league, slug, sep = "_"))
  })
}

# GET /v1/meta/data-version -- {il, euroleague}: the version every season
# cache of that league is keyed on. EuroCup shares EuroLeague's.
handle_data_version <- function(req, res) {
  api_guard(res, {
    q <- api_query(req)
    svc_check_names(q, character(0))
    api_respond(res, list(il = svc_data_version("il"), euroleague = svc_data_version("euroleague")))
  })
}

# GET /v1/{league}/seasons -- seasons with data, newest first, with the label
# Shiny's selector shows.
handle_seasons <- function(req, res, league) {
  api_reference(req, res, league, "seasons", allowed = "format", function(rp) {
    s <- svc_seasons(league)
    data.frame(season = s, label = season_label(s, ending_year = identical(league, API_IL)),
               stringsAsFactors = FALSE)
  })
}

# GET /v1/{league}/teams?season=
handle_teams <- function(req, res, league) {
  api_reference(req, res, league, "teams", function(rp) {
    if (identical(league, API_IL)) fetch_teams_min(rp$season) else euro_fetch_teams(rp$competition, rp$season)
  })
}

# GET /v1/{league}/players?season=
handle_players <- function(req, res, league) {
  api_reference(req, res, league, "players", function(rp) {
    if (identical(league, API_IL)) fetch_players_basic(rp$season) else euro_fetch_players_basic(rp$competition, rp$season)
  })
}

# GET /v1/il/game-numbers?season=
handle_game_numbers <- function(req, res, league) {
  api_reference(req, res, league, "game_numbers", leagues = API_IL, function(rp) fetch_gn_values(rp$season))
}

# GET /v1/il/game-types -- the game-type choices: `value` is what `game_type`
# accepts (any one code of it), `label` what Shiny shows.
handle_game_types <- function(req, res, league) {
  api_reference(req, res, league, "game_types", allowed = "format", leagues = API_IL, function(rp) {
    ch <- GAME_TYPE_CHOICES_UI[nzchar(GAME_TYPE_CHOICES_UI)]
    data.frame(value = unname(ch), label = names(ch), stringsAsFactors = FALSE)
  })
}

# GET /v1/{euroleague|eurocup}/phases?season= -- provider phase codes and
# their labels.
handle_phases <- function(req, res, league) {
  api_reference(req, res, league, "phases", leagues = API_EURO, function(rp) {
    ph <- euro_fetch_phases(rp$competition, rp$season)
    codes <- as.character(ph$phase)
    data.frame(phase = codes, label = unname(euro_phase_label(codes)), stringsAsFactors = FALSE)
  })
}

# GET /v1/{euroleague|eurocup}/rounds?season=
handle_rounds <- function(req, res, league) {
  api_reference(req, res, league, "rounds", leagues = API_EURO, function(rp) {
    r <- euro_fetch_round_values(rp$competition, rp$season)
    data.frame(round_number = r$gn)
  })
}
