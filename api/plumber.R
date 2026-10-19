# plumber.R - routes. Each route only calls a handler (api/R/handlers_*.R),
# which run.R has loaded with api_bootstrap(); logic never lives here.
# {league} is il | euroleague | eurocup.

#* @plumber
function(pr) {
  pr |>
    plumber::pr_set_404(function(req, res) {
      api_respond(res, list(error = "Not found.", param = NA), status = 404L)
    }) |>
    plumber::pr_set_error(function(req, res, err) {
      api_respond_error(res, err)
    })
}

#* @get /v1/meta/data-version
function(req, res) handle_data_version(req, res)

#* @get /v1/<league>/seasons
function(req, res, league) handle_seasons(req, res, league)

#* @get /v1/<league>/teams
function(req, res, league) handle_teams(req, res, league)

#* @get /v1/<league>/players
function(req, res, league) handle_players(req, res, league)

#* @get /v1/<league>/game-numbers
function(req, res, league) handle_game_numbers(req, res, league)

#* @get /v1/<league>/game-types
function(req, res, league) handle_game_types(req, res, league)

#* @get /v1/<league>/phases
function(req, res, league) handle_phases(req, res, league)

#* @get /v1/<league>/rounds
function(req, res, league) handle_rounds(req, res, league)
