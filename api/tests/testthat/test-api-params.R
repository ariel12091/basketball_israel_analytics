# svc_game_logs_params(): an untrusted query -> svc_game_logs()'s `p`.
# Malformed values are 400s naming the parameter (Review Focus 3); a valid
# query gives exactly the list Shiny builds for the same filters (Ruling R1:
# preset_to_p() is the Shiny-input side, beside the presets).

params_env <- api_test_env()

gl_params <- function(qs, league = "il") {
  params_env$svc_game_logs_params(params_env$api_query(api_req(qs)), league, data_version = "golden")
}

test_that("malformed or unsupported values are 400s naming the parameter", {
  covers("GL-01", "GL-02", "X-02", "X-05", "X-09", "X-20")
  cases <- list(
    list("team=abc", "team"),
    list("last_n=-3", "last_n"),
    list("last_n=0", "last_n"),
    list("last_n=81", "last_n"),
    list("gn_min=20&gn_max=5", "gn_min"),
    list("last_n=5&gn_min=3", "last_n"),
    list("season=2026&date_from=2026-02-01&date_to=2026-01-01", "date_from"),
    list("date_from=2026-02-30", "date_from"),
    list("season=2026&date_to=2027-01-01", "date_to"),
    list("home_away=both", "home_away"),
    list("outcome=draw", "outcome"),
    list("stat=net_rtg:gt:0", "stat"),
    list("stat=net_rtg:ge:abc", "stat"),
    list("stat=off_efg_pct:ge:50", "stat"),
    list("view=four_factors&stat=net_rtg:ge:0", "stat"),
    list("foo=1", "foo"),
    list("season=1999", "season"),
    list("season=x", "season"),
    list("season=2026&season=2027", "season"),
    list("team=", "team"),
    list("team=2,3", "team"),
    list("view=traditional", "view"),
    list("game_type=99", "game_type"),
    list("opponents=2,,3", "opponents"),
    list("own_starters=gte:6", "own_starters"),
    list("opp_starters=eq:2", "opp_starters"),
    list("format=xml", "format"),
    list("sort=net_rtg", "sort"),
    # The other league's names are unknown here, not ignored.
    list("teams=2", "teams"),
    list("phase=RS", "phase"),
    list("round_min=1", "round_min")
  )
  for (case in cases) {
    err <- api_error_of(gl_params(case[[1]]))
    expect_identical(err$status, 400L, info = case[[1]])
    expect_identical(err$param, case[[2]], info = case[[1]])
    expect_true(nzchar(conditionMessage(err)), info = case[[1]])
  }
})

test_that("undecodable or oversized input is a 400 naming the parameter, never echoed raw", {
  covers("X-20")
  cases <- list(
    list("season=%00", "season"),
    list("season=%FF", "season"),
    list("opponents=%FF", "opponents"),
    list("game_type=%FF", "game_type"),
    list("stat=%FF", "stat"),
    list("sort=%FF", "sort"),
    list("date_from=%FF", "date_from"),
    list("home_away=%FF", "home_away"),
    list("own_starters=%FF", "own_starters"),
    list("foo%FF=1", "query"),
    list("%FF=1", "query"),
    list("%00=1", "query"),
    list("=1", "query"),
    list(paste0("stat=net_rtg:ge:", strrep("9", 400)), "stat"),
    list(paste0("home_away=", strrep("x", 500)), "home_away"),
    list(paste0(strrep("y", 500), "=1"), strrep("y", 500))
  )
  for (case in cases) {
    label <- substr(case[[1]], 1, 40)
    err <- api_error_of(gl_params(case[[1]]))
    expect_identical(err$status, 400L, info = label)
    expect_identical(err$param, case[[2]], info = label)
    expect_true(nchar(conditionMessage(err)) < 400L, info = label)
    body <- params_env$api_respond_error(api_res(), err)$body
    expect_true(validUTF8(body), info = label)
    expect_false(grepl(strrep("x", 65), body, fixed = TRUE), info = label)
  }
})

test_that("EuroLeague rejects Israeli-only names and checks its own values", {
  covers("X-01", "X-05")
  cases <- list(
    list("season=2025&team=1", "team"),
    list("season=2025&gn_min=1", "gn_min"),
    list("season=2025&stat=net_rtg:ge:0", "stat"),
    list("season=2025&game_type=5", "game_type"),
    list("season=2025&phase=XX", "phase"),
    list("season=2025&teams=a,b", "teams"),
    list("season=2025&round_min=9&round_max=3", "round_min"),
    list("season=1999", "season")
  )
  for (case in cases) {
    err <- api_error_of(gl_params(case[[1]], "euroleague"))
    expect_identical(err$status, 400L, info = case[[1]])
    expect_identical(err$param, case[[2]], info = case[[1]])
  }
  err <- api_error_of(gl_params("", "nba"))
  expect_identical(err$status, 404L)
  expect_identical(err$param, "league")
})

test_that("a valid query gives exactly the p Shiny builds for the same filters", {
  covers("GL-01", "GL-02", "X-03", "X-05", "X-09")
  # The preset -> query table lives in helper-presets.R (preset_to_query()).
  queries <- GL_PRESET_QUERIES
  # Every preset, so a new preset without a query here fails.
  expect_setequal(names(queries), names(GAME_LOGS_PRESETS))
  for (name in names(queries)) {
    preset <- GAME_LOGS_PRESETS[[name]]
    expect_identical(gl_params(preset_to_query(name), preset_api_league(preset)),
                     preset_to_p(preset, params_env), info = name)
  }
})

test_that("Winner Cup is one choice reached by either code; codes keep their order", {
  covers("X-05")
  expect_identical(gl_params("season=2026&game_type=10")$game_type, "10,34")
  expect_identical(gl_params("season=2026&game_type=34")$game_type, "10,34")
  expect_identical(gl_params("season=2026&game_type=35,10,34,5")$game_type, c("35", "10,34", "5"))
})

test_that("defaults are Shiny's: league default season, the season's own date window, Summary", {
  covers("X-02", "X-03")
  p <- gl_params("")
  expect_identical(p$season, as.integer(params_env$DEFAULT_GAME_YEAR))
  b <- params_env$season_date_bounds_for_year(p$season)
  expect_identical(p$dates, c(b$start, b$end))
  expect_identical(p$view, "summary")
  expect_identical(p$team, "")
  expect_identical(p$stat_filters, list())
  expect_null(p$competition)

  el <- gl_params("season=2025", "euroleague")
  eb <- params_env$euro_season_date_bounds(2025L)
  expect_identical(el$dates, c(eb$start, eb$end))
  expect_identical(el$competition, "E")
  expect_identical(el$teams, numeric(0))
})

test_that("the data version is read where Shiny reads it unless given", {
  covers("X-08")
  p <- params_env$svc_game_logs_params(list(season = "2026"), "il")
  expect_identical(p$data_version, params_env$shared_data_version(
    list(data_version = function() params_env$etl_last_success_db())
  ))
  expect_false(identical(p$data_version, "unknown"))
})

test_that("the raw query string keeps empty values and repeated names", {
  q <- params_env$api_query(api_req("?a=1&b=&c&a=2&d=x%2Cy&e=two+words"))
  expect_identical(names(q), c("a", "b", "c", "a", "d", "e"))
  expect_identical(unname(unlist(q)), c("1", "", "", "2", "x,y", "two words"))
  expect_identical(params_env$api_query(api_req("")), list())
  expect_length(gl_params("stat=net_rtg:ge:0&stat=off_ppp:le:110")$stat_filters, 2L)
})

test_that("the router's query filter leaves the raw query to api_query()", {
  covers("X-20")
  req <- new.env()
  req$QUERY_STRING <- "?season=%00"
  req$.internal <- new.env()
  params_env$api_query_filter(req)
  expect_identical(req$argsQuery, list())
  expect_true(req$.internal$queryStringHandled)
})
