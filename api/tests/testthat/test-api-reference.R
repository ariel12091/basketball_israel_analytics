# Reference endpoints answer with Shiny's own lookups (replayed answers), the
# data version follows app_meta (Review Focus 5), and errors map to the right
# status without leaking query text.

ref_env <- api_test_env()

call_handler <- function(env, handler, qs = "", ...) {
  res <- api_res()
  out <- env[[handler]](api_req(qs), res, ...)
  expect_identical(out, res)
  res
}

test_that("Israeli teams equal fetch_teams_min()", {
  covers("X-04")
  res <- call_handler(ref_env, "handle_teams", "season=2026", "il")
  expect_identical(res$status, 200L)
  body <- api_body(res)
  expected <- ref_env$fetch_teams_min(2026)
  expect_equal(body$rows, expected, ignore_attr = TRUE)
  expect_identical(body$meta$league, "il")
  expect_identical(body$meta$season, 2026L)
  expect_null(body$meta$competition)
})

test_that("every reference list equals the lookup Shiny calls", {
  covers("X-04")
  rows_of <- function(handler, qs, league) api_body(call_handler(ref_env, handler, qs, league))$rows

  expect_equal(rows_of("handle_players", "season=2026", "il"),
               ref_env$fetch_players_basic(2026), ignore_attr = TRUE)
  expect_identical(rows_of("handle_game_numbers", "season=2026", "il")$gn,
                   ref_env$fetch_gn_values(2026)$gn)
  expect_equal(rows_of("handle_teams", "season=2025", "euroleague"),
               ref_env$euro_fetch_teams("E", 2025), ignore_attr = TRUE)
  expect_identical(rows_of("handle_phases", "season=2025", "euroleague")$phase,
                   as.character(ref_env$euro_fetch_phases("E", 2025)$phase))
  expect_identical(rows_of("handle_phases", "season=2025", "euroleague")$label,
                   unname(ref_env$euro_phase_label(ref_env$euro_fetch_phases("E", 2025)$phase)))
  expect_identical(rows_of("handle_rounds", "season=2025", "euroleague")$round_number,
                   ref_env$euro_fetch_round_values("E", 2025)$gn)

  gt <- rows_of("handle_game_types", "", "il")
  ui <- ref_env$GAME_TYPE_CHOICES_UI[nzchar(ref_env$GAME_TYPE_CHOICES_UI)]
  expect_identical(gt$value, unname(ui))
  expect_identical(gt$label, names(ui))

  il_seasons <- rows_of("handle_seasons", "", "il")
  expect_identical(il_seasons$season, sort(as.integer(names(ref_env$STATIC_TEAM_ROSTERS)), decreasing = TRUE))
  expect_identical(il_seasons$label, ref_env$season_label(il_seasons$season))
  el_seasons <- rows_of("handle_seasons", "", "euroleague")
  expect_identical(el_seasons$season, sort(as.integer(ref_env$euro_fetch_seasons("E")$game_year), decreasing = TRUE))
  expect_identical(el_seasons$label, ref_env$season_label(el_seasons$season, ending_year = FALSE))
})

test_that("EuroLeague players come from euro_fetch_players_basic()", {
  covers("X-04")
  # Not among the Game Logs recordings: store a hand-made answer under the
  # key of the query the lookup actually runs.
  key <- api_fixture_key_of(function(env) env$euro_fetch_players_basic("E", 2025))
  dir <- api_copy_fixtures()
  players <- data.frame(player_id = c(11L, 12L), name = c("A PLAYER", "B PLAYER"), team_id = c(1L, 2L))
  saveRDS(players, file.path(dir, paste0(key, ".rds")))
  env <- api_test_env(dir)
  body <- api_body(call_handler(env, "handle_players", "season=2025", "euroleague"))
  expect_equal(body$rows, players, ignore_attr = TRUE)
  expect_identical(body$meta$competition, "E")
})

test_that("reference endpoints reject bad parameters and the wrong league", {
  covers("X-01", "X-20")
  check <- function(handler, qs, league, status, param) {
    res <- call_handler(ref_env, handler, qs, league)
    body <- api_body(res)
    expect_identical(res$status, status, info = paste(handler, qs, league))
    expect_identical(body$param, param, info = paste(handler, qs, league))
    expect_true(nzchar(body$error))
  }
  check("handle_teams", "season=x", "il", 400L, "season")
  check("handle_teams", "season=1999", "il", 400L, "season")
  check("handle_teams", "season=2026&team=2", "il", 400L, "team")
  check("handle_seasons", "season=2026", "il", 400L, "season")
  dv <- call_handler(ref_env, "handle_data_version", "format=csv")
  expect_identical(c(dv$status, api_body(dv)$param), c(400L, "format"))
  check("handle_teams", "season=2025", "nba", 404L, "league")

  for (case in list(c("handle_game_numbers", "euroleague"), c("handle_game_types", "eurocup"),
                    c("handle_phases", "il"), c("handle_rounds", "il"))) {
    res <- call_handler(ref_env, case[[1]], "", case[[2]])
    expect_identical(res$status, 404L, info = paste(case, collapse = " "))
    expect_null(api_body(res)$param)
  }
})

test_that("the data version follows app_meta, and a new version is a new cache key (Review Focus 5)", {
  covers("X-08")
  # Two recordings of the same database at two ETL versions: B has a later
  # app_meta stamp, a later EuroLeague load, and a schedule that lost games.
  dir_a <- api_copy_fixtures()
  dir_b <- api_copy_fixtures()
  app_meta_key <- "b46a5063b28c26688e0642a3d39a8c056f5920af"
  load_runs_key <- "cb020d3ac9ec5e72483264ef7798b1717c140e5b"
  schedule_key <- "4b853ab9133ac3d121818e01f1bfcf840c6f09f5"
  read_fixture <- function(dir, key) readRDS(file.path(dir, paste0(key, ".rds")))
  write_fixture <- function(dir, key, value) saveRDS(value, file.path(dir, paste0(key, ".rds")))
  expect_identical(names(read_fixture(dir_a, app_meta_key)), "value")
  write_fixture(dir_b, app_meta_key, data.frame(value = "2099-01-02 03:04:05"))
  write_fixture(dir_b, load_runs_key, data.frame(v = as.POSIXct("2099-01-01 00:00:00", tz = "UTC")))
  sched <- read_fixture(dir_a, schedule_key)
  write_fixture(dir_b, schedule_key, sched[sched$game_id %in% utils::head(unique(sched$game_id), 5L), ])

  env <- api_test_env(dir_a)
  version_now <- function() api_body(call_handler(env, "handle_data_version", ""))
  use_recording <- function(dir) {
    env$pg_pool <- golden_replay_conn(dir)
    # Shiny's app_meta poll is cached for 55 s and the EuroLeague version for
    # the reference TTL; a new ETL is seen when they lapse. Lapse them now.
    suppressWarnings(rm(list = c("app_meta_last_success", "euro_data_version"), envir = env$.ref_cache_env))
  }
  game_logs <- function() {
    p <- env$svc_game_logs_params(list(season = "2026"), "il")
    list(p = p, res = env$svc_game_logs(p))
  }

  v_a <- version_now()
  expect_identical(v_a$il, read_fixture(dir_a, app_meta_key)$value)
  expect_identical(v_a$euroleague, as.character(read_fixture(dir_a, load_runs_key)$v))
  first_a <- game_logs()
  expect_identical(first_a$p$data_version, v_a$il)
  keys_a <- env$GL_DATA_CACHE$keys()

  use_recording(dir_b)
  v_b <- version_now()
  expect_identical(v_b$il, "2099-01-02 03:04:05")
  expect_false(identical(v_b$euroleague, v_a$euroleague))
  first_b <- game_logs()
  expect_identical(first_b$p$data_version, v_b$il)
  # New keys for the new version; the old version's entries are untouched.
  expect_true(length(setdiff(env$GL_DATA_CACHE$keys(), keys_a)) > 0L)
  expect_true(all(keys_a %in% env$GL_DATA_CACHE$keys()))

  # Each version is served its own pull: B's lost games are gone under B ...
  expect_true(nrow(first_b$res$rows) < nrow(first_a$res$rows))
  expect_true(all(first_b$res$rows$game_id %in% sched$game_id[sched$game_id %in% utils::head(unique(sched$game_id), 5L)]))
  # ... and A, asked for again by its version, is A's answer, not B's.
  again_a <- env$svc_game_logs(first_a$p)
  expect_identical(again_a$rows, first_a$res$rows)
})

test_that("JSON and CSV bodies; errors never leak query text", {
  covers("X-14")
  res <- call_handler(ref_env, "handle_teams", "season=2026&format=csv", "il")
  expect_identical(res$status, 200L)
  expect_match(res$headers[["Content-Type"]], "^text/csv")
  expect_match(res$headers[["Content-Disposition"]], "^attachment; filename=\"il_teams_[0-9]{8}_[0-9]{6}[.]csv\"$")
  csv <- utils::read.csv(text = res$body, stringsAsFactors = FALSE)
  expect_equal(csv, ref_env$fetch_teams_min(2026), ignore_attr = TRUE)

  json <- call_handler(ref_env, "handle_teams", "season=2026", "il")
  expect_match(json$headers[["Content-Type"]], "^application/json")

  sql_text <- "SELECT secret FROM basketball_test.app_meta"
  timeout <- ref_env$api_respond_error(api_res(), simpleError(
    paste("ERROR: canceling statement due to statement timeout", sql_text)))
  expect_identical(timeout$status, 504L)
  expect_false(grepl("SELECT", timeout$body, fixed = TRUE))
  expect_match(api_body(timeout)$error, "statement timeout")

  other <- ref_env$api_respond_error(api_res(), simpleError(paste("relation does not exist:", sql_text)))
  expect_identical(other$status, 500L)
  expect_false(grepl("SELECT", other$body, fixed = TRUE))

  # A query that fails on the database inside a handler is the same 500.
  empty <- tempfile("api_none_")
  dir.create(empty)
  broken <- api_test_env(empty)
  res <- call_handler(broken, "handle_players", "season=2026", "il")
  expect_identical(res$status, 500L)
  expect_false(grepl("full_rosters", res$body, fixed = TRUE))

  na_body <- ref_env$api_respond(api_res(), list(rows = data.frame(a = c(1.25, NA)), x = NULL))
  expect_identical(na_body$body, "{\"rows\":[{\"a\":1.25},{\"a\":null}],\"x\":null}")
})
