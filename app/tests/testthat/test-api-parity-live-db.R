library(testthat)

# Nightly: Shiny (Game Logs, both leagues) and the API, side by side on the
# LIVE database in the same run, must agree preset by preset. The golden tests
# prove each side against recorded answers; this proves they still agree on
# today's data, including the current season, which no golden records.
#
# Reads only. One pool, one connection, port 5432 whatever PG_PORT says: the
# 6543 transaction pooler crosses concurrent prepared statements (CLAUDE.md,
# port-6543 pitfall). Do not run beside a local app or another DB test.

skip_if_not(Sys.getenv("RUN_DB_TESTS", "0") == "1")
skip_if_not_installed("DBI")
skip_if_not_installed("RPostgres")
skip_if_not_installed("pool")
# Fails, not skips: CI installs plumber for this test, and a nightly parity
# check that silently never runs proves nothing.
if (!requireNamespace("plumber", quietly = TRUE)) {
  stop("test-api-parity-live-db: RUN_DB_TESTS=1 but plumber is not installed ",
       "(.github/workflows/r-tests.yml installs it)", call. = FALSE)
}

pg_env <- c("PG_HOST", "PG_DB", "PG_USER", "PG_PASS")
missing_env <- pg_env[!nzchar(Sys.getenv(pg_env))]
if (length(missing_env)) {
  skip(paste("Missing DB env:", paste(missing_env, collapse = ", ")))
}

parity_root <- repo_file("..")
source(file.path(parity_root, "app", "tests", "goldens", "harness.R"), local = TRUE)
source(file.path(parity_root, "app", "tests", "goldens", "presets", "game_logs.R"), local = TRUE)
source(file.path(parity_root, "api", "R", "bootstrap.R"), local = TRUE)
source(file.path(parity_root, "api", "tests", "testthat", "helper-presets.R"), local = TRUE)
source(file.path(parity_root, "api", "tests", "testthat", "helper-game-logs-compare.R"), local = TRUE)

# The API's own pool builder, held to port 5432, one connection, and the
# capture script's 120 s statement timeout (season pulls on a cold server).
if (!identical(trimws(Sys.getenv("PG_PORT", "5432")), "5432")) {
  message("test-api-parity-live-db: PG_PORT=", Sys.getenv("PG_PORT"), " overridden; using 5432.")
}
parity_pool <- withr::with_envvar(
  c(PG_PORT = "5432", PG_STATEMENT_TIMEOUT_MS = "120000", POOL_MAX = "1"),
  api_create_pool()
)
withr::defer(pool::poolClose(parity_pool))

parity_api <- new.env(parent = globalenv())
suppressMessages(api_bootstrap(parity_pool, envir = parity_api, root = parity_root))

# The presets, plus the two defaults on the current season, which grows every
# night: Israeli DEFAULT_GAME_YEAR and EuroLeague euro_default_season(), with
# the date input at that season's own bounds as the app sets it.
parity_presets <- local({
  gy <- parity_api$DEFAULT_GAME_YEAR
  es <- parity_api$euro_default_season()
  il_b <- parity_api$season_date_bounds_for_year(gy)
  el_b <- parity_api$euro_season_date_bounds(es)
  # A season may be bumped before its first game; only these two may be empty.
  il <- GAME_LOGS_PRESETS$il_summary_default
  il$inputs <- utils::modifyList(il$inputs, list(game_year = gy, gl_dates = c(il_b$start, il_b$end)))
  il$query <- paste0("season=", gy)
  il$may_be_empty <- TRUE
  el <- GAME_LOGS_PRESETS$el_summary_default
  el$inputs <- utils::modifyList(el$inputs, list(euro_game_year = es,
                                                 eurogl_dates = c(el_b$start, el_b$end)))
  el$query <- paste0("season=", es)
  el$may_be_empty <- TRUE
  out <- lapply(stats::setNames(nm = names(GAME_LOGS_PRESETS)), function(name) {
    p <- GAME_LOGS_PRESETS[[name]]
    p$query <- preset_to_query(name)
    p
  })
  out[[paste0("il_summary_default_", gy)]] <- il
  out[[paste0("el_summary_default_", es)]] <- el
  out
})

test_that("the current-season copies are on the current season", {
  gy <- parity_api$DEFAULT_GAME_YEAR
  es <- parity_api$euro_default_season()
  il <- parity_presets[[paste0("il_summary_default_", gy)]]
  el <- parity_presets[[paste0("el_summary_default_", es)]]
  expect_identical(il$inputs$game_year, gy)
  expect_identical(el$inputs$euro_game_year, es)
  expect_identical(il$inputs$gl_dates, c(parity_api$season_date_bounds_for_year(gy)$start,
                                         parity_api$season_date_bounds_for_year(gy)$end))
  expect_identical(el$inputs$eurogl_dates, c(parity_api$euro_season_date_bounds(es)$start,
                                             parity_api$euro_season_date_bounds(es)$end))
})

for (parity_name in names(parity_presets)) {
  local({
    name <- parity_name
    preset <- parity_presets[[name]]
    test_that(paste("live parity, Shiny vs API, Game Logs:", name), {
      covers("GL-01", "GL-02", "GL-03", "GL-04", "GL-05", "GL-06")
      started <- proc.time()[["elapsed"]]
      captured <- golden_run_server(
        golden_app_env(parity_pool), preset$inputs,
        golden_capture_dt(preset$slug, allow_none = TRUE)
      )
      rows <- parity_api$gl_api_payload(
        parity_api$api_query(list(QUERY_STRING = preset$query)), preset_api_league(preset)
      )$payload$rows

      # A table that should have rows but rendered none is a failure, not an
      # agreement: the app catches query errors in observers and only logs them.
      if (isTRUE(preset$empty)) {
        expect_null(captured)
      } else if (!isTRUE(preset$may_be_empty)) {
        expect_false(is.null(captured), info = name)
        expect_gt(nrow(rows), 0L)
      }
      gl_expect_rows_match(rows, captured, name)
      cat(sprintf("\n[live parity] %-30s %5d rows  %.1fs", name, nrow(rows),
                  proc.time()[["elapsed"]] - started))
    })
  })
}
rm(parity_name)
