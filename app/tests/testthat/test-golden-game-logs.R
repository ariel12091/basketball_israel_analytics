# Game Logs goldens: what today's Shiny renders for Israeli Tab 4 and
# EuroLeague Tab 11, for every preset in tests/goldens/presets/game_logs.R.
# Each preset replays the recorded database answers through the real app
# server and must reproduce its golden exactly. Later tasks move this logic
# into shared code; these goldens are what that move must keep identical.
#
# Fixtures and goldens are (re)written by scripts/capture_goldens.R.

source(repo_file("tests", "goldens", "harness.R"), local = TRUE)
source(repo_file("tests", "goldens", "presets", "game_logs.R"), local = TRUE)

gl_fixture_dir <- repo_file("tests", "goldens", "fixtures", "game_logs")
gl_golden_path <- function(name) repo_file("tests", "goldens", "out", "game_logs", paste0(name, ".rds"))

test_that("every preset is well formed and the literal coverage set matches", {
  # Ruling R5: the coverage scanner sees only literals, so the union of the
  # presets' ids is written out here and pinned to the preset file.
  covers("GL-01", "GL-02", "GL-03", "GL-04", "GL-05", "GL-06", "GL-07", "X-09")
  literal <- c("GL-01", "GL-02", "GL-03", "GL-04", "GL-05", "GL-06", "GL-07", "X-09")
  expect_setequal(literal, unique(unlist(lapply(GAME_LOGS_PRESETS, `[[`, "covers"))))

  expect_length(GAME_LOGS_PRESETS, 15L)
  for (name in names(GAME_LOGS_PRESETS)) {
    p <- GAME_LOGS_PRESETS[[name]]
    expect_true(p$league %in% c("il", "euroleague"), info = name)
    steps <- golden_input_steps(p$inputs)
    first <- steps[[1]]
    expect_true(nzchar(first$main_tabs %||% ""), info = name)
    if (identical(p$league, "il")) {
      expect_identical(first$game_year, "2026", info = name)
      expect_identical(first$main_tabs, "game_logs", info = name)
    } else {
      expect_identical(first$league_select, "E", info = name)
      expect_identical(first$euro_game_year, "2025", info = name)
      expect_identical(first$main_tabs, "euro_game_logs", info = name)
    }
  }
})

test_that("golden files exist for every preset", {
  covers("GL-01")
  missing <- Filter(function(n) !file.exists(gl_golden_path(n)), names(GAME_LOGS_PRESETS))
  expect_identical(missing, character(0))
  expect_true(dir.exists(gl_fixture_dir))
})

for (gl_preset_name in names(GAME_LOGS_PRESETS)) {
  local({
    name <- gl_preset_name
    preset <- GAME_LOGS_PRESETS[[name]]
    test_that(paste("Game Logs golden replays:", name), {
      covers(preset$covers)
      skip_if_not(file.exists(gl_golden_path(name)), "golden not captured")
      env <- golden_app_env(golden_replay_conn(gl_fixture_dir))
      captured <- golden_run_server(
        env, preset$inputs,
        golden_capture_dt(preset$slug, allow_none = isTRUE(preset$empty))
      )
      golden <- readRDS(gl_golden_path(name))
      expect_identical(captured, golden)
      if (isTRUE(preset$empty)) {
        expect_null(golden)
      } else {
        expect_gt(nrow(golden$data), 0L)
      }
    })
  })
}
rm(gl_preset_name)
