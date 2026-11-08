# Nightly only: a replay of every preset takes minutes, so PRs skip it (RUN_GOLDENS=1 runs it).
testthat::skip_if_not(identical(Sys.getenv("RUN_GOLDENS"), "1"), "goldens run nightly (RUN_GOLDENS=1)")

source(repo_file("tests", "goldens", "harness.R"), local = TRUE)
source(repo_file("tests", "goldens", "presets", "onoff.R"), local = TRUE)

for (id in names(ONOFF_PRESETS)) local({
  name <- id
  p <- ONOFF_PRESETS[[name]]
  test_that(paste("On/Off golden replays:", name), {
    path <- repo_file("tests", "goldens", "out", "onoff", paste0(name, ".rds"))
    expect_true(file.exists(path), info = name)
    env <- golden_app_env(golden_replay_conn(
      repo_file("tests", "goldens", "fixtures", "onoff"), GOLDEN_STRICT_READERS_BY_TAB$onoff))
    got <- golden_run_server(env, p$inputs, golden_capture_dt(has_col = p$has_col))
    expect_identical(got, readRDS(path))
  })
})
