source(repo_file("tests", "goldens", "harness.R"), local = TRUE)
source(repo_file("tests", "goldens", "presets", "lineups.R"), local = TRUE)
for (id in names(LINEUPS_PRESETS)) local({
  name <- id
  p <- LINEUPS_PRESETS[[name]]
  test_that(paste("Lineups golden replays:", name), {
    path <- repo_file("tests", "goldens", "out", "lineups", paste0(name, ".rds"))
    expect_true(file.exists(path), info = name)
    env <- golden_app_env(golden_replay_conn(
      repo_file("tests", "goldens", "fixtures", "lineups"), GOLDEN_STRICT_READERS_BY_TAB$lineups))
    got <- golden_run_server(env, p$inputs,
      golden_capture_dt(has_col = p$has_col, render_pattern = p$render_pattern))
    expect_identical(got, readRDS(path))
  })
})
