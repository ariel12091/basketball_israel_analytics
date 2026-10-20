# svc_game_logs(): the one Game Logs service for both leagues. Each preset in
# tests/goldens/presets/game_logs.R is turned into the service's `p` by hand
# (preset_to_p(), beside the presets; svc_game_logs_params() builds the same
# shape from API input, pinned in api/tests) and run against the recorded database answers; the rows must equal what
# Shiny rendered for that preset -- the golden -- on every column the golden
# has except the Gameflow link, which Shiny adds after the service.

source(repo_file("tests", "goldens", "harness.R"), local = TRUE)
source(repo_file("tests", "goldens", "presets", "game_logs.R"), local = TRUE)

svc_gl_fixture_dir <- repo_file("tests", "goldens", "fixtures", "game_logs")
svc_gl_golden <- function(name) {
  readRDS(repo_file("tests", "goldens", "out", "game_logs", paste0(name, ".rds")))
}

# Golden columns minus the link, as a plain data frame with fresh row names.
svc_gl_comparable <- function(df, cols) {
  out <- as.data.frame(df)[, cols, drop = FALSE]
  rownames(out) <- NULL
  out
}

svc_gl_env <- golden_app_env(golden_replay_conn(svc_gl_fixture_dir))

test_that("svc_game_logs reproduces every Game Logs golden", {
  # Ruling R5: the union of the presets' ids, written as literals.
  covers("GL-01", "GL-02", "GL-03", "GL-04", "GL-05", "GL-06", "GL-07", "X-09")
  literal <- c("GL-01", "GL-02", "GL-03", "GL-04", "GL-05", "GL-06", "GL-07", "X-09")
  expect_setequal(literal, unique(unlist(lapply(GAME_LOGS_PRESETS, `[[`, "covers"))))

  for (name in names(GAME_LOGS_PRESETS)) {
    preset <- GAME_LOGS_PRESETS[[name]]
    p <- preset_to_p(preset, svc_gl_env)
    res <- svc_gl_env$svc_game_logs(p)
    golden <- svc_gl_golden(name)

    if (isTRUE(preset$empty)) {
      expect_null(golden, info = name)
      expect_null(res$rows, info = name)
    } else {
      cols <- setdiff(names(golden$data), "gameflow")
      expect_true(all(cols %in% names(res$rows)), info = name)
      expect_identical(svc_gl_comparable(res$rows, cols),
                       svc_gl_comparable(golden$data, cols), info = name)
    }

    round_col <- if (identical(p$league, "il")) "gn" else "round_number"
    expect_identical(res$meta$default_sort, list(
      list(column = "game_date", direction = "desc"),
      list(column = round_col, direction = "desc")
    ), info = name)
    expect_identical(res$meta$path, "fast", info = name)
    expect_identical(res$meta$percentile_population, "season", info = name)
    expect_identical(res$meta$filters, p, info = name)
  }
})

test_that("shot averages are reported for Israeli Summary only", {
  covers("GL-07")
  for (name in names(GAME_LOGS_PRESETS)) {
    preset <- GAME_LOGS_PRESETS[[name]]
    p <- preset_to_p(preset, svc_gl_env)
    res <- svc_gl_env$svc_game_logs(p)
    if (identical(p$league, "il") && p$view == "summary" && !is.null(res$rows)) {
      expect_identical(res$meta$shot_averages, svc_gl_env$gl_shot_averages(res$rows), info = name)
      expect_named(res$meta$shot_averages, c("off", "def"))
    } else {
      expect_null(res$meta$shot_averages, info = name)
    }
  }
})

test_that("the Shiny defaults match the descriptor", {
  covers("GL-01")
  expect_named(svc_gl_env$GL_LEAGUE, c("il", "euroleague"))
  expect_identical(svc_gl_env$GL_LEAGUE$il$round_col, "gn")
  expect_identical(svc_gl_env$GL_LEAGUE$euroleague$round_col, "round_number")
  expect_named(svc_gl_env$gl_default_readers(),
               c("schedule", "lineup_totals", "lineup_ff", "euro_rows", "scoreless"))
})

test_that("a blank end of the date range: open in Israel, no games in EuroLeague (D25)", {
  covers("GL-02")
  three <- data.frame(
    game_id = 1:3, team_id = 1L,
    game_date = as.Date(c("2025-10-01", "2025-11-01", "2025-12-01"))
  )
  base <- list(gn = svc_gl_env$resolve_gn_last_n_values("", "", ""))
  half_open <- utils::modifyList(base, list(dates = as.Date(c("2025-10-15", NA))))
  closed <- utils::modifyList(base, list(dates = as.Date(c("2025-10-15", "2025-11-15"))))

  expect_identical(svc_gl_env$gl_filter_games(three, half_open, "il")$game_id, 2:3)
  expect_identical(nrow(svc_gl_env$gl_filter_games(three, half_open, "euroleague")), 0L)
  for (league in c("il", "euroleague")) {
    expect_identical(svc_gl_env$gl_filter_games(three, closed, league)$game_id, 2L, info = league)
    expect_identical(svc_gl_env$gl_filter_games(three, base, league)$game_id, 1:3, info = league)
  }
  expect_error(svc_gl_env$gl_filter_games(three, base, "nba"), "unknown league")
})

test_that("last N counts games, not starter-split rows (D24 fixed)", {
  covers("GL-02")
  # Team 1: game 3 (newest) has three starter-split rows, game 2 two, game 1 one.
  rows <- data.frame(
    game_id = c(3L, 3L, 3L, 2L, 2L, 1L, 9L),
    team_id = c(1L, 1L, 1L, 1L, 1L, 1L, 2L),
    game_date = as.Date(c(rep("2025-12-01", 3), rep("2025-11-01", 2), "2025-10-01", "2025-12-05")),
    own_starters = c(5L, 4L, 3L, 5L, 2L, 5L, 5L)
  )
  p <- function(n) list(gn = svc_gl_env$resolve_gn_last_n_values("", "", as.character(n)))
  for (league in c("euroleague", "il")) {
    one <- svc_gl_env$gl_filter_games(rows, p(1), league)
    expect_setequal(one$game_id[one$team_id == 1L], 3L)
    expect_identical(sum(one$team_id == 1L), 3L, info = league)
    two <- svc_gl_env$gl_filter_games(rows, p(2), league)
    expect_setequal(two$game_id[two$team_id == 1L], c(3L, 2L))
    expect_identical(sum(two$team_id == 1L), 5L, info = league)
    expect_identical(sum(two$team_id == 2L), 1L, info = league)
  }
})

test_that("starters bounds: one reading for both leagues, one pinned NULL difference", {
  e <- svc_gl_env
  # Every value a mode/value pair of selectInputs (or the API) can hold.
  for (mode in list(NULL, "", "gte", "lte")) {
    for (val in list(NULL, "", "0", "3", "5")) {
      if (!is.null(val) || is.null(mode) || !nzchar(mode)) {
        il <- e$gl_starters_bounds(mode, val, mode, val)
        el <- e$gl_starters_bounds(mode, val, mode, val, keep_empty = TRUE)
        sb <- e$resolve_starters_bounds(mode, val, mode, val)
        info <- paste(mode %||% "NULL", val %||% "NULL")
        expect_identical(il, el, info = info)
        expect_identical(unname(il), unname(sb[c("num_starters_off_min", "num_starters_off_max",
                                                 "num_starters_def_min", "num_starters_def_max")]),
                         info = info)
      }
    }
  }
  expect_identical(e$gl_starters_bounds("gte", "4", "lte", "2"),
                   list(off_min = 4L, off_max = NA_integer_, def_min = NA_integer_, def_max = 2L))
  # A mode with a NULL value, which no control sends: Israeli Game Logs reads
  # no bound, EuroLeague keeps resolve_starters_bounds()'s integer(0).
  expect_identical(e$gl_starters_bounds("gte", NULL, "", NULL)$off_min, NA_integer_)
  expect_identical(e$gl_starters_bounds("gte", NULL, "", NULL, keep_empty = TRUE)$off_min, integer(0))
})

rm(svc_gl_env)
