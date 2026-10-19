# How a Game Logs API response is compared to what Shiny rendered: shared by
# test-api-game-logs.R (API vs the recorded goldens) and the app's nightly
# test-api-parity-live-db.R (API vs Shiny, both on the live database).

# A Shiny column's API key: the shot cells' display names become snake_case
# and the Gameflow link becomes the has_scores flag it was drawn from.
# Deliberately restates the rename contract (GL_API_KEYS in
# handlers_game_logs.R) instead of reading it, so a changed key fails here.
gl_api_key <- function(cols) {
  renames <- c("Off Shot" = "off_shot_fga", "Def Shot" = "def_shot_fga", gameflow = "has_scores")
  out <- renames[cols]
  unname(ifelse(is.na(out), cols, out))
}

# The API's rows (the payload frame, before serialisation) equal a captured
# Shiny table (golden_capture_dt()): the same rows in the same order, every
# column value for value, and a Gameflow link exactly where has_scores is
# TRUE. A NULL capture (the renderer drew no table) must meet no rows.
gl_expect_rows_match <- function(rows, captured, info) {
  if (is.null(captured)) {
    testthat::expect_identical(nrow(rows), 0L, info = info)
    return(invisible())
  }
  testthat::expect_identical(nrow(rows), nrow(captured$data), info = info)
  for (col in setdiff(names(captured$data), "gameflow")) {
    testthat::expect_identical(rows[[gl_api_key(col)]], captured$data[[col]], info = paste(info, col))
  }
  testthat::expect_true(is.logical(rows$has_scores), info = info)
  testthat::expect_false(anyNA(rows$has_scores), info = info)
  testthat::expect_identical(rows$has_scores,
                             grepl("ribbon-link", captured$data$gameflow, fixed = TRUE), info = info)
  invisible()
}
