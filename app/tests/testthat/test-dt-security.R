test_that("DT escaping is fail-closed with explicit HTML allowlists", {
  df <- data.frame(
    Team = "<img src=x onerror='window.__xss = true'>",
    Rating = "112.4<br>#1<br>\u25b21",
    check.names = FALSE
  )

  escape_cols <- dt_escape_except(df, "Rating")
  expect_equal(escape_cols, 1L)

  widget <- DT::datatable(df, rownames = FALSE, escape = escape_cols)
  rendered <- widget[["preRenderHook"]](widget)[["x"]][["data"]]

  expect_match(rendered[[1]], "&lt;img", fixed = TRUE)
  expect_false(grepl("<img", rendered[[1]], fixed = TRUE))
  expect_match(rendered[[2]], "<br>", fixed = TRUE)
})

test_that("active DT tables do not disable escaping globally", {
  server_files <- list.files(
    repo_file("R"),
    pattern = "^server_tab.*\\.R$",
    full.names = TRUE
  )
  code <- paste(unlist(lapply(server_files, readLines, warn = FALSE)), collapse = "\n")

  expect_false(grepl("escape\\s*=\\s*FALSE", code))
})
