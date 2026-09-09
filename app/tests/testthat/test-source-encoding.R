test_that("production R source is native-encoding safe", {
  files <- c(
    repo_file("app.R"),
    list.files(repo_file("R"), pattern = "[.]R$", full.names = TRUE)
  )

  raw_unicode <- vapply(files, function(path) {
    bytes <- readBin(path, what = "raw", n = file.info(path)$size)
    any(as.integer(bytes) > 127L)
  }, logical(1))

  r_unicode_escape <- vapply(files, function(path) {
    text <- paste(readLines(path, warn = FALSE), collapse = "\n")
    grepl("(^|[^\\\\])\\\\u[0-9A-Fa-f]{4}", text, perl = TRUE)
  }, logical(1))

  expect_false(
    any(raw_unicode),
    info = paste("Raw non-ASCII source:", paste(basename(files[raw_unicode]), collapse = ", "))
  )
  expect_false(
    any(r_unicode_escape),
    info = paste("Parse-time Unicode escapes:", paste(basename(files[r_unicode_escape]), collapse = ", "))
  )
})
