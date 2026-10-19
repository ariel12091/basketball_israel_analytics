css_rule_body <- function(css, selector) {
  escaped <- gsub("([][{}()+*^$|\\?.])", "\\\\\\1", selector)
  match <- regexec(paste0(escaped, "\\s*\\{([^}]*)\\}"), css, perl = TRUE)
  parts <- regmatches(css, match)[[1L]]
  if (length(parts) < 2L) "" else parts[[2L]]
}

css_z_index <- function(rule) {
  match <- regexec("z-index\\s*:\\s*([0-9]+)", rule, perl = TRUE)
  parts <- regmatches(rule, match)[[1L]]
  if (length(parts) < 2L) NA_integer_ else as.integer(parts[[2L]])
}

test_that("wrapped navbar links stay above hover menus", {
  css <- paste(
    readLines(testthat::test_path("..", "..", "www", "app.css"), warn = FALSE),
    collapse = "\n"
  )
  link_rule <- css_rule_body(css, ".navbar .navbar-nav .nav-link")
  menu_rule <- css_rule_body(css, ".tab-hover-menu")

  expect_match(link_rule, "position\\s*:\\s*relative")
  expect_gt(css_z_index(link_rule), css_z_index(menu_rule))
})
