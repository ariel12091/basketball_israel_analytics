test_that("the viewport meta allows pinch zoom", {
  shared_head_tags <- global_defs("shared_head_tags")$shared_head_tags
  # shared_head_tags() returns a bare tags$head(...): htmltools::renderTags()
  # hoists head content into $head (via takeHeads()), leaving $html empty for
  # this input regardless of content -- so the meta tag is asserted on $head.
  html <- htmltools::renderTags(shared_head_tags())$head

  expect_match(html, "width=device-width", fixed = TRUE)
  # maximum-scale=1 blocks pinch zoom, which is an accessibility failure and is
  # unnecessary once the layout actually fits.
  expect_false(grepl("maximum-scale", html, fixed = TRUE))
})

test_that("the mobile layer is loaded after app.css and app.js", {
  app_r <- read_repo_txt("app.R")

  expect_true(grepl("www/mobile.css", app_r, fixed = TRUE))
  expect_true(grepl("www/mobile.js", app_r, fixed = TRUE))
  # Load order: app.css carries load-bearing !important rules the mobile layer
  # must be able to override without removing them.
  expect_lt(
    regexpr("www/app.css", app_r, fixed = TRUE),
    regexpr("www/mobile.css", app_r, fixed = TRUE)
  )
  expect_lt(
    regexpr("www/app.js", app_r, fixed = TRUE),
    regexpr("www/mobile.js", app_r, fixed = TRUE)
  )
})

test_that("the mobile layer has a kill switch defaulting to on", {
  global_r <- read_repo_txt("R", "global.R")

  expect_true(grepl("IBPL_MOBILE", global_r, fixed = TRUE))
  expect_true(grepl('Sys.getenv("IBPL_MOBILE", "true")', global_r, fixed = TRUE))
})

test_that("mode is carried by a body class, not a bare media query", {
  css <- read_repo_txt("www", "mobile.css")
  js <- read_repo_txt("www", "mobile.js")

  expect_true(grepl("body.ibpl-mobile", css, fixed = TRUE))
  expect_true(grepl("max-width: 767.98px", js, fixed = TRUE))
  # A bare @media in mobile.css would be a second source of truth for "are we
  # mobile", which could disagree with the class the JS sets.
  expect_false(grepl("@media (max-width", css, fixed = TRUE))
})
