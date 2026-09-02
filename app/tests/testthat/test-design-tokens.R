REQUIRED_TOKENS <- c(
  "--ibpl-bg", "--ibpl-bg-sunken", "--ibpl-surface", "--ibpl-surface-alt",
  "--ibpl-surface-2", "--ibpl-surface-3", "--ibpl-surface-hover",
  "--ibpl-surface-selected", "--ibpl-border",
  "--ibpl-text", "--ibpl-text-body", "--ibpl-text-muted", "--ibpl-text-dim",
  "--ibpl-text-faint",
  "--ibpl-accent", "--ibpl-accent-rgb", "--ibpl-accent-hover",
  "--ibpl-pos", "--ibpl-neg", "--ibpl-info", "--ibpl-side-a",
  "--ibpl-fg2", "--ibpl-fg3",
  "--ibpl-chip-game-border", "--ibpl-chip-game-text"
)

test_that("app.css defines the full design token set in one :root block", {
  css <- read_repo_txt("www", "app.css")
  tokens <- css_tokens(css)

  expect_true(length(tokens) > 0)
  for (tok in REQUIRED_TOKENS) {
    expect_true(tok %in% names(tokens), info = paste("missing token:", tok))
  }
})

test_that("every colour token is a parseable colour", {
  css <- read_repo_txt("www", "app.css")
  tokens <- css_tokens(css)
  # Tokens ending in -rgb hold a bare "r, g, b" triplet for use inside
  # rgba(), not a colour literal, so col2rgb() cannot parse them. Task 3
  # adds three more of them, so exclude by suffix rather than by name.
  # --ibpl-font-* hold font stacks, also not colour literals.
  colour_tokens <- tokens[!grepl("-rgb$|^--ibpl-font-", names(tokens))]

  for (nm in names(colour_tokens)) {
    expect_silent(grDevices::col2rgb(colour_tokens[[nm]]))
  }
})

test_that("every rgb triplet token parses as three 0-255 integers", {
  css <- read_repo_txt("www", "app.css")
  tokens <- css_tokens(css)
  triplets <- tokens[grepl("-rgb$", names(tokens))]

  for (nm in names(triplets)) {
    parts <- suppressWarnings(
      as.integer(strsplit(gsub("\\s", "", triplets[[nm]]), ",")[[1]])
    )
    expect_length(parts, 3)
    expect_false(any(is.na(parts)), info = nm)
    expect_true(all(parts >= 0 & parts <= 255), info = nm)
  }
})

test_that("the accent rgb triplet matches the accent hex", {
  css <- read_repo_txt("www", "app.css")
  tokens <- css_tokens(css)

  triplet <- as.integer(strsplit(gsub("\\s", "", tokens[["--ibpl-accent-rgb"]]), ",")[[1]])
  expect_equal(triplet, as.integer(grDevices::col2rgb(tokens[["--ibpl-accent"]])[, 1]))
})

test_that("app.css carries no raw hex outside the :root token block", {
  css <- read_repo_txt("www", "app.css")
  root <- css_root_block(css)
  outside <- sub(root, "", css, fixed = TRUE)

  found <- regmatches(outside, gregexpr("#[0-9a-fA-F]{6}", outside))[[1]]
  expect_equal(
    sort(unique(found)), character(0),
    info = paste("raw hex outside :root:", paste(sort(unique(found)), collapse = ", "))
  )
})

test_that("app.css expresses brand alpha through the accent rgb token", {
  css <- read_repo_txt("www", "app.css")
  root <- css_root_block(css)
  outside <- sub(root, "", css, fixed = TRUE)

  # The literal amber triplet must not appear in an rgba() outside :root.
  expect_false(grepl("rgba\\(\\s*232\\s*,\\s*164\\s*,\\s*53", outside))
  expect_true(grepl("rgba\\(var\\(--ibpl-accent-rgb\\)", outside))
})

test_that("UI files style themselves through tokens, not literal hex", {
  ui_files <- c(
    "ui_tab0_home.R", "ui_tab1_onoff.R", "ui_tab2_lineup.R",
    "ui_tab4_gamelogs.R", "ui_tab7_compare.R",
    "ui_tab9_euro_team.R", "ui_tab10_euro_lineups.R"
  )

  for (f in ui_files) {
    txt <- read_repo_txt("R", f)
    found <- regmatches(txt, gregexpr("#[0-9a-fA-F]{6}", txt))[[1]]
    expect_equal(
      sort(unique(found)), character(0),
      info = paste(f, "still carries literal hex:", paste(sort(unique(found)), collapse = ", "))
    )
  }
})

# Every neutral moved from Primer's cool blue-black to a warm ground. The
# contract is that hue changed and luminance did not: contrast ratios across
# the app are a function of luminance alone, so matching it means no text
# pair got harder to read.
WARM_NEUTRALS <- c(
  "--ibpl-bg"              = 0.00548,
  "--ibpl-bg-sunken"       = 0.00948,
  "--ibpl-surface"         = 0.01070,
  "--ibpl-surface-alt"     = 0.01374,
  "--ibpl-surface-2"       = 0.01688,
  "--ibpl-surface-3"       = 0.01899,
  "--ibpl-surface-hover"   = 0.02589,
  "--ibpl-border"          = 0.03604,
  "--ibpl-text-faint"      = 0.07674,
  "--ibpl-text-dim"        = 0.17857,
  "--ibpl-text-muted"      = 0.29137,
  "--ibpl-text-body"       = 0.63028,
  "--ibpl-text"            = 0.83862
)

test_that("warming the palette preserved every neutral's luminance", {
  tokens <- css_tokens(read_repo_txt("www", "app.css"))

  for (nm in names(WARM_NEUTRALS)) {
    got <- rel_luminance(tokens[[nm]])
    expect_lt(
      abs(got - WARM_NEUTRALS[[nm]]), 0.02,
      label = sprintf("%s luminance %.5f vs baseline %.5f", nm, got, WARM_NEUTRALS[[nm]])
    )
  }
})

test_that("the neutral ramp is warm, not cool", {
  tokens <- css_tokens(read_repo_txt("www", "app.css"))

  for (nm in names(WARM_NEUTRALS)) {
    rgb <- grDevices::col2rgb(tokens[[nm]])[, 1]
    # Warm means red leads blue. Primer's neutrals do the opposite.
    expect_gt(
      as.integer(rgb[["red"]]), as.integer(rgb[["blue"]]),
      label = sprintf("%s (%s) is not warm", nm, tokens[[nm]])
    )
  }
})

test_that("body text on the page ground still clears WCAG AA", {
  tokens <- css_tokens(read_repo_txt("www", "app.css"))
  contrast <- function(a, b) {
    la <- rel_luminance(a); lb <- rel_luminance(b)
    (max(la, lb) + 0.05) / (min(la, lb) + 0.05)
  }

  expect_gt(contrast(tokens[["--ibpl-text"]], tokens[["--ibpl-bg"]]), 4.5)
  expect_gt(contrast(tokens[["--ibpl-text-body"]], tokens[["--ibpl-bg"]]), 4.5)
  expect_gt(contrast(tokens[["--ibpl-text-muted"]], tokens[["--ibpl-bg"]]), 4.5)
  expect_gt(contrast(tokens[["--ibpl-accent"]], tokens[["--ibpl-bg"]]), 4.5)
})

test_that("bs_theme literals track the token values they mirror", {
  tokens <- css_tokens(read_repo_txt("www", "app.css"))
  app_txt <- read_repo_txt("app.R")

  expect_true(grepl(sprintf('bg = "%s"', tokens[["--ibpl-bg"]]), app_txt, fixed = TRUE))
  expect_true(grepl(sprintf('fg = "%s"', tokens[["--ibpl-text"]]), app_txt, fixed = TRUE))
  expect_true(grepl(sprintf('primary = "%s"', tokens[["--ibpl-accent"]]), app_txt, fixed = TRUE))
  expect_true(grepl(sprintf('"navbar-bg" = "%s"', tokens[["--ibpl-bg"]]), app_txt, fixed = TRUE))
})

test_that("the display face is loaded and tokenised", {
  global_txt <- read_repo_txt("R", "global.R")
  css <- read_repo_txt("www", "app.css")
  tokens <- css_tokens(css)

  expect_true(grepl("family=Archivo", global_txt, fixed = TRUE))
  expect_true("--ibpl-font-display" %in% names(tokens))
  expect_true("--ibpl-font-body" %in% names(tokens))
  expect_true("--ibpl-font-mono" %in% names(tokens))
  expect_true(grepl("Archivo", tokens[["--ibpl-font-display"]], fixed = TRUE))
})

test_that("numeric surfaces use tabular figures", {
  css <- read_repo_txt("www", "app.css")

  # Every surface that stacks numbers in a column must line them up.
  for (sel in c("table.dataTable", ".diff-val", ".sub-text",
                ".hub-stat-value", ".cmp-stat-value", ".cmp-gap-num")) {
    expect_true(
      grepl(sel, css, fixed = TRUE),
      info = paste("selector missing:", sel)
    )
  }
  expect_true(grepl("font-variant-numeric:\\s*tabular-nums", css))
  # One shared rule, not six copies.
  expect_gte(length(regmatches(css, gregexpr("tabular-nums", css))[[1]]), 1)
})

test_that("the tabular-figures rule is set in a face that has the feature", {
  # Measured in the browser: DM Sans' digit advances are identical with and
  # without font-variant-numeric, because the served face carries no tnum
  # feature. Archivo does. So the rule asking for tabular figures must also set
  # the display face, or it is inert and the columns still shimmer on re-sort.
  css <- read_repo_txt("www", "app.css")

  rule <- regmatches(
    css,
    regexpr("[^}]*font-variant-numeric:\\s*tabular-nums[^}]*", css)
  )
  expect_length(rule, 1)
  expect_true(
    grepl("font-family:\\s*var\\(--ibpl-font-display\\)", rule),
    info = "the tabular-nums rule does not set --ibpl-font-display"
  )
})
