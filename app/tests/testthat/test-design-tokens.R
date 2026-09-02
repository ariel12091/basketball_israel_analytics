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
  colour_tokens <- tokens[!grepl("-rgb$", names(tokens))]

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
