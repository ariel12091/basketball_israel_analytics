library(testthat)

# basketball_test.game_types is the source of truth for game-type labels. The
# Shiny constants are a copy generated from it by scripts/sync_catalogues.R,
# kept because the filter dropdowns are built before any database connection
# exists. This fails when the nightly sync stopped landing.

skip_if_not(Sys.getenv("RUN_DB_TESTS", "0") == "1")
skip_if_not_installed("DBI")
skip_if_not_installed("RPostgres")

pg_env <- c("PG_HOST", "PG_PORT", "PG_DB", "PG_USER", "PG_PASS")
missing_env <- pg_env[!nzchar(Sys.getenv(pg_env))]
if (length(missing_env)) {
  skip(paste("Missing DB env:", paste(missing_env, collapse = ", ")))
}

.game_type_defs <- global_defs("GAME_TYPE_LABELS", "GAME_TYPE_CHOICES_UI")

con <- DBI::dbConnect(
  RPostgres::Postgres(),
  host = Sys.getenv("PG_HOST"),
  port = as.integer(Sys.getenv("PG_PORT")),
  dbname = Sys.getenv("PG_DB"),
  user = Sys.getenv("PG_USER"),
  password = Sys.getenv("PG_PASS"),
  sslmode = Sys.getenv("PG_SSLMODE", unset = "require")
)
on.exit(DBI::dbDisconnect(con), add = TRUE)

game_types <- DBI::dbGetQuery(con, "
  SELECT game_type, label, short_label
  FROM basketball_test.game_types
  ORDER BY sort_order")

test_that("game-type dropdown choices match the game_types table", {
  # Codes sharing a label are one comma-separated choice, in sort_order.
  labels <- unique(game_types$label)
  from_table <- vapply(labels, function(lbl) {
    paste(game_types$game_type[game_types$label == lbl], collapse = ",")
  }, character(1))

  expect_identical(c(All = "", from_table), .game_type_defs$GAME_TYPE_CHOICES_UI)
})

test_that("game-type chip labels match the game_types table", {
  from_table <- stats::setNames(game_types$short_label, game_types$game_type)
  pinned <- .game_type_defs$GAME_TYPE_LABELS

  # The constant also carries one key per multi-code choice ("10,34").
  single_code <- pinned[!grepl(",", names(pinned), fixed = TRUE)]
  expect_identical(single_code[order(names(single_code))],
                   from_table[order(names(from_table))])

  multi_code <- pinned[grepl(",", names(pinned), fixed = TRUE)]
  for (key in names(multi_code)) {
    codes <- strsplit(key, ",", fixed = TRUE)[[1]]
    expect_identical(unique(unname(from_table[codes])), unname(multi_code[[key]]),
                     info = key)
  }
})
