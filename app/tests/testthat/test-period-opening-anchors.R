library(testthat)

source(repo_file("..", "etl", "period_opening_anchors.R"), local = TRUE)

# Minimal actions_clean shape. compute_lineups_lookup() consumes exactly these
# columns from the substitution rows it unions the anchors into.
make_anchor_action <- function(
  id,
  quarter,
  clock,
  game_id = 900L,
  team_id = 10L,
  player_id = 1L,
  type = "shot",
  player_in = NA_integer_,
  player_out = NA_integer_
) {
  data.frame(
    id = as.integer(id),
    game_id = as.integer(game_id),
    team_id = as.integer(team_id),
    player_id = as.integer(player_id),
    quarter = as.integer(quarter),
    quarter_time = sprintf("%02d:%02d", floor(clock / 60), clock %% 60),
    end_game_seconds_remaining = as.numeric(clock),
    type = type,
    parameters_player_in = as.integer(player_in),
    parameters_player_out = as.integer(player_out),
    stringsAsFactors = FALSE
  )
}

# The (game_id, team_id) pairs compute_lineups_lookup() joins against, taken
# from full_rosters.
make_anchor_teams <- function(game_id = 900L, team_ids = c(10L, 20L)) {
  data.frame(game_id = as.integer(game_id), team_id = as.integer(team_ids),
             stringsAsFactors = FALSE)
}

multi_game_teams <- rbind(make_anchor_teams(901L), make_anchor_teams(902L))

# A two-period game: Q1 opens at 2400, Q2 opens at 1800.
make_anchor_actions <- function() {
  do.call(rbind, list(
    make_anchor_action(1001L, 1L, 2400, type = "start-of-quarter"),
    make_anchor_action(1002L, 1L, 2390),
    make_anchor_action(1003L, 1L, 1805),
    make_anchor_action(1004L, 2L, 1800, type = "start-of-quarter"),
    make_anchor_action(1005L, 2L, 1800, type = "substitution",
                       player_in = 6L, player_out = 3L),
    make_anchor_action(1006L, 2L, 1792)
  ))
}

test_that("one anchor per team is emitted per period from Q2 onward", {
  anchors <- period_opening_anchors(make_anchor_actions(), make_anchor_teams())

  expect_equal(nrow(anchors), 2L)
  expect_true(all(anchors$game_id == 900L))
  expect_true(all(anchors$quarter == 2L))
})

test_that("the anchor is the period's lowest action id, not its lowest clock", {
  # Provider clocks can regress within a period, so id ordering is canonical.
  actions <- rbind(
    make_anchor_actions(),
    make_anchor_action(1007L, 2L, 1799)
  )
  actions <- actions[order(-actions$end_game_seconds_remaining), ]

  anchors <- period_opening_anchors(actions, make_anchor_teams())

  expect_true(all(anchors$id == 1004L))
  expect_true(all(anchors$end_game_seconds_remaining == 1800))
})

test_that("Q1 never produces an anchor", {
  anchors <- period_opening_anchors(make_anchor_actions(), make_anchor_teams())

  expect_false(1L %in% anchors$quarter)
})

test_that("the anchor carries no substitution payload", {
  anchors <- period_opening_anchors(make_anchor_actions(), make_anchor_teams())

  expect_true(all(is.na(anchors$player_id)))
  expect_true(all(is.na(anchors$parameters_player_in)))
  expect_true(all(is.na(anchors$parameters_player_out)))
})

test_that("an anchor is emitted per team, because a NULL team_id joins to nothing", {
  # compute_lineups_lookup() joins full_rosters to subs ON team_id AND game_id.
  # A NULL team_id matches no roster row, so the anchor must name its team.
  anchors <- period_opening_anchors(make_anchor_actions(), make_anchor_teams())

  expect_equal(nrow(anchors), 2L)
  expect_equal(sort(anchors$team_id), c(10L, 20L))
  expect_true(all(anchors$quarter == 2L))
  expect_true(all(anchors$id == 1004L))
})

test_that("the anchor is skipped for a team that already substitutes at that id", {
  # Game 401 Q4 shape: the period's lowest action IS a substitution. Anchoring
  # that team again at the same id would give one player two states at one
  # instant, and the payload-free anchor could win the slice_max tie-break and
  # blank the substitution.
  actions <- rbind(
    make_anchor_action(2001L, 2L, 1800, team_id = 10L, type = "substitution",
                       player_in = 6L, player_out = 3L),
    make_anchor_action(2002L, 2L, 1800, team_id = 10L, type = "substitution",
                       player_in = 7L, player_out = 4L),
    make_anchor_action(2003L, 2L, 1792, team_id = 20L)
  )

  anchors <- period_opening_anchors(actions, make_anchor_teams())

  expect_equal(nrow(anchors), 1L)
  expect_equal(anchors$team_id, 20L)
  expect_equal(anchors$id, 2001L)
})

test_that("every period from Q2 onward in every game is anchored", {
  actions <- do.call(rbind, c(
    lapply(1:4, function(q) {
      make_anchor_action(2000L + q * 10L, q, 2400 - (q - 1) * 600,
                         game_id = 901L, type = "start-of-quarter")
    }),
    lapply(1:5, function(q) {
      clock <- if (q <= 4) 2400 - (q - 1) * 600 else 300
      make_anchor_action(3000L + q * 10L, q, clock,
                         game_id = 902L, type = "start-of-quarter")
    })
  ))

  anchors <- period_opening_anchors(actions, multi_game_teams)

  expect_equal(nrow(anchors), 14L)
  expect_equal(sort(unique(anchors$quarter[anchors$game_id == 901L])), 2:4)
  expect_equal(sort(unique(anchors$quarter[anchors$game_id == 902L])), 2:5)
})

test_that("an empty actions frame yields zero anchors with the input columns", {
  empty <- make_anchor_actions()[0, ]

  anchors <- period_opening_anchors(empty, make_anchor_teams())

  expect_equal(nrow(anchors), 0L)
  expect_equal(names(anchors), names(empty))
})

test_that("min_quarter is configurable", {
  anchors <- period_opening_anchors(make_anchor_actions(), make_anchor_teams(), min_quarter = 1L)

  expect_equal(sort(unique(anchors$quarter)), 1:2)
  expect_true(all(anchors$id[anchors$quarter == 1L] == 1001L))
})

# ---- Gate 4: the anchor must be the period's opening clock ----
# The lowest action id should also carry the period's maximum remaining
# seconds. When it does not, the provider stamped the period's opening with a
# later clock -- the games 398/399 defect class -- and the anchor would seed
# the period from the wrong moment.

test_that("a well-formed period reports no clock violation", {
  violations <- period_anchor_clock_violations(make_anchor_actions(), make_anchor_teams())

  expect_equal(nrow(violations), 0L)
})

test_that("an anchor stamped below the period maximum is reported", {
  actions <- rbind(
    make_anchor_action(1004L, 2L, 1740, type = "start-of-quarter"),
    make_anchor_action(1005L, 2L, 1800),
    make_anchor_action(1006L, 2L, 1792)
  )

  violations <- period_anchor_clock_violations(actions, make_anchor_teams())

  expect_equal(nrow(violations), 1L)
  expect_equal(violations$quarter, 2L)
  expect_equal(violations$anchor_id, 1004L)
  expect_equal(violations$anchor_clock, 1740)
  expect_equal(violations$period_max_clock, 1800)
})

test_that("the clock check ignores periods below min_quarter", {
  actions <- rbind(
    make_anchor_action(1001L, 1L, 2340, type = "start-of-quarter"),
    make_anchor_action(1002L, 1L, 2400)
  )

  expect_equal(nrow(period_anchor_clock_violations(actions, make_anchor_teams())), 0L)
  expect_equal(nrow(period_anchor_clock_violations(actions, make_anchor_teams(), min_quarter = 1L)), 1L)
})

# ---- SQL twin: period_opening_anchors_tbl() ----
# compute_lineups_lookup() is dbplyr against Postgres, so the anchor rule is
# applied there by a lazy-table twin of the pure helper. Parity is proven on
# Postgres itself, the only engine the SQL runs on: fixtures go in through
# dbplyr::copy_inline() (a VALUES subquery -- no temp tables, read-only).

parity_fixture_actions <- function() {
  rbind(
    make_anchor_actions(),
    # Sub-first period: team 10's substitution is the lowest id, so only
    # team 20 is anchored there.
    make_anchor_action(2001L, 2L, 1800, game_id = 903L, team_id = 10L,
                       type = "substitution", player_in = 6L, player_out = 3L),
    make_anchor_action(2002L, 2L, 1800, game_id = 903L, team_id = 10L,
                       type = "substitution", player_in = 7L, player_out = 4L),
    make_anchor_action(2003L, 2L, 1792, game_id = 903L, team_id = 20L),
    # Lowest id is not the lowest clock row.
    make_anchor_action(3002L, 3L, 1199, game_id = 903L),
    make_anchor_action(3001L, 3L, 1200, game_id = 903L, type = "start-of-quarter"),
    # Q1 must never anchor; OT must.
    make_anchor_action(4001L, 1L, 2400, game_id = 904L, type = "start-of-quarter"),
    make_anchor_action(4501L, 5L, 300, game_id = 904L, type = "start-of-quarter"),
    make_anchor_action(4502L, 5L, 290, game_id = 904L)
  )
}

parity_fixture_teams <- function() {
  rbind(make_anchor_teams(900L), make_anchor_teams(903L), make_anchor_teams(904L))
}

sort_anchors <- function(df) {
  cols <- c("id", "game_id", "team_id", "quarter", "quarter_time",
            "end_game_seconds_remaining", "type", "player_id",
            "parameters_player_in", "parameters_player_out")
  df <- as.data.frame(df)[, cols, drop = FALSE]
  df$end_game_seconds_remaining <- as.numeric(df$end_game_seconds_remaining)
  for (col in c("id", "game_id", "team_id", "quarter", "player_id",
                "parameters_player_in", "parameters_player_out")) {
    df[[col]] <- as.integer(df[[col]])
  }
  df <- df[order(df$game_id, df$quarter, df$team_id), , drop = FALSE]
  rownames(df) <- NULL
  df
}

test_that("the SQL twin renders for Postgres without a database", {
  con <- dbplyr::simulate_postgres()
  actions <- dbplyr::lazy_frame(parity_fixture_actions(), con = con)
  teams <- dbplyr::lazy_frame(parity_fixture_teams(), con = con)

  sql <- dbplyr::sql_render(period_opening_anchors_tbl(actions, teams))

  expect_match(sql, "MIN", fixed = TRUE)
  expect_match(sql, "substitution", fixed = TRUE)
})

test_that("the SQL twin returns exactly the pure helper's anchors on Postgres", {
  skip_if_not(nzchar(Sys.getenv("RUN_DB_TESTS")), "RUN_DB_TESTS not enabled")
  skip_if_not(nzchar(Sys.getenv("PG_HOST")), "no database configured")
  skip_if_not_installed("RPostgres")
  con <- DBI::dbConnect(
    RPostgres::Postgres(),
    host = Sys.getenv("PG_HOST"), port = as.integer(Sys.getenv("PG_PORT", "6543")),
    dbname = Sys.getenv("PG_DB"), user = Sys.getenv("PG_USER"),
    password = Sys.getenv("PG_PASS"), sslmode = Sys.getenv("PG_SSLMODE", "require"),
    connect_timeout = 15L, bigint = "numeric")
  on.exit(DBI::dbDisconnect(con), add = TRUE)

  actions <- parity_fixture_actions()
  teams <- parity_fixture_teams()

  via_sql <- period_opening_anchors_tbl(
    dbplyr::copy_inline(con, actions),
    dbplyr::copy_inline(con, teams)
  ) |> dplyr::collect()
  via_helper <- period_opening_anchors(actions, teams)

  expect_equal(nrow(via_helper), 7L)
  expect_equal(sort_anchors(via_sql), sort_anchors(via_helper))
  expect_setequal(names(via_sql), names(actions))
})

# ---- Runtime gates on the computed lineups ----

make_lineup_rows <- function(game_id, team_id, quarter, id, n_on,
                             period_anchor, players = 1:6) {
  data.frame(
    game_id = as.integer(game_id), team_id = as.integer(team_id),
    quarter = as.integer(quarter), id = as.integer(id),
    player_id = as.integer(players), n_on = as.numeric(n_on),
    period_anchor = period_anchor, stringsAsFactors = FALSE
  )
}

test_that("Gate 1 drops an anchor whose carried state is not five players", {
  lineups <- rbind(
    make_lineup_rows(900L, 10L, 2L, 1004L, 4, TRUE),
    make_lineup_rows(900L, 20L, 2L, 1004L, 5, TRUE),
    make_lineup_rows(900L, 10L, 2L, 1006L, 5, FALSE)
  )

  out <- drop_malformed_period_anchors(lineups)

  expect_equal(nrow(out$dropped), 1L)
  expect_equal(out$dropped$team_id, 10L)
  expect_equal(out$dropped$n_on, 4)
  expect_false(any(out$lineups$period_anchor & out$lineups$team_id == 10L))
  expect_equal(sum(out$lineups$team_id == 20L & out$lineups$period_anchor), 6L)
})

test_that("Gate 1 never touches provider-derived states, even malformed ones", {
  lineups <- make_lineup_rows(900L, 10L, 2L, 1006L, 4, FALSE)

  out <- drop_malformed_period_anchors(lineups)

  expect_equal(nrow(out$dropped), 0L)
  expect_equal(out$lineups, lineups)
})

test_that("anchor parity passes when retained anchors are the helper's", {
  lineups <- rbind(
    make_lineup_rows(900L, 20L, 2L, 1004L, 5, TRUE),
    make_lineup_rows(900L, 10L, 2L, 1005L, 5, FALSE)
  )

  errors <- period_anchor_parity_errors(lineups, make_anchor_actions(), make_anchor_teams())

  expect_equal(nrow(errors), 0L)
})

test_that("anchor parity flags an anchor at an id the helper did not choose", {
  lineups <- make_lineup_rows(900L, 20L, 2L, 1005L, 5, TRUE)

  errors <- period_anchor_parity_errors(lineups, make_anchor_actions(), make_anchor_teams())

  expect_equal(nrow(errors), 1L)
  expect_equal(errors$id, 1005L)
})

test_that("anchor parity flags an anchor for a team the skip rule excludes", {
  actions <- rbind(
    make_anchor_action(2001L, 2L, 1800, team_id = 10L, type = "substitution",
                       player_in = 6L, player_out = 3L),
    make_anchor_action(2003L, 2L, 1792, team_id = 20L)
  )
  lineups <- make_lineup_rows(900L, 10L, 2L, 2001L, 5, TRUE)

  errors <- period_anchor_parity_errors(lineups, actions, make_anchor_teams())

  expect_equal(nrow(errors), 1L)
  expect_equal(errors$team_id, 10L)
})

test_that("the gate runner drops Gate 1 failures and removes the marker", {
  lineups <- rbind(
    make_lineup_rows(900L, 10L, 2L, 1004L, 6, TRUE),
    make_lineup_rows(900L, 20L, 2L, 1004L, 5, TRUE)
  )
  logged <- character(0)

  out <- apply_period_anchor_gates(
    lineups, make_anchor_actions(), make_anchor_teams(),
    log_msg = function(msg, level = "INFO") logged <<- c(logged, msg)
  )

  expect_false("period_anchor" %in% names(out))
  expect_true(all(out$team_id == 20L))
  expect_true(any(grepl("n_on=6", logged, fixed = TRUE)))
})

test_that("the gate runner stops on an anchor parity mismatch", {
  lineups <- make_lineup_rows(900L, 20L, 2L, 1005L, 5, TRUE)

  expect_error(
    apply_period_anchor_gates(lineups, make_anchor_actions(), make_anchor_teams()),
    "anchor parity"
  )
})

test_that("the gate runner stops on a Gate 4 clock violation", {
  actions <- rbind(
    make_anchor_action(1004L, 2L, 1740, type = "start-of-quarter"),
    make_anchor_action(1005L, 2L, 1800)
  )
  lineups <- make_lineup_rows(900L, 20L, 2L, 1004L, 5, TRUE)

  expect_error(
    apply_period_anchor_gates(lineups, actions, make_anchor_teams()),
    "clock"
  )
})

test_that("compute_lineups_lookup() with anchors unioned in plans on Postgres", {
  # The UNION ALL is where NULL typing bites: an untyped NULL in a subquery
  # resolves to text, and "UNION types integer and text cannot be matched"
  # only surfaces on the real engine against the real column types.
  skip_if_not(nzchar(Sys.getenv("RUN_DB_TESTS")), "RUN_DB_TESTS not enabled")
  skip_if_not(nzchar(Sys.getenv("PG_HOST")), "no database configured")
  skip_if_not_installed("RPostgres")
  con <- DBI::dbConnect(
    RPostgres::Postgres(),
    host = Sys.getenv("PG_HOST"), port = as.integer(Sys.getenv("PG_PORT", "6543")),
    dbname = Sys.getenv("PG_DB"), user = Sys.getenv("PG_USER"),
    password = Sys.getenv("PG_PASS"), sslmode = Sys.getenv("PG_SSLMODE", "require"),
    connect_timeout = 15L, bigint = "numeric")
  on.exit(DBI::dbDisconnect(con), add = TRUE)

  # etl_onoff.R connects to the database at source time, so evaluate only the
  # compute_lineups_lookup() definition from it.
  env <- new.env()
  exprs <- parse(repo_file("..", "etl", "etl_onoff.R"), keep.source = FALSE)
  is_target <- vapply(exprs, function(e) {
    is.call(e) && identical(as.character(e[[1]]), "<-") &&
      identical(as.character(e[[2]]), "compute_lineups_lookup")
  }, logical(1))
  expect_equal(sum(is_target), 1L)
  eval(exprs[[which(is_target)]], env)
  env$SCHEMA <- "basketball_test"
  env$sched_subset <- data.frame(game_id = 404L)
  env$period_opening_anchors_tbl <- period_opening_anchors_tbl
  environment(env$compute_lineups_lookup) <- env
  suppressPackageStartupMessages({
    library(dplyr); library(dbplyr); library(tidyr)
  })

  sql <- dbplyr::sql_render(env$compute_lineups_lookup(con))

  expect_match(sql, "UNION ALL", fixed = TRUE)
  expect_no_error(DBI::dbGetQuery(con, paste("EXPLAIN", sql)))
})

# ---- Wiring contracts ----
# The helpers above are only useful if the ETL calls them. These pin the call
# sites: a unit test on a helper cannot prove its caller calls it.

etl_code_lines <- function(file) {
  lines <- readLines(repo_file("..", "etl", file), warn = FALSE)
  lines <- sub("\r$", "", lines)
  lines[!grepl("^[[:space:]]*#", lines)]
}

test_that("etl_onoff.R sources the anchor helpers in its definitions section", {
  lines <- etl_code_lines("etl_onoff.R")
  src <- grep('source("etl/period_opening_anchors.R")', lines, fixed = TRUE)
  usage <- grep("^#[[:space:]]*Usage",readLines(repo_file("..", "etl", "etl_onoff.R"), warn = FALSE))

  expect_length(src, 1L)
  # etl_full.R evaluates etl_onoff.R only up to its Usage marker.
  if (length(usage)) expect_lt(src, usage[1] - 2L)
})

test_that("both lineups_lookup call sites apply the anchor gates before writing", {
  for (file in c("etl_full.R", "etl_onoff.R")) {
    lines <- etl_code_lines(file)
    calls <- grep("df_lineups_df <- compute_lineups_lookup(pg)", lines, fixed = TRUE)
    gates <- grep("df_lineups_df <- apply_period_anchor_gates(", lines, fixed = TRUE)
    writes <- grep('upsert_by_like(pg, SCHEMA, "lineups_lookup"', lines, fixed = TRUE)

    expect_length(calls, 1L)
    expect_length(gates, 1L)
    expect_length(writes, 1L)
    expect_true(calls < gates && gates < writes, info = file)
  }
})
