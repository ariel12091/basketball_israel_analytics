# The readers are the only league-aware code in the feature. Database calls
# are separately env-gated; the canonical-frame checks remain pure.

test_that("ribbon_normalise_lanes labels the clicked team own and the other opp", {
  raw <- data.frame(
    team_id = c(7L, 7L, 10L), player_id = c(1L, 2L, 3L),
    player_label = c("A", "B", "C"), start_elapsed = c(0, 0, 0),
    end_elapsed = c(100, 100, 100), lineup_key = c("h1", "h1", "h2"),
    stringsAsFactors = FALSE
  )
  out <- ribbon_normalise_lanes(raw, own_team_id = 7L)
  expect_identical(out$side, c("own", "own", "opp"))
  expect_identical(out$player_key, c("1", "2", "3"))
})

test_that("ribbon_normalise_lanes returns only canonical columns", {
  raw <- data.frame(team_id = 7L, player_id = 1L, player_label = "A",
                    start_elapsed = 0, end_elapsed = 10, lineup_key = "h1",
                    extra_junk = "drop me")
  out <- ribbon_normalise_lanes(raw, own_team_id = 7L)
  expect_identical(sort(names(out)),
                   sort(c("side", "player_key", "player_label",
                          "start_elapsed", "end_elapsed", "lineup_key")))
})

test_that("ribbon_normalise_lanes keys on player_id, never on the label", {
  raw <- data.frame(team_id = c(7L, 7L), player_id = c(101L, 202L),
                    player_label = c("NEW NEW", "NEW NEW"),
                    start_elapsed = c(0, 0), end_elapsed = c(100, 100),
                    lineup_key = c("h1", "h1"))
  out <- ribbon_normalise_lanes(raw, own_team_id = 7L)
  expect_identical(out$player_key, c("101", "202"))
  expect_identical(length(unique(out$player_key)), 2L)
})

test_that("normalised lanes carry a lineup key", {
  raw <- data.frame(team_id = c(6L, 6L), player_id = c(1L, 2L),
                    player_label = c("A", "B"),
                    start_elapsed = c(0, 0), end_elapsed = c(60, 60),
                    lineup_key = c("h1", "h1"), stringsAsFactors = FALSE)
  out <- ribbon_normalise_lanes(raw, 6L)
  expect_identical(out$lineup_key, c("h1", "h1"))
})

test_that("ribbon_health_message speaks only when segments were excluded", {
  expect_null(ribbon_health_message(0))
  expect_null(ribbon_health_message(NULL))
  expect_match(ribbon_health_message(3), "3")
  expect_match(ribbon_health_message(3), "not drawn")
})

# ribbon_sign_margin() was removed 2026-09-05 (final review, live-data bug):
# ribbon_margin_v is now team-perspective (sourced from
# euroleague.action_team_context_actions, the same source and `points > 0`
# predicate as the clutch read layer, euroleague/sql/019_clutch_read_layer.sql)
# and already carries a signed `margin` column, exactly like the Israeli
# reader. There is no longer a home/away sign to flip in R.

test_that("the euro reader never pairs lineup names with ids positionally", {
  src <- paste(readLines(testthat::test_path("..", "..", "R", "global.R"),
                         warn = FALSE), collapse = "\n")
  sql <- regmatches(src, regexpr('RIBBON_SQL_EURO <- "(.|\n)*?"\n', src, perl = TRUE))
  expect_true(nzchar(sql))
  expect_false(grepl("unnest\\s*\\([^)]*own_lineup[^)]*,", sql))
  expect_match(sql, "unnest\\(s\\.player_ids\\)")
})

test_that("the ribbon_segments_v view never pairs lineup names with ids positionally", {
  # readers.R:51 (the test above) forbids `own_lineup` inside an unnest() in
  # RIBBON_SQL_EURO, but that string never appears in the R query at all --
  # RIBBON_SQL_EURO only unnests s.player_ids. The positional-pairing risk
  # this whole feature guards against (31,907 mismatches in 40,000 pairs when
  # own_lineup and player_ids are zipped by position) lives entirely in the
  # VIEW definition, so that is what must be checked, or this guard is
  # trivially true regardless of what the view does.
  sql <- paste(readLines(testthat::test_path("..", "..", "..", "euroleague", "sql",
                        "053_stint_ribbon_read_layer.sql"), warn = FALSE), collapse = "\n")
  view_sql <- regmatches(sql, regexpr(
    "CREATE OR REPLACE VIEW euroleague\\.ribbon_segments_v AS(.|\n)*?;", sql, perl = TRUE))
  expect_true(nzchar(view_sql))
  expect_false(grepl("unnest\\s*\\([^)]*own_lineup", view_sql, ignore.case = TRUE))
  # The named-key join this view uses instead.
  expect_match(view_sql, "l\\.own_lineup\\s*=\\s*m\\.own_lineup")
})

test_that("the Israeli lane label comes from the per-game roster", {
  src <- paste(readLines(testthat::test_path("..", "..", "R", "global.R"),
                         warn = FALSE), collapse = "\n")
  sql <- regmatches(src, regexpr('RIBBON_SQL_ISRAEL <- "(.|\n)*?"\n', src, perl = TRUE))
  expect_true(nzchar(sql))
  expect_match(sql, "full_rosters r")
  expect_match(sql, "r\\.game_id\\s*=\\s*\\$1")
})

test_that("the Israeli ribbon SQL carries no type_lineup predicate", {
  src <- paste(readLines(testthat::test_path("..", "..", "R", "global.R"),
                         warn = FALSE), collapse = "\n")
  sql <- regmatches(src, regexpr('RIBBON_SQL_ISRAEL <- "(.|\n)*?"\n', src, perl = TRUE))
  expect_true(nzchar(sql))
  expect_false(grepl("type_lineup\\s*=", sql))
  expect_false(grepl("type_lineup\\s+IS\\s+NOT\\s+NULL", sql, ignore.case = TRUE))
  expect_false(grepl("GROUP BY[^)]*type_lineup", sql, ignore.case = TRUE))
})

test_that("the Israeli ribbon SQL keeps both HAVING guards on segs/lineup_players", {
  # Removing either lets a bad row back in:
  #   - segs: without MAX(segment_seconds) > 0, zero-length segments (two
  #     substitutions at the same clock, ~45% of raw rows) occupy a lane slot
  #     as an invisible sliver.
  #   - lineup_players: without cardinality(...) = 5, a partially-resolved
  #     lineup would label a segment with fewer than five players.
  src <- paste(readLines(testthat::test_path("..", "..", "R", "global.R"),
                         warn = FALSE), collapse = "\n")
  sql <- regmatches(src, regexpr('RIBBON_SQL_ISRAEL <- "(.|\n)*?"\n', src, perl = TRUE))
  expect_true(nzchar(sql))
  expect_match(sql, "HAVING\\s+MAX\\(segment_seconds\\)\\s*>\\s*0")
  expect_match(sql, "HAVING\\s+cardinality\\(ARRAY_AGG\\(DISTINCT l\\.player_id\\)\\)\\s*=\\s*5")
})

test_that("euroleague.ribbon_margin_v has no NULL margin (2026-09-05 live-data bug)", {
  # The OLD view read actions.points_a/points_b directly: the provider leaves
  # the running score NULL until that side has scored, so every early event
  # in every one of 593 games carried a NULL score, which propagated to NA
  # margin and then to an invalid "V NaN" in the SVG path -- blanking the
  # WHOLE curve, not just the affected span. The view now sources
  # own_team_score/opp_team_score from euroleague.action_team_context_actions
  # (the same source the clutch read layer uses, migration 019), which is
  # NULL-free by construction. This is the regression guard.
  skip_if_not(nzchar(Sys.getenv("RUN_DB_TESTS")), "RUN_DB_TESTS not enabled")
  skip_if_not(nzchar(Sys.getenv("PG_HOST")), "no database configured")

  con <- DBI::dbConnect(RPostgres::Postgres(),
    host = Sys.getenv("PG_HOST"), port = as.integer(Sys.getenv("PG_PORT")),
    dbname = Sys.getenv("PG_DB"), user = Sys.getenv("PG_USER"),
    password = Sys.getenv("PG_PASS"), sslmode = Sys.getenv("PG_SSLMODE"),
    connect_timeout = 15L, bigint = "numeric")
  on.exit(DBI::dbDisconnect(con), add = TRUE)

  counts <- DBI::dbGetQuery(con, "
    SELECT
      (SELECT COUNT(*) FROM euroleague.ribbon_margin_v WHERE margin IS NULL) AS null_margin,
      (SELECT COUNT(*) FROM euroleague.ribbon_margin_v) AS total_rows")
  expect_identical(as.numeric(counts$null_margin[1]), 0)
  expect_gt(counts$total_rows[1], 1000)
})

test_that("games with no score data are identified dynamically", {
  skip_if_not(nzchar(Sys.getenv("RUN_DB_TESTS")), "RUN_DB_TESTS not enabled")
  skip_if_not(nzchar(Sys.getenv("PG_HOST")), "no database configured")

  con <- DBI::dbConnect(RPostgres::Postgres(),
    host = Sys.getenv("PG_HOST"), port = as.integer(Sys.getenv("PG_PORT")),
    dbname = Sys.getenv("PG_DB"), user = Sys.getenv("PG_USER"),
    password = Sys.getenv("PG_PASS"), sslmode = Sys.getenv("PG_SSLMODE"),
    connect_timeout = 15L, bigint = "numeric")
  on.exit(DBI::dbDisconnect(con), add = TRUE)

  bad <- DBI::dbGetQuery(con, "
    SELECT game_id, team_id
    FROM basketball_test.df_pts_poss_lineups_longer_mv
    GROUP BY game_id, team_id
    HAVING COUNT(own_team_score) = 0")

  # Not an equality against a fixed list: these four are what the condition
  # catches today, not the definition of it. The ETL can produce more.
  expect_gt(nrow(bad), 0)
  expect_true(all(c(139L, 140L, 141L, 143L) %in% as.integer(bad$game_id)))
})

# This guard deliberately reads euroleague.ribbon_segments_v, NOT the base
# table euroleague.matchup_segments_actions. app_readonly (the role the
# deployed app actually runs as) is denied on the base table on purpose -- the
# EuroLeague schema keeps raw/derived-fact tables closed and exposes only a
# curated read layer, of which this view is the entire point (see
# euroleague/sql/053_stint_ribbon_read_layer.sql). A guard that only runs
# under an elevated ETL role isn't guarding what the application sees, and it
# fails outright (not skip) for anyone who runs the suite with app/.Renviron
# credentials. Do not point this back at the base table.
test_that("euro segment lineups resolve to one player-id set", {
  skip_if_not(nzchar(Sys.getenv("RUN_DB_TESTS")), "RUN_DB_TESTS not enabled")
  skip_if_not(nzchar(Sys.getenv("PG_HOST")), "no database configured")

  con <- DBI::dbConnect(RPostgres::Postgres(),
    host = Sys.getenv("PG_HOST"), port = as.integer(Sys.getenv("PG_PORT")),
    dbname = Sys.getenv("PG_DB"), user = Sys.getenv("PG_USER"),
    password = Sys.getenv("PG_PASS"), sslmode = Sys.getenv("PG_SSLMODE"),
    connect_timeout = 15L, bigint = "numeric")
  on.exit(DBI::dbDisconnect(con), add = TRUE)

  # Three checks in one round trip against the view:
  #   - empty_ids: a segment whose player_ids came back NULL or empty means
  #     the join in ribbon_segments_v missed -- a lane set with no players.
  #   - fanout: (game_id, team_id, segment_id) appearing more than once means
  #     the own_lineup join produced two different player-id sets for the
  #     same segment -- a player would get duplicate lanes.
  #   - total_rows: a bare "zero violations" on empty_ids/fanout would also
  #     pass vacuously if the view were empty or the filter wrong, so this
  #     asserts a healthy lower bound too -- it is not sufficient on its own,
  #     but it is a necessary companion to the other two.
  counts <- DBI::dbGetQuery(con, "
    SELECT
      (SELECT COUNT(*) FROM euroleague.ribbon_segments_v
         WHERE player_ids IS NULL OR cardinality(player_ids) = 0) AS empty_ids,
      (SELECT COUNT(*) FROM (
         SELECT game_id, team_id, segment_id
         FROM euroleague.ribbon_segments_v
         GROUP BY game_id, team_id, segment_id
         HAVING COUNT(*) > 1) dup) AS fanout,
      (SELECT COUNT(*) FROM euroleague.ribbon_segments_v) AS total_rows")

  expect_identical(as.numeric(counts$empty_ids[1]), 0)
  expect_identical(as.numeric(counts$fanout[1]), 0)
  expect_gt(counts$total_rows[1], 1000)
})

test_that("the ribbon segment count matches type-lineup-absent grouping", {
  skip_if_not(nzchar(Sys.getenv("RUN_DB_TESTS")), "RUN_DB_TESTS not enabled")
  skip_if_not(nzchar(Sys.getenv("PG_HOST")), "no database configured")

  con <- DBI::dbConnect(RPostgres::Postgres(),
    host = Sys.getenv("PG_HOST"), port = as.integer(Sys.getenv("PG_PORT")),
    dbname = Sys.getenv("PG_DB"), user = Sys.getenv("PG_USER"),
    password = Sys.getenv("PG_PASS"), sslmode = Sys.getenv("PG_SSLMODE"),
    connect_timeout = 15L, bigint = "numeric")
  on.exit(DBI::dbDisconnect(con), add = TRUE)
  counts <- DBI::dbGetQuery(con, "
    SELECT
      (SELECT COUNT(*) FROM (
         SELECT team_id, segment_id, lineup_hash
         FROM basketball_test.df_pts_poss_lineups_longer_mv WHERE game_id=115
         GROUP BY team_id, segment_id, lineup_hash HAVING MAX(segment_seconds)>0) a)
         AS all_perspectives,
      (SELECT COUNT(*) FROM (
         SELECT team_id, segment_id, lineup_hash
         FROM basketball_test.df_pts_poss_lineups_longer_mv
         WHERE game_id=115 AND type_lineup='offense'
         GROUP BY team_id, segment_id, lineup_hash HAVING MAX(segment_seconds)>0) b)
         AS offense_only")
  expect_gt(counts$all_perspectives[1], counts$offense_only[1])
})

test_that("the reader returns a raw steps frame alongside the drawn margin", {
  # steps is the RAW series: one row per recorded event, order_key intact and
  # no padding. The drawn `margin` stays completed by ribbon_complete_margin().
  # They are different frames on purpose -- as-of lookups must not read the
  # padding. Asserted on the source because tests do not source global.R.
  src <- paste(readLines(testthat::test_path("..", "..", "R", "global.R"),
                         warn = FALSE), collapse = "\n")
  expect_match(src, "steps = steps", fixed = TRUE)
  expect_match(src, "steps <- margin", fixed = TRUE)
})

test_that("completing the margin does not disturb the raw step series", {
  # The pure half of the same contract: completion collapses duplicate
  # seconds and pads both ends, so reading it as-of would answer from
  # padding rather than from recorded events.
  raw <- data.frame(elapsed = c(30, 30, 900), margin = c(2, 4, 9),
                    order_key = c(1, 2, 3))
  completed <- ribbon_complete_margin(raw, 2400)
  expect_equal(nrow(raw), 3)
  expect_true(nrow(completed) != nrow(raw))
  expect_equal(completed$elapsed[1], 0)              # padded start
  expect_equal(completed$elapsed[nrow(completed)], 2400)  # padded end
  expect_null(completed$order_key)                   # dropped by completion
})

test_that("the Israeli ribbon SQL selects own_team_score in its margin CTE", {
  # own_team_score is already a column on the MV the marg CTE scans, so this
  # costs no new relation, no new grant and no second round trip. pf + pa is
  # not derivable from the margin alone -- this one column is what makes
  # points for/against possible.
  #
  # Read from source: tests do not source global.R, so RIBBON_SQL_ISRAEL is
  # not bound here. Same approach as the euro reader tests above.
  src <- paste(readLines(testthat::test_path("..", "..", "R", "global.R"),
                         warn = FALSE), collapse = "\n")
  sql <- regmatches(src, regexpr('RIBBON_SQL_ISRAEL <- "(.|\n)*?"\n', src,
                                 perl = TRUE))
  expect_true(nzchar(sql))
  expect_match(sql, "own_team_score AS own", fixed = TRUE)
  expect_match(sql, "basketball_test.df_pts_poss_lineups_longer_mv",
               fixed = TRUE)
})

test_that("the EuroLeague ribbon SQL selects own_team_score in its margin CTE", {
  # Source-read for the same reason as the Israeli test: global.R is not
  # sourced by the suite. This regex is the one already used by the
  # "euro reader never pairs lineup names with ids positionally" test above.
  src <- paste(readLines(testthat::test_path("..", "..", "R", "global.R"),
                         warn = FALSE), collapse = "\n")
  sql <- regmatches(src, regexpr('RIBBON_SQL_EURO <- "(.|\n)*?"\n', src,
                                 perl = TRUE))
  expect_true(nzchar(sql))
  expect_match(sql, "own_team_score AS own", fixed = TRUE)
})

test_that("both readers select a lineup key into the lanes CTE", {
  # Israeli already carries lineup_hash in `segs` and dropped it in the
  # unnest; EuroLeague has no hash so the sorted, verified player_ids array
  # IS the identity -- its text form, not a minted second identifier.
  # Source-read for the same reason as the tests above: global.R is not
  # sourced by the suite.
  src <- paste(readLines(testthat::test_path("..", "..", "R", "global.R"),
                         warn = FALSE), collapse = "\n")
  israel_sql <- regmatches(src, regexpr('RIBBON_SQL_ISRAEL <- "(.|\n)*?"\n', src,
                                        perl = TRUE))
  euro_sql <- regmatches(src, regexpr('RIBBON_SQL_EURO <- "(.|\n)*?"\n', src,
                                      perl = TRUE))
  expect_match(israel_sql, "s.lineup_hash AS lineup_key", fixed = TRUE)
  expect_match(euro_sql, "s.player_ids::text AS lineup_key", fixed = TRUE)
})

test_that("migration 054 appends the column with CREATE OR REPLACE, not a drop", {
  # CREATE OR REPLACE VIEW preserves the app_readonly grant; DROP + CREATE
  # wipes it. The column must be appended at the END of the select list,
  # which is the only shape CREATE OR REPLACE allows.
  sql <- paste(readLines(testthat::test_path(
    "..", "..", "..", "euroleague", "sql",
    "054_ribbon_margin_own_score.sql"), warn = FALSE), collapse = "\n")
  expect_match(sql, "CREATE OR REPLACE VIEW euroleague.ribbon_margin_v",
               fixed = TRUE)
  expect_false(grepl("DROP VIEW", sql, fixed = TRUE))
  # own_team_score must be APPENDED after source_event_order in the SELECT
  # list -- the only shape CREATE OR REPLACE VIEW permits. Two reasons this
  # is scoped and uses the LAST occurrence rather than comparing first
  # positions in the whole file: the comment header names own_team_score
  # before any SQL, and inside the statement the margin expression
  # (own_team_score - opp_team_score) necessarily precedes
  # source_event_order. Neither is the select-list position under test.
  body <- regmatches(sql, regexpr(
    "CREATE OR REPLACE VIEW euroleague[.]ribbon_margin_v AS(.|
)*?;", sql,
    perl = TRUE))
  expect_true(nzchar(body))
  last_own <- max(gregexpr("own_team_score", body, fixed = TRUE)[[1]])
  expect_true(regexpr("source_event_order", body, fixed = TRUE) < last_own)
})

# Shared by the reconciliation tests below: bind RIBBON_SQL_ISRAEL or
# RIBBON_SQL_EURO by evaluating just that assignment out of global.R, which
# the suite does not source. Reading the real constant (rather than pasting a
# copy here) is the point -- a copy would keep passing after the query changed.
ribbon_sql_for <- function(league) {
  const <- if (identical(league, "israel")) "RIBBON_SQL_ISRAEL" else "RIBBON_SQL_EURO"
  src <- paste(readLines(testthat::test_path("..", "..", "R", "global.R"),
                         warn = FALSE), collapse = "\n")
  assign_txt <- regmatches(src, regexpr(paste0(const, ' <- "(.|\n)*?"\n'),
                                        src, perl = TRUE))
  stopifnot(nzchar(assign_txt))
  eval(parse(text = assign_txt))
}

# Every reconciliation below runs against BOTH leagues. The feature's standing
# constraint is "both leagues or neither", and the spec's evidence covers both
# (301/301 Israeli and 323/323 EuroLeague player-games). A test that checked
# only one would let the other drift silently -- which is exactly how the two
# leagues' tab code diverged three ways before.
RIBBON_LEAGUES <- list(
  israel = list(
    games = "SELECT DISTINCT ON (game_id) game_id, team_id
             FROM basketball_test.final_schedule_mv
             WHERE game_year = 2026 ORDER BY game_id LIMIT %d",
    minutes = "SELECT player_id::text AS player_key, SUM(minutes) AS mv_min
               FROM basketball_test.player_four_factors_by_game
               WHERE game_id = $1 AND team_id = $2
                 AND is_on_key = 1 AND type_lineup = 'offense'
               GROUP BY player_id"),
  euroleague = list(
    games = "SELECT DISTINCT ON (game_id) game_id, team_id
             FROM euroleague.final_schedule ORDER BY game_id DESC LIMIT %d",
    minutes = "SELECT player_id::text AS player_key, SUM(minutes) AS mv_min
               FROM euroleague.player_four_factors_by_game
               WHERE game_id = $1 AND team_id = $2
                 AND is_on_key = 1 AND type_lineup = 'offense'
               GROUP BY player_id")
)

ribbon_db_con <- function() {
  DBI::dbConnect(RPostgres::Postgres(),
    host = Sys.getenv("PG_HOST"), port = as.integer(Sys.getenv("PG_PORT")),
    dbname = Sys.getenv("PG_DB"), user = Sys.getenv("PG_USER"),
    password = Sys.getenv("PG_PASS"), sslmode = Sys.getenv("PG_SSLMODE"),
    connect_timeout = 15L, bigint = "numeric")
}

# Run the real query and rebuild what the builder would draw.
ribbon_fixture <- function(con, league, game_id, team_id) {
  row <- DBI::dbGetQuery(con, ribbon_sql_for(league),
                         params = list(game_id, team_id))
  lanes_raw <- jsonlite::fromJSON(row$lanes[1], simplifyDataFrame = TRUE)
  marg_raw <- jsonlite::fromJSON(row$margin[1], simplifyDataFrame = TRUE)
  steps <- data.frame(elapsed = as.numeric(marg_raw$elapsed),
                      order_key = as.numeric(marg_raw$order_key),
                      margin = as.numeric(marg_raw$margin),
                      own = as.numeric(marg_raw$own))
  lanes <- merge_adjacent_stints(ribbon_normalise_lanes(lanes_raw, team_id))
  lanes <- ribbon_stint_points(lanes, steps)
  lanes <- ribbon_side_perspective(lanes)
  list(lanes = lanes, steps = steps)
}

# The PRE-MERGE lanes, plus the same step series. ribbon_fixture() above
# returns the merged bars the chart draws; the two reconciliations at the end
# of this file need the rows those bars were merged FROM, so this stops short
# of merge_adjacent_stints() and of the side flip.
ribbon_raw_fixture <- function(con, league, game_id, team_id) {
  row <- DBI::dbGetQuery(con, ribbon_sql_for(league),
                         params = list(game_id, team_id))
  lanes_raw <- jsonlite::fromJSON(row$lanes[1], simplifyDataFrame = TRUE)
  marg_raw <- jsonlite::fromJSON(row$margin[1], simplifyDataFrame = TRUE)
  list(lanes = ribbon_normalise_lanes(lanes_raw, team_id),
       steps = data.frame(elapsed = as.numeric(marg_raw$elapsed),
                          order_key = as.numeric(marg_raw$order_key),
                          margin = as.numeric(marg_raw$margin),
                          own = as.numeric(marg_raw$own)))
}

test_that("ribbon floor time equals the app's published per-game minutes", {
  # Measured 2026-09-06 before any of this was built: 301/301 Israeli
  # player-games agreed to 7e-15 and 323/323 EuroLeague ones to 0.002 min
  # (the MV stores 3 decimals). This asserts it, so a change to either
  # minutes path is caught here rather than by a reader noticing two
  # different totals for one player in one app.
  skip_if_not(nzchar(Sys.getenv("RUN_DB_TESTS")), "RUN_DB_TESTS not enabled")
  skip_if_not(nzchar(Sys.getenv("PG_HOST")), "no database configured")
  con <- ribbon_db_con()
  on.exit(DBI::dbDisconnect(con), add = TRUE)

  for (league in names(RIBBON_LEAGUES)) {
    cfg <- RIBBON_LEAGUES[[league]]
    games <- DBI::dbGetQuery(con, sprintf(cfg$games, 5L))
    expect_gt(nrow(games), 0)

    for (i in seq_len(nrow(games))) {
      fx <- ribbon_fixture(con, league, games$game_id[i], games$team_id[i])
      tot <- ribbon_player_totals(fx$lanes)
      tot <- tot[tot$side == "own", , drop = FALSE]

      mv <- DBI::dbGetQuery(con, cfg$minutes,
                            params = list(games$game_id[i], games$team_id[i]))

      both <- merge(tot, mv, by = "player_key")
      # info = so a failure names the league and game rather than just a row.
      expect_gt(nrow(both), 0)
      expect_true(all(abs(both$secs / 60 - both$mv_min) < 0.01),
                  info = sprintf("%s game %s", league, games$game_id[i]))
    }
  }
})

test_that("each bar's +/- equals the margin curve's rise across that bar", {
  # The self-consistency the whole chart rests on: a reader can check a
  # printed number against the curve drawn above it by eye, so it must never
  # be possible for the two to disagree.
  #
  # L7 narrowed this to the OWN block: ribbon_side_perspective() (called by
  # ribbon_fixture() above, matching build_stint_ribbon_svg()) negates pm and
  # swaps pf/pa on opp rows so an opponent five reads its own result, not the
  # clicked team's. That is a deliberate design decision, not a regression --
  # see the mirrored opp assertion in the next test below.
  skip_if_not(nzchar(Sys.getenv("RUN_DB_TESTS")), "RUN_DB_TESTS not enabled")
  skip_if_not(nzchar(Sys.getenv("PG_HOST")), "no database configured")
  con <- ribbon_db_con()
  on.exit(DBI::dbDisconnect(con), add = TRUE)

  for (league in names(RIBBON_LEAGUES)) {
    games <- DBI::dbGetQuery(con, sprintf(RIBBON_LEAGUES[[league]]$games, 3L))
    expect_gt(nrow(games), 0)

    for (i in seq_len(nrow(games))) {
      fx <- ribbon_fixture(con, league, games$game_id[i], games$team_id[i])
      # Without this, an empty fx$lanes would make `rise` and `own_bars$pm`
      # both numeric(0), and expect_equal() passes vacuously on that pair.
      expect_gt(nrow(fx$lanes), 0)

      own_bars <- fx$lanes[fx$lanes$side == "own", , drop = FALSE]
      opp_bars <- fx$lanes[fx$lanes$side == "opp", , drop = FALSE]
      expect_gt(nrow(own_bars), 0)
      expect_gt(nrow(opp_bars), 0)

      mar <- data.frame(elapsed = fx$steps$elapsed,
                        order_key = fx$steps$order_key,
                        value = fx$steps$margin)
      curve_rise_over <- function(bars) {
        sum(ribbon_score_as_of(mar, bars$end_elapsed) -
              ribbon_score_as_of(mar, bars$start_elapsed))
      }
      rise <- ribbon_score_as_of(mar, own_bars$end_elapsed) -
              ribbon_score_as_of(mar, own_bars$start_elapsed)
      expect_equal(own_bars$pm, rise)

      # And pf - pa must reproduce it, which ties the printed number to the
      # for/against pair the strip shows.
      expect_equal(own_bars$pm, own_bars$pf - own_bars$pa)

      # The same property, mirrored: an opponent bar's +/- is the NEGATIVE
      # of the clicked team's curve rise across it.
      expect_equal(sum(opp_bars$pm), -(curve_rise_over(opp_bars)), tolerance = 1e-9)
    }
  }
})

test_that("a player's bars sum to their gutter total", {
  skip_if_not(nzchar(Sys.getenv("RUN_DB_TESTS")), "RUN_DB_TESTS not enabled")
  skip_if_not(nzchar(Sys.getenv("PG_HOST")), "no database configured")
  con <- ribbon_db_con()
  on.exit(DBI::dbDisconnect(con), add = TRUE)

  for (league in names(RIBBON_LEAGUES)) {
    games <- DBI::dbGetQuery(con, sprintf(RIBBON_LEAGUES[[league]]$games, 1L))
    expect_gt(nrow(games), 0)

    fx <- ribbon_fixture(con, league, games$game_id[1], games$team_id[1])
    # Without this, an empty fx$lanes would make `tot` zero-row and the
    # expect_equal() below would compare numeric(0) to numeric(0), which
    # testthat's expect_equal() passes vacuously.
    expect_gt(nrow(fx$lanes), 0)

    tot <- ribbon_player_totals(fx$lanes)

    key <- paste(fx$lanes$side, fx$lanes$player_key, sep = "\r")
    by_hand <- tapply(fx$lanes$pm, key, sum)
    tkey <- paste(tot$side, tot$player_key, sep = "\r")
    expect_equal(as.numeric(by_hand[tkey]), tot$pm)
  }
})

test_that("fetch_stint_ribbon captures the raw steps series before ribbon_complete_margin runs", {
  # The comment/token test above ("the reader returns a raw steps frame
  # alongside the drawn margin") only asserts that "steps = steps" and
  # "steps <- margin" appear SOMEWHERE in global.R -- a regression that
  # computed `steps` from the completed margin (e.g. moved the assignment
  # below ribbon_complete_margin(), or read from the already-completed
  # `margin` variable after that call) would still satisfy it. This asserts
  # SOURCE ORDER within fetch_stint_ribbon's own body: `steps <- margin` must
  # occur strictly before the `margin <- ribbon_complete_margin(` call, or
  # per-stint as-of lookups would read the padded/collapsed curve instead of
  # the recorded events.
  #
  # Scoped to the function body (not grep-anywhere-in-file) because the body
  # itself contains an explanatory comment mentioning
  # "ribbon_complete_margin()" ABOVE the real `steps <- margin` line -- a
  # bare-token search for "ribbon_complete_margin(" would match that comment
  # first and report the wrong order. Matching the actual assignment
  # "margin <- ribbon_complete_margin(" avoids that trap.
  src <- paste(readLines(testthat::test_path("..", "..", "R", "global.R"),
                        warn = FALSE), collapse = "\n")
  body <- regmatches(src, regexpr(
    "fetch_stint_ribbon <- function\\((.|\n)*?\n\\}\n", src, perl = TRUE))
  expect_true(nzchar(body))

  steps_pos <- regexpr("steps <- margin", body, fixed = TRUE)
  complete_pos <- regexpr("margin <- ribbon_complete_margin\\(", body, perl = TRUE)
  expect_true(steps_pos > 0)
  expect_true(complete_pos > 0)
  expect_true(steps_pos < complete_pos)
})


test_that("a lineup key identifies exactly five players, both leagues", {
  # Task 4 reused the identities each league already had -- Israel's
  # lineup_hash, EuroLeague's sorted player_ids array -- instead of minting a
  # new identifier. The entire justification for that is that they ALREADY
  # partition the floor into fives, so it is asserted here on live data rather
  # than assumed. Restores brief 4 Step 4, which the shelved batch never wrote.
  skip_if_not(nzchar(Sys.getenv("RUN_DB_TESTS")), "RUN_DB_TESTS not enabled")
  skip_if_not(nzchar(Sys.getenv("PG_HOST")), "no database configured")
  con <- ribbon_db_con()
  on.exit(DBI::dbDisconnect(con), add = TRUE)

  checked <- 0L
  for (league in names(RIBBON_LEAGUES)) {
    games <- DBI::dbGetQuery(con, sprintf(RIBBON_LEAGUES[[league]]$games, 3L))
    expect_gt(nrow(games), 0)

    for (i in seq_len(nrow(games))) {
      fx <- ribbon_raw_fixture(con, league, games$game_id[i], games$team_id[i])
      expect_gt(nrow(fx$lanes), 0)
      expect_false(any(is.na(fx$lanes$lineup_key)))
      expect_true(all(nzchar(fx$lanes$lineup_key)))

      # One (side, lineup_key, start_elapsed) is one five on the floor. The
      # start is part of the key because the same five can retake the floor
      # later in the game, which is a second occupancy of one lineup.
      per <- tapply(fx$lanes$player_key,
                    paste(fx$lanes$side, fx$lanes$lineup_key,
                          fx$lanes$start_elapsed),
                    function(x) length(unique(x)))
      expect_true(all(per == 5),
                  info = sprintf("%s game %s", league, games$game_id[i]))
      checked <- checked + length(per)
    }
  }
  # Non-vacuous: proves fives were actually counted, not that the loops were
  # skipped. Four stub-passing tests shipped in the predecessor plan for want
  # of exactly this line.
  expect_gt(checked, 50L)
})

test_that("segments reconcile with their bar on live data, both leagues", {
  # The offline invariant proves the telescoping property on a synthetic
  # three-segment fixture. This proves it where the feature lives: real bars,
  # 86.3% of which span more than one five (median 4). Restores brief 6 Step 4,
  # which the shelved batch never wrote.
  #
  # Deliberately reads the PRE-flip numbers (ribbon_stint_points() straight off
  # merge_adjacent_stints()), because the telescoping identity holds on both
  # sides before ribbon_side_perspective() and the flip is a presentation step.
  skip_if_not(nzchar(Sys.getenv("RUN_DB_TESTS")), "RUN_DB_TESTS not enabled")
  skip_if_not(nzchar(Sys.getenv("PG_HOST")), "no database configured")
  con <- ribbon_db_con()
  on.exit(DBI::dbDisconnect(con), add = TRUE)

  checked <- 0L
  scored <- 0L
  for (league in names(RIBBON_LEAGUES)) {
    games <- DBI::dbGetQuery(con, sprintf(RIBBON_LEAGUES[[league]]$games, 3L))
    expect_gt(nrow(games), 0)

    for (i in seq_len(nrow(games))) {
      fx <- ribbon_raw_fixture(con, league, games$game_id[i], games$team_id[i])
      expect_gt(nrow(fx$lanes), 0)
      bars <- ribbon_stint_points(merge_adjacent_stints(fx$lanes), fx$steps)
      expect_gt(nrow(bars), 0)

      for (b in seq_len(nrow(bars))) {
        seg <- ribbon_stint_points(
          ribbon_stint_segments(fx$lanes, bars$side[b], bars$player_key[b],
                                bars$start_elapsed[b], bars$end_elapsed[b]),
          fx$steps)
        expect_gt(nrow(seg), 0)
        expect_equal(sum(seg$pm), bars$pm[b], tolerance = 1e-9,
                     info = sprintf("%s game %s bar %d", league,
                                    games$game_id[i], b))
        checked <- checked + 1L
        if (!is.na(bars$pm[b])) scored <- scored + 1L
      }
    }
  }
  # Non-vacuous: proves bars were actually compared.
  expect_gt(checked, 100L)
  # And that most of them carried real numbers. A scoreless game (Task 1:
  # games 139/140/141/143) yields NA on both sides of the comparison, and
  # expect_equal(NA, NA) passes without testing the arithmetic.
  expect_gt(scored, 50L)
})
