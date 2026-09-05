# Helpers for the minutes migration; sourcing this file does not access the DB.
rebuild_minutes_relations <- function(con, targets, definitions) {
  for (target in rev(targets)) {
    kind <- DBI::dbGetQuery(con,
      "SELECT relkind::text AS kind FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
       WHERE n.nspname='basketball_test' AND c.relname=$1", params = list(target$name))$kind
    if (!length(kind)) next
    type <- switch(kind, m = "MATERIALIZED VIEW", r = "TABLE",
                   stop("Unexpected relation type: ", target$name))
    # RESTRICT deliberately aborts on any dependent object outside the registry.
    DBI::dbExecute(con, sprintf("DROP %s %s RESTRICT", type,
      DBI::dbQuoteIdentifier(con, DBI::Id(schema = "basketball_test", table = target$name))))
  }
  for (i in seq_along(targets)) {
    message("Creating ", targets[[i]]$name)
    # Execute complete SQL: splitting on semicolons corrupts comments/DO blocks.
    # immediate = TRUE uses the simple query protocol. The extended one takes
    # a single statement, and these files carry the CREATE plus its indexes;
    # splitting on semicolons is not an option (comments, DO blocks).
    DBI::dbExecute(con, definitions[[i]], immediate = TRUE)
    DBI::dbGetQuery(con, sprintf("SELECT count(*) FROM %s",
      DBI::dbQuoteIdentifier(con, DBI::Id(schema = "basketball_test", table = targets[[i]]$name))))
    index_matches <- regmatches(definitions[[i]], gregexpr(
      "(?i)CREATE\\s+(?:UNIQUE\\s+)?INDEX\\s+(?:IF\\s+NOT\\s+EXISTS\\s+)?[a-zA-Z0-9_]+",
      definitions[[i]], perl = TRUE))[[1]]
    for (match in index_matches) {
      index_name <- sub(".*\\s", "", match, perl = TRUE)
      valid <- DBI::dbGetQuery(con,
        "SELECT i.indisvalid AND i.indisready AS valid
         FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid
         JOIN pg_namespace n ON n.oid=c.relnamespace
         WHERE n.nspname='basketball_test' AND c.relname=$1
           AND i.indrelid=to_regclass($2)",
        params = list(index_name, paste0("basketball_test.", targets[[i]]$name)))$valid
      if (length(valid) != 1L || !isTRUE(valid[[1]])) stop("Missing/invalid index: ", index_name)
    }
  }
}

# Games whose SOURCE data cannot conserve minutes, excluded from the
# conservation gates by game_id and reported when they are hit. Verified
# 2026-09-05 to be broken BEFORE this migration, not by it:
#
#   178    truth x5 199.00, already 210.12 / 215.42 today. The documented
#          invalid Q2 reset -- a provider 10:00 reset block after live Q2
#          00:00 actions inflates segment coverage.
#          See docs/game_178_invalid_q2_reset_2026-05-30.md.
#   62452  already 191.00 / 172.83 today against 199.67. Same old-season id
#          family as 62506/62511, which the canonical-clock handoff records
#          as carrying segment-start jitter. The migration IMPROVES both
#          teams here (one back inside tolerance).
#
# docs/canonical_clock_minutes_handoff_2026-07-21.md names 'historical
# player-minute conservation defects' as known and out of scope. Excluding
# them keeps the tolerance tight enough to catch a real regression; widening
# it to 27 minutes would not.
KNOWN_MINUTE_DEFECT_GAMES <- c(178L, 62452L)

assert_minute_rows <- function(truth, actual, label, tolerance = 1e-6) {
  keys <- c("game_id", "team_id")
  hit <- intersect(KNOWN_MINUTE_DEFECT_GAMES,
                   union(truth$game_id, actual$game_id))
  if (length(hit)) {
    message(label, ": excluding known source-defect game(s) ",
            paste(hit, collapse = ", "))
    truth  <- truth[!truth$game_id   %in% KNOWN_MINUTE_DEFECT_GAMES, , drop = FALSE]
    actual <- actual[!actual$game_id %in% KNOWN_MINUTE_DEFECT_GAMES, , drop = FALSE]
  }
  if (!nrow(truth) || !nrow(actual) || anyDuplicated(truth[keys]) ||
      anyDuplicated(actual[keys])) stop(label, ": empty or duplicate team-game keys")
  joined <- merge(truth, actual, by = keys, all = TRUE, suffixes = c("_truth", "_actual"))
  # 14 team-games across 7 games sit in the team MVs with no segments at all in
  # df_pts_poss_lineups_longer_mv, and already carry NULL minutes. A full outer
  # join keeps them, NA fails is.finite(), and the migration would roll back on
  # rows that have nothing to conserve. Exempt only the case where BOTH sides
  # are absent -- a value on one side and not the other is still a failure.
  both_missing <- is.na(joined$minutes_truth) & is.na(joined$minutes_actual)
  if (any(both_missing)) {
    message(label, ": ", sum(both_missing),
            " team-game(s) absent from both sides, skipped")
  }
  bad <- !both_missing &
    (!is.finite(joined$minutes_truth) | !is.finite(joined$minutes_actual) |
       abs(joined$minutes_truth - joined$minutes_actual) > tolerance)
  if (any(bad)) {
    print(utils::head(joined[bad, ], 20))
    stop(label, ": minute conservation failed for ", sum(bad), " team-games")
  }
  invisible(TRUE)
}

# onoff_default_mv and player_traditional_stats_mv now compute the same thing:
# the sum of distinct on-court segment durations per player. They are built by
# different CTE chains, so agreeing to a rounding tolerance is a real
# cross-check rather than a restatement. player_traditional_stats_mv is NOT
# rebuilt by this migration, so it is an independent reference.
assert_onoff_matches_traditional <- function(con, tolerance = 0.051) {
  cmp <- DBI::dbGetQuery(con, '
    SELECT o.player_id, o.team_id, o."Year"::int AS game_year,
           o.minutes AS onoff_minutes, t.minutes AS traditional_minutes
      FROM basketball_test.onoff_default_mv o
      JOIN basketball_test.player_traditional_stats_mv t
        ON t.player_id = o.player_id AND t.team_id = o.team_id
       AND t.game_year = o."Year"::int')
  if (!nrow(cmp)) stop('onoff vs traditional: join produced no rows')
  bad <- !is.finite(cmp$onoff_minutes) | !is.finite(cmp$traditional_minutes) |
    abs(cmp$onoff_minutes - cmp$traditional_minutes) > tolerance
  if (any(bad)) {
    print(utils::head(cmp[bad, ], 20))
    stop('onoff vs traditional: ', sum(bad), ' of ', nrow(cmp),
         ' player-seasons disagree on minutes')
  }
  message('onoff vs traditional: ', nrow(cmp), ' player-seasons agree')
  invisible(TRUE)
}

# Five players are on court at all times, so a team-game's player-minutes come
# to 5x its floor time. This is approximate at the margins (the source can
# record other than five), so the tolerance is wide -- it exists to catch the
# whole-minutes class of regression this migration fixes (195.30 vs 200.04
# per team-game), not to police rounding.
assert_player_minutes_conserved <- function(con, truth, tolerance = 0.5) {
  # minutes sits once per slice, on the offense row
  # (player x is_on_key x num_starters x own_starters x opp_starters), so the
  # slices simply sum. An earlier version deduplicated with DISTINCT on the
  # minutes VALUE, which silently collapsed two distinct slices that happened
  # to hold the same number -- undercounting, and making this gate weaker than
  # the others. Slices whose minutes never made it into the MV are exactly the
  # defect being tested, so they must stay missing here rather than be
  # papered over.
  actual <- DBI::dbGetQuery(con, "
    SELECT game_id, team_id, sum(minutes) AS minutes
    FROM basketball_test.player_four_factors_by_game
    WHERE is_on_key = 1 AND type_lineup = 'offense' AND minutes IS NOT NULL
    GROUP BY game_id, team_id")
  scaled <- truth; scaled$minutes <- scaled$minutes * 5
  assert_minute_rows(scaled, actual, 'player_four_factors_by_game minutes',
                     tolerance = tolerance)
}

# onoff_minutes is a second, independent minute path on the same relation. It
# is summed across the type_lineup/starters slices rather than deduplicated,
# so it is checked on its own rather than folded into the published-minutes
# assertion above.
assert_onoff_minutes_conserved <- function(con, truth, tolerance = 0.5) {
  actual <- DBI::dbGetQuery(con, "
    SELECT game_id, team_id, sum(onoff_minutes) AS minutes
    FROM basketball_test.player_four_factors_by_game
    WHERE is_on_key = 1 GROUP BY game_id, team_id")
  scaled <- truth; scaled$minutes <- scaled$minutes * 5
  assert_minute_rows(scaled, actual, 'player_four_factors_by_game onoff_minutes',
                     tolerance = tolerance)
}

verify_minutes_migration <- function(con) {
  truth <- DBI::dbGetQuery(con, "
    SELECT game_id, team_id, sum(seconds)/60.0 AS minutes FROM (
      SELECT game_id, team_id, lineup_hash, segment_id, max(segment_seconds) AS seconds
      FROM basketball_test.df_pts_poss_lineups_longer_mv
      WHERE segment_id IS NOT NULL
      GROUP BY game_id, team_id, lineup_hash, segment_id
    ) s GROUP BY game_id, team_id")
  for (name in c("mv_lineup_totals_by_day", "lineup_four_factors_by_game")) {
    actual <- DBI::dbGetQuery(con, sprintf(
      "SELECT game_id, team_id, sum(minutes) AS minutes FROM basketball_test.%s
       WHERE type_lineup='offense' GROUP BY game_id, team_id", name))
    assert_minute_rows(truth, actual, name)
  }
  for (name in c("team_metrics_by_game_mv", "team_metrics_rolling_mv")) {
    for (column in c("off_minutes", "def_minutes")) {
      actual <- DBI::dbGetQuery(con, sprintf(
        "SELECT game_id, team_id, %s AS minutes FROM basketball_test.%s", column, name))
      # These published columns are numeric(10,1), unlike the raw lineup minutes.
      assert_minute_rows(truth, actual, paste(name, column), tolerance = 0.050001)
    }
  }
  assert_player_minutes_conserved(con, truth)
  assert_onoff_minutes_conserved(con, truth)
  assert_onoff_matches_traditional(con)
}

# Exercise the installed ETL writers, not just the CREATE TABLE definitions.
# Snapshots live only in the migration transaction and are dropped on commit.
verify_minutes_refresh_parity <- function(con) {
  surfaces <- list(
    pff = list(table = "player_four_factors_by_game", columns = "*",
      refresh = "refresh_player_four_factors_by_game_for_games(NULL::int4[])"),
    onoff = list(table = "onoff_default_mv", columns = 'player_id, team_id, "Year", minutes',
      refresh = "refresh_onoff_default_for_games(NULL::int4[])"),
    team = list(table = "team_metrics_by_game_mv", columns = "game_id, team_id, off_minutes, def_minutes",
      refresh = "refresh_team_metrics_by_game_for_games(NULL::int4[])"),
    sub = list(table = "sub_lineups_stats", columns = "*",
      refresh = "refresh_sub_lineups_stats_for_games(ARRAY(SELECT DISTINCT game_id FROM basketball_test.df_pts_poss_lineups_longer_mv))")
  )
  for (key in names(surfaces)) {
    s <- surfaces[[key]]
    query <- sprintf("SELECT %s FROM basketball_test.%s", s$columns, s$table)
    snapshot <- paste0("minutes_before_", key)
    DBI::dbExecute(con, sprintf("CREATE TEMP TABLE %s ON COMMIT DROP AS %s", snapshot, query))
    message("ETL parity: ", s$table)
    DBI::dbGetQuery(con, paste0("SELECT basketball_test.", s$refresh))
    differences <- DBI::dbGetQuery(con, sprintf(
      "SELECT count(*) AS n FROM ((SELECT * FROM %s EXCEPT ALL %s)
       UNION ALL (%s EXCEPT ALL SELECT * FROM %s)) d", snapshot, query, query, snapshot))$n[[1]]
    if (differences != 0) stop(s$table, ": ETL refresh changed ", differences, " rows from rebuilt results")
    message("ETL parity PASS: ", s$table)
  }
  verify_minutes_migration(con)
}
