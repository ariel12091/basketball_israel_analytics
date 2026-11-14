# D23: which earlier games the per-game On/Off table must refresh with a night's
# new games.
#
# The season views (onoff_default_mv, player_advanced_stats_mv) and the
# per-game table (player_four_factors_by_game) both give a player rows only for
# the lineups lineups_lookup pairs him with, matched on (team, lineup) without
# a game or season key (their base0). A new game adds pairings, and one can be
# for a lineup that also played in earlier games: the season views are rebuilt
# whole and pick it up; the per-game table has to refresh those earlier games.
#
# Until 2026-11-14 the trigger keyed on new (season, team, player) members, so
# only a player new to a team set it off. An existing player gaining a pairing
# for an old lineup left those games stale: teams 6 and 17 from the games of
# 2026-11-08/09, failing test-player-reader-paths-db.R every night after.
# Now: the pairings themselves (normal path), plus a stateless check of each
# affected team's earlier games against what the lookup implies today (retry
# after a failed Phase 4, when the snapshot diff no longer sees the change).

onoff_lookup_pairs_sql <- function(qs) paste0(
  "SELECT DISTINCT team_id, player_id, lineup_hash,
          coalesce(is_on_verdict, 0)::int AS is_on_key
   FROM ", qs, ".lineups_lookup")

onoff_roster_snapshot <- function(pg, schema) {
  qs <- as.character(DBI::dbQuoteIdentifier(pg, schema))
  DBI::dbGetQuery(pg, onoff_lookup_pairs_sql(qs))
}

# processed_ids plus every game of the same team that played a lineup which
# gained a (player, on/off) pairing. `games`: (game_id, team_id, lineup_hash).
onoff_roster_refresh_ids <- function(processed_ids, before, after, games) {
  pair <- function(d) paste(d$team_id, d$player_id, d$lineup_hash, d$is_on_key, sep = ":")
  gained <- after[!pair(after) %in% pair(before), , drop = FALSE]
  lineup <- function(d) paste(d$team_id, d$lineup_hash, sep = ":")
  historical <- games$game_id[lineup(games) %in% lineup(gained)]
  sort(unique(as.integer(c(processed_ids, historical))))
}

# Earlier games of the seasons and teams in processed_ids whose stored
# possessions or points per (player, on/off) differ from the lookup's pairings
# today: the refresh function's own sums (credited lineup, possession ends,
# team_score), read back from the table it writes. NULL sums count as 0: the
# function writes 0 for slices that carry only time.
onoff_stale_games <- function(pg, schema, processed_ids) {
  if (!length(processed_ids)) return(integer(0))
  ids <- paste(sort(unique(as.integer(processed_ids))), collapse = ",")
  qs <- as.character(DBI::dbQuoteIdentifier(pg, schema))
  sql <- paste0("WITH affected AS (
      SELECT DISTINCT s.game_year, ll.team_id
      FROM ", qs, ".lineups_lookup ll
      JOIN ", qs, ".schedule s ON s.game_id = ll.game_id
      WHERE ll.game_id IN (", ids, ")
    ), games AS (
      SELECT DISTINCT s.game_id, a.team_id
      FROM ", qs, ".schedule s
      JOIN affected a ON a.game_year = s.game_year
      WHERE s.game_id NOT IN (", ids, ")
    ), lookup AS (", onoff_lookup_pairs_sql(qs), "
    ), expected AS (
      SELECT d.game_id, d.team_id, l.player_id, l.is_on_key,
             SUM(CASE WHEN d.final_end_poss IS TRUE THEN 1 ELSE 0 END) AS poss,
             coalesce(SUM(d.team_score), 0) AS pts
      FROM ", qs, ".df_pts_poss_lineups_longer_mv d
      JOIN games g ON g.game_id = d.game_id AND g.team_id = d.team_id
      JOIN lookup l ON l.team_id = d.team_id AND l.lineup_hash = d.credit_lineup_hash
      GROUP BY 1, 2, 3, 4
    ), stored AS (
      SELECT p.game_id, p.team_id, p.player_id, p.is_on_key,
             coalesce(SUM(p.total_poss), 0) AS poss,
             coalesce(SUM(p.total_points), 0) AS pts
      FROM ", qs, ".player_four_factors_by_game p
      JOIN games g ON g.game_id = p.game_id AND g.team_id = p.team_id
      GROUP BY 1, 2, 3, 4
    )
    SELECT DISTINCT coalesce(e.game_id, s.game_id) AS game_id
    FROM expected e
    FULL JOIN stored s USING (game_id, team_id, player_id, is_on_key)
    WHERE e.poss IS DISTINCT FROM s.poss OR e.pts IS DISTINCT FROM s.pts
    ORDER BY 1")
  as.integer(DBI::dbGetQuery(pg, sql)$game_id)
}

onoff_refresh_games <- function(pg, schema, processed_ids, before) {
  qs <- as.character(DBI::dbQuoteIdentifier(pg, schema))
  after <- onoff_roster_snapshot(pg, schema)
  games <- DBI::dbGetQuery(pg, paste0(
    "SELECT DISTINCT game_id, team_id, lineup_hash FROM ", qs, ".lineups_lookup"))
  ids <- onoff_roster_refresh_ids(processed_ids, before, after, games)
  sort(unique(c(ids, onoff_stale_games(pg, schema, processed_ids))))
}
