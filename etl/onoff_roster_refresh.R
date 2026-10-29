# D23: season readers use the current roster, including OFF before joining.
# Snapshot before importing games, then refresh earlier games for teams gaining
# a player. Use lookup membership: it is the base0 owner of both season readers
# and the per-game refresh, rather than names or the provider's roster flags.
onoff_roster_snapshot <- function(pg, schema) {
  qs <- as.character(DBI::dbQuoteIdentifier(pg, schema))
  DBI::dbGetQuery(pg, paste0("SELECT DISTINCT game_year, team_id, player_id FROM ", qs, ".lineups_lookup"))
}

onoff_roster_refresh_ids <- function(processed_ids, before, after, games) {
  key <- function(d) paste(d$game_year, d$team_id, d$player_id, sep = ":")
  gained <- after[!key(after) %in% key(before), , drop = FALSE]
  team_key <- function(d) paste(d$game_year, d$team_id, sep = ":")
  historical <- games$game_id[team_key(games) %in% team_key(gained)]
  sort(unique(as.integer(c(processed_ids, historical))))
}

# A failed Phase 4 can leave the new roster already committed. Recover missing
# historical coverage too, so rerunning does not rely only on an in-memory diff.
onoff_missing_roster_games <- function(pg, schema, processed_ids) {
  if (!length(processed_ids)) return(integer(0))
  ids <- paste(sort(unique(as.integer(processed_ids))), collapse = ",")
  qs <- as.character(DBI::dbQuoteIdentifier(pg, schema))
  sql <- paste0("WITH affected AS (
      SELECT DISTINCT game_year, team_id FROM ", qs, ".lineups_lookup
      WHERE game_id IN (", ids, ")
    ), lookup AS (
      SELECT DISTINCT ll.player_id, ll.team_id, ll.lineup_hash,
        coalesce(ll.is_on_verdict, 0)::int AS is_on_key
      FROM ", qs, ".lineups_lookup ll
    ), expected AS (
      SELECT DISTINCT d.game_id, s.game_year, d.team_id, ll.player_id, ll.is_on_key
      FROM ", qs, ".df_pts_poss_lineups_longer_mv d
      JOIN ", qs, ".schedule s ON s.game_id = d.game_id
      JOIN affected a ON a.game_year = s.game_year AND a.team_id = d.team_id
      JOIN lookup ll ON ll.team_id = d.team_id AND ll.lineup_hash = d.credit_lineup_hash
    ), stale_teams AS (
      SELECT DISTINCT e.game_year, e.team_id FROM expected e
      WHERE NOT EXISTS (
      SELECT 1 FROM ", qs, ".player_four_factors_by_game p
      WHERE p.game_id = e.game_id AND p.team_id = e.team_id
        AND p.player_id = e.player_id AND p.is_on_key = e.is_on_key
      ) AND e.game_id NOT IN (", ids, ")
    ) SELECT DISTINCT ll.game_id FROM ", qs, ".lineups_lookup ll
      JOIN stale_teams t ON t.game_year = ll.game_year AND t.team_id = ll.team_id
      ORDER BY ll.game_id")
  as.integer(DBI::dbGetQuery(pg, sql)$game_id)
}

onoff_refresh_games <- function(pg, schema, processed_ids, before) {
  qs <- as.character(DBI::dbQuoteIdentifier(pg, schema))
  after <- onoff_roster_snapshot(pg, schema)
  games <- DBI::dbGetQuery(pg, paste0("SELECT DISTINCT game_year, game_id, team_id FROM ", qs, ".lineups_lookup"))
  ids <- onoff_roster_refresh_ids(processed_ids, before, after, games)
  sort(unique(c(ids, onoff_missing_roster_games(pg, schema, processed_ids))))
}
