-- The "Player display name" owner (docs/shared_definitions.md): one English and
-- one Hebrew name per (game_year, team_id, player_id), for every surface that
-- shows a player -- the stored lineup names, Shiny, the web API and the Ask chat.
--
-- resolved_player_identity_v resolves identity per GAME, because the provider
-- reuses player ids inside a season (2026 team 12, 1183 is DeAndre Williams in
-- 6 games and Itay Zlotolov in 21). This view collapses it to the season pair:
--
--   one identity in the pair's games
--     English: basket.co.il's name (upper-cased, as every page shows names)
--              -> the identity dictionary's name
--              -> the roster spelling of the most games (ties: A-Z first)
--     Hebrew:  basket.co.il's name -> the dictionary's local name
--              -> the roster's local spelling of the most games
--   two or more identities (a reused id): both names NULL, so each consumer
--     keeps the spelling of its own row; no season name is right for the pair.
--
-- Games are the player's real roster rows. The identity view joins each season
-- mapping to every game of the team (basket identity handoff 2026-09-23 §3.2),
-- so it is restricted to full_rosters on game_id here.
--
-- Basket rows are taken only when verified, and joined on the identity AND the
-- team, so a mid-season transfer (one identity, two registrations) gets each
-- team's own registration.

CREATE OR REPLACE VIEW basketball_test.player_display_names_v AS
WITH roster AS (
  SELECT
    fr.game_id,
    fr.game_year,
    fr.team_id,
    fr.player_id,
    max(NULLIF(btrim(concat_ws(' ', fr.firstnamelocal, fr.lastnamelocal)), '')) AS name_he
  FROM basketball_test.full_rosters fr
  WHERE fr.player_id IS NOT NULL
  GROUP BY fr.game_id, fr.game_year, fr.team_id, fr.player_id
),
per_game AS (
  SELECT
    r.game_year,
    r.team_id,
    r.source_player_id AS player_id,
    r.identity_id,
    r.resolution_scope,
    r.display_name,
    ro.name_he
  FROM basketball_test.resolved_player_identity_v r
  JOIN roster ro
    ON ro.game_id = r.game_id
   AND ro.team_id = r.team_id
   AND ro.player_id = r.source_player_id
),
pairs AS (
  SELECT
    game_year,
    team_id,
    player_id,
    count(DISTINCT identity_id) AS identities,
    min(identity_id) AS identity_id,
    bool_or(resolution_scope <> 'source') AS in_dictionary
  FROM per_game
  GROUP BY game_year, team_id, player_id
),
spelling_en AS (
  SELECT DISTINCT ON (game_year, team_id, player_id)
    game_year, team_id, player_id, display_name
  FROM (
    SELECT game_year, team_id, player_id, display_name, count(*) AS games
    FROM per_game
    WHERE display_name IS NOT NULL
    GROUP BY game_year, team_id, player_id, display_name
  ) s
  ORDER BY game_year, team_id, player_id, games DESC, display_name
),
spelling_he AS (
  SELECT DISTINCT ON (game_year, team_id, player_id)
    game_year, team_id, player_id, name_he
  FROM (
    SELECT game_year, team_id, player_id, name_he, count(*) AS games
    FROM per_game
    WHERE name_he IS NOT NULL
    GROUP BY game_year, team_id, player_id, name_he
  ) s
  ORDER BY game_year, team_id, player_id, games DESC, name_he
),
basket AS (
  SELECT
    b.game_year,
    d.team_id_rosters AS team_id,
    b.identity_id,
    min(NULLIF(btrim(b.player_name_en), '')) AS name_en,
    min(NULLIF(btrim(b.player_name), '')) AS name_he
  FROM basketball_test.basket_player_season b
  JOIN basketball_test.schedule_team_dict d
    ON d.game_year = b.game_year
   AND d.team_name_basket = b.basket_team_name
  WHERE b.identity_match_status = 'verified'
  GROUP BY b.game_year, d.team_id_rosters, b.identity_id
)
SELECT
  p.game_year,
  p.team_id,
  p.player_id,
  CASE WHEN p.identities = 1 THEN p.identity_id END AS identity_id,
  CASE WHEN p.identities = 1
    THEN COALESCE(upper(bk.name_en), en.display_name) END AS display_name,
  CASE WHEN p.identities = 1
    THEN COALESCE(
      bk.name_he,
      NULLIF(btrim(concat_ws(' ', pi.first_name_local, pi.last_name_local)), ''),
      he.name_he
    ) END AS display_name_he,
  CASE
    WHEN p.identities > 1 THEN 'reused_id'
    WHEN bk.name_en IS NOT NULL THEN 'basket'
    WHEN p.in_dictionary THEN 'dictionary'
    ELSE 'roster'
  END AS name_source
FROM pairs p
LEFT JOIN spelling_en en USING (game_year, team_id, player_id)
LEFT JOIN spelling_he he USING (game_year, team_id, player_id)
LEFT JOIN basket bk
  ON p.identities = 1
 AND bk.game_year = p.game_year
 AND bk.team_id = p.team_id
 AND bk.identity_id = p.identity_id
LEFT JOIN basketball_test.player_identities pi
  ON p.identities = 1
 AND p.in_dictionary
 AND pi.identity_id = p.identity_id;

COMMENT ON VIEW basketball_test.player_display_names_v IS
  'Player display name owner (docs/shared_definitions.md): one English and one Hebrew name per (game_year, team_id, player_id). Basket first, then the identity dictionary, then the roster spelling of the most games. NULL names for a reused provider id (two identities in the pair).';
