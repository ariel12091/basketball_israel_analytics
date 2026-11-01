-- One-time: rewrite the lineup names already stored in sub_lineups_stats with
-- the "Player display name" owner (player_display_names_v), as the two refresh
-- functions now do for every row they write. A player the owner has no name
-- for (a reused provider id) keeps the spelling already stored. Only rows whose
-- names change are touched.

WITH renamed AS (
  SELECT
    s.team_id,
    s.sub_lineup_hash,
    s.game_year,
    ARRAY_AGG(
      COALESCE(dn.display_name, s.player_names[u.ord], '#' || u.player_id::text)
      ORDER BY u.ord
    ) AS player_names,
    STRING_AGG(
      COALESCE(dn.display_name, s.player_names[u.ord], '#' || u.player_id::text),
      ', '
      ORDER BY u.ord
    ) AS player_names_str
  FROM basketball_test.sub_lineups_stats s
  JOIN LATERAL UNNEST(s.player_ids) WITH ORDINALITY AS u(player_id, ord) ON TRUE
  LEFT JOIN basketball_test.player_display_names_v dn
    ON dn.team_id = s.team_id
   AND dn.player_id = u.player_id
   AND dn.game_year = s.game_year
  GROUP BY s.team_id, s.sub_lineup_hash, s.game_year
)
UPDATE basketball_test.sub_lineups_stats s
SET player_names = r.player_names,
    player_names_str = r.player_names_str
FROM renamed r
WHERE s.team_id = r.team_id
  AND s.sub_lineup_hash = r.sub_lineup_hash
  AND s.game_year = r.game_year
  AND s.player_names_str IS DISTINCT FROM r.player_names_str;
