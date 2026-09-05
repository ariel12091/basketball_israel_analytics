-- DROP FUNCTION basketball_test.refresh_sub_lineups_stats();

CREATE OR REPLACE FUNCTION basketball_test.refresh_sub_lineups_stats()
 RETURNS void
 LANGUAGE sql
 SET enable_nestloop = off
AS $function$
WITH
-- One display name per (team, player, game_year)
fr_latest AS (
  SELECT
    fr.team_id,
    fr.player_id,
    fr.game_year,
    MIN(btrim(fr.firstname) || ' ' || btrim(fr.lastname)) AS name
  FROM basketball_test.full_rosters fr
  GROUP BY
    fr.team_id,
    fr.player_id,
    fr.game_year
),

-- 2/3/4: one deduped row per sub-lineup identity *per season*
sub_dedup_234 AS (
  SELECT DISTINCT
    s.team_id,
    s.sub_lineup_hash,
    s.player_ids,
    s.num_lineup,
    s.game_year
  FROM basketball_test.sub_lineups s
  WHERE s.num_lineup IN (2,3,4)
),

-- 5-man: full lineup arrays from lineups_lookup_on (sorted unique), per season
full_lineups AS (
  SELECT
    l.team_id,
    l.game_year,
    l.lineup_hash,
    ARRAY_AGG(DISTINCT l.player_id ORDER BY l.player_id)::int4[] AS player_ids
  FROM basketball_test.lineups_lookup_on l
  GROUP BY
    l.team_id,
    l.game_year,
    l.lineup_hash
),
sub_dedup_5 AS (
  SELECT
    fl.team_id,
    fl.lineup_hash AS sub_lineup_hash,         -- use lineup_hash as sub_lineup_hash for 5-man
    fl.player_ids,
    cardinality(fl.player_ids)::int2 AS num_lineup,
    fl.game_year
  FROM full_lineups fl
  WHERE cardinality(fl.player_ids) = 5        -- only 5-man groups
),

-- union of all sizes for identity + names, per season
sub_dedup AS (
  SELECT * FROM sub_dedup_234
  UNION ALL
  SELECT * FROM sub_dedup_5
),

-- build ordered names once per (team, sub_lineup_hash, game_year)
names_by_sub AS (
  SELECT
    d.team_id,
    d.sub_lineup_hash,
    d.game_year,
    ARRAY_AGG(
      COALESCE(fr.name, '#' || u.player_id::text)
      ORDER BY u.ord
    ) AS player_names,
    STRING_AGG(
      COALESCE(fr.name, '#' || u.player_id::text),
      ', '
      ORDER BY u.ord
    ) AS player_names_str
  FROM sub_dedup d
  JOIN LATERAL UNNEST(d.player_ids) WITH ORDINALITY AS u(player_id, ord) ON TRUE
  LEFT JOIN fr_latest fr
    ON fr.team_id   = d.team_id
   AND fr.player_id = u.player_id
   AND fr.game_year = d.game_year
  GROUP BY
    d.team_id,
    d.sub_lineup_hash,
    d.game_year
),

-- sub_lineup -> lineup_hash mappings, per season
sub_map_234 AS (
  SELECT DISTINCT
    s.team_id,
    s.sub_lineup_hash,
    s.lineup_hash,
    s.game_year
  FROM basketball_test.sub_lineups s
  WHERE s.num_lineup IN (2,3,4)
),
sub_map_5 AS (
  SELECT
    fl.team_id,
    fl.lineup_hash AS sub_lineup_hash,
    fl.lineup_hash AS lineup_hash,
    fl.game_year
  FROM full_lineups fl
  WHERE cardinality(fl.player_ids) = 5
),
sub_map AS (
  SELECT * FROM sub_map_234
  UNION ALL
  SELECT * FROM sub_map_5
),

-- schedule → game_year for df_pts_poss_lineups_longer_mv
sched AS (
  SELECT DISTINCT
    s.game_id,
    s.game_year
  FROM basketball_test.schedule s
),

-- only aggregate the fact once per (lineup_hash, type_lineup, game_year)
needed_lineups AS (
  SELECT DISTINCT
    sm.lineup_hash,
    sm.game_year
  FROM sub_map sm
),

-- Stint duration per segment (no type_lineup - captures full floor time)
-- Note: segment_id repeats across games, so must include game_id in GROUP BY
segment_times AS (
  SELECT
    f.lineup_hash,
    f.game_id,
    sc.game_year,
    f.segment_id,
    MAX(f.segment_seconds) AS stint_seconds
  FROM basketball_test.df_pts_poss_lineups_longer_mv f
  JOIN sched sc
    ON sc.game_id = f.game_id
  JOIN needed_lineups nl
    ON nl.lineup_hash = f.lineup_hash
   AND nl.game_year   = sc.game_year
  WHERE f.segment_seconds IS NOT NULL
  GROUP BY
    f.lineup_hash,
    f.game_id,
    sc.game_year,
    f.segment_id
),

-- Poss/pts per segment per type_lineup
segment_stats AS (
  SELECT
    f.lineup_hash,
    f.game_id,
    sc.game_year,
    f.type_lineup,
    f.segment_id,
    SUM(CASE WHEN COALESCE(f.final_end_poss, FALSE) THEN 1 ELSE 0 END) AS total_poss,
    COALESCE(SUM(f.team_score), 0)                                      AS total_pts,
    SUM(CASE WHEN f.type = 'shot' AND f.parameters_points = 2 AND f.parameters_made = 'made' THEN 1 ELSE 0 END) AS fg2_made,
    SUM(CASE WHEN f.type = 'shot' AND f.parameters_points = 2 THEN 1 ELSE 0 END) AS fg2_att,
    SUM(CASE WHEN f.type = 'shot' AND f.parameters_points = 3 AND f.parameters_made = 'made' THEN 1 ELSE 0 END) AS fg3_made,
    SUM(CASE WHEN f.type = 'shot' AND f.parameters_points = 3 THEN 1 ELSE 0 END) AS fg3_att,
    -- Numerator for the possession-weighted own-starters average. Summed at the
    -- possession grain rather than added to the GROUP BY, so the join to
    -- segment_times below cannot fan out if num_starters ever varies inside a
    -- segment. Equivalent to fetch_lineups_all's SUM(num_starters * total_poss),
    -- including its handling of NULLs: a NULL contributes nothing to the
    -- numerator while its possession still counts in the denominator.
    SUM(CASE WHEN COALESCE(f.final_end_poss, FALSE) THEN COALESCE(f.num_starters, 0) ELSE 0 END) AS starters_poss_num
  FROM basketball_test.df_pts_poss_lineups_longer_mv f
  JOIN sched sc
    ON sc.game_id = f.game_id
  JOIN needed_lineups nl
    ON nl.lineup_hash = f.lineup_hash
   AND nl.game_year   = sc.game_year
  GROUP BY
    f.lineup_hash,
    f.game_id,
    sc.game_year,
    f.type_lineup,
    f.segment_id
),

lineup_totals AS (
  SELECT
    ss.lineup_hash,
    ss.game_year,
    ss.type_lineup,
    SUM(ss.total_poss) AS total_poss,
    SUM(ss.total_pts) AS total_pts,
    SUM(ss.fg2_made) AS fg2_made,
    SUM(ss.fg2_att) AS fg2_att,
    SUM(ss.fg3_made) AS fg3_made,
    SUM(ss.fg3_att) AS fg3_att,
    SUM(ss.starters_poss_num) AS starters_poss_num
  FROM segment_stats ss
  JOIN segment_times st
    ON st.lineup_hash = ss.lineup_hash
    AND st.game_id = ss.game_id
    AND st.game_year = ss.game_year
    AND st.segment_id = ss.segment_id
  GROUP BY
    ss.lineup_hash,
    ss.game_year,
    ss.type_lineup
),

-- totals per (team, sub_lineup_hash, game_year, type)
per_type AS (
  SELECT
    sm.team_id,
    sm.sub_lineup_hash,
    sm.game_year,
    lt.type_lineup,
    SUM(lt.total_poss) AS total_poss,
    SUM(lt.total_pts)  AS total_pts,
    SUM(lt.fg2_made) AS fg2_made,
    SUM(lt.fg2_att) AS fg2_att,
    SUM(lt.fg3_made) AS fg3_made,
    SUM(lt.fg3_att) AS fg3_att,
    SUM(lt.starters_poss_num) AS starters_poss_num
  FROM sub_map sm
  JOIN lineup_totals lt
    ON lt.lineup_hash = sm.lineup_hash
   AND lt.game_year   = sm.game_year
  GROUP BY
    sm.team_id,
    sm.sub_lineup_hash,
    sm.game_year,
    lt.type_lineup
),

-- pivot Off/Def and join names + identity, per season
sub_minutes AS (
  SELECT sm.team_id, sm.sub_lineup_hash, sm.game_year,
         SUM(st.stint_seconds) / 60.0 AS minutes
  FROM sub_map sm
  JOIN segment_times st
    ON st.lineup_hash = sm.lineup_hash AND st.game_year = sm.game_year
  GROUP BY sm.team_id, sm.sub_lineup_hash, sm.game_year
),

final_rows AS (
  SELECT
    d.team_id,
    d.sub_lineup_hash,
    d.num_lineup,
    d.player_ids,
    n.player_names,
    n.player_names_str,
    d.game_year,

    COALESCE(SUM(p.total_poss) FILTER (WHERE p.type_lineup = 'offense'), 0) AS off_poss,
    COALESCE(SUM(p.total_pts)  FILTER (WHERE p.type_lineup = 'offense'), 0) AS off_pts,
    ROUND(
      NULLIF(SUM(p.total_pts)  FILTER (WHERE p.type_lineup = 'offense'), 0)::numeric
      / NULLIF(SUM(p.total_poss) FILTER (WHERE p.type_lineup = 'offense'), 0) * 100,
      1
    ) AS off_ppp,

    COALESCE(SUM(p.total_poss) FILTER (WHERE p.type_lineup = 'defense'), 0) AS def_poss,
    COALESCE(SUM(p.total_pts)  FILTER (WHERE p.type_lineup = 'defense'), 0) AS def_pts,
    ROUND(
      NULLIF(SUM(p.total_pts)  FILTER (WHERE p.type_lineup = 'defense'), 0)::numeric
      / NULLIF(SUM(p.total_poss) FILTER (WHERE p.type_lineup = 'defense'), 0) * 100,
      1
    ) AS def_ppp,

    -- Duration is independent of offensive-event row existence.
    ROUND(COALESCE(MAX(pm.minutes), 0)::numeric, 1) AS minutes,

    COALESCE(SUM(p.fg2_made) FILTER (WHERE p.type_lineup = 'offense'), 0) AS off_fg2_made,
    COALESCE(SUM(p.fg2_att)  FILTER (WHERE p.type_lineup = 'offense'), 0) AS off_fg2_att,
    COALESCE(SUM(p.fg3_made) FILTER (WHERE p.type_lineup = 'offense'), 0) AS off_fg3_made,
    COALESCE(SUM(p.fg3_att)  FILTER (WHERE p.type_lineup = 'offense'), 0) AS off_fg3_att,
    COALESCE(SUM(p.fg2_made) FILTER (WHERE p.type_lineup = 'defense'), 0) AS def_fg2_made,
    COALESCE(SUM(p.fg2_att)  FILTER (WHERE p.type_lineup = 'defense'), 0) AS def_fg2_att,
    COALESCE(SUM(p.fg3_made) FILTER (WHERE p.type_lineup = 'defense'), 0) AS def_fg3_made,
    COALESCE(SUM(p.fg3_att)  FILTER (WHERE p.type_lineup = 'defense'), 0) AS def_fg3_att,

    -- Deliberately unfiltered by type_lineup: the average is weighted by
    -- offensive AND defensive possessions, matching fetch_lineups_all lines
    -- 315/485. num_starters is always own-perspective, so both sides are the
    -- same quantity. The denominator is the stored off_poss + def_poss.
    COALESCE(SUM(p.starters_poss_num), 0) AS starters_poss_num

  FROM sub_dedup d
  JOIN names_by_sub n
    ON n.team_id        = d.team_id
   AND n.sub_lineup_hash = d.sub_lineup_hash
   AND n.game_year       = d.game_year
  LEFT JOIN sub_minutes pm
    ON pm.team_id = d.team_id AND pm.sub_lineup_hash = d.sub_lineup_hash
   AND pm.game_year = d.game_year
  LEFT JOIN per_type p
    ON p.team_id        = d.team_id
   AND p.sub_lineup_hash = d.sub_lineup_hash
   AND p.game_year       = d.game_year
  GROUP BY
    d.team_id,
    d.sub_lineup_hash,
    d.num_lineup,
    d.player_ids,
    n.player_names,
    n.player_names_str,
    d.game_year
)

INSERT INTO basketball_test.sub_lineups_stats (
  team_id,
  sub_lineup_hash,
  num_lineup,
  player_ids,
  player_names,
  player_names_str,
  off_poss,
  off_pts,
  off_ppp,
  def_poss,
  def_pts,
  def_ppp,
  minutes,
  game_year,
  off_fg2_made,
  off_fg2_att,
  off_fg3_made,
  off_fg3_att,
  def_fg2_made,
  def_fg2_att,
  def_fg3_made,
  def_fg3_att,
  starters_poss_num
)
SELECT
  team_id,
  sub_lineup_hash,
  num_lineup,
  player_ids,
  player_names,
  player_names_str,
  off_poss,
  off_pts,
  off_ppp,
  def_poss,
  def_pts,
  def_ppp,
  minutes,
  game_year,
  off_fg2_made,
  off_fg2_att,
  off_fg3_made,
  off_fg3_att,
  def_fg2_made,
  def_fg2_att,
  def_fg3_made,
  def_fg3_att,
  starters_poss_num
FROM final_rows
ON CONFLICT (team_id, sub_lineup_hash, game_year) DO UPDATE
SET
  num_lineup       = EXCLUDED.num_lineup,
  player_ids       = EXCLUDED.player_ids,
  player_names     = EXCLUDED.player_names,
  player_names_str = EXCLUDED.player_names_str,
  off_poss         = EXCLUDED.off_poss,
  off_pts          = EXCLUDED.off_pts,
  off_ppp          = EXCLUDED.off_ppp,
  def_poss         = EXCLUDED.def_poss,
  def_pts          = EXCLUDED.def_pts,
  def_ppp          = EXCLUDED.def_ppp,
  minutes          = EXCLUDED.minutes,
  off_fg2_made     = EXCLUDED.off_fg2_made,
  off_fg2_att      = EXCLUDED.off_fg2_att,
  off_fg3_made     = EXCLUDED.off_fg3_made,
  off_fg3_att      = EXCLUDED.off_fg3_att,
  def_fg2_made     = EXCLUDED.def_fg2_made,
  def_fg2_att      = EXCLUDED.def_fg2_att,
  def_fg3_made     = EXCLUDED.def_fg3_made,
  def_fg3_att      = EXCLUDED.def_fg3_att,
  starters_poss_num = EXCLUDED.starters_poss_num;
$function$
;
