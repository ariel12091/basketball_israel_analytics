-- EuroLeague shadow schema -- migration 041: single-pass player exposure and
-- usage in player_traditional_stats_mv.
--
-- player_traditional_stats_mv took 60.2s to produce 657 rows. EXPLAIN
-- (ANALYZE, BUFFERS) attributed 48 of 51 execution seconds to scanning
-- player_four_factors_by_game TWICE:
--
--   exposure      Index Scan euroleague_pff_season_team_idx   168,632 rows  18.4s
--                 Buffers: shared hit=28143 read=7610
--   player_usage  Index Scan player_four_factors_by_game_pkey 383,073 rows  29.7s
--                 Buffers: shared hit=56254 read=21462
--
-- Both CTEs filter type_lineup = 'offense' and group by the identical key
-- (game_id, team_id, player_id). They differ only in that `exposure` adds
-- is_on_key = 1 -- which is a FILTER clause, not a second pass over the table.
-- Neither index condition is selective (offense is about half the relation),
-- so each scan pays whole-table random heap access; the pkey scan is chosen
-- only to supply sort order for a GroupAggregate.
--
-- Merging them is output-identical. An aggregate FILTER matching no rows
-- returns NULL, exactly as the absent LEFT JOIN row did, so the downstream
-- COALESCE(eu.minutes, fr.minutes_seconds / 60.0, 0) fallback is unchanged.
-- Measured against the live schema before writing this migration:
--
--   current (2 scans)   69.5s   13,874 rows
--   merged  (1 scan)    31.5s   13,874 rows
--   row counts equal: True      differing rows: 0
--
-- REFRESH re-runs the stored definition, so changing it requires DROP+CREATE.
-- That wipes the MV's GRANTs and its indexes, both of which are restored
-- below. Nothing depends on this MV (checked via pg_depend/pg_rewrite), so no
-- CASCADE is needed and none is used.

BEGIN;
SET LOCAL search_path TO euroleague, public;

DROP MATERIALIZED VIEW euroleague.player_traditional_stats_mv;

CREATE MATERIALIZED VIEW euroleague.player_traditional_stats_mv AS
WITH exposure_usage AS (
         SELECT pf.game_id,
            pf.team_id,
            pf.player_id,
            (sum(pf.total_poss) FILTER (WHERE (pf.is_on_key = 1)))::bigint AS poss_on_floor,
            sum(pf.minutes) FILTER (WHERE (pf.is_on_key = 1)) AS minutes,
            sum(pf.player_ts_poss_count) AS player_ts_poss,
            sum(pf.player_tov_count) AS player_tov
           FROM euroleague.player_four_factors_by_game pf
          WHERE (pf.type_lineup = 'offense'::text)
          GROUP BY pf.game_id, pf.team_id, pf.player_id
        ), team_usage AS (
         SELECT tf.game_id,
            tf.team_id,
            sum(tf.off_ts_poss) AS team_ts_poss,
            sum(tf.off_tov) AS team_tov,
            sum(tf.off_poss) AS team_poss
           FROM euroleague.team_four_factors_by_game tf
          GROUP BY tf.game_id, tf.team_id
        ), player_games AS (
         SELECT s.competition,
            s.season AS game_year,
            fr.game_id,
            fr.team_id,
            fr.player_id,
            t.display_name AS team_name,
            euroleague.person_display_name(p.display_name) AS player_name,
            COALESCE(eu.poss_on_floor, (0)::bigint) AS poss_on_floor,
            COALESCE(eu.minutes, ((fr.minutes_seconds)::numeric / 60.0), (0)::numeric) AS minutes,
            COALESCE(eu.player_ts_poss, (0)::numeric) AS player_ts_poss,
            COALESCE(eu.player_tov, (0)::numeric) AS player_tov,
            COALESCE(tu.team_ts_poss, (0)::numeric) AS team_ts_poss,
            COALESCE(tu.team_tov, (0)::numeric) AS team_tov,
            COALESCE(tu.team_poss, (0)::numeric) AS team_poss,
            COALESCE(((fr.boxscore_stats ->> 'Points'::text))::numeric, (0)::numeric) AS pts,
            COALESCE(((fr.boxscore_stats ->> 'TotalRebounds'::text))::numeric, (0)::numeric) AS reb,
            COALESCE(((fr.boxscore_stats ->> 'OffensiveRebounds'::text))::numeric, (0)::numeric) AS oreb,
            COALESCE(((fr.boxscore_stats ->> 'DefensiveRebounds'::text))::numeric, (0)::numeric) AS dreb,
            COALESCE(((fr.boxscore_stats ->> 'Assistances'::text))::numeric, (0)::numeric) AS ast,
            COALESCE(((fr.boxscore_stats ->> 'Steals'::text))::numeric, (0)::numeric) AS stl,
            COALESCE(((fr.boxscore_stats ->> 'BlocksFavour'::text))::numeric, (0)::numeric) AS blk,
            COALESCE(((fr.boxscore_stats ->> 'Turnovers'::text))::numeric, (0)::numeric) AS tov,
            COALESCE(((fr.boxscore_stats ->> 'FieldGoalsMade2'::text))::numeric, (0)::numeric) AS fg2m,
            COALESCE(((fr.boxscore_stats ->> 'FieldGoalsAttempted2'::text))::numeric, (0)::numeric) AS fg2a,
            COALESCE(((fr.boxscore_stats ->> 'FieldGoalsMade3'::text))::numeric, (0)::numeric) AS fg3m,
            COALESCE(((fr.boxscore_stats ->> 'FieldGoalsAttempted3'::text))::numeric, (0)::numeric) AS fg3a,
            COALESCE(((fr.boxscore_stats ->> 'FreeThrowsMade'::text))::numeric, (0)::numeric) AS ftm,
            COALESCE(((fr.boxscore_stats ->> 'FreeThrowsAttempted'::text))::numeric, (0)::numeric) AS fta
           FROM (((((euroleague.full_rosters fr
             JOIN euroleague.schedule s ON ((s.game_id = fr.game_id)))
             JOIN euroleague.teams t ON ((t.team_id = fr.team_id)))
             JOIN euroleague.players p ON ((p.player_id = fr.player_id)))
             LEFT JOIN exposure_usage eu ON (((eu.game_id = fr.game_id) AND (eu.team_id = fr.team_id) AND (eu.player_id = fr.player_id))))
             LEFT JOIN team_usage tu ON (((tu.game_id = fr.game_id) AND (tu.team_id = fr.team_id))))
          WHERE ((lower(p.provider_player_id) <> ALL (ARRAY['team'::text, 'total'::text])) AND (lower(btrim(p.display_name)) <> ALL (ARRAY['team'::text, 'total'::text])))
        ), agg AS (
         SELECT player_games.competition,
            player_games.game_year,
            player_games.team_id,
            player_games.player_id,
            min(player_games.team_name) AS team_name,
            min(player_games.player_name) AS player_name,
            (count(*) FILTER (WHERE (player_games.minutes > (0)::numeric)))::integer AS gp,
            sum(player_games.poss_on_floor) AS poss_on_floor,
            sum(player_games.minutes) AS minutes,
            sum(player_games.player_ts_poss) AS player_ts_poss,
            sum(player_games.player_tov) AS player_tov,
            sum(player_games.team_ts_poss) AS team_ts_poss,
            sum(player_games.team_tov) AS team_tov,
            sum(player_games.team_poss) AS team_poss,
            sum(player_games.pts) AS pts,
            sum(player_games.reb) AS reb,
            sum(player_games.oreb) AS oreb,
            sum(player_games.dreb) AS dreb,
            sum(player_games.ast) AS ast,
            sum(player_games.stl) AS stl,
            sum(player_games.blk) AS blk,
            sum(player_games.tov) AS tov,
            sum(player_games.fg2m) AS fg2m,
            sum(player_games.fg2a) AS fg2a,
            sum(player_games.fg3m) AS fg3m,
            sum(player_games.fg3a) AS fg3a,
            sum(player_games.ftm) AS ftm,
            sum(player_games.fta) AS fta
           FROM player_games
          GROUP BY player_games.competition, player_games.game_year, player_games.team_id, player_games.player_id
        )
 SELECT competition,
    game_year,
    team_id,
    player_id,
    team_name,
    player_name AS "Player",
    gp,
    poss_on_floor,
    minutes,
    pts,
    reb,
    oreb,
    dreb,
    ast,
    stl,
    blk,
    NULL::numeric AS dfl,
    tov,
    (fg2m + fg3m) AS fgm,
    (fg2a + fg3a) AS fga,
    round((((100)::numeric * (fg2m + fg3m)) / NULLIF((fg2a + fg3a), (0)::numeric)), 1) AS fg_pct,
    fg3m AS "3pm",
    fg3a AS "3pa",
    round((((100)::numeric * fg3m) / NULLIF(fg3a, (0)::numeric)), 1) AS tp_pct,
    ftm,
    fta,
    round((((100)::numeric * ftm) / NULLIF(fta, (0)::numeric)), 1) AS ft_pct,
    round((((100)::numeric * (fg2m + (1.5 * fg3m))) / NULLIF((fg2a + fg3a), (0)::numeric)), 1) AS efg,
    round((((100)::numeric * pts) / NULLIF(((2)::numeric * player_ts_poss), (0)::numeric)), 1) AS ts,
    round(((((100)::numeric * (player_ts_poss + player_tov)) * team_poss) / NULLIF(((team_ts_poss + team_tov) * poss_on_floor), (0)::numeric)), 1) AS usg_pct
   FROM agg
  WHERE (gp > 0);

-- Recreate both indexes exactly as they were. The unique index is what makes
-- REFRESH ... CONCURRENTLY possible for this relation.
CREATE UNIQUE INDEX euroleague_player_traditional_stats_mv_pk
  ON euroleague.player_traditional_stats_mv
  USING btree (competition, game_year, team_id, player_id);

CREATE INDEX euroleague_player_traditional_stats_mv_team_idx
  ON euroleague.player_traditional_stats_mv
  USING btree (competition, game_year, team_id);

-- DROP wiped app_readonly=r/postgres. Restore it in the same transaction so
-- the app never sees the MV without its grant.
GRANT SELECT ON euroleague.player_traditional_stats_mv TO app_readonly;

COMMIT;
