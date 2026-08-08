-- EuroLeague shadow schema -- migration 007.
-- Make the player four-factor refresh usable at season scale, and put its
-- output grain back on the Israeli contract.
--
-- Applied twice. Revision 1 (2026-08-07) was a pure query-plan fix and left
-- output untouched. Revision 2 (2026-08-08) deliberately changes output: it
-- corrects a grain deviation from the Israeli materialized view this function
-- was ported from. See "Revision 2" below.
--
-- ---------------------------------------------------------------------------
-- Revision 1 -- query plan only. Behaviour unchanged. Verified by replacing
-- the function inside a transaction, re-deriving gamecodes 1-3, and running
-- EXCEPT ALL in both directions against the rows the original load had already
-- stored: 6,240 rows, zero either way, then rolled back.
--
-- One column is expected to move and is excluded from that comparison:
-- derived_at, the row's derivation timestamp. Note that comparing two fresh
-- runs inside ONE transaction will not show it, because now() is the
-- transaction timestamp -- compare against stored rows instead.
--
-- Two changes, both copied from the Israeli schema's function of the same
-- name, which does the same job without either problem:
--
-- 1. MATERIALIZED on `counts` and `player_minutes`. PostgreSQL 12+ inlines a
--    CTE referenced exactly once. Both were, so both were folded into the
--    final LEFT JOINs, where the estimate collapsed to rows=1 and the planner
--    chose a nested loop -- re-running the whole aggregate once per output
--    row. EXPLAIN ANALYZE showed the `counts` GroupAggregate executing with
--    loops=1824, sorting 17,934 rows and spilling each time: 553,583 temp
--    block reads (~4.2 GB) to produce 1,824 rows. The Israeli function never
--    hits this because its heavy CTEs (`clean_stats`) are referenced several
--    times and are therefore materialised automatically.
--
-- 2. Predicate pushdown onto the base relations. The game filter lived only in
--    `target_games`, and every base table reached it through a join, so
--    `game_id` was never a predicate on actions_raw/full_rosters and the
--    planner could not use their statistics -- it estimated 6 rows where 546
--    were returned. The Israeli function applies the same
--    `game_ids IS NULL OR <t>.game_id = ANY(game_ids)` test directly to its
--    base relation, which is why its estimates stay sane. target_games is
--    kept for the columns it supplies (season, home/away team, load run).
--
-- Measured on the live 84-game schema, per refresh call:
--
--     games in call     before        after
--     1                 25.18 s        1.13 s
--     2                123.58 s        3.56 s
--     4                 >2 min          10.99 s   (hit statement_timeout before)
--
-- The remaining structural gap is unchanged and deliberate: the Israeli
-- pipeline reads a persisted event x team-perspective fact
-- (df_pts_poss_lineups_longer_mv) that already carries type_lineup,
-- lineup_hash, starter context and segment seconds, while this function still
-- re-derives all of that from actions_raw on every refresh. See
-- euroleague/CLAUDE.md, table remark 7.
--
-- ---------------------------------------------------------------------------
-- Revision 2 -- output grain. This one is intended to change stored rows.
--
-- Migration 008's acceptance gate rebuilds this table's grain from the new
-- action_team_context fact and diffs both ways. It failed, and the fault was
-- here, not in the fact: complete_grid was an unconditional cross join.
--
--     real_roster
--       JOIN starter_contexts   (every (own_starters, opp_starters) pair the
--                                team saw anywhere in the game)
--       CROSS JOIN is_on_key {0, 1}
--       CROSS JOIN type_lineup {offense, defense}
--
-- That asserts every roster player occupied every starter-count bucket in both
-- on-court states. The measured counts then arrive by LEFT JOIN, so a
-- (player, is_on_key, own_starters, opp_starters) combination that never
-- happened survives as a row that is zero on every measure column. On the
-- three controlled games the stored population was exactly
-- players x buckets x 4 for all six team-games (912, 1248, 960) and 3,341 of
-- the 6,240 rows -- 53.5% -- were zero on every measure.
--
-- The Israeli source has no cross join at all. Its grain is observation
-- driven: base0 (player x lineup x on/off) INNER JOINs lineup_totals, so a row
-- exists if and only if a real lineup with real totals existed in that bucket
-- and the player was associated with it. Starter-count buckets are a
-- consequence of which lineups actually faced each other, not a dimension to
-- densify.
--
-- The fix restricts the grid to observed combinations, and does so through the
-- join rather than by filtering all-zero rows afterwards -- the two are not the
-- same thing. An observed combination whose events happened to measure nothing
-- is a legitimate row and the Israeli logic emits it; only never-occurred
-- combinations may disappear. player_minutes already carries exactly the
-- observed set: it is built from joint_segments (the real matchup segments)
-- joined to real_roster and lineup_players, at grain
-- (game_id, team_id, player_id, is_on_key, own_starters, opp_starters). So
-- complete_grid now reads player_minutes and crosses it only with the two
-- type_lineup sides, starter_contexts is gone, and the LEFT JOIN to counts is
-- kept so an observed-but-scoreless combination still yields its zero row.
--
-- complete_grid therefore has to be defined after player_minutes; a
-- non-recursive WITH item can only reference earlier ones. That reordering is
-- the whole of the textual change besides the removed CTE.
--
-- The second deviation the 008 investigation named -- player_minutes carrying
-- floor time onto the defense row -- was already correct in revision 1 and is
-- untouched. player_minutes has no type_lineup column and the final LEFT JOIN
-- does not mention one, but the final SELECT wraps both minutes and
-- onoff_minutes in CASE WHEN type_lineup = 'offense' ... ELSE 0, which is the
-- same zeroing the Israeli onoff_lineup_minutes does. Confirmed on the stored
-- data: zero defense rows with a non-zero minutes or onoff_minutes value.
--
-- Verified on gamecodes 1-3 (game_id 1, 4, 5):
--   * every rate column of player_onoff_by_season and
--     player_four_factors_by_season for game_year 2025 is byte-identical
--     before and after -- the removed rows were inert;
--   * stored rows fall 6,240 -> 3,910, the surviving set is a strict subset of
--     the previous one with no measure changed, and all 2,330 removed rows were
--     zero on every measure. 1,011 all-zero rows survive: those are observed
--     combinations that simply measured nothing, and they are meant to stay;
--   * offense-row minutes unchanged per player and bucket, defense rows still
--     zero, player_onoff_by_season.minutes_on unmoved.

BEGIN;

SET LOCAL search_path TO euroleague, public;

CREATE OR REPLACE FUNCTION euroleague.refresh_player_four_factors_by_game_for_games(
  game_ids bigint[]
)
RETURNS bigint
LANGUAGE plpgsql
AS $function$
DECLARE
  inserted_count bigint := 0;
BEGIN
  PERFORM euroleague.refresh_stint_timing_for_games(game_ids);

  IF game_ids IS NULL OR array_length(game_ids, 1) IS NULL THEN
    DELETE FROM euroleague.player_four_factors_by_game;
  ELSE
    DELETE FROM euroleague.player_four_factors_by_game
    WHERE game_id = ANY(game_ids);
  END IF;

  INSERT INTO euroleague.player_four_factors_by_game (
    player_id, team_id, game_id, game_year, is_on_key, type_lineup,
    num_starters, own_starters, opp_starters, total_points, total_poss,
    ts_poss_count, oreb_count, oreb_opportunities, tov_count, steal_count,
    deflection_count, total_ft_attempts, total_fga, total_fgm,
    total_fg3_made, player_ts_poss_count, player_tov_count, minutes,
    fg2_made, fg2_att, fg3_made, fg3_att, layup_made, layup_att,
    dunk_made, dunk_att, c3_made, c3_att, c3_known_att, onoff_minutes,
    load_run_id, derivation_version
  )
  WITH target_games AS (
    SELECT s.*
    FROM euroleague.schedule s
    WHERE game_ids IS NULL OR s.game_id = ANY(game_ids)
  ),
  real_roster AS (
    SELECT fr.game_id, fr.team_id, fr.player_id
    FROM euroleague.full_rosters fr
    JOIN euroleague.players p ON p.player_id = fr.player_id
    JOIN target_games tg ON tg.game_id = fr.game_id
    WHERE lower(p.provider_player_id) NOT IN ('team', 'total')
      AND lower(btrim(p.display_name)) NOT IN ('team', 'total')
      AND (game_ids IS NULL OR fr.game_id = ANY(game_ids))
  ),
  clock_parts AS (
    SELECT
      ar.game_id,
      ar.source_event_order,
      ar.period,
      CASE WHEN ar.period <= 4 THEN (ar.period - 1) * 600
           ELSE 2400 + (ar.period - 5) * 300 END::numeric AS period_start,
      CASE WHEN ar.period <= 4 THEN 600 ELSE 300 END::numeric AS period_length,
      CASE
        WHEN ar.marker_time ~ '^\d{1,2}:\d{2}$' THEN
          split_part(ar.marker_time, ':', 1)::integer * 60
          + split_part(ar.marker_time, ':', 2)::integer
      END::numeric AS clock_remaining
    FROM euroleague.actions_raw ar
    JOIN target_games tg ON tg.game_id = ar.game_id
    WHERE game_ids IS NULL OR ar.game_id = ANY(game_ids)
  ),
  raw_elapsed AS (
    SELECT
      cp.*,
      CASE
        WHEN cp.clock_remaining IS NOT NULL THEN
          cp.period_start + cp.period_length
          - least(greatest(cp.clock_remaining, 0), cp.period_length)
        ELSE NULL
      END::numeric AS raw_event_elapsed_seconds
    FROM clock_parts cp
  ),
  event_clock AS (
    SELECT
      re.game_id,
      re.source_event_order,
      coalesce(
        max(re.raw_event_elapsed_seconds) OVER (
          PARTITION BY re.game_id
          ORDER BY re.source_event_order
          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ),
        re.period_start
      )::numeric AS event_elapsed_seconds
    FROM raw_elapsed re
  ),
  game_ends AS (
    SELECT
      ar.game_id,
      (2400 + greatest(max(ar.period) - 4, 0) * 300)::numeric
        AS game_end_elapsed_seconds
    FROM euroleague.actions_raw ar
    JOIN target_games tg ON tg.game_id = ar.game_id
    WHERE game_ids IS NULL OR ar.game_id = ANY(game_ids)
    GROUP BY ar.game_id
  ),
  event_base AS (
    SELECT
      ar.game_id,
      ar.source_event_order,
      ar.team_id AS event_team_id,
      ar.player_id AS action_player_id,
      ar.play_type,
      ar.play_info,
      ac.synthetic_ft_trip_id,
      root.play_type AS parent_play_type,
      al.home_lineup_id,
      al.away_lineup_id,
      tg.home_team_id,
      tg.away_team_id,
      p.offense_team_id AS endpoint_offense_team_id,
      row_number() OVER (
        PARTITION BY ar.game_id, ac.synthetic_ft_trip_id
        ORDER BY ar.source_event_order DESC
      ) AS ft_reverse_order
    FROM target_games tg
    JOIN euroleague.actions_raw ar ON ar.game_id = tg.game_id
    JOIN euroleague.actions_clean ac
      ON ac.game_id = ar.game_id
     AND ac.source_event_order = ar.source_event_order
    JOIN euroleague.actions_raw root
      ON root.game_id = ac.game_id
     AND root.source_event_order = ac.synthetic_parent_order
    JOIN euroleague.action_lineups al
      ON al.game_id = ar.game_id
     AND al.source_event_order = ar.source_event_order
    LEFT JOIN euroleague.possessions p
      ON p.game_id = ar.game_id
     AND p.endpoint_source_event_order = ar.source_event_order
    WHERE game_ids IS NULL OR ar.game_id = ANY(game_ids)
  ),
  event_metrics AS (
    SELECT
      eb.*,
      CASE eb.play_type
        WHEN '2FGM' THEN 2 WHEN '3FGM' THEN 3 WHEN 'FTM' THEN 1 ELSE 0
      END::integer AS points,
      CASE WHEN eb.play_type IN ('2FGM', '2FGA', '3FGM', '3FGA') THEN 1
           WHEN eb.play_type IN ('FTM', 'FTA')
            AND eb.synthetic_ft_trip_id IS NOT NULL
            AND eb.parent_play_type = 'CM'
            AND eb.ft_reverse_order = 1 THEN 1 ELSE 0 END::integer
        AS ts_possessions,
      CASE WHEN eb.play_type = 'O' THEN 1 ELSE 0 END::integer AS orebounds,
      CASE WHEN eb.play_type IN ('2FGA', '3FGA') THEN 1
           WHEN eb.play_type = 'FTA'
            AND eb.synthetic_ft_trip_id IS NOT NULL
            AND eb.parent_play_type = 'CM'
            AND eb.ft_reverse_order = 1 THEN 1 ELSE 0 END::integer
        AS oreb_opportunities,
      CASE WHEN eb.play_type = 'TO' THEN 1 ELSE 0 END::integer AS turnovers,
      CASE WHEN eb.play_type = 'ST' THEN 1 ELSE 0 END::integer AS steals,
      CASE WHEN eb.play_type IN ('FTM', 'FTA') THEN 1 ELSE 0 END::integer
        AS ft_attempts,
      CASE WHEN eb.play_type IN ('2FGM', '2FGA', '3FGM', '3FGA') THEN 1 ELSE 0 END::integer AS fga,
      CASE WHEN eb.play_type IN ('2FGM', '3FGM') THEN 1 ELSE 0 END::integer AS fgm,
      CASE WHEN eb.play_type = '3FGM' THEN 1 ELSE 0 END::integer AS fg3_made,
      CASE WHEN eb.play_type = '2FGM' THEN 1 ELSE 0 END::integer AS fg2_made,
      CASE WHEN eb.play_type IN ('2FGM', '2FGA') THEN 1 ELSE 0 END::integer AS fg2_att,
      CASE WHEN eb.play_type IN ('3FGM', '3FGA') THEN 1 ELSE 0 END::integer AS fg3_att,
      CASE WHEN eb.play_type = '2FGM' AND eb.play_info ILIKE '%lay%up%' THEN 1 ELSE 0 END::integer AS layup_made,
      CASE WHEN eb.play_type IN ('2FGM', '2FGA') AND eb.play_info ILIKE '%lay%up%' THEN 1 ELSE 0 END::integer AS layup_att,
      CASE WHEN eb.play_type = '2FGM' AND eb.play_info ILIKE '%dunk%' THEN 1 ELSE 0 END::integer AS dunk_made,
      CASE WHEN eb.play_type IN ('2FGM', '2FGA') AND eb.play_info ILIKE '%dunk%' THEN 1 ELSE 0 END::integer AS dunk_att
    FROM event_base eb
  ),
  team_event_context AS (
    SELECT
      em.game_id,
      em.source_event_order,
      side.team_id,
      side.opponent_team_id,
      side.own_lineup_id,
      side.opp_lineup_id,
      own_lineup.starter_count AS own_starters,
      opp_lineup.starter_count AS opp_starters,
      ec.event_elapsed_seconds,
      ge.game_end_elapsed_seconds,
      CASE WHEN em.event_team_id = side.team_id THEN em.action_player_id END
        AS off_action_player_id,
      CASE WHEN em.event_team_id = side.team_id THEN em.points ELSE 0 END AS off_points,
      CASE WHEN em.event_team_id = side.opponent_team_id THEN em.points ELSE 0 END AS def_points,
      CASE WHEN em.endpoint_offense_team_id = side.team_id THEN 1 ELSE 0 END AS off_possessions,
      CASE WHEN em.endpoint_offense_team_id = side.opponent_team_id THEN 1 ELSE 0 END AS def_possessions,
      CASE WHEN em.event_team_id = side.team_id THEN em.ts_possessions ELSE 0 END AS off_ts_possessions,
      CASE WHEN em.event_team_id = side.opponent_team_id THEN em.ts_possessions ELSE 0 END AS def_ts_possessions,
      CASE WHEN em.event_team_id = side.team_id THEN em.orebounds ELSE 0 END AS off_orebounds,
      CASE WHEN em.event_team_id = side.opponent_team_id THEN em.orebounds ELSE 0 END AS def_orebounds,
      CASE WHEN em.event_team_id = side.team_id THEN em.oreb_opportunities ELSE 0 END AS off_oreb_opportunities,
      CASE WHEN em.event_team_id = side.opponent_team_id THEN em.oreb_opportunities ELSE 0 END AS def_oreb_opportunities,
      CASE WHEN em.event_team_id = side.team_id THEN em.turnovers ELSE 0 END AS off_turnovers,
      CASE WHEN em.event_team_id = side.opponent_team_id THEN em.turnovers ELSE 0 END AS def_turnovers,
      CASE WHEN em.event_team_id = side.team_id THEN em.steals ELSE 0 END AS def_steals,
      CASE WHEN em.event_team_id = side.opponent_team_id THEN em.steals ELSE 0 END AS off_steals,
      CASE WHEN em.event_team_id = side.team_id THEN em.ft_attempts ELSE 0 END AS off_ft_attempts,
      CASE WHEN em.event_team_id = side.opponent_team_id THEN em.ft_attempts ELSE 0 END AS def_ft_attempts,
      CASE WHEN em.event_team_id = side.team_id THEN em.fga ELSE 0 END AS off_fga,
      CASE WHEN em.event_team_id = side.opponent_team_id THEN em.fga ELSE 0 END AS def_fga,
      CASE WHEN em.event_team_id = side.team_id THEN em.fgm ELSE 0 END AS off_fgm,
      CASE WHEN em.event_team_id = side.opponent_team_id THEN em.fgm ELSE 0 END AS def_fgm,
      CASE WHEN em.event_team_id = side.team_id THEN em.fg3_made ELSE 0 END AS off_fg3_made,
      CASE WHEN em.event_team_id = side.opponent_team_id THEN em.fg3_made ELSE 0 END AS def_fg3_made,
      CASE WHEN em.event_team_id = side.team_id THEN em.fg2_made ELSE 0 END AS off_fg2_made,
      CASE WHEN em.event_team_id = side.opponent_team_id THEN em.fg2_made ELSE 0 END AS def_fg2_made,
      CASE WHEN em.event_team_id = side.team_id THEN em.fg2_att ELSE 0 END AS off_fg2_att,
      CASE WHEN em.event_team_id = side.opponent_team_id THEN em.fg2_att ELSE 0 END AS def_fg2_att,
      CASE WHEN em.event_team_id = side.team_id THEN em.fg3_att ELSE 0 END AS off_fg3_att,
      CASE WHEN em.event_team_id = side.opponent_team_id THEN em.fg3_att ELSE 0 END AS def_fg3_att,
      CASE WHEN em.event_team_id = side.team_id THEN em.layup_made ELSE 0 END AS off_layup_made,
      CASE WHEN em.event_team_id = side.opponent_team_id THEN em.layup_made ELSE 0 END AS def_layup_made,
      CASE WHEN em.event_team_id = side.team_id THEN em.layup_att ELSE 0 END AS off_layup_att,
      CASE WHEN em.event_team_id = side.opponent_team_id THEN em.layup_att ELSE 0 END AS def_layup_att,
      CASE WHEN em.event_team_id = side.team_id THEN em.dunk_made ELSE 0 END AS off_dunk_made,
      CASE WHEN em.event_team_id = side.opponent_team_id THEN em.dunk_made ELSE 0 END AS def_dunk_made,
      CASE WHEN em.event_team_id = side.team_id THEN em.dunk_att ELSE 0 END AS off_dunk_att,
      CASE WHEN em.event_team_id = side.opponent_team_id THEN em.dunk_att ELSE 0 END AS def_dunk_att
    FROM event_metrics em
    JOIN event_clock ec
      ON ec.game_id = em.game_id
     AND ec.source_event_order = em.source_event_order
    JOIN game_ends ge ON ge.game_id = em.game_id
    CROSS JOIN LATERAL (
      VALUES
        (em.home_team_id, em.away_team_id, em.home_lineup_id, em.away_lineup_id),
        (em.away_team_id, em.home_team_id, em.away_lineup_id, em.home_lineup_id)
    ) AS side(team_id, opponent_team_id, own_lineup_id, opp_lineup_id)
    JOIN euroleague.lineups own_lineup ON own_lineup.lineup_id = side.own_lineup_id
    JOIN euroleague.lineups opp_lineup ON opp_lineup.lineup_id = side.opp_lineup_id
  ),
  player_exposure AS (
    SELECT
      tec.*,
      rr.player_id,
      CASE WHEN lp.player_id IS NULL THEN 0 ELSE 1 END::smallint AS is_on_key,
      CASE WHEN tec.off_action_player_id = rr.player_id
        THEN tec.off_ts_possessions ELSE 0 END AS off_player_ts_possessions,
      CASE WHEN tec.off_action_player_id = rr.player_id
        THEN tec.off_turnovers ELSE 0 END AS off_player_turnovers
    FROM team_event_context tec
    JOIN real_roster rr
      ON rr.game_id = tec.game_id AND rr.team_id = tec.team_id
    LEFT JOIN euroleague.lineup_players lp
      ON lp.lineup_id = tec.own_lineup_id AND lp.player_id = rr.player_id
  ),
  player_context AS (
    SELECT
      pe.game_id, pe.team_id, pe.player_id, pe.is_on_key,
      pe.own_starters, pe.opp_starters, context.type_lineup,
      context.total_points, context.total_poss, context.ts_poss_count,
      context.oreb_count, context.oreb_opportunities, context.tov_count,
      context.steal_count, context.total_ft_attempts, context.total_fga,
      context.total_fgm, context.total_fg3_made,
      context.player_ts_poss_count, context.player_tov_count,
      context.fg2_made, context.fg2_att, context.fg3_made, context.fg3_att,
      context.layup_made, context.layup_att, context.dunk_made, context.dunk_att
    FROM player_exposure pe
    CROSS JOIN LATERAL (
      VALUES
        ('offense', pe.off_points, pe.off_possessions, pe.off_ts_possessions,
         pe.off_orebounds, pe.off_oreb_opportunities, pe.off_turnovers,
         pe.off_steals, pe.off_ft_attempts, pe.off_fga, pe.off_fgm,
         pe.off_fg3_made, pe.off_player_ts_possessions,
         pe.off_player_turnovers, pe.off_fg2_made, pe.off_fg2_att,
         pe.off_fg3_made, pe.off_fg3_att, pe.off_layup_made,
         pe.off_layup_att, pe.off_dunk_made, pe.off_dunk_att),
        ('defense', pe.def_points, pe.def_possessions, pe.def_ts_possessions,
         pe.def_orebounds, pe.def_oreb_opportunities, pe.def_turnovers,
         pe.def_steals, pe.def_ft_attempts, pe.def_fga, pe.def_fgm,
         pe.def_fg3_made, 0, 0, pe.def_fg2_made, pe.def_fg2_att,
         pe.def_fg3_made, pe.def_fg3_att, pe.def_layup_made,
         pe.def_layup_att, pe.def_dunk_made, pe.def_dunk_att)
    ) AS context(
      type_lineup, total_points, total_poss, ts_poss_count, oreb_count,
      oreb_opportunities, tov_count, steal_count, total_ft_attempts,
      total_fga, total_fgm, total_fg3_made, player_ts_poss_count,
      player_tov_count, fg2_made, fg2_att, fg3_made, fg3_att,
      layup_made, layup_att, dunk_made, dunk_att
    )
  ),
  counts AS MATERIALIZED (
    SELECT
      pc.game_id, pc.team_id, pc.player_id, pc.is_on_key, pc.type_lineup,
      pc.own_starters, pc.opp_starters,
      sum(pc.total_points)::numeric AS total_points,
      sum(pc.total_poss)::bigint AS total_poss,
      sum(pc.ts_poss_count)::bigint AS ts_poss_count,
      sum(pc.oreb_count)::bigint AS oreb_count,
      sum(pc.oreb_opportunities)::bigint AS oreb_opportunities,
      sum(pc.tov_count)::bigint AS tov_count,
      sum(pc.steal_count)::bigint AS steal_count,
      sum(pc.total_ft_attempts)::bigint AS total_ft_attempts,
      sum(pc.total_fga)::bigint AS total_fga,
      sum(pc.total_fgm)::bigint AS total_fgm,
      sum(pc.total_fg3_made)::bigint AS total_fg3_made,
      sum(pc.player_ts_poss_count)::bigint AS player_ts_poss_count,
      sum(pc.player_tov_count)::bigint AS player_tov_count,
      sum(pc.fg2_made)::integer AS fg2_made,
      sum(pc.fg2_att)::integer AS fg2_att,
      sum(pc.fg3_made)::integer AS fg3_made,
      sum(pc.fg3_att)::integer AS fg3_att,
      sum(pc.layup_made)::integer AS layup_made,
      sum(pc.layup_att)::integer AS layup_att,
      sum(pc.dunk_made)::integer AS dunk_made,
      sum(pc.dunk_att)::integer AS dunk_att
    FROM player_context pc
    GROUP BY pc.game_id, pc.team_id, pc.player_id, pc.is_on_key,
             pc.type_lineup, pc.own_starters, pc.opp_starters
  ),
  joint_lagged AS (
    SELECT
      tec.*,
      lag(tec.own_lineup_id) OVER (
        PARTITION BY tec.game_id, tec.team_id ORDER BY tec.source_event_order
      ) AS previous_own_lineup_id,
      lag(tec.opp_lineup_id) OVER (
        PARTITION BY tec.game_id, tec.team_id ORDER BY tec.source_event_order
      ) AS previous_opp_lineup_id
    FROM team_event_context tec
  ),
  joint_marked AS (
    SELECT
      jl.*,
      CASE
        WHEN jl.previous_own_lineup_id IS DISTINCT FROM jl.own_lineup_id
          OR jl.previous_opp_lineup_id IS DISTINCT FROM jl.opp_lineup_id
        THEN 1 ELSE 0
      END AS new_segment
    FROM joint_lagged jl
  ),
  joint_numbered AS (
    SELECT
      jm.*,
      sum(jm.new_segment) OVER (
        PARTITION BY jm.game_id, jm.team_id
        ORDER BY jm.source_event_order
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
      ) AS segment_number
    FROM joint_marked jm
  ),
  joint_starts AS (
    SELECT
      jn.game_id, jn.team_id, jn.segment_number,
      jn.own_lineup_id, jn.opp_lineup_id,
      jn.own_starters, jn.opp_starters,
      min(jn.source_event_order) AS segment_start_order,
      min(jn.event_elapsed_seconds) AS segment_start_elapsed_seconds,
      max(jn.game_end_elapsed_seconds) AS game_end_elapsed_seconds
    FROM joint_numbered jn
    GROUP BY jn.game_id, jn.team_id, jn.segment_number,
             jn.own_lineup_id, jn.opp_lineup_id,
             jn.own_starters, jn.opp_starters
  ),
  joint_ordered AS (
    SELECT
      js.*,
      lead(js.segment_start_elapsed_seconds) OVER (
        PARTITION BY js.game_id, js.team_id ORDER BY js.segment_number
      ) AS next_segment_start_elapsed_seconds
    FROM joint_starts js
  ),
  joint_segments AS (
    SELECT
      jo.*,
      greatest(
        coalesce(jo.next_segment_start_elapsed_seconds,
                 jo.game_end_elapsed_seconds)
        - jo.segment_start_elapsed_seconds,
        0
      )::numeric AS segment_seconds
    FROM joint_ordered jo
  ),
  player_minutes AS MATERIALIZED (
    SELECT
      rr.game_id, rr.team_id, rr.player_id,
      CASE WHEN lp.player_id IS NULL THEN 0 ELSE 1 END::smallint AS is_on_key,
      js.own_starters, js.opp_starters,
      round(sum(js.segment_seconds) / 60.0, 3) AS minutes
    FROM joint_segments js
    JOIN real_roster rr
      ON rr.game_id = js.game_id AND rr.team_id = js.team_id
    LEFT JOIN euroleague.lineup_players lp
      ON lp.lineup_id = js.own_lineup_id AND lp.player_id = rr.player_id
    GROUP BY rr.game_id, rr.team_id, rr.player_id,
             CASE WHEN lp.player_id IS NULL THEN 0 ELSE 1 END,
             js.own_starters, js.opp_starters
  ),
  -- The output grain. One row per observed
  -- (game, team, player, is_on_key, own_starters, opp_starters) combination
  -- per side. player_minutes is the observed set: it comes from the real
  -- matchup segments, so a combination appears here only if the player was in
  -- that on-court state while the team was in that starter-count bucket.
  complete_grid AS (
    SELECT
      pm.game_id, pm.team_id, pm.player_id, pm.is_on_key,
      side.type_lineup, pm.own_starters, pm.opp_starters
    FROM player_minutes pm
    CROSS JOIN (VALUES ('offense'::text), ('defense'::text)) AS side(type_lineup)
  )
  SELECT
    cg.player_id,
    cg.team_id,
    cg.game_id,
    tg.season AS game_year,
    cg.is_on_key,
    cg.type_lineup,
    cg.own_starters AS num_starters,
    cg.own_starters,
    cg.opp_starters,
    coalesce(c.total_points, 0),
    coalesce(c.total_poss, 0),
    coalesce(c.ts_poss_count, 0),
    coalesce(c.oreb_count, 0),
    coalesce(c.oreb_opportunities, 0),
    coalesce(c.tov_count, 0),
    coalesce(c.steal_count, 0),
    0,
    coalesce(c.total_ft_attempts, 0),
    coalesce(c.total_fga, 0),
    coalesce(c.total_fgm, 0),
    coalesce(c.total_fg3_made, 0),
    coalesce(c.player_ts_poss_count, 0),
    coalesce(c.player_tov_count, 0),
    CASE WHEN cg.type_lineup = 'offense'
      THEN coalesce(pm.minutes, 0) ELSE 0 END,
    coalesce(c.fg2_made, 0),
    coalesce(c.fg2_att, 0),
    coalesce(c.fg3_made, 0),
    coalesce(c.fg3_att, 0),
    coalesce(c.layup_made, 0),
    coalesce(c.layup_att, 0),
    coalesce(c.dunk_made, 0),
    coalesce(c.dunk_att, 0),
    0, 0, 0,
    CASE WHEN cg.type_lineup = 'offense'
      THEN coalesce(pm.minutes, 0) ELSE 0 END,
    tg.last_seen_load_run_id,
    'existing-israeli-contract-v1'
  FROM complete_grid cg
  JOIN target_games tg ON tg.game_id = cg.game_id
  LEFT JOIN counts c
    ON c.game_id = cg.game_id
   AND c.team_id = cg.team_id
   AND c.player_id = cg.player_id
   AND c.is_on_key = cg.is_on_key
   AND c.type_lineup = cg.type_lineup
   AND c.own_starters = cg.own_starters
   AND c.opp_starters = cg.opp_starters
  LEFT JOIN player_minutes pm
    ON pm.game_id = cg.game_id
   AND pm.team_id = cg.team_id
   AND pm.player_id = cg.player_id
   AND pm.is_on_key = cg.is_on_key
   AND pm.own_starters = cg.own_starters
   AND pm.opp_starters = cg.opp_starters;

  GET DIAGNOSTICS inserted_count = ROW_COUNT;
  RETURN inserted_count;
END;
$function$;

COMMIT;
