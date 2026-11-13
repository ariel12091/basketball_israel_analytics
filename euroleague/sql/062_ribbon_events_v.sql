-- Credit-attribution definition of ribbon_events_v, EuroLeague migration 062 (EUROLEAGUE SHADOW SCHEMA),
-- written by scripts/apply_062_readers.py from the live definition.

CREATE OR REPLACE VIEW euroleague.ribbon_events_v AS  SELECT a.game_id,
    a.team_id,
    a.period,
    a.event_elapsed_seconds AS elapsed_seconds,
    a.source_event_order,
    a.type_lineup,
    l.player_ids,
    a.points,
    a.fg2_att,
    a.fg2_made,
    a.fg3_att,
    a.fg3_made,
    a.ft_attempts,
    a.turnovers,
    a.orebounds,
    a.final_end_poss,
    a.own_team_score,
    a.opp_team_score
   FROM euroleague.action_team_context_actions a
     LEFT JOIN ( SELECT DISTINCT lineup_totals_by_game.game_id,
            lineup_totals_by_game.team_id,
            lineup_totals_by_game.own_lineup,
            lineup_totals_by_game.player_ids
           FROM euroleague.lineup_totals_by_game) l ON l.game_id = a.game_id AND l.team_id = a.team_id AND l.own_lineup = a.credit_own_lineup
  WHERE (a.type_lineup = ANY (ARRAY['offense'::text, 'defense'::text])) AND (a.final_end_poss IS TRUE OR a.points > 0 OR a.fg2_att > 0 OR a.fg3_att > 0 OR a.ft_attempts > 0 OR a.turnovers > 0 OR a.orebounds > 0);
