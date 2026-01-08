-- basketball_test.mv_lineup_totals_by_day source

CREATE MATERIALIZED VIEW basketball_test.mv_lineup_totals_by_day
TABLESPACE pg_default
AS SELECT d.team_id,
    d.lineup_hash,
    d.type_lineup,
    s.game_date AS g_date,
    d.game_id,
    s.game_year,
    sum(
        CASE
            WHEN COALESCE(d.final_end_poss, false) THEN 1
            ELSE 0
        END) AS total_poss,
    COALESCE(sum(d.team_score), 0::numeric) AS total_pts
   FROM df_pts_poss_lineups_longer_mv d
     JOIN schedule s USING (game_id)
  GROUP BY d.team_id, d.lineup_hash, d.type_lineup, s.game_date, d.game_id, s.game_year
WITH DATA;

-- View indexes:
CREATE INDEX idx_mv_ltotals_day_date ON basketball_test.mv_lineup_totals_by_day USING btree (g_date, lineup_hash, type_lineup);
CREATE UNIQUE INDEX idx_mv_ltotals_day_pk ON basketball_test.mv_lineup_totals_by_day USING btree (lineup_hash, type_lineup, g_date);