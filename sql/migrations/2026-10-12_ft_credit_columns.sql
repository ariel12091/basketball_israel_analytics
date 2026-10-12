-- Which lineup each action is CREDITED to. Equal to the physical columns for
-- every row except a free throw, which takes its parent action's (the foul,
-- or the made shot of an and-one). Events are aggregated on these columns;
-- floor time stays on the physical ones.
-- Populated by basketball_test.refresh_ft_credit_fields_for_games().
-- Adding nullable columns with no default is metadata-only: no table rewrite.
ALTER TABLE basketball_test.df_pts_poss_lineups_longer_mv
  ADD COLUMN IF NOT EXISTS credit_lineup_hash  text,
  ADD COLUMN IF NOT EXISTS credit_segment_id   bigint,
  ADD COLUMN IF NOT EXISTS credit_num_starters integer,
  ADD COLUMN IF NOT EXISTS credit_own_starters integer,
  ADD COLUMN IF NOT EXISTS credit_opp_starters integer;
