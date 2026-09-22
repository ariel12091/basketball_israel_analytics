-- Link Basket season registrations to play-by-play player identities.
--
-- The link lives on the Basket row and points into the app's identity space,
-- because that is the only direction it is ever traversed: given a segev
-- player, what does basket.co.il know about them (height, position,
-- nationality, prior-season shooting). Basket is a lookup layer, not another
-- source of players.
--
-- It deliberately does NOT go in player_identity_map. That table resolves a
-- provider's player ids into identities and feeds resolved_player_identity_v,
-- whose mapped_source_players CTE selects every active, season-scoped row
-- without filtering on provider -- so active Basket rows there would surface
-- as phantom source players carrying Basket ids in a segev id column. Keeping
-- the link here avoids that entirely and leaves that view untouched.
--
-- canonical_player_id is the segev id every roster and play-by-play fact keys
-- on, denormalised here so consumers join without a dictionary hop. It is
-- exactly one per (identity, season, team), verified across 2025-2027.

ALTER TABLE basketball_test.basket_player_season
  ADD COLUMN IF NOT EXISTS identity_id bigint
    REFERENCES basketball_test.player_identities(identity_id),
  ADD COLUMN IF NOT EXISTS canonical_player_id integer,
  ADD COLUMN IF NOT EXISTS identity_match_status text
    CHECK (identity_match_status IS NULL OR identity_match_status IN
           ('verified', 'proposal', 'ambiguous', 'unmatched')),
  ADD COLUMN IF NOT EXISTS identity_matched_on text
    CHECK (identity_matched_on IS NULL OR identity_matched_on IN
           ('name_en', 'name_he', 'name_both', 'surname_team', 'manual')),
  ADD COLUMN IF NOT EXISTS identity_matched_at timestamptz;

-- Only a resolved status carries an identity, and only a resolved status says
-- how it was reached. 'ambiguous' and 'unmatched' record that the matcher ran
-- and did not conclude, which is different from NULL (never attempted: the
-- 2022-2024 seasons have no play-by-play counterpart to match against).
ALTER TABLE basketball_test.basket_player_season
  DROP CONSTRAINT IF EXISTS basket_player_season_identity_consistent;
ALTER TABLE basketball_test.basket_player_season
  ADD CONSTRAINT basket_player_season_identity_consistent CHECK (
    (identity_match_status IN ('verified', 'proposal')
       AND identity_id IS NOT NULL
       AND canonical_player_id IS NOT NULL
       AND identity_matched_on IS NOT NULL)
    OR (identity_match_status IN ('ambiguous', 'unmatched')
       AND identity_id IS NULL
       AND canonical_player_id IS NULL)
    OR (identity_match_status IS NULL
       AND identity_id IS NULL
       AND canonical_player_id IS NULL)
  );

-- The consumer's access path: everything Basket knows about one segev player.
CREATE INDEX IF NOT EXISTS basket_player_season_canonical_idx
  ON basketball_test.basket_player_season (canonical_player_id, game_year)
  WHERE identity_match_status = 'verified';

-- One Basket registration per player per season. A person holds a different
-- basket_player_id each season (the ids are re-minted in ascending blocks and
-- no two seasons overlap), so this is a per-season uniqueness claim, not a
-- per-person one.
CREATE UNIQUE INDEX IF NOT EXISTS basket_player_season_identity_unique_idx
  ON basketball_test.basket_player_season (game_year, canonical_player_id)
  WHERE identity_match_status = 'verified';
