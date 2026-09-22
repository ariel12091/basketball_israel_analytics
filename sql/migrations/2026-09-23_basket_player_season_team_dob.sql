-- Capture the team and date of birth the profile pages already carry.
--
-- Both are present on 1160/1160 cached profiles and were simply not parsed.
-- They matter for identity matching: date of birth is a near-unique,
-- language-independent discriminator, and the team lets a Basket
-- season-registration be tied to a specific squad rather than only a name.
--
-- basket_team_name holds the site's own string, sponsor and all
-- ("הפועל IBI תל אביב"). It is deliberately NOT the app's team_id: mapping the
-- 14 league teams to team_id is a separate, reviewable step.
--
-- Additive and nullable: rows written before this migration stay valid until
-- the next (cache-only) backfill fills them.

ALTER TABLE basketball_test.basket_player_season
  ADD COLUMN IF NOT EXISTS basket_team_name text
    CHECK (basket_team_name IS NULL OR btrim(basket_team_name) <> ''),
  ADD COLUMN IF NOT EXISTS date_of_birth date
    -- Observed range in the five loaded seasons is 1983-2009; the bound is
    -- wide enough to be a typo guard, not a business rule.
    CHECK (date_of_birth IS NULL
           OR date_of_birth BETWEEN DATE '1940-01-01' AND CURRENT_DATE);

CREATE INDEX IF NOT EXISTS basket_player_season_dob_idx
  ON basketball_test.basket_player_season (date_of_birth)
  WHERE date_of_birth IS NOT NULL;
