-- basket.co.il's cross-season links, kept on the registration they came from.
--
-- A player's basket.co.il PlayerId is re-minted every season (2022-2027: no
-- id in two seasons), so nothing in the database tied his registrations
-- together. His profile page does: its past-seasons table links each earlier
-- season's PlayerId, and a tab links the previous season's. Checked
-- 2026-11-12 on the cached profiles: 460 links between consecutive seasons of
-- this table, Hebrew name and date of birth equal on all 460.
--
--   linked_basket_player_ids  the PlayerIds his profile page links, without
--                             his own. NULL = page never read; '{}' = read,
--                             no links. Ids before 2022 have no row here and
--                             are kept anyway: they still join a person.
--   basket_person_id          one person across seasons: the smallest PlayerId
--                             among the registrations the links join (the
--                             earliest registration basket.co.il knows), set
--                             by etl/basket_person_ids.R.
--
-- Filled by scripts/load_basket_registration_links.R from the cache, then by
-- the Basket refresh (profile pages of the stats rows and the team-page rows).

ALTER TABLE basketball_test.basket_player_registration
  ADD COLUMN linked_basket_player_ids integer[],
  ADD COLUMN basket_person_id integer CHECK (basket_person_id IS NULL OR basket_person_id > 0);

CREATE INDEX basket_player_registration_links_idx
  ON basketball_test.basket_player_registration USING gin (linked_basket_player_ids);
CREATE INDEX basket_player_registration_person_idx
  ON basketball_test.basket_player_registration (basket_person_id);

COMMENT ON COLUMN basketball_test.basket_player_registration.linked_basket_player_ids IS
  'PlayerIds the player''s basket.co.il profile links (past-seasons table, previous-season tab), own id excluded. NULL = profile not read; {} = no links.';
COMMENT ON COLUMN basketball_test.basket_player_registration.basket_person_id IS
  'One person across seasons: smallest PlayerId in the component the links join (etl/basket_person_ids.R).';
