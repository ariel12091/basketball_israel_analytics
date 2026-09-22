-- Drop the foreign key from basket_player_season.identity_id to
-- player_identities, added earlier today.
--
-- The column mirrors resolved_player_identity_v.identity_id, which is
--   COALESCE(g.identity_id, y.identity_id, sp.source_player_id::bigint)
-- and therefore falls back to the raw provider id when a player has no
-- curated identity record. That fallback is the view's documented behaviour
-- (resolution_scope = 'source'), not a defect, so a foreign key to
-- player_identities is the wrong constraint for this column.
--
-- It is rare but real: 18 view rows across 2025-2026, one player -- Bnei
-- Herzliya's D.J. Burns (1982), who lost his auto-assigned identity when the
-- incorrect merge with Rishon Lezion's DJ Burns (1143) was retired, because
-- Herzliya then held two source ids displaying the same name. Two more
-- players are in the same state in 2027, predating any of this.
--
-- identity_id stays the career key; it is simply an opaque resolved id rather
-- than a guaranteed player_identities reference.

ALTER TABLE basketball_test.basket_player_season
  DROP CONSTRAINT IF EXISTS basket_player_season_identity_id_fkey;
