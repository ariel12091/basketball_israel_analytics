-- Correct, in the schema itself, a claim made twice in earlier migrations
-- today: that one of these columns identifies a person across seasons.
-- Neither does.
--
--   canonical_player_id  is RECYCLED between seasons. 18 of 712 canonical ids
--                        in resolved_player_identity_v belong to two different
--                        people: 1119 is AMIT GERSHON in 2025 and MICHAEL
--                        FOSTER JR. in 2026.
--
--   identity_id          is SPLIT by the provider re-minting ids. identity_key
--                        embeds the source id, so 'segev:1025:TAMIR BLATT' and
--                        'segev:1091:TAMIR BLATT' are two records for one man.
--                        Only 109 of 732 identities span more than one season.
--
-- Both are therefore season-scoped. There is no cross-season person key in
-- this schema today; date_of_birth on this table is the strongest candidate
-- for building one, since the play-by-play side carries no birth date at all.

COMMENT ON COLUMN basketball_test.basket_player_season.canonical_player_id IS
  'Segev player id for joining play-by-play facts WITHIN a season. Recycled '
  'between seasons -- always pair it with game_year, never use it alone.';

COMMENT ON COLUMN basketball_test.basket_player_season.identity_id IS
  'Resolved identity for this registration, mirroring '
  'resolved_player_identity_v.identity_id. Season-scoped: the provider '
  're-mints player ids, which splits one person across several identity '
  'records. Falls back to the raw source id when a player has no curated '
  'identity, so it is not a player_identities foreign key.';

COMMENT ON COLUMN basketball_test.basket_player_season.identity_match_status IS
  'verified | proposal | ambiguous | unmatched, or NULL when no match was '
  'attempted (2022-2024 have no play-by-play counterpart). Consumers should '
  'filter on verified.';

COMMENT ON COLUMN basketball_test.basket_player_season.date_of_birth IS
  'From the profile page, read day-first (dd/mm/yyyy). Present on every row '
  'and absent from every play-by-play relation, which makes it the only '
  'cross-source discriminator available for identity work.';

COMMENT ON COLUMN basketball_test.basket_player_season.basket_team_name IS
  'basket.co.il''s own team string, sponsor included. Resolve it to a team_id '
  'through schedule_team_dict.team_name_basket, per season -- sponsors change '
  'yearly and the abbreviations do not match the roster names.';

COMMENT ON TABLE basketball_test.basket_player_season IS
  'One row per player-TEAM-season from basket.co.il, not per player-season: a '
  'mid-season transfer holds two registrations with per-stint totals. Lookup '
  'layer for questions about play-by-play players (height, position, '
  'nationality, birth date, prior shooting), not a source of players.';
