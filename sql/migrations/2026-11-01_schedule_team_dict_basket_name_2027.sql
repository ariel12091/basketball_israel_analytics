-- Record basket.co.il's 2027 team strings in the per-season team dictionary,
-- as 2026-09-23_schedule_team_dict_basket_name.sql did for 2025-2026.
--
-- The pairings were inferred the same way: from player overlap, i.e. which app
-- team each Basket team's name-matched players (Hebrew or English, normalised by
-- normalize_person_name()) appear for in the 2027 rosters. Read off the cached
-- 2026-11-01 scrape (166 registrations): a clean bijection, 14 Basket teams to
-- 14 distinct team_ids, with the supporting count on each row. HaEmek's one
-- shortfall is Ran Solomon (Basket 26089), not yet on an app roster; its one
-- extra overlap is a Bnei Herzliya roster name.

UPDATE basketball_test.schedule_team_dict d
SET team_name_basket = v.team_name_basket
FROM (VALUES
  (2027, 2111, 'מכבי Rapyd תל אביב'),  -- MACCABI TEL AVIV, 14/14 players
  (2027, 2112, 'הפועל IBI תל אביב'),  -- HAPOEL TEL AVIV, 14/14 players
  (2027, 2113, 'מכבי קבוצת כנען רמת גן'),  -- MACCABI RAMAT GAN, 10/10 players
  (2027, 2114, 'הפועל מידטאון י-ם'),  -- HAPOEL JERUSALEM, 14/14 players
  (2027, 2115, 'הפועל מעוז דניאל חולון'),  -- HAPOEL HOLON, 12/12 players
  (2027, 2116, 'עירוני לאטי קריית אתא'),  -- IRONI KIRYAT ATA, 11/11 players
  -- Basket writes a non-breaking space (U+00A0) after the first word here.
  (2027, 2117, 'הפועל' || chr(160) || 'גילת טלקום העמק'),  -- HAPOEL HAEMEK, 12/13 players
  (2027, 2118, 'עירוני חי מוטורס נס ציונה'),  -- NESS ZIONA, 11/11 players
  (2027, 2119, 'הפועל אלטשולר שחם ב"ש/דימונה'),  -- BEER SHEVA, 13/13 players
  (2027, 2120, 'בני Penlink הרצליה'),  -- BNEI HERZLIYA, 8/8 players
  (2027, 2121, 'הפועל אלפי בנדק גליל עליון'),  -- GALIL ELION, 12/12 players
  (2027, 2122, 'מכבי תפוזינה ראשון לציון'),  -- RISHON LEZION, 11/11 players
  (2027, 2123, 'מכבי רוטשטיין אשדוד'),  -- MACCABI ASHDOD, 12/12 players
  (2027, 2124, 'הפועל עירוני אילת')  -- IRONI EILAT, 11/11 players
) AS v(game_year, team_id_schedule, team_name_basket)
WHERE d.game_year = v.game_year
  AND d.team_id_schedule = v.team_id_schedule;

-- Every Basket team name loaded for 2027 must now resolve, or the matcher
-- would silently drop a whole squad.
DO $$
DECLARE unmapped bigint;
BEGIN
  SELECT count(*) INTO unmapped
  FROM (SELECT DISTINCT game_year, basket_team_name
          FROM basketball_test.basket_player_season
         WHERE game_year = 2027) b
  WHERE NOT EXISTS (
    SELECT 1 FROM basketball_test.schedule_team_dict d
     WHERE d.game_year = b.game_year
       AND d.team_name_basket = b.basket_team_name);
  IF unmapped > 0 THEN
    RAISE EXCEPTION '% Basket team name(s) do not resolve to a team_id', unmapped;
  END IF;
  RAISE NOTICE 'all Basket team names for 2027 resolve';
END
$$;
