-- Record basket.co.il's own team string in the per-season team dictionary.
--
-- schedule_team_dict already maps the schedule and roster sources per season;
-- basket.co.il is a third source for the same teams, so its name belongs here
-- rather than in a parallel dictionary.
--
-- The pairings below were not read off the Hebrew strings, which differ by
-- sponsor and by standard abbreviation ("i-m" vs "Jerusalem"), and in one 2026
-- case only by a Hebrew maqaf where the roster uses an ASCII hyphen. They were
-- inferred from player overlap: for each Basket team-season, which app team did
-- its name-matched players actually appear for. Both seasons came out as clean
-- bijections, 14 Basket teams to 14 distinct team_ids, and the per-row comment
-- records the supporting count. The shortfalls are mid-season transfers, where
-- a player appears on one Basket team but two rosters.
--
-- Sponsors change yearly (Maccabi Tel Aviv is Playtika in 2025, Rapyd in 2026),
-- so this mapping is per season by construction, like the rest of the table.

ALTER TABLE basketball_test.schedule_team_dict
  ADD COLUMN IF NOT EXISTS team_name_basket text
    CHECK (team_name_basket IS NULL OR btrim(team_name_basket) <> '');

-- A Basket team string must not resolve to two teams in one season.
CREATE UNIQUE INDEX IF NOT EXISTS schedule_team_dict_basket_name_idx
  ON basketball_test.schedule_team_dict (game_year, team_name_basket)
  WHERE team_name_basket IS NOT NULL;

UPDATE basketball_test.schedule_team_dict d
SET team_name_basket = v.team_name_basket
FROM (VALUES
  (2025, 1092, 'הפועל קבוצת נתנאל חולון'),  -- HAPOEL HOLON, 16/17 players
  (2025, 1093, 'בני PenLink הרצליה'),  -- BNEI HERZLIYA, 14/14 players
  (2025, 1094, 'עירוני לאטי קריית אתא'),  -- KIRYAT ATA, 17/18 players
  (2025, 1095, 'הפועל בנק יהב י-ם'),  -- HAPOEL JERUSALEM, 18/19 players
  (2025, 1096, 'מכבי Playtika תל אביב'),  -- MACCABI TEL AVIV, 17/18 players
  (2025, 1097, 'הפועל Rivulis גליל עליון'),  -- HAPOEL GALIL ELION, 16/19 players
  (2025, 1098, 'עירוני חי מוטורס נס ציונה'),  -- NESS ZIONA, 16/16 players
  (2025, 1099, 'הפועל "שלמה" תל אביב'),  -- HAPOEL TEL AVIV, 14/14 players
  (2025, 1100, 'הפועל אלטשולר שחם ב"ש/דימונה'),  -- BEER SHEVA, 16/17 players
  (2025, 1102, 'הפועל שובל חיפה'),  -- HAPOEL HAIFA, 20/22 players
  (2025, 1103, 'הפועל עפולה'),  -- HAPOEL AFULA, 17/17 players
  (2025, 1104, 'מכבי קבוצת כנען רמת גן'),  -- MACCABI RAMAT GAN, 16/18 players
  (2025, 1105, 'אליצור BRIGA נתניה'),  -- ELITZUR NETANYA, 17/18 players
  (2025, 1108, 'הפועל גלבוע גליל'),  -- GILBOA/GALIL, 13/13 players
  (2026, 1109, 'מכבי Rapyd תל אביב'),  -- MACCABI TEL AVIV, 17/17 players
  (2026, 1110, 'הפועל IBI תל אביב'),  -- HAPOEL TEL AVIV, 22/22 players
  (2026, 1111, 'מכבי קבוצת כנען רמת גן'),  -- MACCABI RAMAT GAN, 21/23 players
  (2026, 1112, 'הפועל מידטאון י-ם'),  -- HAPOEL JERUSALEM, 16/17 players
  (2026, 1113, 'הפועל נתנאל חולון'),  -- HAPOEL HOLON, 24/24 players
  (2026, 1114, 'עירוני לאטי קריית אתא'),  -- KIRYAT ATA, 22/22 players
  (2026, 1116, 'עירוני חי מוטורס נס ציונה'),  -- NESS ZIONA, 21/22 players
  (2026, 1118, 'בני Penlink הרצליה'),  -- BNEI HERZLIYA, 20/20 players
  (2026, 1119, 'הפועל Rivulis גליל עליון'),  -- GALIL ELION, 23/27 players
  (2026, 1120, 'הפועל אלטשולר שחם ב"ש/דימונה'),  -- BEER SHEVA, 19/19 players
  (2026, 1122, 'הפועל גילת טלקום העמק'),  -- HAPOEL HAEMEK, 18/18 players
  (2026, 1123, 'מכבי תפוזינה ראשון לציון'),  -- RISHON LEZION, 17/17 players
  (2026, 1124, 'מכבי אב־גד רעננה'),  -- MACCABI RAANANA, 25/28 players
  (2026, 2109, 'אליצור נתניה')  -- ELIZUR NETANYA, 27/29 players
) AS v(game_year, team_id_schedule, team_name_basket)
WHERE d.game_year = v.game_year
  AND d.team_id_schedule = v.team_id_schedule;

-- Every Basket team string loaded for 2025-2026 must now resolve, or the
-- matcher would silently drop a whole squad.
DO $$
DECLARE unmapped bigint;
BEGIN
  SELECT count(*) INTO unmapped
  FROM (SELECT DISTINCT game_year, basket_team_name
          FROM basketball_test.basket_player_season
         WHERE game_year IN (2025, 2026)) b
  WHERE NOT EXISTS (
    SELECT 1 FROM basketball_test.schedule_team_dict d
     WHERE d.game_year = b.game_year
       AND d.team_name_basket = b.basket_team_name);
  IF unmapped > 0 THEN
    RAISE EXCEPTION '% Basket team name(s) do not resolve to a team_id', unmapped;
  END IF;
  RAISE NOTICE 'all Basket team names for 2025-2026 resolve';
END
$$;
