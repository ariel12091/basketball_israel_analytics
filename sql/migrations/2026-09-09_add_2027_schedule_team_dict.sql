-- Map 2026-27 Israeli schedule IDs to the stable PBP/roster team IDs used by
-- all app-facing facts and dimensions. The mapping was verified from the
-- provider gameInfo payloads for games 393 and 395-406.

INSERT INTO basketball_test.schedule_team_dict
  (game_year, team_id_schedule, team_name_rosters, team_id_rosters, team_name_schedule)
VALUES
  (2027, 2111, 'MACCABI TEL AVIV',    2, 'MACCABI TEL AVIV'),
  (2027, 2112, 'HAPOEL TEL AVIV',     3, 'HAPOEL TEL AVIV'),
  (2027, 2113, 'MACCABI RAMAT GAN',   7, 'MACCABI RAMAT GAN'),
  (2027, 2114, 'HAPOEL JERUSALEM',    4, 'HAPOEL JERUSALEM'),
  (2027, 2115, 'HAPOEL HOLON',        5, 'HAPOEL HOLON'),
  (2027, 2116, 'IRONI KIRYAT ATA',   12, 'IRONI KIRYAT ATA'),
  (2027, 2117, 'HAPOEL HAEMEK',       8, 'HAPOEL HAEMEK'),
  (2027, 2118, 'NESS ZIONA',          9, 'NESS ZIONA'),
  (2027, 2119, 'BEER SHEVA',         11, 'BEER SHEVA/DIMONA'),
  (2027, 2120, 'BNEI HERZLIYA',       6, 'BNEI HERZLIYA'),
  (2027, 2121, 'GALIL ELION',        10, 'HAPOEL GALIL ELION'),
  (2027, 2122, 'RISHON LEZION',      14, 'M. RISHON'),
  (2027, 2123, 'MACCABI ASHDOD',     33, 'MACCABI ASHDOD'),
  (2027, 2124, 'IRONI EILAT',        17, 'HAPOEL EILAT')
ON CONFLICT (game_year, team_id_schedule) DO UPDATE
SET team_name_rosters = EXCLUDED.team_name_rosters,
    team_id_rosters = EXCLUDED.team_id_rosters,
    team_name_schedule = EXCLUDED.team_name_schedule;
