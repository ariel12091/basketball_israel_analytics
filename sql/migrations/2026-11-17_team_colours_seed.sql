-- Club colours approved by the user on 2026-11-17 ("use basket.co.il colors").
-- Clubs in basket.co.il's own Gameflow team table (livestats game.js, TEAMS)
-- take those colours; the rest take the draft read from their crest, shown on
-- the approval page. Each line names its source. Applied by
-- scripts/apply_team_colours_seed.R; re-runnable (plain UPDATEs).

-- 24 Israeli club spellings, every season they appear in.

UPDATE basketball_test.season_teams SET primary_color = '#E8001C', secondary_color = '#FFFFFF' WHERE team_name = 'BEER SHEVA'; -- basket.co.il Gameflow
UPDATE basketball_test.season_teams SET primary_color = '#E8001C', secondary_color = '#FFFFFF' WHERE team_name = 'BEER SHEVA/DIMONA'; -- basket.co.il Gameflow
UPDATE basketball_test.season_teams SET primary_color = '#003087', secondary_color = '#FFFFFF' WHERE team_name = 'BNEI HERZLIYA'; -- basket.co.il Gameflow
UPDATE basketball_test.season_teams SET primary_color = '#EAB308', secondary_color = '#000000' WHERE team_name = 'ELITZUR NETANYA'; -- basket.co.il Gameflow
UPDATE basketball_test.season_teams SET primary_color = '#EAB308', secondary_color = '#000000' WHERE team_name = 'ELIZUR NETANYA'; -- basket.co.il Gameflow
UPDATE basketball_test.season_teams SET primary_color = '#E8001C', secondary_color = '#FFFFFF' WHERE team_name = 'GALIL ELION'; -- basket.co.il Gameflow
UPDATE basketball_test.season_teams SET primary_color = '#ED1D24', secondary_color = '#FFFFFF' WHERE team_name = 'GILBOA/GALIL'; -- crest draft
UPDATE basketball_test.season_teams SET primary_color = '#D71920', secondary_color = '#FFFFFF' WHERE team_name = 'HAPOEL AFULA'; -- crest draft (check: crest read brown)
UPDATE basketball_test.season_teams SET primary_color = '#4FC3F7', secondary_color = '#003087' WHERE team_name = 'HAPOEL EILAT'; -- basket.co.il Gameflow
UPDATE basketball_test.season_teams SET primary_color = '#E8001C', secondary_color = '#FFFFFF' WHERE team_name = 'HAPOEL GALIL ELION'; -- basket.co.il Gameflow
UPDATE basketball_test.season_teams SET primary_color = '#E8001C', secondary_color = '#FFFFFF' WHERE team_name = 'HAPOEL HAEMEK'; -- basket.co.il Gameflow
UPDATE basketball_test.season_teams SET primary_color = '#84191D', secondary_color = '#FFFFFF' WHERE team_name = 'HAPOEL HAIFA'; -- crest draft
UPDATE basketball_test.season_teams SET primary_color = '#6B21A8', secondary_color = '#FACC15' WHERE team_name = 'HAPOEL HOLON'; -- basket.co.il Gameflow
UPDATE basketball_test.season_teams SET primary_color = '#E8001C', secondary_color = '#FFFFFF' WHERE team_name = 'HAPOEL JERUSALEM'; -- basket.co.il Gameflow
UPDATE basketball_test.season_teams SET primary_color = '#E8001C', secondary_color = '#FFFFFF' WHERE team_name = 'HAPOEL TEL AVIV'; -- basket.co.il Gameflow
UPDATE basketball_test.season_teams SET primary_color = '#003087', secondary_color = '#FFFFFF' WHERE team_name = 'IRONI KIRYAT ATA'; -- basket.co.il Gameflow
UPDATE basketball_test.season_teams SET primary_color = '#003087', secondary_color = '#FFFFFF' WHERE team_name = 'KIRYAT ATA'; -- basket.co.il Gameflow
UPDATE basketball_test.season_teams SET primary_color = '#EA580C', secondary_color = '#FFFFFF' WHERE team_name = 'M. RISHON'; -- basket.co.il Gameflow
UPDATE basketball_test.season_teams SET primary_color = '#1D4ED8', secondary_color = '#FFD700' WHERE team_name = 'MACCABI ASHDOD'; -- basket.co.il Gameflow
UPDATE basketball_test.season_teams SET primary_color = '#1D4ED8', secondary_color = '#FFD700' WHERE team_name = 'MACCABI RAANANA'; -- basket.co.il Gameflow
UPDATE basketball_test.season_teams SET primary_color = '#0EA5E9', secondary_color = '#003087' WHERE team_name = 'MACCABI RAMAT GAN'; -- basket.co.il Gameflow
UPDATE basketball_test.season_teams SET primary_color = '#FFD700', secondary_color = '#003087' WHERE team_name = 'MACCABI TEL AVIV'; -- basket.co.il Gameflow
UPDATE basketball_test.season_teams SET primary_color = '#EA580C', secondary_color = '#FFFFFF' WHERE team_name = 'NESS ZIONA'; -- basket.co.il Gameflow
UPDATE basketball_test.season_teams SET primary_color = '#EA580C', secondary_color = '#FFFFFF' WHERE team_name = 'RISHON LEZION'; -- basket.co.il Gameflow
