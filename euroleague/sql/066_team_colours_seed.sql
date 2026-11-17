-- 066_team_colours_seed.sql
--
-- EuroLeague shadow schema. Club colours approved by the user on 2026-11-17 ("use basket.co.il colors").
-- Clubs in basket.co.il's own Gameflow team table (livestats game.js, TEAMS)
-- take those colours; the rest take the draft read from their crest, shown on
-- the approval page. Each line names its source. Applied by
-- scripts/apply_team_colours_seed.R; re-runnable (plain UPDATEs).

-- 56 clubs.

UPDATE euroleague.teams SET primary_color = '#00A651', secondary_color = '#FFFFFF' WHERE competition = 'E' AND provider_team_code = 'ASV'; -- LDLC ASVEL Villeurbanne: crest draft (check: crest gave nothing)
UPDATE euroleague.teams SET primary_color = '#A50044', secondary_color = '#004D98' WHERE competition = 'E' AND provider_team_code = 'BAR'; -- FC Barcelona: crest draft (crest read yellow)
UPDATE euroleague.teams SET primary_color = '#891631', secondary_color = '#09213F' WHERE competition = 'E' AND provider_team_code = 'BAS'; -- Kosner Baskonia Vitoria-Gasteiz: crest draft
UPDATE euroleague.teams SET primary_color = '#1A1A1A', secondary_color = '#FFFFFF' WHERE competition = 'E' AND provider_team_code = 'BES'; -- Besiktas Istanbul: crest draft (crest read the flag)
UPDATE euroleague.teams SET primary_color = '#745335', secondary_color = '#182029' WHERE competition = 'E' AND provider_team_code = 'DUB'; -- Dubai Basketball: crest draft
UPDATE euroleague.teams SET primary_color = '#E8001C', secondary_color = '#FFFFFF' WHERE competition = 'E' AND provider_team_code = 'HTA'; -- Hapoel IBI Tel Aviv: basket.co.il Gameflow
UPDATE euroleague.teams SET primary_color = '#082B73', secondary_color = '#DA211F' WHERE competition = 'E' AND provider_team_code = 'IST'; -- Anadolu Efes Istanbul: crest draft
UPDATE euroleague.teams SET primary_color = '#FEBE10', secondary_color = '#00529F' WHERE competition = 'E' AND provider_team_code = 'MAD'; -- Real Madrid: crest draft (check: crest gold, kit white)
UPDATE euroleague.teams SET primary_color = '#C32123', secondary_color = '#A99A67' WHERE competition = 'E' AND provider_team_code = 'MCO'; -- AS Monaco: crest draft
UPDATE euroleague.teams SET primary_color = '#C8102E', secondary_color = '#FFFFFF' WHERE competition = 'E' AND provider_team_code = 'MIL'; -- Armani Olimpia Milan: crest draft
UPDATE euroleague.teams SET primary_color = '#DC052D', secondary_color = '#0066B2' WHERE competition = 'E' AND provider_team_code = 'MUN'; -- FC Bayern Munich: crest draft
UPDATE euroleague.teams SET primary_color = '#E30613', secondary_color = '#FFFFFF' WHERE competition = 'E' AND provider_team_code = 'OLY'; -- Olympiacos Piraeus: crest draft (crest read pink)
UPDATE euroleague.teams SET primary_color = '#EF6C23', secondary_color = '#1A1A1A' WHERE competition = 'E' AND provider_team_code = 'PAM'; -- Valencia Basket: crest draft
UPDATE euroleague.teams SET primary_color = '#00843D', secondary_color = '#FFFFFF' WHERE competition = 'E' AND provider_team_code = 'PAN'; -- Panathinaikos AKTOR Athens: crest draft (crest read the sponsor)
UPDATE euroleague.teams SET primary_color = '#1A1A1A', secondary_color = '#FFFFFF' WHERE competition = 'E' AND provider_team_code = 'PAR'; -- Partizan Mozzart Bet Belgrade: crest draft (crest read the sponsor)
UPDATE euroleague.teams SET primary_color = '#FC0205', secondary_color = '#094EFB' WHERE competition = 'E' AND provider_team_code = 'PRS'; -- Paris Basketball: crest draft
UPDATE euroleague.teams SET primary_color = '#E00614', secondary_color = '#FFFFFF' WHERE competition = 'E' AND provider_team_code = 'RED'; -- Crvena Zvezda Meridianbet Belgrade: crest draft
UPDATE euroleague.teams SET primary_color = '#FFD700', secondary_color = '#003087' WHERE competition = 'E' AND provider_team_code = 'TEL'; -- Maccabi Rapyd Tel Aviv: basket.co.il Gameflow
UPDATE euroleague.teams SET primary_color = '#FFED00', secondary_color = '#002D72' WHERE competition = 'E' AND provider_team_code = 'ULK'; -- Fenerbahce Tarfin Istanbul: crest draft (crest read gold)
UPDATE euroleague.teams SET primary_color = '#1A1A1A', secondary_color = '#FFFFFF' WHERE competition = 'E' AND provider_team_code = 'VIR'; -- Virtus Bologna: crest draft (crest gave nothing)
UPDATE euroleague.teams SET primary_color = '#006633', secondary_color = '#FFFFFF' WHERE competition = 'E' AND provider_team_code = 'ZAL'; -- Zalgiris Kaunas: crest draft
UPDATE euroleague.teams SET primary_color = '#F7CD18', secondary_color = '#FFFFFF' WHERE competition = 'U' AND provider_team_code = 'ARI'; -- Aris Thessaloniki: crest draft
UPDATE euroleague.teams SET primary_color = '#292567', secondary_color = '#ED232A' WHERE competition = 'U' AND provider_team_code = 'BAH'; -- Bahcesehir College Istanbul: crest draft
UPDATE euroleague.teams SET primary_color = '#EEB21F', secondary_color = '#B72227' WHERE competition = 'U' AND provider_team_code = 'BCR'; -- Roma Basketball: crest draft
UPDATE euroleague.teams SET primary_color = '#1A1A1A', secondary_color = '#FFFFFF' WHERE competition = 'U' AND provider_team_code = 'BES'; -- Besiktas GAIN Istanbul: crest draft (crest read the flag)
UPDATE euroleague.teams SET primary_color = '#F27918', secondary_color = '#1B4A9B' WHERE competition = 'U' AND provider_team_code = 'BGS'; -- Recoletas Salud San Pablo Burgos: crest draft
UPDATE euroleague.teams SET primary_color = '#008B3B', secondary_color = '#FFFFFF' WHERE competition = 'U' AND provider_team_code = 'BLK'; -- Balkan Botevgrad: crest draft
UPDATE euroleague.teams SET primary_color = '#D0AB66', secondary_color = '#6A0311' WHERE competition = 'U' AND provider_team_code = 'BOS'; -- BOSNA BH Telecom Sarajevo: crest draft
UPDATE euroleague.teams SET primary_color = '#D60919', secondary_color = '#FFFFFF' WHERE competition = 'U' AND provider_team_code = 'BOU'; -- Cosea JL Bourg-en-Bresse: crest draft
UPDATE euroleague.teams SET primary_color = '#EA540C', secondary_color = '#154A72' WHERE competition = 'U' AND provider_team_code = 'BUD'; -- Buducnost VOLI Podgorica: crest draft
UPDATE euroleague.teams SET primary_color = '#17136E', secondary_color = '#5DB564' WHERE competition = 'U' AND provider_team_code = 'BUR'; -- Tofas Bursa: crest draft
UPDATE euroleague.teams SET primary_color = '#1A1A1A', secondary_color = '#FFFFFF' WHERE competition = 'U' AND provider_team_code = 'CLU'; -- U-BT Cluj-Napoca: crest draft (check: crest gave nothing)
UPDATE euroleague.teams SET primary_color = '#004F97', secondary_color = '#EC9F16' WHERE competition = 'U' AND provider_team_code = 'FRA'; -- Skyliners Frankfurt: crest draft
UPDATE euroleague.teams SET primary_color = '#1A1A1A', secondary_color = '#E30613' WHERE competition = 'U' AND provider_team_code = 'HAM'; -- Veolia Towers Hamburg: crest draft (check: crest gave nothing)
UPDATE euroleague.teams SET primary_color = '#E8001C', secondary_color = '#FFFFFF' WHERE competition = 'U' AND provider_team_code = 'JER'; -- Hapoel Midtown Jerusalem: basket.co.il Gameflow
UPDATE euroleague.teams SET primary_color = '#1E3E93', secondary_color = '#F9942A' WHERE competition = 'U' AND provider_team_code = 'KLA'; -- Neptunas Klaipeda: crest draft
UPDATE euroleague.teams SET primary_color = '#FE341C', secondary_color = '#FFFFFF' WHERE competition = 'U' AND provider_team_code = 'LEM'; -- Le Mans Sarthe Basket: crest draft
UPDATE euroleague.teams SET primary_color = '#00632C', secondary_color = '#F58131' WHERE competition = 'U' AND provider_team_code = 'LJU'; -- Cedevita Olimpija Ljubljana: crest draft
UPDATE euroleague.teams SET primary_color = '#612832', secondary_color = '#D6C998' WHERE competition = 'U' AND provider_team_code = 'LKB'; -- Lietkabelis Panevezys: crest draft
UPDATE euroleague.teams SET primary_color = '#004C97', secondary_color = '#FFFFFF' WHERE competition = 'U' AND provider_team_code = 'LLI'; -- London Lions: crest draft (check: crest gave nothing)
UPDATE euroleague.teams SET primary_color = '#1C2440', secondary_color = '#E49122' WHERE competition = 'U' AND provider_team_code = 'MAN'; -- Kids&Us Manresa: crest draft
UPDATE euroleague.teams SET primary_color = '#EAAE2F', secondary_color = '#FFFFFF' WHERE competition = 'U' AND provider_team_code = 'MRO'; -- Maxima Roma: crest draft
UPDATE euroleague.teams SET primary_color = '#1A233E', secondary_color = '#FFFFFF' WHERE competition = 'U' AND provider_team_code = 'NAP'; -- Napoli Basketball: crest draft
UPDATE euroleague.teams SET primary_color = '#EC7404', secondary_color = '#9E162F' WHERE competition = 'U' AND provider_team_code = 'NIN'; -- NINERS Chemnitz: crest draft
UPDATE euroleague.teams SET primary_color = '#D4222A', secondary_color = '#314C6C' WHERE competition = 'U' AND provider_team_code = 'NIO'; -- Panionios Cosmorama Travel Athens: crest draft
UPDATE euroleague.teams SET primary_color = '#1A1A1A', secondary_color = '#FFFFFF' WHERE competition = 'U' AND provider_team_code = 'PAO'; -- PAOK Thessaloniki: crest draft (crest gave nothing)
UPDATE euroleague.teams SET primary_color = '#FD1FFD', secondary_color = '#010065' WHERE competition = 'U' AND provider_team_code = 'RIG'; -- Riga Zelli: crest draft
UPDATE euroleague.teams SET primary_color = '#182E64', secondary_color = '#F5A608' WHERE competition = 'U' AND provider_team_code = 'RTK'; -- Rostock Seawolves: crest draft
UPDATE euroleague.teams SET primary_color = '#F0A439', secondary_color = '#FFFFFF' WHERE competition = 'U' AND provider_team_code = 'SIA'; -- Siauliai Basketball: crest draft
UPDATE euroleague.teams SET primary_color = '#E6B903', secondary_color = '#2B91C8' WHERE competition = 'U' AND provider_team_code = 'TNF'; -- La Laguna Tenerife: crest draft
UPDATE euroleague.teams SET primary_color = '#532586', secondary_color = '#FFFFFF' WHERE competition = 'U' AND provider_team_code = 'TRN'; -- Dolomiti Energia Trento: crest draft
UPDATE euroleague.teams SET primary_color = '#2C344D', secondary_color = '#E7413F' WHERE competition = 'U' AND provider_team_code = 'TRT'; -- Baglietto Derthona Tortona: crest draft
UPDATE euroleague.teams SET primary_color = '#004788', secondary_color = '#FFFFFF' WHERE competition = 'U' AND provider_team_code = 'TTK'; -- Turk Telekom Ankara: crest draft
UPDATE euroleague.teams SET primary_color = '#EF7A12', secondary_color = '#FFFFFF' WHERE competition = 'U' AND provider_team_code = 'ULM'; -- ratiopharm ulm: crest draft
UPDATE euroleague.teams SET primary_color = '#711230', secondary_color = '#B58C61' WHERE competition = 'U' AND provider_team_code = 'VNC'; -- Umana Reyer Venice: crest draft
UPDATE euroleague.teams SET primary_color = '#D7A947', secondary_color = '#4AA15B' WHERE competition = 'U' AND provider_team_code = 'WRO'; -- Slask Wroclaw: crest draft
