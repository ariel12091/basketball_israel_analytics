-- Canonical player_id aliases for upstream roster identity splits.
-- Default target schema is basketball_test; copy/adjust for basketball before prod use.

CREATE TABLE IF NOT EXISTS basketball_test.player_id_aliases (
  game_year int NOT NULL,
  team_id int NOT NULL,
  alias_player_id int NOT NULL,
  canonical_player_id int NOT NULL,
  player_name text,
  reason text,
  active boolean NOT NULL DEFAULT true,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (game_year, team_id, alias_player_id),
  CHECK (alias_player_id <> canonical_player_id)
);

CREATE INDEX IF NOT EXISTS player_id_aliases_canonical_idx
  ON basketball_test.player_id_aliases (game_year, team_id, canonical_player_id)
  WHERE active;

CREATE TABLE IF NOT EXISTS basketball_test.player_id_game_overrides (
  game_id int NOT NULL,
  game_year int NOT NULL,
  team_id int NOT NULL,
  alias_player_id int NOT NULL,
  canonical_player_id int NOT NULL,
  player_name text,
  reason text,
  active boolean NOT NULL DEFAULT true,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (game_id, team_id, alias_player_id),
  CHECK (alias_player_id <> canonical_player_id)
);

CREATE INDEX IF NOT EXISTS player_id_game_overrides_lookup_idx
  ON basketball_test.player_id_game_overrides (game_year, team_id, alias_player_id, game_id)
  WHERE active;

INSERT INTO basketball_test.player_id_aliases
  (game_year, team_id, alias_player_id, canonical_player_id, player_name, reason)
VALUES
  (2026, 15, 2136, 1251, 'SAGIV DVIR', 'same team/season duplicate display name with split lineup identity'),
  (2026, 7, 2143, 1262, 'AMIR DANON', 'same team/season duplicate display name with split lineup identity'),
  (2025, 13, 27817, 3206, 'ALON DANIELI', 'same team/season duplicate display name with split lineup identity')
ON CONFLICT (game_year, team_id, alias_player_id) DO UPDATE
SET canonical_player_id = EXCLUDED.canonical_player_id,
    player_name = EXCLUDED.player_name,
    reason = EXCLUDED.reason,
    active = true,
    updated_at = now();

UPDATE basketball_test.player_id_aliases
   SET active = false,
       reason = 'retired: Holon 2060 is a reused provider id, not a season-wide duplicate of 2152',
       updated_at = now()
 WHERE game_year = 2026
   AND team_id = 5
   AND alias_player_id = 2152
   AND canonical_player_id = 2060;

UPDATE basketball_test.player_id_aliases
   SET active = false,
       reason = 'retired: Kiryat Ata 1183 is a reused provider id, not a season-wide duplicate of 1277',
       updated_at = now()
 WHERE game_year = 2026
   AND team_id = 12
   AND alias_player_id = 1277
   AND canonical_player_id = 1183;

INSERT INTO basketball_test.player_id_game_overrides
  (game_id, game_year, team_id, alias_player_id, canonical_player_id, player_name, reason)
VALUES
  (165, 2026, 5, 2060, 2152, 'J''VON MCCORMICK', 'provider reused Josh Hagins player_id for J''Von McCormick in this game'),
  (199, 2026, 5, 2060, 2152, 'J''VON MCCORMICK', 'provider reused Josh Hagins player_id for J''Von McCormick in this game'),
  (356, 2026, 5, 2060, 2152, 'J''VON MCCORMICK', 'provider reused Josh Hagins player_id for J''Von McCormick in this game'),
  (361, 2026, 5, 2060, 2152, 'J''VON MCCORMICK', 'provider reused Josh Hagins player_id for J''Von McCormick in this game'),
  (383, 2026, 5, 2060, 2152, 'J''VON MCCORMICK', 'provider reused Josh Hagins player_id for J''Von McCormick in this game'),
  (384, 2026, 5, 2060, 2152, 'J''VON MCCORMICK', 'provider reused Josh Hagins player_id for J''Von McCormick in this game'),
  (385, 2026, 5, 2060, 2152, 'J''VON MCCORMICK', 'provider reused Josh Hagins player_id for J''Von McCormick in this game'),
  (51, 2026, 12, 1183, 1277, 'ITAY ZLOTOLOV', 'provider reused DeAndre Williams player_id for Itay Zlotolov in this game'),
  (57, 2026, 12, 1183, 1277, 'ITAY ZLOTOLOV', 'provider reused DeAndre Williams player_id for Itay Zlotolov in this game'),
  (62, 2026, 12, 1183, 1277, 'ITAY ZLOTOLOV', 'provider reused DeAndre Williams player_id for Itay Zlotolov in this game'),
  (72, 2026, 12, 1183, 1277, 'ITAY ZLOTOLOV', 'provider reused DeAndre Williams player_id for Itay Zlotolov in this game'),
  (78, 2026, 12, 1183, 1277, 'ITAY ZLOTOLOV', 'provider reused DeAndre Williams player_id for Itay Zlotolov in this game'),
  (84, 2026, 12, 1183, 1277, 'ITAY ZLOTOLOV', 'provider reused DeAndre Williams player_id for Itay Zlotolov in this game'),
  (90, 2026, 12, 1183, 1277, 'ITAY ZLOTOLOV', 'provider reused DeAndre Williams player_id for Itay Zlotolov in this game'),
  (97, 2026, 12, 1183, 1277, 'ITAY ZLOTOLOV', 'provider reused DeAndre Williams player_id for Itay Zlotolov in this game'),
  (102, 2026, 12, 1183, 1277, 'ITAY ZLOTOLOV', 'provider reused DeAndre Williams player_id for Itay Zlotolov in this game'),
  (114, 2026, 12, 1183, 1277, 'ITAY ZLOTOLOV', 'provider reused DeAndre Williams player_id for Itay Zlotolov in this game'),
  (119, 2026, 12, 1183, 1277, 'ITAY ZLOTOLOV', 'provider reused DeAndre Williams player_id for Itay Zlotolov in this game'),
  (126, 2026, 12, 1183, 1277, 'ITAY ZLOTOLOV', 'provider reused DeAndre Williams player_id for Itay Zlotolov in this game'),
  (130, 2026, 12, 1183, 1277, 'ITAY ZLOTOLOV', 'provider reused DeAndre Williams player_id for Itay Zlotolov in this game'),
  (142, 2026, 12, 1183, 1277, 'ITAY ZLOTOLOV', 'provider reused DeAndre Williams player_id for Itay Zlotolov in this game'),
  (148, 2026, 12, 1183, 1277, 'ITAY ZLOTOLOV', 'provider reused DeAndre Williams player_id for Itay Zlotolov in this game'),
  (155, 2026, 12, 1183, 1277, 'ITAY ZLOTOLOV', 'provider reused DeAndre Williams player_id for Itay Zlotolov in this game'),
  (160, 2026, 12, 1183, 1277, 'ITAY ZLOTOLOV', 'provider reused DeAndre Williams player_id for Itay Zlotolov in this game'),
  (167, 2026, 12, 1183, 1277, 'ITAY ZLOTOLOV', 'provider reused DeAndre Williams player_id for Itay Zlotolov in this game'),
  (208, 2026, 12, 1183, 1277, 'ITAY ZLOTOLOV', 'provider reused DeAndre Williams player_id for Itay Zlotolov in this game'),
  (226, 2026, 12, 1183, 1277, 'ITAY ZLOTOLOV', 'provider reused DeAndre Williams player_id for Itay Zlotolov in this game'),
  (293, 2026, 12, 1183, 1277, 'ITAY ZLOTOLOV', 'provider reused DeAndre Williams player_id for Itay Zlotolov in this game')
ON CONFLICT (game_id, team_id, alias_player_id) DO UPDATE
SET game_year = EXCLUDED.game_year,
    canonical_player_id = EXCLUDED.canonical_player_id,
    player_name = EXCLUDED.player_name,
    reason = EXCLUDED.reason,
    active = true,
    updated_at = now();

-- Not seeded automatically:
--   2025 HAPOEL JERUSALEM "NEW NEW" ids 33406/33407/33408
-- These look like placeholder identities in the same game, not a verified
-- single real player, so they need manual review before canonicalization.
