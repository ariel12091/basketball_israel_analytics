-- basket.co.il team rosters (team.asp?TeamId=..., Hebrew and English): one row
-- per player per season with his listed position, height and date of birth.
--
-- Why a second table: the season-stat pages (stats-accumulate.asp) behind
-- basket_player_season list only some registered players -- 2026-11-12, 54 of
-- 2026-27's 234 play-by-play players were missing, mostly mid-season arrivals.
-- The team pages list every registered player. Kept apart from
-- basket_player_season so that table keeps meaning "players with season
-- totals"; player_display_names_v uses a roster height only where the
-- season-stat table has none.
--
-- Written by scripts/basket_rosters.R (upsert, never deletes; current season
-- only, since the site menu links the current season's team pages); linked to
-- players by etl/basket_identity_match.R --table=roster, with the same link
-- columns and rules as basket_player_season.

CREATE TABLE IF NOT EXISTS basketball_test.basket_player_roster (
  game_year integer NOT NULL CHECK (game_year BETWEEN 1990 AND 2100),
  basket_player_id integer NOT NULL CHECK (basket_player_id > 0),
  basket_team_id integer NOT NULL CHECK (basket_team_id > 0),
  -- The season-stat table's spelling of the team, from the players both list;
  -- the identity matcher maps it through schedule_team_dict.
  basket_team_name text CHECK (basket_team_name IS NULL OR btrim(basket_team_name) <> ''),
  jersey text,
  player_name text NOT NULL CHECK (btrim(player_name) <> ''),
  player_name_en text CHECK (player_name_en IS NULL OR btrim(player_name_en) <> ''),
  position_name text,
  position_en text,
  height_m numeric CHECK (height_m IS NULL OR height_m BETWEEN 1.5 AND 2.4),
  date_of_birth date CHECK (date_of_birth IS NULL OR date_of_birth BETWEEN DATE '1940-01-01' AND CURRENT_DATE),
  source_url text NOT NULL,
  source_content_md5 text NOT NULL CHECK (source_content_md5 ~ '^[0-9a-f]{32}$'),
  fetched_at timestamptz NOT NULL,
  identity_id bigint,
  canonical_player_id integer,
  identity_match_status text CHECK (identity_match_status IS NULL OR identity_match_status IN ('verified', 'proposal', 'ambiguous', 'unmatched')),
  identity_matched_on text CHECK (identity_matched_on IS NULL OR identity_matched_on IN ('name_en', 'name_he', 'name_both', 'surname_team', 'manual')),
  identity_matched_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (game_year, basket_player_id),
  CONSTRAINT basket_player_roster_identity_consistent CHECK (
    (identity_match_status IN ('verified', 'proposal') AND identity_id IS NOT NULL
       AND canonical_player_id IS NOT NULL AND identity_matched_on IS NOT NULL)
    OR (identity_match_status IN ('ambiguous', 'unmatched') AND identity_id IS NULL AND canonical_player_id IS NULL)
    OR (identity_match_status IS NULL AND identity_id IS NULL AND canonical_player_id IS NULL)
  )
);

CREATE INDEX IF NOT EXISTS basket_player_roster_identity_idx
  ON basketball_test.basket_player_roster (game_year, identity_id)
  WHERE identity_match_status = 'verified';

COMMENT ON TABLE basketball_test.basket_player_roster IS
  'basket.co.il team-page rosters: position, height, date of birth per player-season; fills heights the season-stat pages lack (2026-11-12).';

REVOKE ALL ON TABLE basketball_test.basket_player_roster FROM PUBLIC;
