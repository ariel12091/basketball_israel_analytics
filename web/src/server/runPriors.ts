import "server-only";
import type { Query } from "./db";
import type { RunPriors, PlayerPrior } from "../rules/gameflowRunCause";

/**
 * "Gameflow run player priors" (docs/shared_definitions.md): for every player
 * of one Israeli game, the counts the run explanation judges a run against.
 *   - this season: `player_traditional_by_game` over the season's OTHER games;
 *   - past seasons' shooting: basket.co.il season totals (`basket_player_season`)
 *     for every earlier season, linked through the cross-season person key
 *     (`basket_player_registration.basket_person_id`);
 *   - past seasons' turnovers, rebounds and floor possessions: earlier seasons of
 *     `player_traditional_by_game`, linked the same way.
 * Players without a person key (most new imports) get this season only.
 * Counts only; the rule (rules/gameflowRunCause.ts) shrinks and compares.
 * Plus the season's league rates, from the same table.
 */
const SQL = `
WITH me AS (
  SELECT DISTINCT team_id, player_id
  FROM basketball_test.player_traditional_by_game
  WHERE game_id = $1::int4 AND game_year = $2::int4
),
cur AS (
  SELECT g.team_id, g.player_id,
         SUM(g.fgm - g."3pm")::int AS m2, SUM(g.fga - g."3pa")::int AS a2,
         SUM(g."3pm")::int AS m3, SUM(g."3pa")::int AS a3,
         SUM(g.ftm)::int AS mf, SUM(g.fta)::int AS af,
         SUM(g.tov)::int AS tov, SUM(g.oreb)::int AS oreb, SUM(g.dreb)::int AS dreb,
         SUM(g.poss_on_floor)::float AS poss
  FROM basketball_test.player_traditional_by_game g
  JOIN me USING (team_id, player_id)
  WHERE g.game_year = $2::int4 AND g.game_id <> $1::int4
  GROUP BY 1, 2
),
-- The display-name view, read once for this and every earlier season (it
-- costs ~0.1 s per season; joined per row it ran once per player).
names AS MATERIALIZED (
  SELECT game_year, team_id, player_id, identity_id
  FROM basketball_test.player_display_names_v WHERE game_year <= $2::int4
),
person AS (
  SELECT me.team_id, me.player_id, r.basket_person_id
  FROM me
  JOIN names v
    ON v.game_year = $2::int4 AND v.team_id = me.team_id AND v.player_id = me.player_id
  JOIN LATERAL (
    SELECT r.basket_person_id FROM basketball_test.basket_player_registration r
    WHERE r.game_year = $2::int4 AND r.identity_id = v.identity_id AND r.basket_person_id IS NOT NULL
    ORDER BY (r.source = 'stats') DESC LIMIT 1
  ) r ON TRUE
),
-- One row per (person, earlier season, basket player id): registration can list
-- a season twice (two sources), which must not double a season's totals.
past_ids AS (
  SELECT DISTINCT p.team_id, p.player_id, r.game_year, r.basket_player_id, r.identity_id
  FROM person p
  JOIN basketball_test.basket_player_registration r
    ON r.basket_person_id = p.basket_person_id AND r.game_year < $2::int4
),
past_shoot AS (
  SELECT x.team_id, x.player_id,
         SUM(s.fg2_made)::int AS m2, SUM(s.fg2_attempted)::int AS a2,
         SUM(s.fg3_made)::int AS m3, SUM(s.fg3_attempted)::int AS a3,
         SUM(s.ft_made)::int AS mf, SUM(s.ft_attempted)::int AS af
  FROM (SELECT DISTINCT team_id, player_id, game_year, basket_player_id FROM past_ids) x
  JOIN basketball_test.basket_player_season s
    ON s.game_year = x.game_year AND s.basket_player_id = x.basket_player_id
  GROUP BY 1, 2
),
past_rows AS (
  SELECT DISTINCT x.team_id, x.player_id, v.game_year, v.team_id AS past_team, v.player_id AS past_player
  FROM past_ids x
  JOIN names v
    ON v.game_year = x.game_year AND v.identity_id = x.identity_id
),
past_pbp AS (
  SELECT x.team_id, x.player_id,
         SUM(g.tov)::int AS tov, SUM(g.oreb)::int AS oreb, SUM(g.dreb)::int AS dreb,
         SUM(g.poss_on_floor)::float AS poss
  FROM past_rows x
  JOIN basketball_test.player_traditional_by_game g
    ON g.game_year = x.game_year AND g.team_id = x.past_team AND g.player_id = x.past_player
  GROUP BY 1, 2
),
-- Team-level rates (team rebounds and turnovers included) from the per-game
-- team table the Team Ratings readers sum.
teams AS (
  SELECT SUM(off_tov_raw)::float / NULLIF(SUM(off_poss_raw), 0) AS tov_team,
         SUM(off_points_raw)::float / NULLIF(SUM(off_poss_raw), 0) AS ppp,
         SUM(off_oreb_count_raw)::float / NULLIF(SUM(off_fga_raw - off_fgm_raw), 0) AS oreb_miss
  FROM basketball_test.team_metrics_by_game_mv WHERE game_year = $2::int4
),
league AS (
  SELECT SUM(fgm - "3pm")::float / NULLIF(SUM(fga - "3pa"), 0) AS fg2,
         SUM("3pm")::float / NULLIF(SUM("3pa"), 0) AS fg3,
         SUM("3pa")::float / NULLIF(SUM(fga), 0) AS fg3_share,
         SUM(ftm)::float / NULLIF(SUM(fta), 0) AS ft,
         SUM(tov)::float / NULLIF(SUM(poss_on_floor), 0) AS tov_player,
         SUM(oreb)::float / NULLIF(SUM(poss_on_floor), 0) AS oreb_player,
         SUM(dreb)::float / NULLIF(SUM(poss_on_floor), 0) AS dreb_player,
         (SELECT oreb_miss FROM teams) AS oreb_miss,
         (SELECT ppp FROM teams) AS ppp,
         (SELECT tov_team FROM teams) AS tov_team
  FROM basketball_test.player_traditional_by_game WHERE game_year = $2::int4
)
SELECT
  (SELECT to_jsonb(league) FROM league) AS league,
  (SELECT COALESCE(jsonb_agg(jsonb_build_object(
     'team_id', me.team_id, 'player_id', me.player_id,
     'cur', to_jsonb(c) - 'team_id' - 'player_id',
     'past_shoot', to_jsonb(s) - 'team_id' - 'player_id',
     'past_pbp', to_jsonb(p) - 'team_id' - 'player_id')), '[]'::jsonb)
   FROM me
   LEFT JOIN cur c USING (team_id, player_id)
   LEFT JOIN past_shoot s USING (team_id, player_id)
   LEFT JOIN past_pbp p USING (team_id, player_id)) AS players`;

type Counts = Partial<
  Record<
    "m2" | "a2" | "m3" | "a3" | "mf" | "af" | "tov" | "oreb" | "dreb" | "poss",
    number
  >
>;
type Row = {
  league: Record<string, number | null> | null;
  players: {
    team_id: number;
    player_id: number;
    cur: Counts | null;
    past_shoot: Counts | null;
    past_pbp: Counts | null;
  }[];
};

const n = (v: unknown) => (v == null ? 0 : Number(v));

/** Counts for both teams' players in one Israeli game; null outside the
 *  Israeli league (the run explanation then keeps the standout rule, D43). */
export async function readRunPriors(
  q: Query,
  league: string,
  game: number,
  season: number,
  team: number,
): Promise<RunPriors | null> {
  if (league !== "il") return null;
  const row = (await q<Row>(SQL, [game, season]))[0];
  const L = row?.league;
  if (!L || L.fg2 == null || L.ppp == null || L.tov_team == null) return null;
  const players = new Map<string, PlayerPrior>();
  for (const p of row.players) {
    const c = p.cur ?? {},
      s = p.past_shoot ?? {},
      h = p.past_pbp ?? {};
    const side = Number(p.team_id) === team ? "own" : "opp";
    players.set(`${side}:${p.player_id}`, {
      fg2: [n(c.m2) + n(s.m2), n(c.a2) + n(s.a2)],
      fg3: [n(c.m3) + n(s.m3), n(c.a3) + n(s.a3)],
      ft: [n(c.mf) + n(s.mf), n(c.af) + n(s.af)],
      tov: [n(c.tov) + n(h.tov), n(c.poss) + n(h.poss)],
      oreb: [n(c.oreb) + n(h.oreb), n(c.poss) + n(h.poss)],
      dreb: [n(c.dreb) + n(h.dreb), n(c.poss) + n(h.poss)],
    });
  }
  return {
    players,
    league: {
      fg2: n(L.fg2),
      fg3: n(L.fg3),
      fg3Share: n(L.fg3_share),
      ft: n(L.ft),
      tovPlayer: n(L.tov_player),
      tovTeam: n(L.tov_team),
      orebPlayer: n(L.oreb_player),
      drebPlayer: n(L.dreb_player),
      orebMiss: n(L.oreb_miss),
      ppp: n(L.ppp),
    },
  };
}
