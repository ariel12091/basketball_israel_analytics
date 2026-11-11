import { readdirSync, readFileSync } from "node:fs";
import { ribbonModel, type RawRibbon } from "../src/rules/gameflow";
import { gameflowSummary } from "../src/rules/gameflowSummary";
import type { League } from "../src/rules/gameLogs";
export const gameflowFiles = readdirSync("test/goldens/gameflow").filter((f) =>
  f.endsWith(".json"),
);
/**
 * A Gameflow answer built from a committed reader fixture
 * (src/rules/__fixtures__, written by scripts/save_gameflow_fixture.R), for
 * checks that need the events the stored Shiny goldens predate.
 */
export const GAMEFLOW_FIXTURES = {
  673: { league: "il", team: 2, season: "2027", ownTeam: "Maccabi Tel-Aviv", oppTeam: "Hapoel Tel-Aviv" },
  440: { league: "il", team: 2, season: "2027", ownTeam: "Maccabi Tel-Aviv", oppTeam: "Hapoel Tel-Aviv" },
  1147: { league: "euroleague", team: 9, season: "2026", ownTeam: "Crvena Zvezda Meridianbet Belgrade", oppTeam: "Olympiacos Piraeus" },
} as const;
export function loadFixture(game: keyof typeof GAMEFLOW_FIXTURES) {
  const c = GAMEFLOW_FIXTURES[game],
    raw = JSON.parse(
      readFileSync(`src/rules/__fixtures__/gameflow-${game}.json`, "utf8"),
    ) as RawRibbon;
  const model = ribbonModel(raw, c.team, c.league, game);
  return {
    answer: {
      ...model,
      ...gameflowSummary(model, { own: c.ownTeam, opp: c.oppTeam }),
      meta: { league: c.league as League, game, team: c.team, version: "golden", season: c.season, ownTeam: c.ownTeam, oppTeam: c.oppTeam },
    },
    league: c.league,
    url: `/${c.league}/gameflow?game_id=${game}&team_id=${c.team}`,
  };
}
export function loadGameflow(file: string) {
  const g = JSON.parse(
    readFileSync(`test/goldens/gameflow/${file}`, "utf8"),
  ) as {
    case: {
      league: League;
      game_id: number;
      team_id: number;
      team_name: string;
      opp_team_name: string;
    };
    raw: RawRibbon;
    expected: ReturnType<typeof ribbonModel>;
  };
  const c = g.case;
  return {
    g,
    answer: {
      ...ribbonModel(g.raw, c.team_id, c.league, c.game_id),
      ...gameflowSummary(ribbonModel(g.raw, c.team_id, c.league, c.game_id), {
        own: c.team_name,
        opp: c.opp_team_name,
      }),
      meta: {
        league: c.league,
        game: c.game_id,
        team: c.team_id,
        version: "golden",
        season: c.league === "il" ? "2026" : "2025",
        ownTeam: c.team_name,
        oppTeam: c.opp_team_name,
      },
    },
    url: `/${c.league}/gameflow?game_id=${c.game_id}&team_id=${c.team_id}`,
  };
}
