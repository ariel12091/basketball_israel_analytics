import { readdirSync, readFileSync } from "node:fs";
import { ribbonModel, type RawRibbon } from "../src/rules/gameflow";
import type { League } from "../src/rules/gameLogs";
export const gameflowFiles = readdirSync("test/goldens/gameflow").filter((f) =>
  f.endsWith(".json"),
);
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
      meta: {
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
