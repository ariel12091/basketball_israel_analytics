import { existsSync, readdirSync, readFileSync } from "node:fs";
import type { League, Row } from "../src/rules/gameLogs";
import type { PlayerStatsPayload } from "../src/server/playerStatsRepo";
import {
  playerDisplay,
  type PlayerMode,
  PLAYER_LABELS,
} from "../src/rules/playerStatsTable";
import { parseStats } from "../src/lib/tabQuery";
export const PLAYER_DIR = "test/goldens/player_stats";
export const playerPresets = existsSync(PLAYER_DIR)
  ? readdirSync(PLAYER_DIR).filter(
      (f) => f.endsWith(".json") && !f.endsWith(".answer.json"),
    )
  : [];
export const PLAYER_ORACLE_MAP: Record<string, string> = {
  Team: "team_name",
  Player: "Player",
  ...Object.fromEntries(Object.entries(PLAYER_LABELS).map(([k, v]) => [v, k])),
  "Poss/G": "base_poss_pg",
  "Min/G": "base_min_pg",
  ".eligible_rate": "rate_eligible",
  ".is_total": "is_multi_team_total",
};
export function loadPlayer(file: string) {
  const g = JSON.parse(readFileSync(`${PLAYER_DIR}/${file}`, "utf8")) as {
    id: string;
    league: League;
    view: PlayerMode;
    query: Record<string, string>;
    expected: { data: Row[]; hidden: string[] };
  };
  const answer = JSON.parse(
    readFileSync(`${PLAYER_DIR}/${g.id}.answer.json`, "utf8"),
  ) as PlayerStatsPayload;
  const result = playerDisplay(answer.rows, answer.identities, {
    mode: g.view,
    minGp: Number(g.query.min_gp ?? 1),
    players: (g.query.players ?? "").split(",").filter(Boolean),
    showIneligible: g.query.show_ineligible === "true",
    filters: parseStats(g.query.stats ?? ""),
  });
  return { g, answer, ...result };
}
