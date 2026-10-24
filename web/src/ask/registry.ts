import "server-only";
// The ask tools, one per /api/v1/[league]/* route, calling that route's answer
// function in-process (spec 2026-10-24-home-ask-box-design.md, "API mapping").
// Only this file may import answer functions; registry.test.ts enforces it.
import type { League } from "@/rules/gameLogs";
import { compareAnswer } from "@/server/compareRepo";
import { gameLogsAnswer } from "@/server/gameLogsApi";
import { gameflowAnswer } from "@/server/gameflowRepo";
import { homeAnswer } from "@/server/homeRepo";
import { lineupGames } from "@/server/lineupGamesRepo";
import { lineupRosterAnswer, lineupsAnswer } from "@/server/lineupsApi";
import { onoffAnswer } from "@/server/onoffApi";
import { playerStatsAnswer } from "@/server/playerStatsRepo";
import { teamRatingsAnswer } from "@/server/teamRatingsRepo";
import { compareTable } from "./compareTool";

export type ToolName =
  | "onoff"
  | "lineups"
  | "lineup_games"
  | "lineup_roster"
  | "team_ratings"
  | "game_logs"
  | "player_stats"
  | "compare"
  | "team_overview"
  | "gameflow"
  | "list_teams";

export type ToolEntry = {
  name: ToolName;
  /** Directory under app/api/v1/[league]/; null for list_teams. */
  route: string | null;
  /** The identifier the route imports. */
  answerName: string | null;
  answer: ((league: League, s: URLSearchParams) => Promise<unknown>) | null;
  /** Path segment of the page the link opens. */
  page: string | null;
  leagues: League[];
  /** Feature inventory IDs (2026-10-19-api-mcp-feature-inventory.md). */
  inventory: string[];
  /** false: link-only, no numbers in the answer text (gameflow in v1). */
  numbers: boolean;
};

const ALL: League[] = ["il", "euroleague", "eurocup"];
const ids = (prefix: string, from: number, to: number) =>
  Array.from(
    { length: to - from + 1 },
    (_, i) => `${prefix}-${String(from + i).padStart(2, "0")}`,
  );
const tool = (
  name: ToolName,
  route: string | null,
  answerName: string | null,
  answer: ToolEntry["answer"],
  page: string | null,
  inventory: string[],
  extra: Partial<ToolEntry> = {},
): ToolEntry => ({
  name,
  route,
  answerName,
  answer,
  page,
  leagues: ALL,
  inventory,
  numbers: true,
  ...extra,
});

export const TOOLS: Record<ToolName, ToolEntry> = {
  onoff: tool("onoff", "onoff", "onoffAnswer", onoffAnswer, "onoff", [
    ...ids("ON", 1, 14),
    "X-05",
    "X-11",
  ]),
  lineups: tool("lineups", "lineups", "lineupsAnswer", lineupsAnswer, "lineups", [
    ...ids("LD", 1, 11),
    "LD-13",
    "LD-14",
  ]),
  lineup_games: tool(
    "lineup_games",
    "lineup-games",
    "lineupGames",
    async (l, s) => lineupGames(l, s),
    "lineups",
    ["LD-12"],
  ),
  lineup_roster: tool(
    "lineup_roster",
    "lineup-roster",
    "lineupRosterAnswer",
    async (l, s) => lineupRosterAnswer(l, s),
    "lineups",
    ["LD-03"],
  ),
  team_ratings: tool(
    "team_ratings",
    "team-ratings",
    "teamRatingsAnswer",
    teamRatingsAnswer,
    "team-ratings",
    ids("TR", 1, 10),
  ),
  game_logs: tool(
    "game_logs",
    "game-logs",
    "gameLogsAnswer",
    gameLogsAnswer,
    "game-logs",
    ids("GL", 1, 7),
  ),
  player_stats: tool(
    "player_stats",
    "player-stats",
    "playerStatsAnswer",
    playerStatsAnswer,
    "player-stats",
    ids("PS", 1, 9),
  ),
  compare: tool(
    "compare",
    "compare",
    "compareAnswer",
    // The route answers one side; the tool is the page's paired table.
    async (_l, s) => compareTable((side) => compareAnswer(side), s),
    "compare",
    ids("CMP", 1, 17),
    { leagues: ["il"] },
  ),
  team_overview: tool(
    "team_overview",
    "home",
    "homeAnswer",
    async (_l, s) => homeAnswer(s),
    "home",
    ids("HOME", 1, 7),
    { leagues: ["il"] },
  ),
  gameflow: tool(
    "gameflow",
    "gameflow",
    "gameflowAnswer",
    async (l, s) => gameflowAnswer(l, s),
    "gameflow",
    ids("GF", 1, 6),
    { numbers: false },
  ),
  list_teams: tool("list_teams", null, null, null, null, ["X-04"]),
};

/** Routes that are not tools, with the reason (spec: API mapping). */
export const NOT_TOOLS: Record<string, string> = {
  "player-stat-filters":
    "Internal helper the On/Off page uses for its Player Stats chips; the onoff tool reaches it through onoffAnswer's parameters.",
};
