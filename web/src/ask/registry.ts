import "server-only";
// The ask tools, one per /api/v1/[league]/* route, calling that route's answer
// function in-process (spec 2026-10-24-home-ask-box-design.md, "API mapping").
// Only this file may import answer functions; registry.test.ts enforces it.
import ui from "@/data/shinyUi.json";
import {
  LINEUP_QUERY_DEFAULTS,
  PLAYER_QUERY_DEFAULTS,
  TEAM_QUERY_DEFAULTS,
  gameLogsPath,
  lineupsPath,
  onoffPath,
  playerStatsPath,
  teamRatingsPath,
  type Query,
} from "@/lib/tabQuery";
import type { League } from "@/rules/gameLogs";
import { compareAnswer } from "@/server/compareRepo";
import { gameLogsAnswer } from "@/server/gameLogsApi";
import { gameflowAnswer } from "@/server/gameflowRepo";
import { GUIDE_TOPICS, guideAnswer } from "@/server/guideApi";
import { homeAnswer } from "@/server/homeRepo";
import { lineupGames } from "@/server/lineupGamesRepo";
import { lineupRosterAnswer, lineupsAnswer } from "@/server/lineupsApi";
import { onoffAnswer } from "@/server/onoffApi";
import { parseContext } from "@/server/contextParams";
import { playerStatFrame } from "@/server/playerStatFilterRepo";
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
  | "guide"
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
  /** false: no markers to quote (gameflow is link-only; guide is text whose
   * own numbers may be repeated, see markers.ts). */
  numbers: boolean;
  /** The page link when it depends on the query (guide: the topic's tab). */
  link?: (league: League, query: Record<string, string>) => string | null;
  /** Table pages: how page.tsx turns its URL into the API request
   * (queryFromSearch with these defaults, then this builder). */
  request?: {
    defaults: (league: League) => Record<string, string>;
    path: (league: League, q: Query) => string;
    pathName: string;
  };
  /** Other tools: the only query keys they take. */
  keys?: string[];
  /** On/Off's Player Stats chips (ps_*): the page's second read, the
   * per-game Player Stats of every team-player for the same game context
   * (OnOffClient psUrl -> /player-stat-filters). */
  playerStats?: (league: League, api: URLSearchParams) => Promise<unknown[]>;
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
  onoff: tool(
    "onoff",
    "onoff",
    "onoffAnswer",
    onoffAnswer,
    "onoff",
    [...ids("ON", 1, 14), "X-05", "X-11"],
    {
      request: {
        // app/[league]/onoff/page.tsx: the league's possession bars.
        defaults: (l) => {
          const d = l === "il" ? ui.onoffDefaults.israel : ui.onoffDefaults.euroleague;
          return { min_all: String(d.minAll), min_on: String(d.minOn) };
        },
        path: onoffPath,
        pathName: "onoffPath",
      },
      playerStats: (l, api) => {
        const q = new URLSearchParams(api);
        for (const k of ["view", "teams", "team", "stats", "own_starters", "opp_starters"])
          q.delete(k);
        return playerStatFrame(parseContext(q, l));
      },
    },
  ),
  lineups: tool(
    "lineups",
    "lineups",
    "lineupsAnswer",
    lineupsAnswer,
    "lineups",
    [...ids("LD", 1, 11), "LD-13", "LD-14"],
    {
      request: {
        defaults: () => LINEUP_QUERY_DEFAULTS,
        path: lineupsPath,
        pathName: "lineupsPath",
      },
    },
  ),
  lineup_games: tool(
    "lineup_games",
    "lineup-games",
    "lineupGames",
    async (l, s) => lineupGames(l, s),
    "lineups",
    ["LD-12"],
    { keys: ["season", "team", "hash", "view"] },
  ),
  lineup_roster: tool(
    "lineup_roster",
    "lineup-roster",
    "lineupRosterAnswer",
    async (l, s) => lineupRosterAnswer(l, s),
    "lineups",
    ["LD-03"],
    { keys: ["season", "team"] },
  ),
  team_ratings: tool(
    "team_ratings",
    "team-ratings",
    "teamRatingsAnswer",
    teamRatingsAnswer,
    "team-ratings",
    ids("TR", 1, 10),
    {
      request: {
        defaults: () => TEAM_QUERY_DEFAULTS,
        path: teamRatingsPath,
        pathName: "teamRatingsPath",
      },
    },
  ),
  game_logs: tool(
    "game_logs",
    "game-logs",
    "gameLogsAnswer",
    gameLogsAnswer,
    "game-logs",
    ids("GL", 1, 7),
    { request: { defaults: () => ({}), path: gameLogsPath, pathName: "gameLogsPath" } },
  ),
  player_stats: tool(
    "player_stats",
    "player-stats",
    "playerStatsAnswer",
    playerStatsAnswer,
    "player-stats",
    ids("PS", 1, 9),
    {
      request: {
        defaults: () => PLAYER_QUERY_DEFAULTS,
        path: playerStatsPath,
        pathName: "playerStatsPath",
      },
    },
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
    { leagues: ["il"], keys: ["season", "team_id"] },
  ),
  gameflow: tool(
    "gameflow",
    "gameflow",
    "gameflowAnswer",
    async (l, s) => gameflowAnswer(l, s),
    "gameflow",
    ids("GF", 1, 6),
    { numbers: false, keys: ["game_id", "team_id"] },
  ),
  guide: tool(
    "guide",
    "guide",
    "guideAnswer",
    async (l, s) => guideAnswer(l, s),
    null,
    ["X-16"],
    {
      numbers: false,
      keys: ["topic", "view"],
      link: (l, q) => {
        const page = GUIDE_PAGES[q.topic];
        return page ? `/${l}/${page}${q.view ? `?view=${encodeURIComponent(q.view)}` : ""}` : null;
      },
    },
  ),
  list_teams: tool("list_teams", null, null, null, null, ["X-04"], {
    keys: ["season"],
  }),
};

export { GUIDE_TOPICS };

/** guide topics that are a tab, and that tab's page. */
const GUIDE_PAGES: Record<string, string> = {
  onoff: "onoff",
  lineups: "lineups",
  team_ratings: "team-ratings",
  game_logs: "game-logs",
  player_stats: "player-stats",
  compare: "compare",
};

/** Routes that are not tools, with the reason (spec: API mapping). */
export const NOT_TOOLS: Record<string, string> = {
  "player-stat-filters":
    "Internal helper the On/Off page uses for its Player Stats chips; the onoff tool reaches it through onoffAnswer's parameters.",
};
