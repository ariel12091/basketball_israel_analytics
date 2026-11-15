import "server-only";
// The OpenAPI 3.1 description of /api/v1, generated from the code that serves
// it: the routes from the Ask registry, each route's parameters from the
// query its page reads (tabQuery defaults) or the registry's key lists, the
// allowed values from the catalogues and the rules' own lists, the operation
// texts from the chat's tool descriptions. openapi.test.ts holds it to the
// routes. Served at /api/v1/openapi.json.
import catalogues from "../data/catalogues.json";
import { DESCRIPTIONS } from "../ask/prompt";
import { NOT_TOOLS, TOOLS, type ToolName } from "../ask/registry";
import { compareDefaults, presetChoices, SIDE_FIELDS } from "../lib/compareQuery";
import { onoffQueryDefaults } from "../lib/pageState/onoff";
import { TEAM_VIEWS } from "../lib/pageState/teamRatings";
import {
  LINEUP_QUERY_DEFAULTS,
  PLAYER_QUERY_DEFAULTS,
  TEAM_QUERY_DEFAULTS,
  queryFromSearch,
} from "../lib/tabQuery";
import { PLAYER_MODES } from "../rules/playerStatsTable";
import type { League } from "../rules/gameLogs";
import { KINDS } from "./compareRepo";

type Schema = { type: "string" | "integer" | "boolean"; enum?: string[]; format?: string; default?: string };
type Param = { name: string; in: "query" | "path"; required?: boolean; description: string; schema: Schema };

const str = (description: string, extra: Partial<Schema> = {}) => ({ description, schema: { type: "string", ...extra } as Schema });
const int = (description: string) => ({ description, schema: { type: "integer" } as Schema });
const bool = (description: string) => ({ description, schema: { type: "string", enum: ["true", "false"] } as Schema });
const date = (description: string) => str(description, { format: "date" });

const gameTypes = catalogues.gameTypeChoices.filter((c) => c.value !== "");
const STAT_FILTER =
  'Stat filters, a JSON array of {"col": <column>, "op": "ge"|"le", "value": <number>}, e.g. [{"col":"net_rtg","op":"ge","value":0}]. Applied to the finished table (shape=table / format=csv).';

/** Every parameter a route takes, described once. */
const KEYS: Record<string, { description: string; schema: Schema }> = {
  season: int("Season. Israeli League: the year the season ends (2025-26 is 2026). EuroLeague and EuroCup: the year it starts (2025-26 is 2025). Default: the current season."),
  view: str("The page's view (allowed values per route)."),
  team: str("One team id (the team's rows; on Game Logs the team whose games are listed)."),
  teams: str("Team ids, comma separated."),
  start: date("First game date (YYYY-MM-DD); default the season's start."),
  end: date("Last game date (YYYY-MM-DD); default the season's end."),
  own_starters: str("The team's own starters on the floor: gte:N or lte:N (N 0-5)."),
  opp_starters: str("The opponents' starters on the floor: gte:N or lte:N (N 0-5)."),
  game_type: str(`Israeli game type code(s), comma separated: ${gameTypes.map((c) => `${c.value} ${c.label}`).join("; ")}.`, {
    enum: ["", ...gameTypes.map((c) => c.value)],
  }),
  phase: str("EuroLeague / EuroCup phase(s), comma separated, as the provider names them."),
  opponents: str("Opponent team ids, comma separated."),
  home_away: str("Home or away games only.", { enum: ["", "home", "away"] }),
  outcome: str("Wins or losses only.", { enum: ["", "win", "loss"] }),
  opp_rank_side: str("Only games against the top or bottom of the league (with opp_rank_n).", { enum: ["", "top", "bottom"] }),
  opp_rank_n: int("How many teams count as the top or bottom (with opp_rank_side)."),
  opp_rank_metric: str("The rating the opponents are ranked by (default net).", { enum: ["", "off", "def", "net"] }),
  min_gn: int("First Israeli game number (round); not with last_n."),
  max_gn: int("Last Israeli game number (round); not with last_n."),
  last_n: int("Each team's last N games; not with a game-number range."),
  stats: str(STAT_FILTER),
  clutch: bool("Clutch possessions only (with clutch_margin, clutch_status, clutch_minutes, ot_margin)."),
  clutch_margin: int("Clutch: the largest score margin, in points, before the shot."),
  clutch_status: str("Clutch: only while leading, trailing or tied.", { enum: ["all", "leading", "trailing", "tied"] }),
  clutch_minutes: int("Clutch: minutes remaining in the fourth quarter at most."),
  ot_margin: bool("Clutch: apply the margin and status in overtime too (overtime is otherwise all clutch)."),
  side: str("Team Ratings Traditional: the team's own box score or its opponents'.", { enum: ["offense", "defense"] }),
  display: str("Team Ratings Traditional: totals or per game.", { enum: ["per_game", "totals"] }),
  players: str("Player Stats: players to show, as team_id:player_id pairs, comma separated."),
  min_gp: int("Player Stats: the fewest games played; capped at the most anyone played."),
  show_ineligible: bool("Player Stats: show players under the games minimum in gray instead of hiding them."),
  min_all: int("On/Off: the fewest possessions on and off the floor (with auto=false; otherwise auto sets it)."),
  min_on: int("On/Off: the fewest possessions on the floor (with auto=false; otherwise auto sets it)."),
  auto: bool("Automatic possession minimum, as a freshly opened page link applies it. On/Off: auto=false keeps min_on / min_all. Lineups: auto=true replaces min_poss."),
  size: str("Lineups: players per group.", { enum: ["2", "3", "4", "5"] }),
  min_poss: int("The fewest possessions a row needs."),
  players_on: str("Lineups: player ids who must all be on the floor (with team)."),
  players_any: str("Lineups: player ids of whom at least any_count are on the floor (with team)."),
  players_off: str("Lineups: player ids who must be off the floor (with team)."),
  any_count: int("Lineups: how many of players_any must be on the floor."),
  any_exact: bool("Lineups: exactly any_count of players_any, not at least."),
  // Compare
  preset: str("Compare: a ready-made comparison of side A against side B.", {
    enum: [...new Set(["", ...presetChoices.map((c) => c.value)])],
  }),
  team_id: str("A team id. Compare (with a preset): pins that team on both sides. Home hub and Gameflow: the team."),
  open_detail: bool("Compare (with a preset and team_id): open that team's Detail grid."),
  auto_min: bool("Compare: the page's automatic possession minimum (the API applies min_poss as given)."),
  split_date: date("Compare date_split preset: side A before this date, side B from it."),
  split_gn: int("Compare gn_split preset: side A before this game number, side B from it."),
  cmp_view: str("Compare: the league table, one entity's Detail grid, or one team's players.", { enum: ["league", "detail", "players"] }),
  entity: str("Compare Detail / players: the team id (or a lineup's sub_lineup_hash)."),
  lu_team: str("Compare Lineups: the team whose lineups are compared."),
  lu_on: str("Compare Lineups: player ids who must be on the floor."),
  lu_any: str("Compare Lineups: player ids of whom at least one is on the floor."),
  lu_off: str("Compare Lineups: player ids who must be off the floor."),
  player_compare: str("Compare Players mode: one player against himself or another.", { enum: ["other", "self"] }),
  player_a: str("Compare Players mode: side A's player id."),
  player_b: str("Compare Players mode: side B's player id."),
  player_team_a: str("Compare Players mode: side A's team id."),
  player_team_b: str("Compare Players mode: side B's team id."),
  list_a: str("Compare Players mode: side A's player list."),
  list_b: str("Compare Players mode: side B's player list."),
  player_view: str("Compare Players mode: the metric group shown."),
  rate: str("Compare Players mode: the rate basis.", { enum: ["Per Game", "Totals", "Per 30 Minutes", "Per 60 Possessions"] }),
  team_rate: str("Compare players view: the rate basis.", { enum: ["Per Game", "Totals", "Per 30 Minutes", "Per 60 Possessions"] }),
  team_metric: str("Compare players view: the stat compared (pts, reb, ast, stl, blk, tov, fg_pct, tp_pct, ft_pct, efg, ts, usg_pct)."),
  auto_players: bool("Compare Players mode: pick default players automatically."),
  metric: str("Compare: the metric compared.", { enum: ["net_rtg", "off_ppp", "def_ppp", "off_efg", "off_tov", "off_oreb", "off_ftr"] }),
  drill_player: str("Compare: a player's drill-down (page only; no table yet)."),
  preset_applied: bool("Compare: true when the preset's side filters are already in the query (they are not applied again)."),
  kind: str("Compare without shape: which side reader to return.", { enum: [...KINDS] }),
  // The smaller routes
  hash: str("Lineup game log: the lineup's sub_lineup_hash."),
  game_id: int("Gameflow: the game id."),
  topic: str("Guide: the topic (glossary, filters, or a tab)."),
  // Response shaping
  shape: str("table: the finished table a freshly opened page link shows (filters, automatic minimum, rate view applied). Absent: the page's population.", { enum: ["table"] }),
  format: str("csv: the finished table as a CSV download (implies shape=table).", { enum: ["json", "csv"] }),
  v: str("The data version (from /api/v1/version). A request naming the current version can be cached by the CDN; it never changes the answer."),
};
for (const side of ["a", "b"] as const)
  for (const k of [...SIDE_FIELDS, "start", "end", "min_gn", "max_gn"])
    KEYS[`${side}_${k}`] = { ...KEYS[k], description: `Compare side ${side.toUpperCase()} only: ${KEYS[k].description}` };

const VIEWS: Partial<Record<string, string[]>> = {
  "team-ratings": TEAM_VIEWS,
  "player-stats": [...PLAYER_MODES],
  onoff: ["summary", "four_factors", "shot_profile"],
  lineups: ["summary", "four_factors"],
  "game-logs": ["summary", "four_factors"],
  "lineup-games": ["summary", "four_factors"],
  compare: ["Teams", "Lineups", "Players"],
};

const keysOf = (defaults: Record<string, string>) => Object.keys(queryFromSearch("il", {}, defaults));
const CONTEXT = keysOf({});
/** The query keys each route takes (before shape / format / v). */
const ROUTE_KEYS: Record<string, string[]> = {
  "team-ratings": keysOf(TEAM_QUERY_DEFAULTS),
  "player-stats": keysOf(PLAYER_QUERY_DEFAULTS),
  onoff: keysOf(onoffQueryDefaults("il")),
  lineups: [...new Set([...keysOf(LINEUP_QUERY_DEFAULTS), "auto"])],
  "game-logs": CONTEXT,
  compare: [...keysOf(compareDefaults), "kind"],
  "player-stat-filters": CONTEXT,
};
/** Routes that serve the finished table on request (shape=table, format=csv). */
const TABLES = new Set(["team-ratings", "player-stats", "onoff", "compare"]);
const CSV_ONLY = new Set(["lineups", "game-logs"]);

const ERROR = {
  description: "A parameter the route refuses: { error, param }.",
  content: {
    "application/json": {
      schema: { type: "object", properties: { error: { type: "string" }, param: { type: ["string", "null"] } } },
    },
  },
};
const answer = (csv: boolean) => ({
  description: "The page's answer: rows (one object per table row, columns as on the page) and meta (version, view, the season's teams and filters).",
  content: {
    "application/json": {
      schema: { type: "object", properties: { rows: { type: "array", items: { type: "object" } }, meta: { type: "object" } } },
    },
    ...(csv ? { "text/csv": { schema: { type: "string" }, description: "With format=csv." } } : {}),
  },
});

function param(route: string, name: string): Param {
  const k = KEYS[name];
  if (!k) throw new Error(`openapi: no description for ${route} parameter ${name}`);
  const views = name === "view" ? VIEWS[route] : undefined;
  return { name, in: "query", ...k, ...(views ? { schema: { ...k.schema, enum: views } } : {}) };
}

function operation(route: string) {
  const tool = Object.values(TOOLS).find((t) => t.route === route);
  const keys = ROUTE_KEYS[route] ?? tool?.keys ?? [];
  const shaped = TABLES.has(route) ? ["shape", "format"] : CSV_ONLY.has(route) ? ["shape", "format"] : [];
  const leagues: League[] = tool?.leagues ?? ["il", "euroleague", "eurocup"];
  const csv = shaped.length > 0;
  return {
    get: {
      operationId: route.replace(/-([a-z])/g, (_, c: string) => c.toUpperCase()),
      summary: tool ? tool.name.replace(/_/g, " ") : route,
      description: tool ? DESCRIPTIONS[tool.name as ToolName] : NOT_TOOLS[route],
      parameters: [
        { name: "league", in: "path", required: true, description: "il (Israeli League), euroleague or eurocup.", schema: { type: "string", enum: leagues } },
        ...[...new Set([...keys, ...shaped, "v"])].map((k) => param(route, k)),
      ],
      responses: { 200: answer(csv), 400: ERROR },
    },
  };
}

let cached: object | undefined;
export function openApiSpec() {
  if (cached) return cached;
  const routes = [...Object.values(TOOLS).flatMap((t) => (t.route ? [t.route] : [])), ...Object.keys(NOT_TOOLS)];
  cached = {
    openapi: "3.1.0",
    info: {
      title: "IBPL Analytics API",
      version: "1",
      description:
        "Israeli League, EuroLeague and EuroCup statistics, the same numbers the site's pages show. Every route is GET and read-only. Table routes take the page's own query and, with shape=table, return the finished table a page link shows; format=csv returns it as a CSV download.",
    },
    servers: [{ url: "https://ibpl-game-logs.vercel.app" }],
    paths: {
      ...Object.fromEntries(routes.sort().map((r) => [`/api/v1/{league}/${r}`, operation(r)])),
      "/api/v1/version": {
        get: {
          operationId: "version",
          summary: "data version",
          description: "The data version per league; pass it as v to make answers cacheable.",
          parameters: [],
          responses: { 200: { description: "{ leagues: [{ league, version }] }" } },
        },
      },
      "/api/v1/openapi.json": {
        get: {
          operationId: "openapi",
          summary: "this description",
          description: "This OpenAPI document.",
          parameters: [],
          responses: { 200: { description: "OpenAPI 3.1 JSON." } },
        },
      },
    },
  };
  return cached;
}
