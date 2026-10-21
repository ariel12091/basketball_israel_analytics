// Tab inputs as URL state, shared by the server pages (first render) and the
// client components, so both build the same API request from the same URL.
import type { League } from "@/rules/gameLogs";
import ui from "@/data/shinyUi.json";

export const CONTEXT_KEYS = [
  "view",
  "team",
  "teams",
  "start",
  "end",
  "own_starters",
  "opp_starters",
  "game_type",
  "phase",
  "opponents",
  "home_away",
  "outcome",
  "opp_rank_side",
  "opp_rank_n",
  "opp_rank_metric",
  "min_gn",
  "max_gn",
  "last_n",
  "stats",
] as const;
export type Query = Record<string, string>;
export type Patch = Record<string, string | null>;
export type SearchParams = Record<string, string | string[] | undefined>;

export const defaultSeason = (league: League) =>
  league === "il" ? ui.defaultGameYear : ui.euroDefaultSeason;

/** Every key of the tab with its default: what nuqs reads from the URL. */
export function queryDefaults(
  league: League,
  extra: Record<string, string> = {},
): Query {
  const q: Query = { season: defaultSeason(league) };
  for (const k of CONTEXT_KEYS) q[k] = k === "view" ? "summary" : "";
  return { ...q, ...extra };
}
/** The same query on the server, from the page's searchParams. */
export function queryFromSearch(
  league: League,
  sp: SearchParams,
  extra: Record<string, string> = {},
): Query {
  const q = queryDefaults(league, extra);
  for (const k of Object.keys(q)) {
    const v = sp[k];
    if (typeof v === "string" && v !== "") q[k] = v;
  }
  return q;
}

const BOUNDS = ui.seasonDateBounds as Record<
  string,
  { start: string; end: string }
>;
/** season_date_bounds_for_year() / euro_season_date_bounds(). */
export function seasonBounds(league: League, season: string) {
  const y = Number(season);
  if (league === "il")
    return BOUNDS[season] ?? { start: `${y - 1}-09-01`, end: `${y}-07-01` };
  return { start: `${y}-09-01`, end: `${y + 1}-07-01` };
}

/** The API parameters for the game context: complete starter bounds only,
 * league-specific keys only, plus `keep`. */
export function contextParams(
  query: Query,
  league: League,
  keep: string[] = [],
): URLSearchParams {
  const euro = league !== "il";
  const q = new URLSearchParams();
  for (const [key, value] of Object.entries(query)) {
    if (!value) continue;
    if (
      (key === "own_starters" || key === "opp_starters") &&
      !/^(gte|lte):[0-5]$/.test(value)
    )
      continue;
    if (euro && (key === "team" || key === "game_type")) continue;
    if (!euro && key === "phase") continue;
    if (
      !(CONTEXT_KEYS as readonly string[]).includes(key) &&
      key !== "season" &&
      !keep.includes(key)
    )
      continue;
    q.set(key, value);
  }
  return q;
}

export type StatFilter = { col: string; op: "ge" | "le"; value: number };
export function parseStats(value: string): StatFilter[] {
  try {
    const data = JSON.parse(value || "[]");
    return Array.isArray(data)
      ? data.filter(
          (f) =>
            f &&
            typeof f.col === "string" &&
            ["ge", "le"].includes(f.op) &&
            typeof f.value === "number" &&
            Number.isFinite(f.value),
        )
      : [];
  } catch {
    return [];
  }
}

/** The Game Logs request for a query (no opponent rank; stat filters and the
 * team key by league). */
export function gameLogsPath(league: League, query: Query): string {
  const q = contextParams(query, league);
  for (const k of ["opp_rank_side", "opp_rank_n", "opp_rank_metric"])
    q.delete(k);
  if (league === "il") q.delete("teams");
  else q.delete("stats");
  return `/api/v1/${league}/game-logs?${q}`;
}

/** The On/Off request: the game context and the view (inventory ON-02/ON-03).
 * Teams travel too: the filtered Summary path filters them in SQL. */
export function onoffPath(league: League, query: Query): string {
  const q = contextParams(query, league);
  q.delete("team");
  q.delete("stats");
  if (query.view && query.view !== "summary") q.set("view", query.view);
  else q.delete("view");
  return `/api/v1/${league}/onoff?${q}`;
}

export const LINEUP_QUERY_DEFAULTS = {
  size: "5",
  min_poss: "20",
  players_on: "",
  players_any: "",
  players_off: "",
  any_count: "1",
  any_exact: "false",
  clutch: "false",
  clutch_margin: "5",
  clutch_status: "all",
  clutch_minutes: "5",
  ot_margin: "false",
};
export const TEAM_QUERY_DEFAULTS = {
  clutch: "false",
  clutch_margin: "5",
  clutch_status: "all",
  clutch_minutes: "5",
  ot_margin: "false",
  side: "offense",
  display: "per_game",
};
export function teamRatingsPath(league: League, query: Query): string {
  const q = contextParams(query, league);
  q.delete("stats");
  if (league === "il") {
    q.delete("teams");
    q.delete("team");
  } else if (query.teams) q.set("teams", query.teams);
  for (const k of Object.keys(TEAM_QUERY_DEFAULTS))
    if (query[k]) q.set(k, query[k]);
  return `/api/v1/${league}/team-ratings?${q}`;
}
/** Team/player/possession/stat filters stay local, after league-wide ranks. */
export function lineupsPath(league: League, query: Query): string {
  const q = contextParams(query, league);
  for (const k of ["team", "teams", "stats"]) q.delete(k);
  for (const k of [
    "size",
    "clutch",
    "clutch_margin",
    "clutch_status",
    "clutch_minutes",
    "ot_margin",
  ])
    if (query[k]) q.set(k, query[k]);
  return `/api/v1/${league}/lineups?${q}`;
}
