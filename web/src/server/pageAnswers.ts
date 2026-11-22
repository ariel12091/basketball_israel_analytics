import "server-only";
// The tables' finished answers (shape=table): what a freshly opened page link
// shows. Each requests the page's population from its reader exactly as the
// page does, then runs the page's own table rules (lib/pageState/,
// lineupPage). The routes, the server-rendered Lineups page and the Ask chat
// call these; the browser pages run the same rules on the population.
import type { League, Row } from "../rules/gameLogs";
import { needsOtherView, withOtherView } from "../rules/statFilters";
import ui from "../data/shinyUi.json";
import { lineupPage, type LineupView } from "../rules/lineups";
import type { View } from "../rules/onoff";
import { lineupPageOptions } from "../lib/lineupPageState";
import {
  onoffPath,
  PLAYER_QUERY_DEFAULTS,
  playerStatsPath,
  TEAM_QUERY_DEFAULTS,
  teamRatingsPath,
} from "../lib/tabQuery";
import { playerStatsTable } from "../lib/pageState/playerStats";
import {
  teamOtherViews,
  teamRatingsTable,
  teamViewQuery,
} from "../lib/pageState/teamRatings";
import {
  onoffOtherViews,
  onoffPageOptions,
  onoffPsPath,
  onoffViewQuery,
  onoffQueryDefaults,
  onoffTable,
} from "../lib/pageState/onoff";
import { pageQuery, ParamError } from "./api";
import { parseContext } from "./contextParams";
import { parseLineups } from "./lineupParams";
import { lineupsAnswer } from "./lineupsApi";
import { onoffAnswer } from "./onoffApi";
import { playerStatFrame } from "./playerStatFilterRepo";
import { playerStatsAnswer } from "./playerStatsRepo";
import { teamRatingsAnswer } from "./teamRatingsRepo";

/** shape=table: what a freshly opened Player Stats link shows. The population
 * is requested exactly as PlayerStatsClient requests it (playerStatsPath),
 * then the page's own table rules run on the full page query. */
export async function playerStatsPageAnswer(league: League, s: URLSearchParams) {
  const query = pageQuery(league, s, PLAYER_QUERY_DEFAULTS);
  const api = new URL(playerStatsPath(league, query), "http://page.local").searchParams;
  return playerStatsTable(await playerStatsAnswer(league, api), query);
}

/** shape=table: what a freshly opened Team Ratings link shows. The population
 * is requested exactly as TeamRatingsClient requests it (teamRatingsPath),
 * then the page's own table rules run on the full page query. */
export async function teamRatingsPageAnswer(league: League, s: URLSearchParams) {
  const query = pageQuery(league, s, TEAM_QUERY_DEFAULTS);
  const api = (q: typeof query) =>
    new URL(teamRatingsPath(league, q), "http://page.local").searchParams;
  // Filters on other views' columns read those views too, one at a time.
  const others = [];
  for (const v of teamOtherViews(query))
    others.push((await teamRatingsAnswer(league, api(teamViewQuery(query, v)))).rows);
  return teamRatingsTable(await teamRatingsAnswer(league, api(query)), query, others);
}
/** shape=table: what a freshly opened On/Off link shows. The population is
 * requested exactly as OnOffClient requests it (onoffPath); auto sets the
 * bars unless the link says auto=false; ps_* chips read the Player Stats
 * frame for the same game context (onoffPsPath), as the page does. */
export async function onoffPageAnswer(league: League, search: URLSearchParams) {
  const query = pageQuery(league, search, onoffQueryDefaults(league));
  const api = (path: string) => new URL(path, "http://page.local").searchParams;
  const answer = await onoffAnswer(league, api(onoffPath(league, query)));
  const ps = onoffPageOptions(query, answer, true).psFilters.length
    ? await playerStatFrame(parseContext(api(onoffPsPath(league, query)), league))
    : [];
  // Filters on other views' columns read those views too, one at a time.
  const view = (answer.meta.view ?? query.view ?? "summary") as View;
  const others = [];
  for (const v of onoffOtherViews(league, query, view))
    others.push(await onoffAnswer(league, api(onoffPath(league, onoffViewQuery(query, v)))));
  return onoffTable(answer, query, ps, true, others);
}

/** The Lineups page's table for its full state (lib/tabQuery lineupsPagePath):
 * lineupsAnswer's population, ranked once per context, stays on the server and
 * only the rows the page shows travel. The Ask tool reads this answer. */
export async function lineupsPageAnswer(
  league: League,
  search: URLSearchParams,
) {
  const p = parseLineups(search, league);
  if (!/^\d*$/.test(search.get("team") ?? ""))
    throw new ParamError("team", "Invalid team");
  for (const k of ["players_on", "players_any", "players_off"])
    if ((search.get(k) ?? "").split(",").length > 15)
      throw new ParamError(k, "At most 15 players");
  const min = search.get("min_poss");
  if (
    search.get("auto") !== "true" &&
    min != null &&
    (!/^\d+$/.test(min) || Number(min) > 100000)
  )
    throw new ParamError("min_poss", "Expected 0-100000");
  const answer = await lineupsAnswer(league, search);
  const options = lineupPageOptions(search, p.view as LineupView);
  // A filter on the other view's column (3P% on Four Factors, say) reads that
  // view's rows for the same context, joined by lineup.
  const otherView = p.view === "summary" ? "four_factors" : "summary";
  const cols = (v: LineupView) => ui.lineupStatFilterCols[v].map((c) => c.col);
  let rows = answer.rows;
  if (
    needsOtherView(options.stats, cols(p.view as LineupView), cols(otherView))
  ) {
    const s = new URLSearchParams(search);
    s.set("view", otherView);
    const key = (r: Row) => `${r.team_id}|${r.sub_lineup_hash}`;
    rows = withOtherView(rows, (await lineupsAnswer(league, s)).rows, key);
  }
  const page = lineupPage(rows, options);
  return {
    ...answer,
    rows: page.rows,
    meta: {
      ...answer.meta,
      autoMin: page.autoMin,
      minPoss: page.min,
      shown: page.shown,
      maxPoss: page.maxPoss,
    },
  };
}
