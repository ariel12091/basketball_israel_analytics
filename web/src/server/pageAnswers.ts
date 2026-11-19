import "server-only";
// The tables' finished answers (shape=table): what a freshly opened page link
// shows. Each requests the page's population from its reader exactly as the
// page does, then runs the page's own table rules (lib/pageState/,
// lineupPage). The routes, the server-rendered Lineups page and the Ask chat
// call these; the browser pages run the same rules on the population.
import type { League } from "../rules/gameLogs";
import { lineupPage, type LineupView } from "../rules/lineups";
import { lineupPageOptions } from "../lib/lineupPageState";
import {
  onoffPath,
  PLAYER_QUERY_DEFAULTS,
  playerStatsPath,
  TEAM_QUERY_DEFAULTS,
  teamRatingsPath,
} from "../lib/tabQuery";
import { playerStatsTable } from "../lib/pageState/playerStats";
import { teamRatingsTable } from "../lib/pageState/teamRatings";
import {
  onoffPageOptions,
  onoffPsPath,
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
  const api = new URL(teamRatingsPath(league, query), "http://page.local").searchParams;
  return teamRatingsTable(await teamRatingsAnswer(league, api), query);
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
  return onoffTable(answer, query, ps);
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
  const page = lineupPage(
    answer.rows,
    lineupPageOptions(search, p.view as LineupView),
  );
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
