import "server-only";
import type { League } from "../rules/gameLogs";
import { lineupPage, type LineupView } from "../rules/lineups";
import { lineupPageOptions } from "../lib/lineupPageState";
import { ParamError } from "./api";
import { parseLineups } from "./lineupParams";
import { getLineupRoster, getLineups } from "./lineupsRepo";
/** The ranked population for one context (the Ask tool filters it itself). */
export function lineupsAnswer(league: League, search: URLSearchParams) {
  return getLineups(parseLineups(search, league));
}
/** The Lineups page's table for its full state (lib/tabQuery lineupsPagePath):
 * lineupsAnswer's population, ranked once per context, stays on the server and
 * only the rows the page shows travel. The Ask tool reads the same population
 * and applies the same lineupPage (ask/pageRows.ts). */
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
    lineupPageOptions(search, league, p.view as LineupView),
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
export function lineupRosterAnswer(league: League, search: URLSearchParams) {
  return getLineupRoster(parseLineups(search, league));
}
