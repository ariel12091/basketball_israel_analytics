import "server-only";
import type { League } from "../rules/gameLogs";
import type { View } from "../rules/onoff";
import { ParamError, pageQuery } from "./api";
import { parseContext } from "./contextParams";
import { playerStatFrame } from "./playerStatFilterRepo";
import { onoffPath } from "../lib/tabQuery";
import {
  onoffPageOptions,
  onoffPsPath,
  onoffQueryDefaults,
  onoffTable,
} from "../lib/pageState/onoff";
import { getOnOff } from "./onoffRepo";

/** The On/Off answer for one request: the route's body and the page's first
 * render. */
export async function onoffAnswer(league: League, search: URLSearchParams) {
  const view = (search.get("view") ?? "summary") as View;
  if (
    !["summary", "four_factors", "shot_profile"].includes(view) ||
    (view === "shot_profile" && league !== "il")
  )
    throw new ParamError("view", "Invalid view");
  const c = parseContext(search, league);
  const a = await getOnOff(c, view);
  return {
    rows: a.rows,
    league3p: a.league3p,
    meta: {
      league,
      season: c.season,
      view,
      filtered: a.filtered,
      version: a.version,
      teams: a.teams,
      rounds: a.rounds,
      phases: a.phases,
      ...(a.seasons ? { seasons: a.seasons } : {}),
    },
  };
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
