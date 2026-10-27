import "server-only";
import type { League } from "../rules/gameLogs";
import type { View } from "../rules/onoff";
import { ParamError } from "./api";
import { parseContext } from "./contextParams";
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
