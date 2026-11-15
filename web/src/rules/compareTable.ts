// Compare's tables from the two sides' rows, one call each for the page
// (CompareClient), the API's shape=table and the chat (server/comparePage.ts):
// the query's fields go to the shared rules in rules/compare.ts.
import type { Row } from "./gameLogs";
import { parseStats, type Query } from "../lib/tabQuery";
import { finalizeCompare, joinCompare, joinTeamPlayers, reverseGap } from "./compare";

/** The possession minimum a Compare table applies (min_poss). */
export const compareMinimum = (q: Query) => Math.max(0, Number(q.min_poss) || 0);

type Teams = { value: string; label: string }[];

/** Each side's team selection, joined and paired by the metric (auto's
 * minimum is measured here, before the minimum applies). */
export function compareJoined(q: Query, a: Row[], b: Row[], teams: Teams) {
  const keep = (rows: Row[], sel: string | undefined) =>
    rows.filter((r) => !sel || sel.split(",").includes(String(r.team_id)));
  return joinCompare(keep(a, q.a_teams), keep(b, q.b_teams), String(q.view), String(q.metric), teams);
}

/** The minimum, the ranks and the stat filter on the joined rows. */
export function compareFinalize(q: Query, joined: ReturnType<typeof compareJoined>) {
  return finalizeCompare(joined, q.view, q.preset, compareMinimum(q), parseStats(q.stats ?? ""));
}

/** The league table. */
export function compareLeagueRows(q: Query, a: Row[], b: Row[], teams: Teams) {
  return compareFinalize(q, compareJoined(q, a, b, teams));
}

/** One team's players under both conditions (cmp_view players). */
export function comparePlayerRows(q: Query, a: Row[], b: Row[]) {
  return joinTeamPlayers(a, b, q.team_metric, q.team_rate, compareMinimum(q), reverseGap(q.preset));
}
