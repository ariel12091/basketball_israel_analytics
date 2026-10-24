// The Compare page's league table for one page URL: CompareClient resolves the
// query, reads side A and side B from the compare route, then pairs them with
// joinCompare() and finalizeCompare(). Keep this in step with CompareClient.
import ui from "@/data/shinyUi.json";
import {
  compareDefaults,
  compareSideParams,
  resolveCompareQuery,
} from "@/lib/compareQuery";
import { parseStats, type Query } from "@/lib/tabQuery";
import type { Row } from "@/rules/gameLogs";
import { finalizeCompare, joinCompare } from "@/rules/compare";
import { ParamError } from "@/server/api";

type Side = (s: URLSearchParams) => Promise<unknown>;
const SHARED = ["season", "start", "end", "min_gn", "max_gn", "stats"];

export async function compareTable(side: Side, s: URLSearchParams) {
  for (const k of s.keys())
    if (!(k in compareDefaults) && !SHARED.includes(k))
      throw new ParamError(k, `Unknown parameter ${k}`);
  const q = resolveCompareQuery({
    ...compareDefaults,
    season: ui.defaultGameYear,
    ...Object.fromEntries(s),
  } as Query);
  if (q.view === "Players")
    throw new ParamError("view", "Players mode is not available in the chat yet");
  if (q.view !== "Teams" && q.view !== "Lineups")
    throw new ParamError("view", "Invalid view");
  const metric = String(q.metric),
    ff = metric.startsWith("off_") && metric !== "off_ppp",
    kind =
      q.view === "Lineups" ? (ff ? "lineup_ff" : "lineups") : ff ? "four_factors" : "ratings";
  // One side after the other, as the route's per-process limits expect.
  const a = (await side(compareSideParams(q, "a", kind))) as Answer,
    b = (await side(compareSideParams(q, "b", kind))) as Answer;
  if (a.meta.version !== b.meta.version)
    throw new Error("The data changed between the two sides; try again");
  const keep = (rows: Row[], teams: string) =>
    rows.filter((r) => !teams || teams.split(",").includes(String(r.team_id)));
  const teams = (
    (ui.ilTeams as Record<string, { id: number; name: string }[]>)[q.season] ?? []
  ).map((t) => ({ value: String(t.id), label: t.name }));
  const rows = finalizeCompare(
    joinCompare(
      keep(a.rows, String(q.a_teams)),
      keep(b.rows, String(q.b_teams)),
      q.view,
      metric,
      teams,
    ),
    q.view,
    q.preset,
    Math.max(0, Number(q.min_poss) || 0),
    parseStats(q.stats),
  );
  return { rows, meta: { version: a.meta.version, view: q.view } };
}
type Answer = { rows: Row[]; meta: { version: string } };
