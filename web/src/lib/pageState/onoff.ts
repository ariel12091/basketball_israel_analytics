// The On/Off table's page state: what OnOffClient, the API's shape=table
// (onoffPageAnswer) and the chat pass to onoffTableRows().
import ui from "../../data/shinyUi.json";
import type { League, Row } from "../../rules/gameLogs";
import { autoBars, autoShare, onoffTableRows, type View } from "../../rules/onoff";
import { applyPlayerStatFilters, type PsRow } from "../../rules/playerStats";
import { contextParams, parseStats, type Query } from "../tabQuery";

type Payload = {
  rows: Row[];
  league3p?: number | null;
  meta: { view?: View; filtered?: boolean };
};

const list = (v: string | undefined) => (v ?? "").split(",").filter(Boolean);

/** The page's query defaults: the league's bars, auto on. `auto=false`
 * (a link that fixes its bars) is the only way to keep min_on / min_all,
 * as on Lineups. */
export function onoffQueryDefaults(league: League): Record<string, string> {
  const d = league === "il" ? ui.onoffDefaults.israel : ui.onoffDefaults.euroleague;
  return { min_all: String(d.minAll), min_on: String(d.minOn), auto: "true" };
}

/** Auto's population (onoff_auto_min_base_df): the selected teams' rows, or
 * every row when the filtered Summary path already narrowed them in SQL. */
export function onoffAutoBase(payload: Payload, view: View, teams: number[]): Row[] {
  if (payload.meta.filtered && view === "summary") return payload.rows;
  return teams.length ? payload.rows.filter((r) => teams.includes(Number(r.team_id))) : payload.rows;
}

/** `applyAuto`: the server and the chat (a freshly opened link) apply auto
 * here unless the link says auto=false; the page passes false, because its
 * own effect (useAutoMin) has already written auto's bars into the query. */
export function onoffPageOptions(query: Query, payload: Payload, applyAuto: boolean) {
  const view = (payload.meta.view ?? query.view ?? "summary") as View;
  const teams = list(query.teams).map(Number);
  const bars =
    applyAuto && query.auto !== "false"
      ? autoBars(onoffAutoBase(payload, view, teams), view, autoShare(teams))
      : null;
  const filters = parseStats(query.stats ?? "");
  return {
    view,
    teams,
    minAll: bars?.minAll ?? (Number(query.min_all) || 0),
    minOn: bars?.minOn ?? (Number(query.min_on) || 0),
    filters,
    psFilters: filters.filter((f) => f.col.startsWith("ps_")),
  };
}

/** The Player Stats frame the ps_* chips read: the same game context. */
export function onoffPsPath(league: League, query: Query): string {
  const q = contextParams(query, league);
  for (const k of ["view", "teams", "team", "stats", "own_starters", "opp_starters"]) q.delete(k);
  return `/api/v1/${league}/player-stat-filters?${q}`;
}

/** shape=table: the answer with its rows replaced by the page's table.
 * `psRows`: the Player Stats frame, needed only for ps_* chips. `applyAuto`
 * as in onoffPageOptions (the page passes false). */
export function onoffTable<A extends Payload>(
  answer: A,
  query: Query,
  psRows: PsRow[] = [],
  applyAuto = true,
) {
  const o = onoffPageOptions(query, answer, applyAuto);
  const rows = onoffTableRows(
    { rows: answer.rows, league3p: answer.league3p ?? null, view: o.view },
    { teams: o.teams, minAll: o.minAll, minOn: o.minOn, filters: o.filters },
    o.psFilters.length ? (r) => applyPlayerStatFilters(r, psRows, o.psFilters) : undefined,
  );
  return { ...answer, rows, meta: { ...answer.meta, shape: "table" as const } };
}
