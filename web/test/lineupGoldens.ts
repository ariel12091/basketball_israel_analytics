import { readdirSync, readFileSync, existsSync } from "node:fs";
import type { Row, League } from "../src/rules/gameLogs";
import {
  localLineups,
  lineupPage,
  lineupTableRows,
  playerIds,
  type LineupView,
} from "../src/rules/lineups";
import { lineupPageOptions } from "../src/lib/lineupPageState";
export const DIR = "test/goldens/lineups";
export const presets = existsSync(DIR)
  ? readdirSync(DIR).filter(
      (f) => f.endsWith(".json") && !f.endsWith(".answer.json"),
    )
  : [];
export function loadLineup(file: string) {
  const g = JSON.parse(readFileSync(`${DIR}/${file}`, "utf8")) as {
    id: string;
    league: League;
    view: LineupView;
    query: Record<string, string>;
    filters: { col: string; op: "ge" | "le"; value: number }[];
    expected: { data: Row[]; hidden: string[] } | null;
  };
  const { answer, roster, games } = JSON.parse(
    readFileSync(`${DIR}/${g.id}.answer.json`, "utf8"),
  ) as {
    answer: { rows: Row[]; meta: { version: string } };
    roster: { rows: Row[] };
    games: { rows: Row[] };
  };
  const q = g.query;
  const local = localLineups(answer.rows, q.team ?? "", {
    on: playerIds(q.players_on),
    any: playerIds(q.players_any),
    off: playerIds(q.players_off),
    count: Number(q.any_count) || 1,
    exact: q.any_exact === "true",
  });
  return {
    g,
    answer,
    roster,
    games,
    local,
    rows: lineupTableRows(local, g.view, Number(q.min_poss), g.filters),
  };
}
/** A substituted /lineups answer as lineupsPageAnswer builds it: the stored
 * population through lineupPage for the page state the request names. Other
 * addresses get `answer` unchanged. */
export function pageAnswer(
  answer: { rows: Row[]; meta: Record<string, unknown> },
  requestUrl: string,
) {
  const url = new URL(requestUrl);
  const m = /\/api\/v1\/([a-z]+)\/lineups$/.exec(url.pathname);
  if (!m) return answer;
  const view =
    url.searchParams.get("view") === "four_factors" ? "four_factors" : "summary";
  const page = lineupPage(
    answer.rows,
    lineupPageOptions(url.searchParams, m[1] as League, view),
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
