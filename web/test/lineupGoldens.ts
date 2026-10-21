import { readdirSync, readFileSync, existsSync } from "node:fs";
import type { Row, League } from "../src/rules/gameLogs";
import {
  localLineups,
  lineupTableRows,
  playerIds,
  type LineupView,
} from "../src/rules/lineups";
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
