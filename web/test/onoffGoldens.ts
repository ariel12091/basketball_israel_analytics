import { readdirSync, readFileSync } from "node:fs";
import type { Row, League } from "../src/rules/gameLogs";
import { onoffTableRows, type View } from "../src/rules/onoff";
import { applyPlayerStatFilters, type PsRow } from "../src/rules/playerStats";
export const DIR = "test/goldens/onoff";
export const presets = readdirSync(DIR).filter(
  (f) => f.endsWith(".json") && !f.endsWith(".answer.json"),
);
export function loadGolden(file: string) {
  const g = JSON.parse(readFileSync(`${DIR}/${file}`, "utf8")) as {
    id: string;
    league: League;
    view: View;
    query: Record<string, string>;
    filters: { col: string; op: "ge" | "le"; value: number }[];
    expected: { data: Row[]; hidden: string[]; order: [number, string][] };
  };
  const { answer, ps } = JSON.parse(
    readFileSync(`${DIR}/${g.id}.answer.json`, "utf8"),
  ) as {
    answer: {
      rows: Row[];
      league3p: number | null;
      meta: { view: View; version: string };
    };
    ps: PsRow[];
  };
  const rows = onoffTableRows(
    { ...answer, view: g.view },
    {
      teams: (g.query.teams ?? "").split(",").filter(Boolean).map(Number),
      minAll: Number(g.query.min_all),
      minOn: Number(g.query.min_on),
      filters: g.filters,
    },
    (r) =>
      applyPlayerStatFilters(
        r,
        ps,
        g.filters.filter((f) => f.col.startsWith("ps_")),
      ),
  );
  return { g, answer, ps, rows };
}
