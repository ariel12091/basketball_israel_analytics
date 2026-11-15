// The columns of an API CSV export (format=csv): each table's page-visible
// columns as ask/display.ts lists them -- the page's own labels and
// formatters, every cell pinned to the page's rendered text by
// display.test.ts. Chat-only and derived columns are left out. A page cell
// that shows a value and its rank together is two columns here.
import { displaySpec } from "../ask/display";
import type { ToolName } from "../ask/registry";
import type { League, Row } from "../rules/gameLogs";
import type { CsvColumn } from "./csv";

export function csvColumns(tool: ToolName, league: League, view: string | null): CsvColumn[] {
  const spec = displaySpec(tool, league, view);
  return Object.entries(spec)
    .filter(([, c]) => !c.chatOnly && !c.derived)
    .map(([key, c]) => ({ label: c.label, text: (r: Row) => c.format(r[key], r) }));
}
