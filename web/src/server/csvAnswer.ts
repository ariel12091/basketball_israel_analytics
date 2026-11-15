// format=csv: a finished table as a CSV download, cached like its JSON
// (cacheHeaders). Columns: server/csvColumns.ts; format: server/csv.ts.
import type { ToolName } from "../ask/registry";
import type { League, Row } from "../rules/gameLogs";
import { cacheHeaders } from "./api";
import { csvText } from "./csv";
import { csvColumns } from "./csvColumns";

export function answerCsv(
  answer: { rows?: Row[]; meta?: { version?: string; view?: unknown; season?: unknown } },
  url: URL,
  tool: ToolName,
  league: League,
): Response {
  const view = answer.meta?.view == null ? null : String(answer.meta.view);
  const route = url.pathname.split("/").filter(Boolean).at(-1) ?? tool;
  const season = String(answer.meta?.season ?? url.searchParams.get("season") ?? "").replace(/[^\w-]/g, "");
  const body = csvText(csvColumns(tool, league, view), answer.rows ?? []);
  return new Response(body, {
    headers: {
      ...cacheHeaders(answer, url),
      "Content-Type": "text/csv; charset=utf-8",
      "Content-Disposition": `attachment; filename="${route}_${league}_${season}.csv"`,
    },
  });
}
