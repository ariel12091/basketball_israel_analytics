import "server-only";
// One ask tool call: the route's answer function, its own parser and guards,
// then presentation only (row ids, sort by a returned column, limit, link).
import ui from "@/data/shinyUi.json";
import type { League } from "@/rules/gameLogs";
import { orderKey } from "@/rules/order";
import { ParamError, rawQueryGuard } from "@/server/api";
import { BusyError, DeadlineError } from "@/server/deadline";
import { flatten } from "./flatten";
import { TOOLS, type ToolName } from "./registry";

export type ToolCall = {
  tool: ToolName;
  league: League;
  query: Record<string, string | string[]>;
  sort?: { key: string; desc: boolean };
  limit?: number;
};
export type AskRow = { id: string; cells: Record<string, unknown> };
export type ToolResult = {
  id: string;
  call: ToolCall;
  link: string | null;
  view: string | null;
  rows: AskRow[];
  total: AskRow | null;
  rowCount: number;
  version: string | null;
};
export type ToolError = {
  id: string;
  call: ToolCall;
  error: string;
  param: string | null;
};

const LEAGUE_NAMES: Record<League, string> = {
  il: "the Israeli League",
  euroleague: "EuroLeague",
  eurocup: "EuroCup",
};

/** URLSearchParams that remembers which names the parser asked for. */
class RecordingParams extends URLSearchParams {
  read = new Set<string>();
  all = false;
  override get(name: string) {
    this.read.add(name);
    return super.get(name);
  }
  override getAll(name: string) {
    this.read.add(name);
    return super.getAll(name);
  }
  override has(name: string, value?: string) {
    this.read.add(name);
    return value === undefined ? super.has(name) : super.has(name, value);
  }
  override entries() {
    this.all = true;
    return super.entries();
  }
  override keys() {
    this.all = true;
    return super.keys();
  }
  override forEach(...a: Parameters<URLSearchParams["forEach"]>) {
    this.all = true;
    return super.forEach(...a);
  }
  override [Symbol.iterator]() {
    this.all = true;
    return super[Symbol.iterator]();
  }
}

function toParams(query: ToolCall["query"]): RecordingParams {
  const s = new RecordingParams();
  for (const [k, v] of Object.entries(query))
    for (const x of Array.isArray(v) ? v : [v]) s.append(k, String(x));
  return s;
}

export async function runTool(
  id: string,
  call: ToolCall,
  deadlineAt: number,
): Promise<ToolResult | ToolError> {
  const fail = (error: string, param: string | null = null): ToolError => ({
    id,
    call,
    error,
    param,
  });
  const entry = TOOLS[call.tool];
  if (!entry) return fail(`Unknown tool ${String(call.tool)}`);
  if (Date.now() >= deadlineAt) return fail("Out of time for this question");
  if (!entry.leagues.includes(call.league))
    return fail(
      `\`${entry.name}\` is available for ${entry.leagues.map((l) => LEAGUE_NAMES[l]).join(", ")} only`,
      "league",
    );
  const params = toParams(call.query);
  let payload: unknown;
  try {
    rawQueryGuard(new URL(`http://ask.local/?${params}`));
    payload =
      entry.name === "list_teams"
        ? await listTeams(call.league, params)
        : await entry.answer!(call.league, params);
  } catch (e) {
    if (e instanceof ParamError) return fail(e.message, e.param);
    if (e instanceof DeadlineError || e instanceof BusyError)
      return fail("The data took too long to load");
    throw e;
  }
  if (!params.all) {
    const unknown = [...new Set(params.keys())].find((k) => !params.read.has(k));
    if (unknown) return fail(`Unknown parameter ${unknown}`, unknown);
  }
  const flat = flatten(entry.name, payload, params);
  let rows = flat.rows;
  if (call.sort) {
    const { key, desc } = call.sort;
    if (rows.length && !rows.some((r) => key in r))
      return fail(`Unknown sort column ${key}`, "sort");
    rows = rows
      .map((r, i) => ({ r, i, k: orderKey(r[key]) }))
      .sort((a, b) =>
        a.k === undefined
          ? b.k === undefined
            ? a.i - b.i
            : 1
          : b.k === undefined
            ? -1
            : a.k < b.k
              ? desc ? 1 : -1
              : a.k > b.k
                ? desc ? -1 : 1
                : a.i - b.i,
      )
      .map((x) => x.r);
  }
  // A reference list must be complete for name matching; tables are capped.
  const limit =
    entry.name === "list_teams"
      ? rows.length
      : Math.min(Math.max(1, Math.trunc(call.limit ?? 5)), 10);
  const meta = ((payload ?? {}) as { meta?: { view?: unknown; version?: unknown } })
    .meta;
  return {
    id,
    call,
    link: entry.page ? `/${call.league}/${entry.page}?${params}` : null,
    view: meta?.view == null ? null : String(meta.view),
    rows: rows
      .slice(0, limit)
      .map((cells, i) => ({ id: `row${i + 1}`, cells })),
    total: flat.total ? { id: "total", cells: flat.total } : null,
    rowCount: rows.length,
    version: meta?.version == null ? null : String(meta.version),
  };
}

/** The pages' own team lists: the generated IL catalogue, or the Euro readers'. */
async function listTeams(league: League, s: URLSearchParams) {
  if (league === "il") {
    const season = s.get("season") ?? ui.defaultGameYear;
    const teams = (ui.ilTeams as Record<string, { id: number; name: string }[]>)[
      season
    ];
    if (!teams) throw new ParamError("season", "Invalid season");
    return { rows: teams };
  }
  const p = (await TOOLS.team_ratings.answer!(league, s)) as {
    meta?: { teams?: unknown[] };
  };
  return { rows: p.meta?.teams ?? [] };
}
