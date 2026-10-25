import "server-only";
// One ask tool call: the route's answer function, its own parser and guards,
// then presentation only (row ids, sort by a returned column, limit, link).
import ui from "@/data/shinyUi.json";
import { parseStats, queryFromSearch } from "@/lib/tabQuery";
import type { League } from "@/rules/gameLogs";
import { orderKey } from "@/rules/order";
import { ParamError, rawQueryGuard } from "@/server/api";
import { BusyError, DeadlineError } from "@/server/deadline";
import { displaySpec } from "./display";
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
  /** Result-level values: the row count, and Compare's side headings. */
  extras?: Record<string, string>;
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

/** URLSearchParams that remembers which names the page's table rules read. */
class RecordingParams extends URLSearchParams {
  read = new Set<string>();
  override get(name: string) {
    this.read.add(name);
    return super.get(name);
  }
}
const BASE = "http://ask.local";

/** The page link for a call (the query exactly as given). */
export function linkFor(call: ToolCall): string | null {
  const page = TOOLS[call.tool]?.page;
  if (!page) return null;
  const s = new URLSearchParams();
  for (const [k, v] of Object.entries(call.query))
    for (const x of Array.isArray(v) ? v : [v]) s.append(k, String(x));
  return `/${call.league}/${page}?${s}`;
}

type Prepared =
  | { error: string; param: string }
  | { api: URLSearchParams; page: URLSearchParams };

/** The request a page would make for this query, or why the query is not one
 * the page understands (app/[league]/<page>/page.tsx). */
function prepare(entry: (typeof TOOLS)[ToolName], call: ToolCall): Prepared {
  const values: Record<string, string> = {};
  for (const [k, v] of Object.entries(call.query)) {
    if (Array.isArray(v) && v.length !== 1)
      return { error: `Give ${k} once, comma separated if it takes several values`, param: k };
    values[k] = String(Array.isArray(v) ? v[0] : v);
  }
  if (!entry.request) {
    const unknown = Object.keys(values).find((k) => !entry.keys?.includes(k));
    if (entry.keys && unknown)
      return { error: `Unknown parameter ${unknown}`, param: unknown };
    return { api: new URLSearchParams(values), page: new URLSearchParams(values) };
  }
  if (!call.sort)
    return { error: "Give sort (a returned column and direction) for this table", param: "sort" };
  const defaults = entry.request.defaults(call.league);
  const q = queryFromSearch(call.league, values, defaults),
    fresh = queryFromSearch(call.league, {}, defaults);
  const unknown = Object.keys(values).find((k) => !(k in q));
  if (unknown) return { error: `Unknown parameter ${unknown}`, param: unknown };
  const api = new URL(entry.request.path(call.league, q), BASE).searchParams;
  const page = new RecordingParams();
  for (const [k, v] of Object.entries(q)) if (v) page.append(k, v);
  // A dry run of the page's table rules shows which keys they read.
  flatten(entry.name, call.league, { rows: [], meta: {} }, page);
  // A key that neither reaches the request nor the table rules is one the
  // page ignores (e.g. teams on Lineups): the answer would not be filtered.
  const ignored = Object.keys(values).find(
    (k) => !api.has(k) && !page.read.has(k) && values[k] !== fresh[k],
  );
  if (ignored)
    return { error: `${entry.name} does not use ${ignored}; check the tool description`, param: ignored };
  return { api, page };
}

/** Lineups ranked by a rating need a sample: the Home hub's 100 possessions,
 * stepping down to 50 and 25 early in a season until LINEUP_MIN_ROWS qualify.
 * Applied through the Lineups page's own stat filter, so the link shows the
 * same list. Not when the model set `stats` itself or ranks by volume. */
const LINEUP_MIN_POSS = [100, 50, 25];
const TOTAL_LABELS = new Set(["Players", "player_names_str", "Team", "team_name"]);
const LINEUP_MIN_ROWS = 3;
const VOLUME = new Set(["total_poss", "minutes", "off_poss", "def_poss", "off_pts", "def_pts"]);
function sampleMinimums(call: ToolCall): ToolCall[] {
  if (call.tool !== "lineups" || !call.sort || "stats" in call.query || VOLUME.has(call.sort.key))
    return [call];
  return LINEUP_MIN_POSS.map((value) => ({
    ...call,
    query: { ...call.query, stats: JSON.stringify([{ col: "total_poss", op: "ge", value }]) },
  }));
}

/** {{rN.min_poss}}: a possession minimum the rows were filtered by. */
function minPossExtra(call: ToolCall): Record<string, string> {
  try {
    const stats = JSON.parse(String(call.query.stats ?? "[]")) as { col: string; op: string; value: number }[];
    const f = stats.find((s) => s.col === "total_poss" && s.op === "ge");
    return f ? { min_poss: String(f.value) } : {};
  } catch {
    return {};
  }
}

export async function runTool(
  id: string,
  requested: ToolCall,
  deadlineAt: number,
): Promise<ToolResult | ToolError> {
  const candidates = sampleMinimums(requested);
  let call = candidates[0];
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
  let flat, payload: unknown;
  try {
    const prep = prepare(entry, call);
    if ("error" in prep) return fail(prep.error, prep.param);
    rawQueryGuard(new URL(`${BASE}/?${prep.api}`));
    payload =
      entry.name === "list_teams"
        ? await listTeams(call.league, prep.api)
        : await entry.answer!(call.league, prep.api);
    // On/Off's Player Stats chips need the page's second read.
    if (entry.playerStats && parseStats(prep.page.get("stats") ?? "").some((f) => f.col.startsWith("ps_")))
      payload = { ...(payload as object), psRows: await entry.playerStats(call.league, prep.api) };
    flat = flatten(entry.name, call.league, payload, prep.page);
    // A lower minimum changes only the page's own filter, not the request, so
    // the same payload is re-filtered; the first that leaves enough rows wins.
    for (const next of candidates.slice(1)) {
      if (flat.rows.length >= LINEUP_MIN_ROWS) break;
      const p = prepare(entry, next);
      if ("error" in p) break;
      call = next;
      flat = flatten(entry.name, call.league, payload, p.page);
    }
  } catch (e) {
    if (e instanceof ParamError) return fail(e.message, e.param);
    if (e instanceof DeadlineError || e instanceof BusyError)
      return fail("The data took too long to load");
    throw e;
  }
  let rows = flat.rows;
  if (call.sort) {
    const { key, desc } = call.sort;
    // A derived value (On/Off "Off Shot ON 3PT") sorts by the number it shows.
    const view = meta0(payload);
    const derived = displaySpec(entry.name, call.league, view)[key]?.derived
      ? displaySpec(entry.name, call.league, view)[key]
      : null;
    const value = (r: Record<string, unknown>) => {
      // Tags (3PT luck, small sample) sort tagged rows first when desc.
      if (typeof r[key] === "boolean") return r[key] ? 1 : 0;
      if (!derived || key in r) return r[key];
      const n = parseFloat(derived.format(undefined, r).replace(/^\+/, ""));
      return Number.isFinite(n) ? n : null;
    };
    if (rows.length && !derived && !rows.some((r) => key in r))
      return fail(
        `Unknown sort column ${key}; sort by one of: ${sortKeys(entry.name, call.league, payload, rows).join(", ")}`,
        "sort",
      );
    rows = rows
      .map((r, i) => ({ r, i, k: orderKey(value(r)) }))
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
  // The team list (for name matching) and the Home hub (a fixed set of
  // sections) are always complete; tables are capped.
  const meta = (
    (payload ?? {}) as {
      meta?: {
        view?: unknown;
        version?: unknown;
        sides?: { a: string; b: string };
        values?: Record<string, string>;
      };
    }
  ).meta;
  // Compare's Detail grid is one fixed set of metrics, like the hub.
  const limit =
    entry.name === "list_teams" || entry.name === "team_overview" || meta?.view === "detail"
      ? rows.length
      : Math.min(Math.max(1, Math.trunc(call.limit ?? 5)), 10);
  return {
    id,
    call,
    link: linkFor(call),
    view: meta?.view == null ? null : String(meta.view),
    rows: rows
      .slice(0, limit)
      .map((cells, i) => ({ id: `row${i + 1}`, cells })),
    // The TOTAL row's name cells hold the label "- All Lineups -": without
    // them a {{rN.total.Players}} marker fails the check and is retried.
    total: flat.total
      ? {
          id: "total",
          cells: Object.fromEntries(Object.entries(flat.total).filter(([k]) => !TOTAL_LABELS.has(k))),
        }
      : null,
    rowCount: rows.length,
    version: meta?.version == null ? null : String(meta.version),
    // Result-level values a marker may quote: {{r1.count}}, {{r1.side_a}}.
    extras: {
      count: String(rows.length),
      ...(meta?.sides ? { side_a: meta.sides.a, side_b: meta.sides.b } : {}),
      ...(meta?.values ?? {}),
      ...minPossExtra(call),
    },
  };
}

/** Columns a table tool can sort by: the quotable ones its rows carry. */
const meta0 = (payload: unknown) => {
  const view = (payload as { meta?: { view?: unknown } } | null)?.meta?.view;
  return view == null ? null : String(view);
};
function sortKeys(tool: ToolName, league: League, payload: unknown, rows: Record<string, unknown>[]) {
  const spec = displaySpec(tool, league, meta0(payload));
  return Object.keys(spec).filter((k) => spec[k].derived || rows.some((r) => k in r));
}

/** The pages' own team lists: the generated IL catalogue, or the Euro readers'. */
async function listTeams(league: League, s: URLSearchParams) {
  if (league === "il") {
    const season = s.get("season") ?? ui.defaultGameYear;
    const teams = (ui.ilTeams as Record<string, { id: number; name: string }[]>)[
      season
    ];
    if (!teams) throw new ParamError("season", "Invalid season");
    return { rows: teams.map((t) => ({ team_id: t.id, name: t.name })) };
  }
  const p = (await TOOLS.team_ratings.answer!(league, s)) as {
    meta?: { teams?: unknown[] };
  };
  return {
    rows: ((p.meta?.teams ?? []) as { id: number; name: string }[]).map((t) => ({ team_id: t.id, name: t.name })),
  };
}
