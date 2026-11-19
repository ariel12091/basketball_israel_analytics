import "server-only";
// Compare's tables on the server, for the API's shape=table and the chat
// (ask/compareTool.ts): the page query resolved, both sides read from the
// compare route's own answer in-process, then the table each view shows --
// the league table and a team's players (rules/compareTable.ts, as
// CompareClient builds them) or one team's or lineup's Detail grid
// (EntityDetails' sections, one row per metric).
import ui from "@/data/shinyUi.json";
import data from "@/data/compareUi.json";
import {
  compareDefaults,
  compareSideParams,
  fullSideLabel,
  resolveCompareQuery,
  shortSideLabel,
} from "@/lib/compareQuery";
import type { Query } from "@/lib/tabQuery";
import type { League, Row } from "@/rules/gameLogs";
import {
  contextText,
  detailSections,
  gapInfo,
  num,
  reverseGap,
  shootingRates,
} from "@/rules/compare";
import { compareLeagueRows, comparePlayerRows } from "@/rules/compareTable";
import { pageQuery, ParamError } from "./api";
import { compareAnswer } from "./compareRepo";

export type Side = (s: URLSearchParams) => Promise<unknown>;
type Answer = { rows: Row[]; meta: { version: string } };
const VIEWS = ["league", "detail", "players"];

export type CompareTable = {
  rows: Row[];
  meta: {
    version: string;
    view: string;
    sides: { a: string; b: string };
    values?: Record<string, string>;
  };
};

/** shape=table: the table a freshly opened Compare link shows. */
export async function comparePageAnswer(
  search: URLSearchParams,
  league: League = "il",
): Promise<CompareTable> {
  const q = resolveCompareQuery(pageQuery(league, search, compareDefaults), league);
  if (q.drill_player) throw new ParamError("drill_player", "A player drill-down has no table");
  // Side headings spell out any filter a side carries (as in the chat).
  const filtered = [...search.keys()].some((k) => /^[ab]_/.test(k));
  return compareTableFor((s) => compareAnswer(s, league), q, filtered);
}

/** The table for a resolved page query; `filtered`: a side carries its own
 * filters, so its heading names them rather than the preset's short label. */
export async function compareTableFor(side: Side, q: Query, filtered: boolean): Promise<CompareTable> {
  if (!VIEWS.includes(q.cmp_view))
    throw new ParamError("cmp_view", "cmp_view is league, detail or players");
  if (q.view === "Players")
    throw new ParamError("view", "Players mode has no table yet");
  if (q.view !== "Teams" && q.view !== "Lineups") throw new ParamError("view", "Invalid view");
  if (q.cmp_view !== "league" && !q.entity)
    throw new ParamError("entity", `cmp_view=${q.cmp_view} needs entity (the team id)`);
  if (q.cmp_view === "players" && q.view !== "Teams")
    throw new ParamError("cmp_view", "The players view is for one team (view Teams)");
  // The page's heading names a plain preset; anything else needs the filters
  // spelled out, or the answer could only call its sides "A" and "B".
  const label = (s: "a" | "b") => {
    const full = fullSideLabel(q, s, teamChoices(q.season));
    return (!q.preset || filtered) && full !== `Side ${s.toUpperCase()}`
      ? full
      : shortSideLabel(q, s);
  };
  const sides = { a: label("a"), b: label("b") };
  const read = pairReader(side, q);
  if (q.cmp_view === "detail") return detail(q, read, sides);
  if (q.cmp_view === "players") return players(q, read, sides);

  const metric = String(q.metric),
    ff = metric.startsWith("off_") && metric !== "off_ppp",
    kind =
      q.view === "Lineups" ? (ff ? "lineup_ff" : "lineups") : ff ? "four_factors" : "ratings";
  const [a, b] = await read(kind);
  const rows = compareLeagueRows(q, a, b, teamChoices(q.season));
  // The page's own side headings (Home / Away, vs Top 4 ...), for the wording.
  return { rows, meta: { version: read.version(), view: q.view, sides } };
}

/** Reads one kind for side A then side B (as the route's per-process limits
 * expect), and refuses to mix data versions across every read of the view. */
function pairReader(side: Side, q: Query) {
  const versions = new Set<string>();
  const one = async (s: "a" | "b", kind: string, teams?: string) => {
    const p = compareSideParams(q, s, kind);
    if (teams) p.set("teams", teams);
    const answer = (await side(p)) as Answer;
    versions.add(answer.meta.version);
    if (versions.size > 1) throw new Error("The data changed between the two sides; try again");
    return answer.rows;
  };
  const read = async (kind: string, teams?: string) =>
    [await one("a", kind, teams), await one("b", kind, teams)] as [Row[], Row[]];
  return Object.assign(read, { version: () => [...versions][0] });
}
type Reader = ReturnType<typeof pairReader>;

const teamChoices = (season: string) =>
  ((ui.ilTeams as Record<string, { id: number; name: string }[]>)[season] ?? []).map((t) => ({
    value: String(t.id),
    label: t.name,
  }));

/** EntityDetails: ratings, four factors and (teams) shooting for one entity. */
async function detail(
  q: Query,
  read: Reader,
  sides: { a: string; b: string },
): Promise<CompareTable> {
  const teams = q.view === "Teams";
  const pick = (rows: Row[]) =>
    rows.find((r) => String(teams ? r.team_id : r.sub_lineup_hash) === q.entity) ?? null;
  const [ra, rb] = await read(teams ? "ratings" : "lineups");
  const [fa, fb] = await read(teams ? "four_factors" : "lineup_ff");
  const a = pick(ra),
    b = pick(rb);
  const [sa, sb] = teams ? await read("shooting", q.entity) : [a ? [a] : [], b ? [b] : []];
  const reverse = reverseGap(q.preset);
  const rows = detailSections(
    teams,
    [a, b],
    [pick(fa), pick(fb)],
    [shootingRates(sa), shootingRates(sb)],
  ).flatMap((section) =>
    section.metrics.map((m) => {
      const g = gapInfo(num(m.a), num(m.b), m.definition.polarity ?? "higher", reverse);
      return {
        section: section.title,
        label: m.definition.label,
        fmt: m.definition.fmt ?? "num",
        a: m.a,
        b: m.b,
        gap: g.gap,
        // The side the page colours as ahead; none for neutral metrics.
        better: g.a_wins == null ? null : g.a_wins ? sides.a : sides.b,
      };
    }),
  );
  const name = teams
    ? (teamChoices(q.season).find((t) => t.value === q.entity)?.label ?? q.entity)
    : String((a ?? b)?.player_names_str ?? q.entity);
  const values: Record<string, string> = { entity: name };
  for (const [k, r] of [
    ["context_a", a],
    ["context_b", b],
  ] as const)
    values[k] = contextText(r, teams) ?? "didn't play";
  return { rows, meta: { version: read.version(), view: "detail", sides, values } };
}

/** A team's players under both conditions (cmp_view players). */
async function players(
  q: Query,
  read: Reader,
  sides: { a: string; b: string },
): Promise<CompareTable> {
  const [a, b] = await read("players", q.entity);
  const pct = (data.TEAM_PLAYER_PCT_COLS as string[]).includes(q.team_metric);
  const rows = comparePlayerRows(q, a, b).map((r) => ({ ...r, pct }));
  return { rows, meta: { version: read.version(), view: "players", sides } };
}
