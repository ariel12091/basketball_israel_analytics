// The Compare page for one page URL: CompareClient resolves the query, reads
// side A and side B from the compare route, then builds what the chosen view
// shows -- the league table (joinCompare + finalizeCompare), one team's or
// lineup's Detail grid (EntityDetails), or a team's players (joinTeamPlayers).
// Keep this in step with CompareClient and CompareDetails.
import ui from "@/data/shinyUi.json";
import data from "@/data/compareUi.json";
import {
  compareDefaults,
  compareSideParams,
  fullSideLabel,
  LAST_N_PRESET,
  presetBlocksMirror,
  resolveCompareQuery,
  shortSideLabel,
  SIDE_FIELDS,
} from "@/lib/compareQuery";
import { parseStats, type Query } from "@/lib/tabQuery";
import type { Row } from "@/rules/gameLogs";
import {
  contextText,
  detailSections,
  finalizeCompare,
  gapInfo,
  joinCompare,
  joinTeamPlayers,
  num,
  reverseGap,
  shootingRates,
} from "@/rules/compare";
import { ParamError } from "@/server/api";

type Side = (s: URLSearchParams) => Promise<unknown>;
type Answer = { rows: Row[]; meta: { version: string } };
const SHARED = ["season", "start", "end", "min_gn", "max_gn", "stats"];
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

/** The side filter each preset defines: a different value there asks for a
 * table that cannot exist, so it is the one combination refused. */
const DEFINED: Record<string, string> = {
  home_away: "home_away",
  win_loss: "outcome",
  starters_bench: "own_starters",
  opp_starters_bench: "opp_starters",
  clutch: "clutch",
  top_bottom_rank: "opp_rank_side",
};

/**
 * What the chat asked for, as the query the page shows it with. A preset and
 * filters combine: the preset is applied first, then the filters on top, and
 * preset_applied keeps the page from applying it again over them. A filter
 * without a side means both sides; with a preset, a filter on one side reaches
 * the other as it does when edited on the page.
 */
function pageQuery(s: URLSearchParams): { q: Query; filtered: boolean } {
  const given: Record<string, string> = {},
    both: Record<string, string> = {},
    sided: Record<string, string> = {};
  for (const [k, v] of s) {
    if (SIDE_FIELDS.includes(k)) both[k] = v;
    else if (/^[ab]_/.test(k) && SIDE_FIELDS.includes(k.slice(2))) sided[k] = v;
    else if (Object.hasOwn(compareDefaults, k) || SHARED.includes(k)) given[k] = v;
    else throw new ParamError(k, `Unknown parameter ${k}`);
  }
  if (given.drill_player)
    throw new ParamError("drill_player", "Player drill-down is not available in the chat");
  delete given.preset_applied;
  const team = given.team_id ?? "";
  if (team && !/^[1-9]\d*$/.test(team))
    throw new ParamError("team_id", "team_id is a team id");
  const base = resolveCompareQuery({
    ...compareDefaults,
    season: ui.defaultGameYear,
    ...given,
  } as Query);
  const preset = base.preset;
  // "Last 5 vs the season": last_n can only mean the preset's side A.
  if (preset === LAST_N_PRESET && both.last_n) {
    sided.a_last_n ??= both.last_n;
    delete both.last_n;
  }
  const filters: Record<string, string> = {};
  for (const [k, v] of Object.entries(both))
    for (const side of ["a", "b"]) filters[`${side}_${k}`] = v;
  Object.assign(filters, sided);
  if (preset)
    for (const [k, v] of Object.entries(sided)) {
      const other = `${k[0] === "a" ? "b" : "a"}_${k.slice(2)}`;
      if (!presetBlocksMirror(preset, k.slice(2)) && !(other in sided)) filters[other] = v;
    }
  for (const [k, v] of Object.entries(filters))
    if (DEFINED[preset] === k.slice(2) && v !== base[k])
      throw new ParamError(k, `The ${preset} preset already sets ${k.slice(2)} on both sides`);
  if (preset === LAST_N_PRESET && filters.b_last_n)
    throw new ParamError("b_last_n", "Side B of this preset is the whole season");
  const q: Query = { ...base, ...filters, preset_applied: "true" };
  if (team && !preset) {
    q.a_teams ||= team;
    q.b_teams ||= team;
    if (given.open_detail === "true")
      Object.assign(q, { view: "Teams", entity: team, cmp_view: "detail" });
  }
  if ((q.a_last_n || q.b_last_n) && (q.min_gn || q.max_gn || preset === "gn_split"))
    throw new ParamError("last_n", "Last N and a game-number range are mutually exclusive");
  return { q, filtered: Object.keys(filters).length > 0 };
}

/** The page link for a Compare call: the query the numbers were read with. */
export function compareLink(query: Record<string, string>): string | null {
  try {
    const { q } = pageQuery(new URLSearchParams(query));
    const out = new URLSearchParams();
    for (const [k, v] of Object.entries(q))
      if (v && v !== compareDefaults[k] && !["team_id", "open_detail"].includes(k)) out.set(k, v);
    if (!q.preset) out.delete("preset_applied");
    return `/il/compare?${out}`;
  } catch (e) {
    if (e instanceof ParamError) return null;
    throw e;
  }
}

export async function compareTable(side: Side, s: URLSearchParams): Promise<CompareTable> {
  const { q, filtered } = pageQuery(s);
  // Opening one team's Detail needs to know which team.
  if (s.get("open_detail") === "true" && q.cmp_view !== "detail")
    throw new ParamError("open_detail", "open_detail=true needs team_id");
  if (!VIEWS.includes(q.cmp_view))
    throw new ParamError("cmp_view", "cmp_view is league, detail or players");
  if (q.view === "Players")
    throw new ParamError("view", "Players mode is not available in the chat yet");
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
  const keep = (rows: Row[], teams: string) =>
    rows.filter((r) => !teams || teams.split(",").includes(String(r.team_id)));
  const rows = finalizeCompare(
    joinCompare(
      keep(a, String(q.a_teams)),
      keep(b, String(q.b_teams)),
      q.view,
      metric,
      teamChoices(q.season),
    ),
    q.view,
    q.preset,
    minimum(q),
    parseStats(q.stats),
  );
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

const minimum = (q: Query) => Math.max(0, Number(q.min_poss) || 0);
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
  const rows = joinTeamPlayers(
    a,
    b,
    q.team_metric,
    q.team_rate,
    minimum(q),
    reverseGap(q.preset),
  ).map((r) => ({ ...r, pct }));
  return { rows, meta: { version: read.version(), view: "players", sides } };
}
