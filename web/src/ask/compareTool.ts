// The Compare tool: what the chat asked for, as the page query the Compare
// page shows it with (pageQuery below), then the page's own table from
// server/comparePage.ts -- the same builder the API's shape=table runs --
// plus the chat-only gap_size column.
import ui from "@/data/shinyUi.json";
import {
  compareDefaults,
  LAST_N_PRESET,
  presetBlocksMirror,
  resolveCompareQuery,
  SIDE_FIELDS,
} from "@/lib/compareQuery";
import type { Query } from "@/lib/tabQuery";
import type { Row } from "@/rules/gameLogs";
import { ParamError } from "@/server/api";
import { compareTableFor, type CompareTable, type Side } from "@/server/comparePage";

export type { CompareTable };
const SHARED = ["season", "start", "end", "min_gn", "max_gn", "stats"];

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
    // The chat's Compare is the Israeli league's, which filters on game_type.
    if (k === "phase" || k === "a_phase" || k === "b_phase")
      throw new ParamError(k, `Unknown parameter ${k}`);
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
      if (v && v !== compareDefaults[k] && !["team_id", "open_detail", "league"].includes(k)) out.set(k, v);
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
  const t = await compareTableFor(side, q, filtered);
  return t.meta.view === "detail" ? t : { ...t, rows: withGapSize(t.rows) };
}

/** The gap without its direction, so "differs the most" is a sort: left to the
 * model, comparing +16.9 with -26.2 sometimes named the smaller one. */
const withGapSize = (rows: Row[]): Row[] =>
  rows.map((r) => ({ ...r, gap_size: r.gap == null ? null : Math.abs(Number(r.gap)) }));
