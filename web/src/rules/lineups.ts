// Faithful ports of helpers.R lineup_*_datatable/apply_local_lineup_filters,
// server_tab2.R ranked frames, and server_tab10_euro_lineups.R add_rates/contract.
import type { Row, StatFilter } from "./gameLogs";
import { adaptiveBaseline } from "./onoff";
import { rRound } from "./round";
import { applyStatFilters } from "./statFilters";

export type LineupView = "summary" | "four_factors";
export type PlayerSelection = {
  on: number[];
  any: number[];
  off: number[];
  count: number;
  exact: boolean;
};
export const zero = (v: unknown) => (v == null ? 0 : Number(v));
export const rate = (num: unknown, den: unknown, scale = 100): number | null =>
  zero(den) > 0 ? (scale * zero(num)) / zero(den) : null;
export function playerIds(v: unknown): number[] {
  if (Array.isArray(v)) return v.map(Number).filter(Number.isFinite);
  return String(v ?? "")
    .replace(/[{}\s]/g, "")
    .split(",")
    .filter(Boolean)
    .map(Number)
    .filter(Number.isFinite);
}
/** helpers.R lineup_on_predicate / apply_local_lineup_filters. */
export function localLineups(
  rows: Row[],
  team: string,
  p: PlayerSelection,
): Row[] {
  return rows.filter((r) => {
    if (team && String(r.team_id) !== team) return false;
    if (!team) return true; // Shiny disables player filtering without a team.
    const ids = playerIds(r.player_ids ?? r.unit_key);
    if (
      !p.on.every((id) => ids.includes(id)) ||
      p.off.some((id) => ids.includes(id))
    )
      return false;
    if (!p.any.length) return true;
    const hits = p.any.filter((id) => ids.includes(id)).length;
    const k = Math.max(1, Math.min(p.count || 1, p.any.length));
    return p.exact ? hits === k : hits >= k;
  });
}
/** helpers.R auto_minposs_from_df: ceil the 150th row to a multiple of ten. */
export function lineupAutoMin(rows: Row[]): number | null {
  const vals = rows
    .map((r) => Number(r.total_poss))
    .filter(Number.isFinite)
    .sort((a, b) => b - a);
  return !vals.length
    ? null
    : vals.length <= 150
      ? 0
      : Math.ceil(vals[149] / 10) * 10;
}
/** server_tab10: derive every rate before copying counts into Tab 2's contract. */
export function euroLineupRates(rows: Row[]): Row[] {
  return rows.map((r) => {
    const out: Row = {
      ...r,
      sub_lineup_hash: r.unit_key,
      Players: r.player_names_str,
      total_poss: zero(r.off_poss) + zero(r.def_poss),
      plus_minus: zero(r.off_pts) - zero(r.def_pts),
    };
    for (const s of ["off", "def"]) {
      out[`${s}_ppp`] = rate(r[`${s}_pts`], r[`${s}_poss`]);
      out[`${s}_ts`] = rate(r[`${s}_pts`], 2 * zero(r[`${s}_ts_poss`]));
      out[`${s}_efg`] = rate(
        zero(r[`${s}_fgm`]) + 0.5 * zero(r[`${s}_fg3_made`]),
        r[`${s}_fga`],
      );
      out[`${s}_oreb`] = rate(r[`${s}_oreb`], r[`${s}_oreb_opp`]);
      out[`${s}_tov`] = rate(r[`${s}_tov`], r[`${s}_poss`]);
      out[`${s}_ftr`] = rate(r[`${s}_fta`], r[`${s}_fga`]);
      for (const [to, from] of Object.entries({
        oreb_cnt: "oreb",
        oreb_opps: "oreb_opp",
        tov_cnt: "tov",
        fga_cnt: "fga",
        fgm_cnt: "fgm",
        fg3m_cnt: "fg3_made",
      }))
        out[`${s}_${to}`] = r[`${s}_${from}`];
    }
    out.net_rtg =
      out.off_ppp == null || out.def_ppp == null
        ? null
        : Number(out.off_ppp) - Number(out.def_ppp);
    out.num_starters = rate(r.starters_poss_num, out.total_poss, 1);
    return out;
  });
}
/** server_tab2 pr_vec / server_tab10 add_pct_ranks (average ties, full population). */
export function rankLineups(rows: Row[], euro: boolean): Row[] {
  const result = rows.map(
    (r) =>
      ({
        ...r,
        Players: r.player_names_str,
        total_poss: zero(r.off_poss) + zero(r.def_poss),
        plus_minus: zero(r.off_pts) - zero(r.def_pts),
      }) as Row,
  );
  const threshold = adaptiveBaseline(result.map((r) => zero(r.total_poss)));
  const specs: [string, string, boolean][] = [
    ["pr_ld_net", "net_rtg", false],
    ["pr_ld_off_ppp", "off_ppp", false],
    ["pr_ld_def_ppp_i", "def_ppp", true],
    ["pr_net", "net_rtg", false],
  ];
  for (const s of ["off", "def"])
    for (const metric of ["ppp", "efg", "oreb", "tov", "ftr"])
      specs.push([
        `pr_${s}_${metric}`,
        `${s}_${metric}`,
        s === "off" ? metric === "tov" : metric !== "tov",
      ]);
  for (const [dest, src, invert] of specs) {
    const vals = result
      .filter((r) => zero(r.total_poss) >= threshold && r[src] != null)
      .map((r) => Number(r[src]))
      .sort((a, b) => a - b);
    const ranks = new Map<number, number>();
    for (let i = 0; i < vals.length;) {
      let j = i + 1;
      while (j < vals.length && vals[j] === vals[i]) j++;
      ranks.set(
        vals[i],
        vals.length === 1 ? 0.5 : (i + j - 1) / 2 / (vals.length - 1),
      );
      i = j;
    }
    for (const r of result) {
      const pr =
        zero(r.total_poss) >= threshold &&
        r[src] != null &&
        (euro || vals.length > 1)
          ? ranks.get(Number(r[src]))
          : undefined;
      r[dest] = pr == null ? null : invert ? 1 - pr : pr;
    }
  }
  return result.sort((a, b) => zero(b.total_poss) - zero(a.total_poss));
}
/** helpers.R lineup_*_datatable: stat filters precede totals; rates from counts. */
export function lineupTableRows(
  rows: Row[],
  view: LineupView,
  min: number,
  filters: StatFilter[] = [],
): Row[] {
  const enriched = rows
    .filter((r) => zero(r.total_poss) >= min)
    .map((r) => {
      const out = { ...r };
      for (const side of ["off", "def"]) {
        const a2 = zero(r[`${side}_fg2_att`]),
          a3 = zero(r[`${side}_fg3_att`]);
        out[side === "off" ? "Off Shot" : "Def Shot"] = a2 + a3;
        for (const [suffix, val] of Object.entries({
          fg2_pct: rate(r[`${side}_fg2_made`], a2),
          fg3_pct: rate(r[`${side}_fg3_made`], a3),
          fg2_freq: rate(a2, a2 + a3),
          fg3_freq: rate(a3, a2 + a3),
        }))
          out[`${side}_${suffix}`] = val == null ? null : rRound(val, 1);
      }
      return out;
    });
  const filtered = applyStatFilters(enriched, filters);
  if (!filtered.length) return [];
  const sum = (col: string) => filtered.reduce((a, r) => a + zero(r[col]), 0);
  const total: Row = {
    Team: "TOTAL",
    Players: "- All Lineups -",
    is_total: 0,
    sub_lineup_hash: "TOTAL",
    team_id: null,
    num_starters: null,
  };
  for (const k of [
    "minutes",
    "total_poss",
    "off_poss",
    "def_poss",
    "off_pts",
    "def_pts",
    "plus_minus",
    "Off Shot",
    "Def Shot",
  ])
    total[k] = sum(k);
  for (const s of ["off", "def"]) {
    const ppp = rate(sum(`${s}_pts`), sum(`${s}_poss`));
    total[`${s}_ppp`] =
      view === "four_factors"
        ? ppp == null
          ? null
          : rRound(ppp, 1)
        : (ppp ?? 0);
    for (const k of ["fg2_made", "fg2_att", "fg3_made", "fg3_att"])
      total[`${s}_${k}`] = sum(`${s}_${k}`);
    for (const [metric, n, d] of [
      [
        "efg",
        sum(`${s}_fgm_cnt`) + 0.5 * sum(`${s}_fg3m_cnt`),
        sum(`${s}_fga_cnt`),
      ],
      ["oreb", sum(`${s}_oreb_cnt`), sum(`${s}_oreb_opps`)],
      ["tov", sum(`${s}_tov_cnt`), sum(`${s}_poss`)],
      ["ftr", sum(`${s}_fta`), sum(`${s}_fga_cnt`)],
    ] as [string, number, number][]) {
      const v = rate(n, d);
      total[`${s}_${metric}`] = v == null ? null : rRound(v, 1);
    }
  }
  total.net_rtg =
    total.off_ppp == null || total.def_ppp == null
      ? null
      : Number(total.off_ppp) - Number(total.def_ppp);
  if (view === "four_factors" && total.net_rtg != null)
    total.net_rtg = rRound(Number(total.net_rtg), 1);
  return [total, ...filtered.map((r) => ({ ...r, is_total: 1 }))];
}
/** Match Shiny's explicit offense/defense filters; ignore unclassified rows. */
export function pivotLineupGames(raw: Row[]): Row[] {
  const games = new Map<string, Row>();
  for (const row of raw) {
    if (row.type_lineup !== "offense" && row.type_lineup !== "defense")
      continue;
    const key = String(row.game_id);
    const out = games.get(key) ?? { game_id: row.game_id };
    const side = row.type_lineup === "offense" ? "off" : "def";
    for (const [column, value] of Object.entries(row))
      if (column !== "game_id" && column !== "type_lineup")
        out[`${side}_${column}`] = value;
    games.set(key, out);
  }
  return [...games.values()];
}
