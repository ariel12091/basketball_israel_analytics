// Faithful server_tab7_compare.R joins, summary, detail and player calculations.
import type { Row } from "./gameLogs";
import { applyStatFilters } from "./statFilters";
import type { StatFilter } from "../lib/tabQuery";
import { rRound } from "./round";
import data from "../data/compareUi.json";
import { teamShotProfile } from "./teamRatings";
import { shotProfileRanked } from "./onoff";
export const num = (v: unknown) => (v == null || v === "" ? NaN : Number(v));
export const known = (v: number) => (Number.isFinite(v) ? v : null);
export type Joined = Row & {
  metric_a: number | null;
  metric_b: number | null;
  poss_a: number | null;
  poss_b: number | null;
  gap: number | null;
  rank: number;
  entity_name: string;
};
export const reverseGap = (preset: string) =>
  preset === "date_split" || preset === "gn_split";
export const gapValue = (a: number, b: number, reverse = false) =>
  known(reverse ? b - a : a - b);
export const metricLower = (m: string) =>
  m === "def_ppp" || m === "off_tov" || m === "tov";
export function gapInfo(
  a: number,
  b: number,
  polarity: string,
  reverse = false,
) {
  if (!Number.isFinite(a) || !Number.isFinite(b))
    return { gap: null, direction: "none", a_wins: null };
  const raw = a - b;
  return {
    gap: gapValue(a, b, reverse),
    direction: raw > 0 ? "a" : raw < 0 ? "b" : "none",
    a_wins:
      polarity === "neutral" || Math.abs(raw) < 1e-9
        ? null
        : polarity === "lower"
          ? a < b
          : a > b,
  };
}
export function joinCompare(
  a: Row[],
  b: Row[],
  mode: string,
  metric: string,
  teams: { value: string; label: string }[] = [],
): Joined[] {
  const keys =
    mode === "Teams"
      ? ["team_id", "team_name"]
      : [
          "sub_lineup_hash",
          ...["player_names_str", "team_id", "team_name"].filter(
            (k) => a.some((r) => k in r) && b.some((r) => k in r),
          ),
        ];
  const key = (r: Row) => JSON.stringify(keys.map((k) => r[k] ?? null));
  const bm = new Map<string, Row[]>();
  for (const r of b) bm.set(key(r), [...(bm.get(key(r)) ?? []), r]);
  const poss = (r: Row) =>
    mode === "Teams"
      ? num(r.off_poss ?? r.total_poss)
      : r.total_poss != null
        ? num(r.total_poss)
        : r.def_poss != null
          ? (num(r.off_poss) || 0) + (num(r.def_poss) || 0)
          : num(r.off_poss);
  return a.flatMap((ra) =>
    (bm.get(key(ra)) ?? []).map((rb) => ({
      ...ra,
      ...(mode === "Lineups" && !keys.includes("team_name")
        ? {
            team_name:
              teams.find((t) => t.value === String(ra.team_id))?.label ?? null,
          }
        : {}),
      metric_a: known(num(ra[metric])),
      metric_b: known(num(rb[metric])),
      poss_a: known(poss(ra)),
      poss_b: known(poss(rb)),
      gap: null,
      rank: 0,
      entity_name: String(
        mode === "Teams"
          ? ra.team_name
          : (ra.player_names_str ?? ra.sub_lineup_hash),
      ),
    })),
  );
}
export function autoCompareMin(rows: Row[]): number | null {
  const values = rows
    .map((r) => Math.min(num(r.poss_a), num(r.poss_b)))
    .filter(Number.isFinite)
    .sort((a, b) => b - a);
  return !values.length
    ? null
    : values.length <= 50
      ? 0
      : Math.ceil(values[49] / 10) * 10;
}
export function finalizeCompare(
  rows: Joined[],
  mode: string,
  preset: string,
  min: number,
  filters: StatFilter[] = [],
): Joined[] {
  let out = rows
    .filter((r) => num(r.poss_a) >= min && num(r.poss_b) >= min)
    .map((r) => ({
      ...r,
      gap: gapValue(num(r.metric_a), num(r.metric_b), reverseGap(preset)),
    }));
  out.sort((a, b) =>
    a.gap == null
      ? b.gap == null
        ? 0
        : 1
      : b.gap == null
        ? -1
        : b.gap - a.gap,
  );
  if (mode === "Lineups") out = out.slice(0, 50);
  return applyStatFilters(out, filters).map((r, i) => ({
    ...r,
    rank: i + 1,
  })) as Joined[];
}
const mean = (xs: number[]) => {
  const a = xs.filter(Number.isFinite);
  return a.length ? a.reduce((s, x) => s + x, 0) / a.length : NaN;
};
function weighted(rows: Row[], s: string) {
  const valid = rows.filter(
    (r) => Number.isFinite(num(r[`metric_${s}`])) && num(r[`poss_${s}`]) > 0,
  );
  return valid.length
    ? valid.reduce(
        (v, r) => v + num(r[`metric_${s}`]) * num(r[`poss_${s}`]),
        0,
      ) / valid.reduce((v, r) => v + num(r[`poss_${s}`]), 0)
    : mean(rows.map((r) => num(r[`metric_${s}`])));
}
export function compareSummary(rows: Joined[], mode: string, metric: string) {
  const a =
      mode === "Lineups"
        ? weighted(rows, "a")
        : mean(rows.map((r) => num(r.metric_a))),
    b =
      mode === "Lineups"
        ? weighted(rows, "b")
        : mean(rows.map((r) => num(r.metric_b)));
  const delta = mode === "Lineups" ? a - b : mean(rows.map((r) => num(r.gap))),
    gapAbs =
      mode === "Lineups"
        ? Math.abs(delta)
        : mean(rows.map((r) => Math.abs(num(r.gap))));
  const tie =
    !Number.isFinite(a) ||
    !Number.isFinite(b) ||
    Math.abs(a - b) <=
      Math.sqrt(Number.EPSILON) * Math.max(1, Math.abs(a), Math.abs(b));
  return {
    a: known(a),
    b: known(b),
    delta: known(delta),
    gap_abs: known(gapAbs),
    poss_a: known(mean(rows.map((r) => num(r.poss_a)))),
    poss_b: known(mean(rows.map((r) => num(r.poss_b)))),
    winner: tie ? "tie" : (metricLower(metric) ? a < b : a > b) ? "a" : "b",
  };
}
export type MetricDef = {
  label: string;
  col?: string;
  col_ratings?: string | null;
  col_ff?: string | null;
  col_shooting?: string;
  polarity?: string;
  fmt?: string;
  type?: string;
  factor?: string;
  raw?: boolean;
  pct?: boolean;
};
export type DetailSection = { title: string; metrics: MetricDef[] };
export const detailSpecs = data.DETAIL_METRICS as Record<string, DetailSection>;
export function shootingRates(rows: Row[]): Row | null {
  if (!rows.length) return null;
  const out = { ...rows[0] },
    sum = (k: string) =>
      rows.reduce(
        (s, r) => s + (Number.isFinite(num(r[k])) ? num(r[k]) : 0),
        0,
      );
  for (const side of ["off", "def"]) {
    const a2 = sum(`${side}_fg2_att`),
      a3 = sum(`${side}_fg3_att`);
    for (const [k, n, d] of [
      ["fg2_acc", sum(`${side}_fg2_made`), a2],
      ["fg3_acc", sum(`${side}_fg3_made`), a3],
      ["fg2_freq", a2, a2 + a3],
      ["fg3_freq", a3, a2 + a3],
    ] as const)
      out[`${side}_${k}`] = d > 0 ? (n / d) * 100 : null;
  }
  return out;
}
export function detailMetric(
  ratings: Row | null,
  ff: Row | null,
  shooting: Row | null,
  m: MetricDef,
) {
  if (m.col_ratings === "win_pct")
    return num(ratings?.games_played) > 0
      ? known((num(ratings?.wins) / num(ratings?.games_played)) * 100)
      : null;
  return known(
    num(
      m.col_ratings
        ? ratings?.[m.col_ratings]
        : m.col_ff
          ? ff?.[m.col_ff]
          : m.col_shooting
            ? shooting?.[m.col_shooting]
            : null,
    ),
  );
}
export function withTeamShots(r: Row | null) {
  return r ? teamShotProfile([r], [])[0] : null;
}
/** The Detail view's sections for one team or lineup (EntityDetails). */
export function detailSections(
  teams: boolean,
  [a, b]: [Row | null, Row | null],
  [aff, bff]: [Row | null, Row | null],
  [shootingA, shootingB]: [Row | null, Row | null],
) {
  return Object.entries(detailSpecs)
    .filter(
      ([key]) =>
        teams ||
        !["def_shooting", "off_shot_profile", "def_shot_profile"].includes(key),
    )
    .map(([, section]) => ({
      title: section.title,
      metrics: section.metrics
        .filter((m) => teams || m.label !== "Win%")
        .map((m) => ({
          definition: m,
          a: detailMetric(withTeamShots(a), aff, shootingA, m),
          b: detailMetric(withTeamShots(b), bff, shootingB, m),
        })),
    }));
}
/** CompareGrid's gap cell. */
export function gridGapText(gap: number | null, fmt?: string, player = false) {
  const suffix = fmt === "pct" ? "%" : "";
  if (gap == null) return "--";
  if (Math.abs(gap) < 1e-9) return `${player ? "+" : "+/-"}0.0${suffix}`;
  return `${formatCompare(gap, "num", true)}${suffix}`;
}
/** CompareContext's line for one side. */
export function contextText(r: Row | null, teams = false, player = false) {
  if (!r) return null;
  return `${teams ? `${formatCompare(r.games_played, "int")} GP | ${r.wins}-${r.losses} | ` : player ? `${formatCompare(r.gp, "int")} GP | ` : ""}${formatCompare(r.off_poss ?? r.total_poss ?? r.poss_on_floor, "int")} Poss`;
}
export function playerMetric(
  r: Row | null,
  metric: string,
  rate: string,
): number | null {
  if (!r) return null;
  if ((data.TEAM_PLAYER_PCT_COLS as string[]).includes(metric))
    return known(num(r[metric]));
  const raw =
    metric === "disruptions" ? num(r.stl) + num(r.dfl) : num(r[metric]);
  if (rate === "Totals") return known(raw);
  const den =
    rate === "Per 75 Possessions" || rate === "Per 60 Possessions"
      ? num(r.poss_on_floor)
      : rate === "Per 30 Minutes"
        ? num(r.minutes)
        : num(r.gp);
  return den > 0
    ? known(
        (raw / den) *
          (rate === "Per 75 Possessions"
            ? 75
            : rate === "Per 60 Possessions"
              ? 60
              : rate === "Per 30 Minutes"
                ? 30
                : 1),
      )
    : null;
}
export function joinTeamPlayers(
  a: Row[],
  b: Row[],
  metric: string,
  rate: string,
  min: number,
  reverse = false,
): Row[] {
  const groups = new Map<string, { a: Row[]; b: Row[] }>();
  for (const [side, rows] of [
    ["a", a],
    ["b", b],
  ] as const)
    for (const r of rows) {
      if (
        r.team_id == null ||
        r.player_id == null ||
        !r.team_name ||
        !(r.player_name ?? r.Player)
      )
        continue;
      const key = `${r.team_id}:${r.player_id}`,
        g = groups.get(key) ?? { a: [], b: [] };
      g[side].push(r);
      groups.set(key, g);
    }
  const played = (r: Row | null) =>
    r != null && ["gp", "poss_on_floor", "minutes"].some((k) => num(r[k]) > 0);
  const out: Row[] = [];
  for (const g of groups.values())
    for (const ra of g.a.length ? g.a : [null])
      for (const rb of g.b.length ? g.b : [null]) {
        const aPlayed = played(ra),
          bPlayed = played(rb),
          va = aPlayed ? playerMetric(ra, metric, rate) : 0,
          vb = bPlayed ? playerMetric(rb, metric, rate) : 0,
          pa = num(ra?.poss_on_floor) || 0,
          pb = num(rb?.poss_on_floor) || 0;
        if (Math.max(pa, pb) < min) continue;
        const gap = gapValue(num(va), num(vb), reverse),
          r = { ...(ra ?? rb!) };
        for (const side of ["a", "b"] as const)
          for (const col of data.TEAM_PLAYER_RAW_COLS)
            r[`${col}_${side}`] = (side === "a" ? ra : rb)?.[col] ?? null;
        out.push({
          ...r,
          player_name: String(
            ra?.player_name ?? ra?.Player ?? rb?.player_name ?? rb?.Player,
          ),
          metric_a: va,
          metric_b: vb,
          played_a: aPlayed,
          played_b: bPlayed,
          poss_a: pa,
          poss_b: pb,
          gap,
          gap_score: gap == null ? null : metricLower(metric) ? -gap : gap,
        });
      }
  out.sort((a, b) => {
    const ga = num(a.gap_score),
      gb = num(b.gap_score),
      missingA = !Number.isFinite(ga),
      missingB = !Number.isFinite(gb);
    return (
      Number(missingA) - Number(missingB) ||
      (!missingA && !missingB ? gb - ga : 0) ||
      String(a.team_name).localeCompare(String(b.team_name)) ||
      String(a.player_name).localeCompare(String(b.player_name))
    );
  });
  return out.map((r, i) => ({ ...r, rank: i + 1 }));
}
export function playerShotRows(row: Row | null) {
  return row ? shotProfileRanked([row])[0] : null;
}
/** Compare get_swing(): activity requires adequate samples on BOTH sides. */
export function playerSwingValue(
  ff: Row | null,
  onoff: Row | null,
  stat: { label: string; col: string; requires_def_poss?: boolean },
) {
  const row = stat.label.endsWith("Diff") ? onoff : ff;
  if (
    !row ||
    (stat.requires_def_poss &&
      (!(num(row.def_on_poss) >= 300) || !(num(row.def_off_poss) >= 300)))
  )
    return null;
  return known(num(row[stat.col]));
}
/** Compare Shot Profile swing(), after add_player_shot_profile_shares(). */
export function playerShotSwing(row: Row | null, side: string, metric: string) {
  const on = num(row?.[`${side}_on_${metric}`]),
    off = num(row?.[`${side}_off_${metric}`]);
  return { d: known(rRound(on - off, 1)), on: known(on), off: known(off) };
}
export function formatCompare(v: unknown, fmt = "num", signed = false): string {
  const n = num(v);
  if (!Number.isFinite(n)) return "--";
  const rounded = rRound(n, fmt === "int" ? 0 : 1),
    text = Math.abs(rounded).toFixed(fmt === "int" ? 0 : 1),
    sign = n < 0 || Object.is(n, -0) ? "-" : signed ? "+" : "";
  return `${sign}${text}${fmt === "pct" ? "%" : ""}`;
}
