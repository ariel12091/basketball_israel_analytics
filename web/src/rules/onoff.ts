// On/Off rules derived from the rows the SQL owners return (spec decision 12):
// faithful ports of app/R/helpers.R and app/R/server_tab1.R. Each function
// names its R source. Used by the API route (ranking on the full population)
// and the page (local filters, auto-min, 3PT luck, sample size).
import type { Row } from "./gameLogs";
import { percentRank } from "./gameLogs";
import { rRound } from "./round";
import ui from "../data/shinyUi.json";

const RANKING_BASELINE = ui.rankingBaseline; // global.R
const RANKING_MIN_PCT = 0.25; // global.R
export const AUTO_TOP_PCT = ui.autoTopPct;
export const ONOFF_PAD_POSS = ui.onoffPadPoss;

export type View = "summary" | "four_factors" | "shot_profile";
const num = (v: unknown) => (v == null || v === "" ? null : Number(v));
const z = (v: unknown) => num(v) ?? 0; // coalesce(x, 0)

/** stats::quantile(type = 7). */
export function quantile7(values: number[], p: number): number {
  const s = [...values].sort((a, b) => a - b);
  if (!s.length) return NaN;
  const h = (s.length - 1) * p,
    lo = Math.floor(h);
  return (
    s[lo] + (h - lo) * ((s[Math.min(lo + 1, s.length - 1)] ?? s[lo]) - s[lo])
  );
}
const median = (v: number[]) => quantile7(v, 0.5);

/** helpers.R adaptive_baseline. */
export function adaptiveBaseline(poss: (number | null)[]): number {
  const v = poss.filter((x): x is number => x != null && Number.isFinite(x));
  if (!v.length) return 0;
  const above = v.filter((x) => x >= RANKING_BASELINE).length / v.length;
  return above >= RANKING_MIN_PCT
    ? RANKING_BASELINE
    : quantile7(v, 1 - RANKING_MIN_PCT);
}

/** helpers.R onoff_clean_display_names. */
export function cleanDisplayNames(rows: Row[]): Row[] {
  return rows.map((r) => {
    const out: Row = { ...r };
    if (out.Player == null) {
      if (r["First Name"] != null || r["Last Name"] != null)
        out.Player = `${r["First Name"] ?? "NA"} ${r["Last Name"] ?? "NA"}`;
      else if (r.firstname != null || r.lastname != null)
        out.Player = `${r.firstname ?? "NA"} ${r.lastname ?? "NA"}`;
    }
    if (r.team_name != null && out.Team == null) out.Team = r.team_name;
    return out;
  });
}

/** server_tab1.R ff_ranked_df, MV branch: the advanced MV joined to the
 * summary MV's ratings (left join on player, team, season). */
export function joinFfRatings(adv: Row[], mv: Row[]): Row[] {
  if (adv.some((r) => "Net RTG Diff" in r)) return adv;
  const key = (r: Row, y: unknown) => `${r.player_id}|${r.team_id}|${y}`;
  const byKey = new Map(mv.map((r) => [key(r, r.Year ?? r.game_year), r]));
  return adv.map((r) => {
    const m = byKey.get(key(r, r.game_year));
    return {
      ...r,
      "Net RTG Diff": m?.["Net RTG Diff"] ?? null,
      "Off ON Diff": m?.["Off ON Diff"] ?? null,
      "Def ON Diff": m?.["Def ON Diff"] ?? null,
      minutes: m?.minutes ?? null,
      off_on_ppp: m?.["Off ON PPP"] ?? null,
      off_off_ppp: m?.["Off OFF PPP"] ?? null,
      def_on_ppp: m?.["Def ON PPP"] ?? null,
      def_off_ppp: m?.["Def OFF PPP"] ?? null,
    };
  });
}

const FF_RANK_RAW = [
  "off_on_efg",
  "off_off_efg",
  "off_on_oreb",
  "off_off_oreb",
  "off_on_tov",
  "off_off_tov",
  "off_on_ftr",
  "off_off_ftr",
  "def_on_efg",
  "def_off_efg",
  "def_on_oreb",
  "def_off_oreb",
  "def_on_tov",
  "def_off_tov",
  "def_on_ftr",
  "def_off_ftr",
  "off_on_ppp",
  "off_off_ppp",
  "def_on_ppp",
  "def_off_ppp",
];
/** helpers.R onoff_add_ff_ranks: ranks on the FULL population. */
export function addFfRanks(input: Row[]): Row[] {
  const rows = input.map((r) => ({
    ...r,
    "Off Rtg Diff":
      r["Off ON Diff"] == null ? null : rRound(Number(r["Off ON Diff"]), 1),
    "Def Rtg Diff":
      r["Def ON Diff"] == null ? null : rRound(Number(r["Def ON Diff"]), 1),
    "Net Diff":
      r["Net RTG Diff"] == null ? null : rRound(Number(r["Net RTG Diff"]), 1),
  })) as Row[];
  const thresh = adaptiveBaseline(rows.map((r) => num(r.off_on_poss)));
  const ok = (r: Row) =>
    num(r.off_on_poss) != null && Number(r.off_on_poss) >= thresh;
  const rank = (name: string, f: (r: Row) => number | null) => {
    const pr = percentRank(rows.map((r) => (ok(r) ? f(r) : null)));
    rows.forEach((r, i) => (r[name] = pr[i]));
  };
  const diff = (a: string, b: string) => (r: Row) =>
    num(r[a]) == null || num(r[b]) == null ? null : Number(r[a]) - Number(r[b]);
  rank("pr_net_diff", (r) => num(r["Net Diff"]) ?? -999);
  rank("pr_off_rtg", (r) => num(r["Off Rtg Diff"]) ?? -999);
  rank("pr_def_rtg", (r) => num(r["Def Rtg Diff"]) ?? 999);
  for (const side of ["off", "def"])
    for (const m of ["efg", "oreb", "ftr", "tov"])
      rank(`pr_diff_${side}_${m}`, diff(`${side}_on_${m}`, `${side}_off_${m}`));
  for (const col of FF_RANK_RAW) {
    if (!rows.some((r) => col in r)) continue;
    const pr = percentRank(rows.map((r) => (ok(r) ? z(r[col]) : null)));
    rows.forEach(
      (r, i) =>
        (r[`${col}_rank`] = pr[i] == null ? null : (pr[i] as number) * 100),
    );
  }
  return rows;
}

export const SP_LABELS = [
  "eFG%",
  "Lay-up",
  "Dunk",
  "Lay+Dunk",
  "3PA",
  "C3",
  "2PT Jumper",
];
export const SP_SUFFIX = [
  "efg",
  "layup_share",
  "dunk_share",
  "rim_share",
  "fg3_share",
  "c3_pct3",
  "mid_share",
];
const SP_PREFIXES = ["off_on", "off_off", "def_on", "def_off"];
const SP_FGA_GATE = 50;
const pct1 = (n: number, d: number) =>
  Number.isFinite(d) && d > 0 ? rRound((n / d) * 100, 1) : null;
/** server_tab1.R sp_ranked_df + helpers.R add_shot_profile_metrics. */
export function shotProfileRanked(input: Row[]): Row[] {
  const need = SP_PREFIXES.flatMap((p) =>
    [
      "_layup_att",
      "_dunk_att",
      "_fg2_att",
      "_fg3_att",
      "_c3_att",
      "_c3_known_att",
      "_fg2_made",
      "_fg3_made",
    ].map((s) => p + s),
  );
  if (!input.length || !need.every((c) => c in input[0])) return input;
  const rows = input.map((r) => {
    const out: Row = { ...r };
    for (const p of SP_PREFIXES) {
      const fga = z(r[`${p}_fg2_att`]) + z(r[`${p}_fg3_att`]);
      const made = z(r[`${p}_fg2_made`]) + 1.5 * z(r[`${p}_fg3_made`]);
      out[`${p}_efg`] = fga > 0 ? rRound((made / fga) * 100, 1) : null;
      const layup = z(r[`${p}_layup_att`]),
        dunk = z(r[`${p}_dunk_att`]),
        fg3a = z(r[`${p}_fg3_att`]);
      const rim = layup + dunk;
      out[`${p}_layup_share`] = pct1(layup, fga);
      out[`${p}_dunk_share`] = pct1(dunk, fga);
      out[`${p}_rim_share`] = pct1(rim, fga);
      out[`${p}_fg3_share`] = pct1(fg3a, fga);
      out[`${p}_mid_share`] = pct1(Math.max(fga - rim - fg3a, 0), fga);
      out[`${p}_c3_pct3`] = pct1(
        z(r[`${p}_c3_att`]),
        z(r[`${p}_c3_known_att`]),
      );
      out[`${p}_fga`] = fga;
    }
    return out;
  });
  SP_SUFFIX.forEach((m, i) => {
    for (const side of ["off", "def"]) {
      const diffCol = `${side === "off" ? "Off" : "Def"} ${SP_LABELS[i]} Diff`;
      rows.forEach((r) => {
        const on = num(r[`${side}_on_${m}`]),
          off = num(r[`${side}_off_${m}`]);
        r[diffCol] = on == null || off == null ? null : rRound(on - off, 1);
      });
      const gate = (r: Row) => z(r[`${side}_on_fga`]) >= SP_FGA_GATE;
      const pr = percentRank(
        rows.map((r) => (gate(r) ? num(r[diffCol]) : null)),
      );
      rows.forEach((r, j) => (r[`pr_sp_${side}_${m}`] = pr[j]));
      for (const phase of ["on", "off"]) {
        const sc = `${side}_${phase}_${m}`;
        const prr = percentRank(rows.map((r) => (gate(r) ? z(r[sc]) : null)));
        rows.forEach(
          (r, j) =>
            (r[`${sc}_rank`] =
              prr[j] == null ? null : (prr[j] as number) * 100),
        );
      }
    }
  });
  return rows;
}

/** helpers.R onoff_filter_summary_rows (Summary, Shot Profile). */
export function filterSummaryRows(
  rows: Row[],
  teams: number[],
  minAll: number,
  minOn: number,
): Row[] {
  return rows.filter(
    (r) =>
      (!teams.length || teams.includes(Number(r.team_id))) &&
      (!("ON Poss" in r && "OFF Poss" in r) ||
        Math.min(z(r["ON Poss"]), z(r["OFF Poss"])) >= minAll) &&
      (!("ON Poss" in r) ||
        (num(r["ON Poss"]) != null && Number(r["ON Poss"]) >= minOn)),
  );
}
/** helpers.R onoff_filter_ff_rows (Four Factors). */
export function filterFfRows(
  rows: Row[],
  teams: number[],
  minAll: number,
  minOn: number,
): Row[] {
  return rows.filter(
    (r) =>
      (!teams.length || teams.includes(Number(r.team_id))) &&
      Math.min(
        z(r.off_on_poss),
        z(r.off_off_poss),
        z(r.def_on_poss),
        z(r.def_off_poss),
      ) >= minAll &&
      num(r.off_on_poss) != null &&
      Number(r.off_on_poss) >= minOn,
  );
}

/** helpers.R resolve_poss_cols. */
export function possCols(view: View): { on: string; off: string } {
  return view === "four_factors"
    ? { on: "off_on_poss", off: "off_off_poss" }
    : { on: "ON Poss", off: "OFF Poss" };
}
function topRows(rows: Row[], usage: string): Row[] {
  const n = rows.length;
  const top = Math.max(1, Math.ceil(n * AUTO_TOP_PCT));
  // arrange(desc(x)): NA last, stable.
  return rows
    .map((r, i) => ({ r, i }))
    .sort((a, b) => {
      const x = num(a.r[usage]),
        y = num(b.r[usage]);
      if (x == null) return y == null ? a.i - b.i : 1;
      if (y == null) return -1;
      return y - x || a.i - b.i;
    })
    .slice(0, Math.min(top, n))
    .map((x) => x.r);
}
/** helpers.R auto_min_on_from_df. */
export function autoMinOn(
  rows: Row[],
  usage: string,
  step = 10,
): number | null {
  if (!rows.length) return null;
  const v = topRows(rows, usage)
    .map((r) => num(r[usage]))
    .filter((x): x is number => x != null);
  if (!v.length) return null;
  return Math.floor(Math.min(...v) / step) * step;
}
/** helpers.R auto_min_all_from_df. */
export function autoMinAll(
  rows: Row[],
  usage: string,
  on: string,
  off: string,
  step = 10,
): number | null {
  if (!rows.length) return null;
  const v = topRows(rows, usage)
    .map((r) =>
      num(r[on]) == null || num(r[off]) == null
        ? null
        : Math.min(Number(r[on]), Number(r[off])),
    )
    .filter((x): x is number => x != null);
  if (!v.length) return null;
  return Math.floor(Math.min(...v) / step) * step;
}

/** helpers.R onoff_league_3p: league 3P% from each team's best-covered row. */
export function league3p(rows: Row[]): number | null {
  const need = [
    "off_on_fg3_made",
    "off_on_fg3_att",
    "off_off_fg3_made",
    "off_off_fg3_att",
  ];
  if (!rows.length || !need.every((c) => c in rows[0])) return null;
  const items = rows
    .map((r, i) => ({
      i,
      team: r.team_id ?? r.Team,
      m:
        num(r.off_on_fg3_made) == null || num(r.off_off_fg3_made) == null
          ? null
          : Number(r.off_on_fg3_made) + Number(r.off_off_fg3_made),
      a:
        num(r.off_on_fg3_att) == null || num(r.off_off_fg3_att) == null
          ? null
          : Number(r.off_on_fg3_att) + Number(r.off_off_fg3_att),
    }))
    .filter((x) => x.m != null && x.a != null)
    .sort((x, y) => (y.a as number) - (x.a as number) || x.i - y.i);
  const seen = new Set<unknown>();
  let m = 0,
    a = 0;
  for (const x of items) {
    if (seen.has(x.team)) continue;
    seen.add(x.team);
    m += x.m as number;
    a += x.a as number;
  }
  return seen.size && a > 0 ? m / a : null;
}

/** helpers.R onoff_3pt_luck. */
export function threePtLuck(
  rows: Row[],
  rate: number | null,
): { ours: (number | null)[]; theirs: (number | null)[] } | null {
  const sides = ["off_on", "off_off", "def_on", "def_off"];
  const need = [
    "ON Poss",
    "OFF Poss",
    ...sides.flatMap((s) => [`${s}_fg3_made`, `${s}_fg3_att`]),
  ];
  if (!rows.length || !need.every((c) => c in rows[0]) || rate == null)
    return null;
  const extra = (r: Row, side: string, poss: number | null) =>
    poss == null ||
    num(r[`${side}_fg3_made`]) == null ||
    num(r[`${side}_fg3_att`]) == null
      ? null
      : (300 *
          (Number(r[`${side}_fg3_made`]) -
            rate * Number(r[`${side}_fg3_att`]))) /
        poss;
  const pos = (v: unknown) =>
    num(v) != null && Number(v) > 0 ? Number(v) : null;
  const ours: (number | null)[] = [],
    theirs: (number | null)[] = [];
  for (const r of rows) {
    const pon = pos(r["ON Poss"]),
      poff = pos(r["OFF Poss"]);
    const a = extra(r, "off_on", pon),
      b = extra(r, "off_off", poff),
      c = extra(r, "def_on", pon),
      d = extra(r, "def_off", poff);
    ours.push(a == null || b == null ? null : a - b);
    theirs.push(c == null || d == null ? null : -(c - d));
  }
  return { ours, theirs };
}
/** helpers.R onoff_pad_shrink. */
export function padShrink(
  rows: Row[],
  pad = ONOFF_PAD_POSS,
): (number | null)[] {
  return rows.map((r) => {
    const pon = num(r["ON Poss"]),
      poff = num(r["OFF Poss"]);
    if (pon == null || poff == null || !(pon > 0) || !(poff > 0)) return null;
    return ((pon * poff) / (pon + poff)) * (1 / (pon + pad) + 1 / (poff + pad));
  });
}
/** helpers.R onoff_sample_flag. */
export function sampleFlag(
  net: (number | null)[],
  shrink: (number | null)[],
  q = 0.2,
  thin = 0.6,
  minRows = 10,
): boolean[] {
  const ok = net.map(
    (n, i) =>
      n != null &&
      Number.isFinite(n) &&
      shrink[i] != null &&
      Number.isFinite(shrink[i] as number),
  );
  const flag = net.map(() => false);
  const n = net.filter((_, i) => ok[i]) as number[];
  const s = shrink.filter((_, i) => ok[i]) as number[];
  if (n.length < minRows) return flag;
  const hi = quantile7(n, 1 - q),
    lo = quantile7(n, q),
    med = median(s);
  let j = 0;
  ok.forEach((good, i) => {
    if (!good) return;
    flag[i] = (n[j] >= hi || n[j] <= lo) && s[j] < thin * med;
    j++;
  });
  return flag;
}

const SPLIT_SIDES: Record<string, string> = {
  on_off: "off_on",
  on_def: "def_on",
  off_off: "off_off",
  off_def: "def_off",
};
/** helpers.R onoff_summary_datatable, data preparation before the stat
 * filters: shot display columns and split metrics, 3PT luck, sample size. */
export function summaryDerive(input: Row[], rate: number | null): Row[] {
  const rows = input.map((r) => {
    const out: Row = { ...r };
    if (out.minutes === undefined) out.minutes = null;
    if ("off_on_fg2_att" in r && "off_on_fg3_att" in r) {
      out["Off Shot ON"] = z(r.off_on_fg2_att) + z(r.off_on_fg3_att);
      out["Def Shot ON"] = z(r.def_on_fg2_att) + z(r.def_on_fg3_att);
      out["Off Shot OFF"] = z(r.off_off_fg2_att) + z(r.off_off_fg3_att);
      out["Def Shot OFF"] = z(r.def_off_fg2_att) + z(r.def_off_fg3_att);
      for (const [prefix, src] of Object.entries(SPLIT_SIDES)) {
        const fg2m = z(r[`${src}_fg2_made`]),
          fg2a = z(r[`${src}_fg2_att`]),
          fg3m = z(r[`${src}_fg3_made`]),
          fg3a = z(r[`${src}_fg3_att`]);
        out[`${prefix}_fg2_pct`] = pct1(fg2m, fg2a);
        out[`${prefix}_fg2_freq`] = pct1(fg2a, fg2a + fg3a);
        out[`${prefix}_fg3_pct`] = pct1(fg3m, fg3a);
        out[`${prefix}_fg3_freq`] = pct1(fg3a, fg2a + fg3a);
      }
    }
    return out;
  });
  const luckRate = rate ?? league3p(rows);
  const luck = threePtLuck(rows, luckRate);
  if (luck)
    rows.forEach((r, i) => {
      r.luck_3pt_ours = luck.ours[i];
      r.luck_3pt_theirs = luck.theirs[i];
      const net = num(r["Net RTG Diff"]);
      const o = luck.ours[i],
        t = luck.theirs[i];
      const adj = net == null || o == null || t == null ? null : net - o - t;
      r.luck_3pt_flag = adj != null && net !== 0 && adj * (net as number) <= 0;
    });
  if (
    rows.length &&
    "Net RTG Diff" in rows[0] &&
    "ON Poss" in rows[0] &&
    "OFF Poss" in rows[0]
  ) {
    const shrink = padShrink(rows);
    const net = rows.map((r) => num(r["Net RTG Diff"]));
    const flags = sampleFlag(net, shrink);
    rows.forEach((r, i) => {
      r.net_padded =
        net[i] == null || shrink[i] == null
          ? null
          : (net[i] as number) * (shrink[i] as number);
      r.sample_flag = flags[i];
    });
  }
  return rows;
}

export const SUMMARY_SHOT_MAP: Record<
  string,
  [string, string, string, string]
> = {
  "Off Shot ON": [
    "off_on_fg2_made",
    "off_on_fg2_att",
    "off_on_fg3_made",
    "off_on_fg3_att",
  ],
  "Def Shot ON": [
    "def_on_fg2_made",
    "def_on_fg2_att",
    "def_on_fg3_made",
    "def_on_fg3_att",
  ],
  "Off Shot OFF": [
    "off_off_fg2_made",
    "off_off_fg2_att",
    "off_off_fg3_made",
    "off_off_fg3_att",
  ],
  "Def Shot OFF": [
    "def_off_fg2_made",
    "def_off_fg2_att",
    "def_off_fg3_made",
    "def_off_fg3_att",
  ],
};
/** onoff_summary_datatable: weighted league averages from rows with >= 50 FGA
 * in that cell (computed on the table's rows after the stat filters). */
export function summaryShotAverages(
  rows: Row[],
  minFga = 50,
): Record<string, { fg2: number; fg3: number }> {
  const out: Record<string, { fg2: number; fg3: number }> = {};
  for (const [dn, cols] of Object.entries(SUMMARY_SHOT_MAP)) {
    const q = rows.filter((r) => num(r[dn]) != null && Number(r[dn]) >= minFga);
    const sum = (c: string) => q.reduce((s, r) => s + z(r[c]), 0);
    const a2 = sum(cols[1]),
      a3 = sum(cols[3]);
    out[dn] = {
      fg2: a2 > 0 ? rRound((sum(cols[0]) / a2) * 100) : 53,
      fg3: a3 > 0 ? rRound((sum(cols[2]) / a3) * 100) : 34,
    };
  }
  return out;
}
