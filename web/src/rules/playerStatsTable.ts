// Faithful Tab 5 pipeline: app/R/server_tab5_traditional.R and helpers.R.
// Rank the full context population before narrowing the displayed players.
import { percentRank, type Row } from "./gameLogs";
import { adaptiveBaseline } from "./onoff";
import {
  addUsagePct,
  cleanTsRows,
  normalizeTsRows,
  perGame,
  tsAttempts,
} from "./playerStats";
import { rRound } from "./round";
import { applyStatFilters } from "./statFilters";
import type { StatFilter } from "../lib/tabQuery";
export type Identity = {
  team_id: number;
  player_id: number;
  identity_id: string;
  display_name: string;
};
export const PLAYER_MODES = [
  "Totals",
  "Per Game",
  "Per 60 Possessions",
  "Per 30 Minutes",
] as const;
export type PlayerMode = (typeof PLAYER_MODES)[number];
export const PLAYER_LABELS: Record<string, string> = {
  gp: "GP",
  poss_on_floor: "Poss On Floor",
  minutes: "Min",
  pts: "PTS",
  reb: "REB",
  oreb: "OREB",
  dreb: "DREB",
  ast: "AST",
  stl: "STL",
  blk: "BLK",
  dfl: "DFL",
  tov: "TOV",
  fgm: "FGM",
  fga: "FGA",
  fg_pct: "FG%",
  "2pm": "2PM",
  "2pa": "2PA",
  two_pct: "2P%",
  "3pm": "3PM",
  "3pa": "3PA",
  tp_pct: "3P%",
  ftm: "FTM",
  fta: "FTA",
  ft_pct: "FT%",
  efg: "eFG%",
  ts: "TS%",
  usg_pct: "USG%",
};
const COUNTS = [
  "pts",
  "reb",
  "oreb",
  "dreb",
  "ast",
  "stl",
  "blk",
  "dfl",
  "tov",
  "fgm",
  "fga",
  "2pm",
  "2pa",
  "3pm",
  "3pa",
  "ftm",
  "fta",
];
const SUMS = [
  "gp",
  "poss_on_floor",
  "minutes",
  ...COUNTS.filter((c) => c !== "2pm" && c !== "2pa"),
];
const n = (v: unknown) => (v == null || v === "" ? NaN : Number(v));
const finite = (v: number) => (Number.isFinite(v) ? v : null);
const pct = (a: number, b: number) =>
  b > 0 ? finite(rRound((a / b) * 100, 1)) : null;
export const playerKey = (r: Row | Identity) => `${r.team_id}:${r.player_id}`;
export const playerPr = (label: string) =>
  `pr_${label.replace(/[^A-Za-z0-9]+/g, "_")}`;
/** ts_rate_threshold: keep R's 1-based type-7 interpolation and 1-keep_pct
 * arithmetic. Rounding this cutoff changes eligibility at an integer boundary. */
export function playerRateThreshold(possessions: number[]) {
  const s = possessions
    .filter((v) => Number.isFinite(v) && v > 0)
    .sort((a, b) => a - b);
  if (s.length < 120) return 0;
  const index = 1 + (s.length - 1) * (1 - 0.85),
    lo = Math.floor(index),
    hi = Math.ceil(index),
    h = index - lo;
  return (1 - h) * s[lo - 1] + h * s[hi - 1];
}
export function playerPopulation(
  raw: Row[],
  lookup: Identity[],
  teams: number[] = [],
): Row[] {
  let rows: Row[] = cleanTsRows(normalizeTsRows(raw))
    .filter((r) => !teams.length || teams.includes(Number(r.team_id)))
    .map((r) => {
      const twoM = n(r.fgm) - n(r["3pm"]),
        twoA = n(r.fga) - n(r["3pa"]);
      return {
        ...r,
        "2pm": finite(twoM),
        "2pa": finite(twoA),
        two_pct: pct(twoM, twoA),
      };
    });
  rows = addUsagePct(rows);
  rows.sort(
    (a, b) =>
      n(b.pts) - n(a.pts) ||
      n(b.minutes) - n(a.minutes) ||
      String(a.team_name).localeCompare(String(b.team_name)) ||
      String(a.Player).localeCompare(String(b.Player)),
  );
  const byKey = new Map(lookup.map((r) => [playerKey(r), r]));
  rows = rows.map((r) => ({
    ...r,
    is_multi_team_total: false,
    ".identity_id": byKey.get(playerKey(r))?.identity_id ?? null,
  }));
  const groups = new Map<string, Row[]>();
  for (const r of rows)
    if (r[".identity_id"] != null) {
      const id = String(r[".identity_id"]);
      groups.set(id, [...(groups.get(id) ?? []), r]);
    }
  const totals: Row[] = [];
  for (const [id, parts] of groups) {
    if (new Set(parts.map((r) => r.team_id)).size < 2) continue;
    const name =
      byKey.get(playerKey(parts[0]))?.display_name || parts[0].Player;
    for (const r of parts) r.Player = name;
    const total: Row = {
      ...parts[0],
      team_id: null,
      player_id: null,
      team_name: "TOTAL",
      Player: name,
      is_multi_team_total: true,
      ".identity_id": id,
    };
    for (const c of SUMS) {
      const values = parts.map((r) => n(r[c])).filter(Number.isFinite);
      if (c in total)
        total[c] = values.length ? values.reduce((a, b) => a + b, 0) : null;
    }
    const m = n(total.fgm),
      a = n(total.fga),
      m3 = n(total["3pm"]),
      a3 = n(total["3pa"]);
    const attempts = parts.reduce((s, r) => s + tsAttempts(r), 0);
    const weighted = parts.filter(
      (r) => Number.isFinite(n(r.usg_pct)) && n(r.poss_on_floor) > 0,
    );
    Object.assign(total, {
      "2pm": finite(m - m3),
      "2pa": finite(a - a3),
      fg_pct: pct(m, a),
      two_pct: pct(m - m3, a - a3),
      tp_pct: pct(m3, a3),
      ft_pct: pct(n(total.ftm), n(total.fta)),
      efg: pct(m + 0.5 * m3, a),
      ts: pct(n(total.pts), 2 * attempts),
      ts_att: attempts,
      usg_pct: weighted.length
        ? rRound(
            weighted.reduce(
              (s, r) => s + n(r.usg_pct) * n(r.poss_on_floor),
              0,
            ) / weighted.reduce((s, r) => s + n(r.poss_on_floor), 0),
            1,
          )
        : null,
    });
    totals.push(total);
  }
  return [...rows, ...totals];
}
export function playerDisplay(
  population: Row[],
  lookup: Identity[],
  options: {
    mode: PlayerMode;
    players?: string[];
    minGp?: number;
    showIneligible?: boolean;
    filters?: StatFilter[];
  },
) {
  const pop = population.filter((r) => r.is_multi_team_total !== true);
  const poss = pop
    .map((r) => n(r.poss_on_floor))
    .filter((v) => Number.isFinite(v) && v > 0);
  const threshold = playerRateThreshold(poss);
  const rate =
    options.mode === "Per 60 Possessions" || options.mode === "Per 30 Minutes";
  let rows: Row[] = population.map((r) => ({
    ...r,
    total_poss: r.poss_on_floor,
    ".poss_rank_base": r.poss_on_floor,
    base_poss_pg: n(r.gp) > 0 ? finite(n(r.poss_on_floor) / n(r.gp)) : null,
    base_min_pg: n(r.gp) > 0 ? finite(n(r.minutes) / n(r.gp)) : null,
    rate_eligible: !rate || n(r.poss_on_floor) >= threshold,
  }));
  if (options.mode === "Per Game") rows = perGame(rows);
  else if (rate)
    rows = rows.map((r) => {
      const divisor = n(
          r[options.mode === "Per 30 Minutes" ? "minutes" : "poss_on_floor"],
        ),
        factor = options.mode === "Per 30 Minutes" ? 30 : 60;
      const out = { ...r };
      for (const c of [...COUNTS, "minutes", "poss_on_floor"])
        if (c in out)
          out[c] = divisor > 0 ? finite((n(r[c]) / divisor) * factor) : null;
      return out;
    });
  const baseline = adaptiveBaseline(
    rows.map((r) => finite(n(r[".poss_rank_base"]))),
  );
  for (const [c, label] of Object.entries(PLAYER_LABELS)) {
    if (["gp", "minutes", "poss_on_floor"].includes(c)) continue;
    const pr = percentRank(
      rows.map((r) =>
        n(r[".poss_rank_base"]) >= baseline ? finite(n(r[c])) : null,
      ),
    );
    rows.forEach((r, i) => (r[playerPr(label)] = pr[i]));
  }
  const selected = options.players ?? [],
    identities = new Set(
      lookup
        .filter((r) => selected.includes(playerKey(r)))
        .map((r) => r.identity_id),
    );
  rows = rows.filter(
    (r) =>
      (!selected.length ||
        (r.is_multi_team_total === true
          ? identities.has(String(r[".identity_id"]))
          : selected.includes(playerKey(r)))) &&
      (n(r.gp) || 0) >= (options.minGp ?? 1),
  );
  const ineligible = rate ? rows.filter((r) => !r.rate_eligible).length : 0;
  if (rate && !options.showIneligible)
    rows = rows.filter((r) => r.rate_eligible);
  rows = applyStatFilters(rows, options.filters ?? []);
  const key = (r: Row, i: number) =>
    r[".identity_id"] == null ? `.row${i + 1}` : String(r[".identity_id"]);
  const scores = new Map<string, number>();
  rows.forEach((r, i) =>
    scores.set(
      key(r, i),
      Math.max(
        scores.get(key(r, i)) ?? -Infinity,
        Number.isNaN(n(r.pts)) ? -Infinity : n(r.pts),
      ),
    ),
  );
  rows = rows
    .map((r, i): Row => ({ ...r, ".group": key(r, i) }))
    .sort(
      (a, b) =>
        scores.get(String(b[".group"]))! - scores.get(String(a[".group"]))! ||
        String(a[".group"]).localeCompare(String(b[".group"])) ||
        Number(a.is_multi_team_total) - Number(b.is_multi_team_total) ||
        n(b.pts) - n(a.pts),
    );
  return {
    rows,
    threshold,
    ineligible,
    removed: options.showIneligible ? 0 : ineligible,
    maxGp: Math.max(
      1,
      ...population.map((r) => n(r.gp)).filter(Number.isFinite),
    ),
  };
}
