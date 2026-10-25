// Player Stats rows -> the On/Off Player Stats filter frame (inventory ON-12):
// faithful ports of helpers.R normalize_ts_result_cols, clean_ts_rows,
// ts_implied_attempts / ts_attempts, add_ts_usage_pct, apply_ts_mode("Per
// Game"), player_stat_filter_projection and round_display_1, and the join and
// filter of apply_player_stat_filters.
import type { Row } from "./gameLogs";
import { rRound } from "./round";
import { applyStatFilters } from "./statFilters";

const num = (v: unknown) => (v == null || v === "" ? NaN : Number(v));
const fin = (v: number) => Number.isFinite(v);

/** helpers.R normalize_ts_result_cols. */
export function normalizeTsRows(rows: Row[]): Row[] {
  return rows.map((r) => {
    const out: Row = { ...r };
    if (out.player_name != null && out.Player == null)
      out.Player = out.player_name;
    if (!("oreb" in out)) out.oreb = null;
    if (!("dreb" in out)) out.dreb = null;
    if (!("reb" in out)) {
      const o = num(out.oreb),
        d = num(out.dreb);
      out.reb = !fin(o) && !fin(d) ? null : (fin(o) ? o : 0) + (fin(d) ? d : 0);
    }
    return out;
  });
}
/** helpers.R clean_ts_rows. */
export function cleanTsRows(rows: Row[]): Row[] {
  const z = (v: unknown) => (fin(num(v)) ? num(v) : 0);
  return rows.filter(
    (r) =>
      r.Player != null &&
      String(r.Player).trim() !== "" &&
      r.team_name != null &&
      String(r.team_name).trim() !== "" &&
      (z(r.gp) > 0 || z(r.poss_on_floor) > 0 || z(r.minutes) > 0),
  );
}
/** helpers.R ts_attempts (with ts_implied_attempts as the fallback). */
export function tsAttempts(r: Row): number {
  const returned = num(r.ts_att);
  if (fin(returned)) return returned;
  const pts = num(r.pts),
    ts = num(r.ts);
  let out = num(r.fga);
  if (!fin(out)) out = 0;
  if (fin(pts) && pts > 0 && fin(ts) && ts > 0) out = pts / (2 * (ts / 100));
  return out;
}
/** helpers.R add_ts_usage_pct. */
export function addUsagePct(rows: Row[]): Row[] {
  if (!rows.length) return rows;
  const has = ["fga", "fta", "tov", "poss_on_floor"].every((c) => c in rows[0]);
  const out = rows.map(
    (r) => ({ ...r, usg_pct: "usg_pct" in r ? r.usg_pct : null }) as Row,
  );
  if (!has) return out;
  const zero = (v: unknown) => (fin(num(v)) ? num(v) : 0);
  const term = out.map((r) => tsAttempts(r) + zero(r.tov));
  const key = out.map((r) =>
    r.team_id == null || r.team_id === "" ? "all" : String(r.team_id),
  );
  const usg = out.map((r) => num(r.usg_pct));
  for (const k of new Set(key)) {
    const idx = key.map((x, i) => (x === k ? i : -1)).filter((i) => i >= 0);
    const teamTerm = idx.reduce((s, i) => s + (fin(term[i]) ? term[i] : 0), 0);
    const teamPoss =
      idx.reduce(
        (s, i) =>
          s + (fin(num(out[i].poss_on_floor)) ? num(out[i].poss_on_floor) : 0),
        0,
      ) / 5;
    for (const i of idx) {
      const pof = num(out[i].poss_on_floor);
      if (
        !fin(usg[i]) &&
        fin(term[i]) &&
        term[i] >= 0 &&
        fin(pof) &&
        pof > 0 &&
        teamTerm > 0 &&
        teamPoss > 0
      )
        usg[i] = (100 * term[i] * teamPoss) / (teamTerm * pof);
    }
  }
  return out.map((r, i) => ({
    ...r,
    usg_pct: fin(usg[i]) ? rRound(usg[i], 1) : null,
  }));
}
const COUNT_COLS = [
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
/** helpers.R apply_ts_mode(df, "Per Game"). */
export function perGame(rows: Row[]): Row[] {
  return rows.map((r) => {
    const out: Row = { ...r };
    const gp = num(r.gp);
    for (const c of [...COUNT_COLS, "poss_on_floor", "minutes"])
      if (c in out) out[c] = gp > 0 && fin(num(r[c])) ? num(r[c]) / gp : null;
    return out;
  });
}
/** round_display_1(): what DT::formatRound(x, 1) shows, i.e. toFixed(1). */
const display1 = (v: unknown) =>
  fin(num(v)) ? Number(num(v).toFixed(1)) : null;

// PLAYER_STAT_FILTER_SOURCE in helpers.R.
const SOURCE: Record<string, string> = {
  ps_gp: "gp",
  ps_min_pg: "minutes",
  ps_pts_pg: "pts",
  ps_reb_pg: "reb",
  ps_ast_pg: "ast",
  ps_stl_pg: "stl",
  ps_blk_pg: "blk",
  ps_3p_pct: "tp_pct",
  ps_efg_pct: "efg",
  ps_ts_pct: "ts",
  ps_usg_pct: "usg_pct",
};
/** The Player Stats chips' column ids (ps_gp, ps_min_pg, ...). */
export const PLAYER_STAT_FILTER_COLS = Object.keys(SOURCE);
export type PsRow = { team_id: number; player_id: number } & Record<
  string,
  number | null
>;
/** helpers.R player_stat_filter_projection: one row per team-player. */
export function playerStatProjection(raw: Row[]): PsRow[] {
  return perGame(addUsagePct(cleanTsRows(normalizeTsRows(raw)))).map((r) => {
    const out: PsRow = {
      team_id: Number(r.team_id),
      player_id: Number(r.player_id),
    };
    for (const [ps, src] of Object.entries(SOURCE))
      out[ps] =
        ps === "ps_gp"
          ? fin(num(r[src]))
            ? num(r[src])
            : null
          : display1(r[src]);
    return out;
  });
}
/** apply_player_stat_filters(): join by team + player, then filter; a row
 * without a match carries NA, which an active chip excludes. */
export function applyPlayerStatFilters(
  rows: Row[],
  frame: PsRow[],
  filters: { col: string; op: "ge" | "le"; value: number }[],
): Row[] {
  if (!filters.length) return rows;
  const byKey = new Map(frame.map((p) => [`${p.team_id}:${p.player_id}`, p]));
  const joined = rows.map((r) => {
    const p = byKey.get(`${Number(r.team_id)}:${Number(r.player_id)}`);
    const out: Row = { ...r };
    for (const k of Object.keys(SOURCE)) out[k] = p ? p[k] : null;
    return out;
  });
  return applyStatFilters(joined, filters);
}
