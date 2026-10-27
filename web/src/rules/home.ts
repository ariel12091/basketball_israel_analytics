// Faithful ports of helpers.R hub_*; input order resolves ties, as in R.
import type { Row } from "./gameLogs";
import { normalizeTsRows } from "./playerStats";
import { rRound } from "./round";
import ui from "../data/homeUi.json";
const n = (v: unknown) => (v == null || v === "" ? NaN : Number(v));
const fin = (v: unknown) => Number.isFinite(n(v));
export type HomeFrames = Record<
  | "ratings"
  | "four_factors"
  | "onoff"
  | "traditional"
  | "lineups"
  | "storylines"
  | "last_games",
  Row[]
>;
export function homeDefaultTeam(
  remembered: string,
  teams: Row[],
  ratings: Row[],
) {
  const ids = teams.map((t) => String(t.team_id));
  if (ids.includes(remembered.trim())) return remembered.trim();
  const leader = ratings
    .filter((r) => fin(r.rank_net_rtg))
    .sort((a, b) => n(a.rank_net_rtg) - n(b.rank_net_rtg))[0];
  return leader && ids.includes(String(leader.team_id))
    ? String(leader.team_id)
    : (ids[0] ?? "");
}
const teamRow = (rows: Row[], team: string) =>
  rows.find((r) => n(r.team_id) === n(team)) ?? null;
export function homeRank(
  rows: Row[],
  team: string,
  metric = "net_rtg",
  higher = true,
) {
  const row = teamRow(rows, team),
    v = n(row?.[metric]);
  return Number.isFinite(v)
    ? 1 +
        rows.filter(
          (r) =>
            fin(r[metric]) && (higher ? n(r[metric]) > v : n(r[metric]) < v),
        ).length
    : null;
}
export function homeOrdinal(value: unknown) {
  if (!fin(value)) return "";
  const v = Math.trunc(n(value)),
    last = v % 10;
  return `${v}${v % 100 >= 11 && v % 100 <= 13 ? "th" : last === 1 ? "st" : last === 2 ? "nd" : last === 3 ? "rd" : "th"}`;
}
export function homeFixed(value: unknown, signed = false) {
  const v = n(value);
  if (!Number.isFinite(v)) return "NA";
  return `${signed ? (v < 0 || Object.is(v, -0) ? "-" : "+") : v < 0 || Object.is(v, -0) ? "-" : ""}${rRound(Math.abs(v), 1).toFixed(1)}`;
}
/** Only formats the R source's %s/%d/%.1f/%+.1f templates. */
export function homeSprintf(template: string, ...values: unknown[]) {
  let i = 0;
  return template.replace(/%\+?\.1f|%d|%s/g, (t) => {
    const value = values[i++];
    return t === "%s"
      ? String(value)
      : t === "%d"
        ? String(Math.trunc(n(value)))
        : homeFixed(value, t.includes("+"));
  });
}
export function homeMini(
  rows: Row[],
  team: string,
  side: "offense" | "defense",
) {
  const prefix = side === "offense" ? "off" : "def",
    row = teamRow(rows, team),
    factors = ["efg", "tov", "oreb", "ftr"];
  if (!row || !rows.every((r) => factors.every((f) => `${prefix}_${f}` in r)))
    return null;
  return factors.map((f, i) => ({
    label: ui.ffLabels[i],
    value: fin(row[`${prefix}_${f}`]) ? n(row[`${prefix}_${f}`]) : null,
    rank: homeRank(
      rows,
      team,
      `${prefix}_${f}`,
      side === "offense" ? f !== "tov" : f === "tov",
    ),
    n: rows.filter((r) => fin(r[`${prefix}_${f}`])).length,
  }));
}
export function homeKeyPlayers(rows: Row[], team: string) {
  const out = rows
    .filter(
      (r) =>
        n(r.team_id) === n(team) &&
        n(r["ON Poss"]) >= 100 &&
        fin(r["Net RTG Diff"]),
    )
    .sort((a, b) => n(b["Net RTG Diff"]) - n(a["Net RTG Diff"]))
    .slice(0, 5);
  return out.length ? out : null;
}
export function homeScorer(rows: Row[], team: string) {
  const out = normalizeTsRows(rows)
    .filter((r) => n(r.team_id) === n(team) && n(r.gp) >= ui.topScorerMinGp)
    .map<Row & { ppg: number }>((r) => ({
      ...r,
      ppg: n(r.pts) / Math.max(n(r.gp), 1),
    }))
    .filter((r) => fin(r.ppg))
    .sort((a, b) => b.ppg - a.ppg);
  return out[0] ?? null;
}
export function homeLineups(rows: Row[]) {
  const out = rows
    .filter((r) => fin(r.net_rtg))
    .map<Row & { total_poss: number }>((r) => ({
      ...r,
      total_poss:
        (fin(r.off_poss) ? n(r.off_poss) : 0) +
        (fin(r.def_poss) ? n(r.def_poss) : 0),
    }));
  if (!out.length) return null;
  return {
    best: out.reduce((a, b) => (n(b.net_rtg) > n(a.net_rtg) ? b : a)),
    worst: out.reduce((a, b) => (n(b.net_rtg) < n(a.net_rtg) ? b : a)),
  };
}
export function homeStorylines(frames: HomeFrames, team: string) {
  const variants = (v: string) =>
    frames.storylines.filter((r) => r.hub_variant === v);
  const pairs: Record<string, [Row[], Row[]]> = {
    starters_bench: [variants("starters_hi"), variants("starters_lo")],
    clutch: [variants("clutch"), frames.ratings],
    last10: [variants("last10"), frames.ratings],
    top_bottom_4: [variants("top4"), variants("bottom4")],
  };
  return ui.storylines.flatMap((spec) => {
    const [as, bs] = pairs[spec.id],
      a = teamRow(as, team),
      b = teamRow(bs, team);
    const poss = (r: Row) =>
      (fin(r.off_poss) ? n(r.off_poss) : 0) +
      (fin(r.def_poss) ? n(r.def_poss) : 0);
    if (!a || !b || poss(a) < spec.min_poss || poss(b) < spec.min_poss)
      return [];
    const av = n(a.net_rtg),
      bv = n(b.net_rtg),
      delta = av - bv,
      t = ui.text;
    if (
      ["starters_bench", "clutch"].includes(spec.id) &&
      !Number.isFinite(delta)
    )
      return [];
    let text = "";
    if (spec.id === "starters_bench")
      text = homeSprintf(
        t.starterSentence,
        delta >= 0 ? t.starterHigh : t.starterLow,
        Math.abs(delta),
      );
    if (spec.id === "clutch")
      text = homeSprintf(
        t.clutchSentence,
        av,
        Math.abs(delta),
        delta >= 0 ? t.better : t.worse,
      );
    if (spec.id === "last10") text = homeSprintf(t.last10Sentence, av, bv);
    if (spec.id === "top_bottom_4") {
      const ar = homeRank(as, team),
        br = homeRank(bs, team);
      if (ar == null || br == null) return [];
      text = homeSprintf(
        t.topBottomSentence,
        av,
        homeOrdinal(ar),
        bv,
        homeOrdinal(br),
      );
    }
    return [{ id: spec.id, preset: spec.preset, text }];
  });
}
export function homeLastGame(rows: Row[], team: string) {
  const g = teamRow(rows, team);
  if (!g) return null;
  const d = String(g.game_date).slice(0, 10),
    month = Number(d.slice(5, 7)),
    day = Number(d.slice(8, 10));
  const months = ui.months;
  return {
    row: g,
    result: `${g.has_won === true ? "W" : "L"} ${Math.trunc(n(g.team_score))}-${Math.trunc(n(g.opp_score))}`,
    context: `${g.is_home === true ? "vs" : "at"} ${g.opp_team_name} · ${months[month - 1]} ${day}`,
  };
}
export function homeDashboard(frames: HomeFrames, team: string) {
  const row = teamRow(frames.ratings, team);
  return {
    identity: row
      ? {
          row,
          n_teams: frames.ratings.length,
          ff: teamRow(frames.four_factors, team),
        }
      : null,
    offense: homeMini(frames.four_factors, team, "offense"),
    defense: homeMini(frames.four_factors, team, "defense"),
    players: homeKeyPlayers(frames.onoff, team),
    scorer: homeScorer(frames.traditional, team),
    lineups: homeLineups(frames.lineups),
    storylines: homeStorylines(frames, team),
    lastGame: homeLastGame(frames.last_games, team),
  };
}
