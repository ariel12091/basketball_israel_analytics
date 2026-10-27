// Faithful ports of server_tab3.R and helpers.R's Team Ratings rules.
import type { Row, StatFilter } from "./gameLogs";
import { applyStatFilters } from "./statFilters";
import { rRound } from "./round";
import { SP_SUFFIX } from "./onoff";
import ui from "../data/shinyUi.json";
export type TeamView =
  "summary" | "four_factors" | "shot_profile" | "traditional";
export const TRAD_COUNTS = [
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
export const TRAD_METRICS = [
  ...TRAD_COUNTS,
  "fg_pct",
  "two_pct",
  "tp_pct",
  "ft_pct",
  "efg",
  "ts",
];
const number = (v: unknown): number | null =>
  v == null || !Number.isFinite(Number(v)) ? null : Number(v);
export function minRanks(
  values: (number | null)[],
  asc = false,
): (number | null)[] {
  return values.map((v) =>
    v == null
      ? null
      : 1 + values.filter((x) => x != null && (asc ? x < v : x > v)).length,
  );
}
/** pr_vec(): average ties, n <= 1 is missing. */
export function teamPercentiles(
  values: (number | null)[],
  asc = false,
): (number | null)[] {
  const valid = values.filter((v) => v != null) as number[];
  return values.map((v) =>
    v == null || valid.length <= 1
      ? null
      : (asc ? 1 : 0) +
        ((asc ? -1 : 1) *
          (valid.filter((x) => x < v).length +
            (valid.filter((x) => x === v).length - 1) / 2)) /
          (valid.length - 1),
  );
}
export function traditionalMode(input: Row[], mode: string): Row[] {
  return input.map((row) => {
    const r = { ...row },
      den = Number(
        mode === "per75"
          ? row.poss_on_floor
          : mode === "per40"
            ? row.minutes
            : row.gp,
      );
    const scale = mode === "per75" ? 75 : mode === "per40" ? 40 : 1;
    for (const c of [...TRAD_COUNTS, "poss_on_floor", "minutes"]) {
      if (mode === "per40" && c === "minutes") continue;
      r[c] = den > 0 && row[c] != null ? (Number(row[c]) / den) * scale : null;
    }
    return r;
  });
}
export function teamPace(input: Row[], minutes: Row[], euro: boolean): Row[] {
  const map = new Map(
    minutes.map((r) => [
      String(r.team_id),
      number(r.game_minutes ?? r.minutes),
    ]),
  );
  return input.map((row) => {
    let min = map.get(String(row.team_id)) ?? null;
    if (min == null || min <= 0)
      min =
        !euro && Number(row.games_played ?? row.gp) > 0
          ? Number(row.games_played ?? row.gp) * 40
          : null;
    return {
      ...row,
      minutes: min,
      off_pace:
        min == null || row.off_poss == null
          ? null
          : (Number(row.off_poss) / min) * 40,
      def_pace:
        min == null || row.def_poss == null
          ? null
          : (Number(row.def_poss) / min) * 40,
    };
  });
}
export function teamShotProfile(input: Row[], ff: Row[]): Row[] {
  return input.map((row) => {
    const r = { ...row };
    const factors = ff.find((f) => f.team_id === row.team_id);
    for (const side of ["off", "def"]) {
      const fga = Number(r[`${side}_fga`] ?? 0),
        rim =
          Number(r[`${side}_layup_att`] ?? 0) +
          Number(r[`${side}_dunk_att`] ?? 0),
        fg3 = Number(r[`${side}_fg3_att`] ?? 0);
      const nums = [
        Number(r[`${side}_layup_att`] ?? 0),
        Number(r[`${side}_dunk_att`] ?? 0),
        rim,
        fg3,
        Number(r[`${side}_c3_att`] ?? 0),
        Math.max(fga - rim - fg3, 0),
      ];
      SP_SUFFIX.slice(1).forEach((s, i) => {
        const den = i === 4 ? Number(r[`${side}_c3_known_att`] ?? 0) : fga;
        r[`${side}_${s}`] = den > 0 ? rRound((nums[i] / den) * 100, 1) : null;
      });
      r[`${side}_efg`] = factors?.[`${side}_efg`] ?? null;
    }
    return r;
  });
}
export function teamTableRows(
  raw: Row[],
  previous: Row[],
  view: TeamView,
  filters: StatFilter[],
  showDelta: boolean,
  defense = false,
): Row[] {
  let rows = raw.map((r) => ({ ...r }));
  // FF percentiles precede local stat filters, exactly as tr_ff_data().
  const config = ui.teamRatingMetrics as {
    metric: string;
    percentile: string;
    best_direction: string;
  }[];
  if (view === "four_factors")
    for (const m of config) {
      const pr = teamPercentiles(
        rows.map((r) => number(r[m.metric])),
        m.best_direction === "asc",
      );
      rows.forEach((r, i) => (r[m.percentile] = pr[i]));
    }
  rows = applyStatFilters(rows, filters);
  const metrics =
    view === "traditional"
      ? TRAD_METRICS
      : view === "shot_profile"
        ? ["off", "def"].flatMap((s) => SP_SUFFIX.map((m) => `${s}_${m}`))
        : view === "summary"
          ? ["off_ppp", "def_ppp", "net_rtg"]
          : config.map((m) => m.metric);
  for (const metric of metrics) {
    const asc =
      view === "traditional"
        ? metric === "tov"
          ? !defense
          : defense
        : view === "shot_profile"
          ? metric === "def_efg"
          : config.find((m) => m.metric === metric)?.best_direction === "asc";
    const ranks = minRanks(
        rows.map((r) => number(r[metric])),
        asc,
      ),
      prevRanks = minRanks(
        previous.map((r) => number(r[metric])),
        asc,
      );
    const prevMap = new Map(
      previous.map((r, i) => [String(r.team_id), prevRanks[i]]),
    );
    const invert =
      view === "shot_profile"
        ? metric.startsWith("def_") !== metric.endsWith("_mid_share")
        : asc;
    const pr = teamPercentiles(
      rows.map((r) => number(r[metric])),
      invert,
    );
    rows.forEach((r, i) => {
      r[`rank_${metric}`] = ranks[i];
      const prior = prevMap.get(String(r.team_id));
      r[`delta_${metric}`] =
        showDelta && prior != null && ranks[i] != null
          ? prior - ranks[i]!
          : null;
      if (view === "traditional" || view === "shot_profile")
        r[`pr_${metric}`] = pr[i];
    });
  }
  if (view === "four_factors")
    rows.sort(
      (a, b) => Number(b.net_rtg ?? -Infinity) - Number(a.net_rtg ?? -Infinity),
    );
  if (view === "shot_profile")
    rows.sort(
      (a, b) =>
        Number(b.off_rim_share ?? -Infinity) -
        Number(a.off_rim_share ?? -Infinity),
    );
  return rows;
}
