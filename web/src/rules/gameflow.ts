// app/R/helpers.R ribbon_* / merge_adjacent_stints and app.js overlap rules.
// Raw scoring events drive as-of values; completed margin is drawing-only.
import { rRound } from "./round";
export type Side = "own" | "opp";
export type Window = { start: number; end: number };
export type Lane = {
  side: Side;
  player_key: string;
  player_label: string;
  start_elapsed: number;
  end_elapsed: number;
  lineup_key: string;
  is_starter: boolean;
};
export type Score = {
  elapsed: number;
  margin: number;
  order_key: number;
  own?: number | null;
};
export type Bar = Lane & {
  pf: number | null;
  pa: number | null;
  pm: number | null;
  lane_index: number;
};
export type Dictionary = { side: Side; lineup_key: string; members: string }[];
export type RawRibbon = {
  lanes:
    | {
        team_id: number;
        player_id: number;
        player_label: string;
        start_elapsed: number;
        end_elapsed: number;
        lineup_key: string;
      }[]
    | null;
  margin: Score[] | null;
  n_periods: number | null;
  excluded_segments?: number;
  false_straddle_segments?: number;
  false_straddle_seconds?: number;
};
const n = (v: unknown) => (v == null ? NaN : Number(v));
const finite = (v: number) => (Number.isFinite(v) ? v : null);
const cmp = (a: string, b: string) => a.localeCompare(b);
export const laneKey = (r: Pick<Lane, "side" | "player_key">) =>
  `${r.side}:${r.player_key}`;
export const periodBounds = (periods: number | null) =>
  Array.from({ length: Math.max(4, Math.trunc(periods || 4)) }, (_, i) =>
    i < 4 ? (i + 1) * 600 : 2400 + (i - 3) * 300,
  );
export const clock = (seconds: number) => {
  const s = Math.max(0, Math.floor(seconds) || 0);
  return `${Math.floor(s / 60)}:${String(s % 60).padStart(2, "0")}`;
};
export const pmLabel = (pm: number | null) =>
  pm == null || !Number.isFinite(pm)
    ? ""
    : `${rRound(pm, 0) > 0 ? "+" : ""}${rRound(pm, 0)}`;
export function normalizeLanes(raw: RawRibbon["lanes"], team: number): Lane[] {
  const rows = (raw ?? []).map((r) => ({
    side: r.team_id === team ? ("own" as const) : ("opp" as const),
    player_key: String(r.player_id),
    player_label: r.player_label,
    start_elapsed: n(r.start_elapsed),
    end_elapsed: n(r.end_elapsed),
    lineup_key: r.lineup_key,
    is_starter: false,
  }));
  const first = new Map<Side, number>();
  for (const r of rows)
    first.set(r.side, Math.min(first.get(r.side) ?? Infinity, r.start_elapsed));
  const starters = new Set(
    rows.filter((r) => r.start_elapsed === first.get(r.side)).map(laneKey),
  );
  return rows.map((r) => ({ ...r, is_starter: starters.has(laneKey(r)) }));
}
export function mergeStints(lanes: Lane[]): Lane[] {
  const sorted = [...lanes].sort(
      (a, b) =>
        cmp(a.side, b.side) ||
        cmp(a.player_key, b.player_key) ||
        a.start_elapsed - b.start_elapsed,
    ),
    out: Lane[] = [];
  for (let i = 0; i < sorted.length; i++) {
    const r = sorted[i],
      previous = sorted[i - 1];
    // R compares the immediately preceding RAW endpoint, including overlaps.
    if (
      !previous ||
      laneKey(r) !== laneKey(previous) ||
      r.start_elapsed > previous.end_elapsed
    )
      out.push({ ...r });
    else
      out[out.length - 1].end_elapsed = Math.max(
        out[out.length - 1].end_elapsed,
        r.end_elapsed,
      );
  }
  return out;
}
export function completeMargin(steps: Score[], bounds: number[]) {
  const total = bounds[bounds.length - 1],
    bySecond = new Map<number, { elapsed: number; margin: number }>();
  for (const r of steps
    .map((r) => ({ ...r, elapsed: Math.min(total, Math.max(0, n(r.elapsed))) }))
    .sort((a, b) => a.elapsed - b.elapsed || a.order_key - b.order_key))
    if (Number.isFinite(n(r.margin))) {
      const elapsed = Math.min(total, Math.max(0, n(r.elapsed)));
      bySecond.set(elapsed, { elapsed, margin: n(r.margin) });
    }
  const rows = [...bySecond.values()].sort((a, b) => a.elapsed - b.elapsed);
  if (!rows.length)
    return [
      { elapsed: 0, margin: 0 },
      { elapsed: total, margin: 0 },
    ];
  if (rows[0].elapsed > 0) rows.unshift({ elapsed: 0, margin: 0 });
  const last = rows[rows.length - 1],
    close = bounds.find((b) => b >= last.elapsed) ?? total;
  if (last.elapsed < close) rows.push({ elapsed: close, margin: last.margin });
  return rows;
}
export function scoreAt(
  steps: Score[],
  time: number,
  field: "margin" | "own" = "margin",
) {
  let value = 0;
  for (const r of steps)
    if (r.elapsed <= time) value = n(r[field]);
    else break;
  return value;
}
export function points(side: Side, start: number, end: number, steps: Score[]) {
  if (!steps.length) return { pf: null, pa: null, pm: null };
  const pm = scoreAt(steps, end) - scoreAt(steps, start),
    pf = scoreAt(steps, end, "own") - scoreAt(steps, start, "own"),
    pa = pf - pm;
  return side === "opp"
    ? { pm: finite(-pm), pf: finite(pa), pa: finite(pf) }
    : { pm: finite(pm), pf: finite(pf), pa: finite(pa) };
}
export function lineupDictionary(lanes: Lane[]): Dictionary {
  const groups = new Map<string, Lane[]>();
  for (const r of lanes) {
    const key = `${r.side}\r${r.lineup_key}`,
      g = groups.get(key) ?? [];
    if (!g.some((p) => p.player_key === r.player_key)) g.push(r);
    groups.set(key, g);
  }
  return [...groups.entries()]
    .sort(([a], [b]) => cmp(a, b))
    .map(([, g]) => ({
      side: g[0].side,
      lineup_key: g[0].lineup_key,
      members: g
        .map((r) => r.player_label)
        .sort(cmp)
        .join(" | "),
    }));
}
export type Segment = Window & {
  side: Side;
  dictIndex: number;
  pm: number | null;
};
export function windowSegments(
  lanes: Lane[],
  steps: Score[],
  dict: Dictionary,
  side: Side,
  player: string,
  start: number,
  end: number,
): Segment[] {
  return lanes
    .filter(
      (r) =>
        r.side === side &&
        r.player_key === player &&
        r.end_elapsed > start &&
        r.start_elapsed < end,
    )
    .map((r) => {
      const from = Math.max(start, r.start_elapsed),
        to = Math.min(end, r.end_elapsed);
      return {
        side,
        start: from,
        end: to,
        dictIndex: dict.findIndex(
          (d) => d.side === side && d.lineup_key === r.lineup_key,
        ),
        pm: points(side, from, to, steps).pm,
      };
    })
    .sort((a, b) => a.start - b.start);
}
export function windowDetail(
  lanes: Lane[],
  steps: Score[],
  dict: Dictionary,
  lane: Lane,
  start = lane.start_elapsed,
  end = lane.end_elapsed,
) {
  const pts = points(lane.side, start, end, steps),
    segments = windowSegments(
      lanes,
      steps,
      dict,
      lane.side,
      lane.player_key,
      start,
      end,
    );
  const members = segments
      .map((s) => dict[s.dictIndex]?.members)
      .filter(Boolean),
    pm = pmLabel(pts.pm);
  return {
    start,
    end,
    window: `${clock(start)}-${clock(end)}`,
    pm,
    pf: pts.pf == null ? "" : String(pts.pf),
    pa: pts.pa == null ? "" : String(pts.pa),
    segments: segments
      .map(
        (s) =>
          `${rRound(s.start, 0).toFixed(0)},${rRound(s.end, 0).toFixed(0)},${pmLabel(s.pm)},${s.dictIndex}`,
      )
      .join(";"),
    label: `${lane.player_label}, ${clock(end - start)} on the floor, plus-minus ${pm}${pts.pf == null ? "" : `, ${pts.pf} points for and ${pts.pa} against`}${members.length ? `, made up of ${members.length} lineups: ${members.join("; ")}` : ""}`,
  };
}
export function ribbonModel(
  raw: RawRibbon,
  team: number,
  league: string,
  game: number,
) {
  const lanes = normalizeLanes(raw.lanes, team),
    bounds = periodBounds(raw.n_periods),
    steps = [...(raw.margin ?? [])].sort(
      (a, b) => a.elapsed - b.elapsed || a.order_key - b.order_key,
    ),
    dict = lineupDictionary(lanes);
  const bars = mergeStints(lanes).map((r) => ({
    ...r,
    ...points(r.side, r.start_elapsed, r.end_elapsed, steps),
    lane_index: 0,
  }));
  const groups = new Map<string, Bar[]>();
  for (const b of bars)
    groups.set(laneKey(b), [...(groups.get(laneKey(b)) ?? []), b]);
  const totals = [...groups.entries()]
    .sort(([a], [b]) => cmp(a, b))
    .map(([, g]) => ({
      side: g[0].side,
      player_key: g[0].player_key,
      secs: g.reduce((s, r) => s + r.end_elapsed - r.start_elapsed, 0),
      pm: g.some((r) => r.pm == null) ? null : g.reduce((s, r) => s + r.pm!, 0),
    }));
  for (const side of ["own", "opp"] as Side[]) {
    const ordered = totals
      .filter((r) => r.side === side)
      .sort(
        (a, b) =>
          Number(groups.get(laneKey(b))![0].is_starter) -
            Number(groups.get(laneKey(a))![0].is_starter) ||
          b.secs - a.secs ||
          cmp(a.player_key, b.player_key),
      );
    for (const b of bars.filter((r) => r.side === side))
      b.lane_index =
        ordered.findIndex((r) => r.player_key === b.player_key) + 1;
  }
  const warnings: string[] = [];
  if (n(raw.excluded_segments) > 0)
    warnings.push(
      `Lineup data is incomplete for this game: ${raw.excluded_segments} gameplay segment(s) had no valid five-player lineup and are not drawn.`,
    );
  if (n(raw.false_straddle_segments) > 0)
    warnings.push(
      `${raw.false_straddle_segments} period-opening lineup interval(s), totaling ${rRound(n(raw.false_straddle_seconds) || 0, 0)} seconds, may show the previous lineup because substitutions occurred before the next valid lineup state.`,
    );
  const health =
    league === "il" && game === 406
      ? "Q4 timing is approximate: the provider's game clock was unusable, so Q4 events were positioned on the Gameflow using their wall-clock timestamps."
      : warnings.join(" ") || null;
  return {
    lanes,
    bars,
    steps,
    bounds,
    margin: completeMargin(steps, bounds),
    dict,
    totals,
    details: bars.map((b) => windowDetail(lanes, steps, dict, b)),
    health,
  };
}
export type RibbonModel = ReturnType<typeof ribbonModel>;
export function shortLabels(labels: string[], sides: string[]) {
  const split = labels.map((label) => {
    const comma = label.includes(","),
      words = label.trim().split(/\s+/),
      family =
        (comma ? label.split(",")[0] : words.slice(1).join(" ")).trim() ||
        label.trim(),
      given =
        family === label.trim()
          ? ""
          : (comma ? label.slice(label.indexOf(",") + 1) : words[0]).trim(),
      parts = family.split(/\s+/);
    return {
      family:
        family.length > 11 && parts.length > 1
          ? parts[parts.length - 1]
          : family,
      given,
    };
  });
  return split.map((s, i) => {
    const clash =
      new Set(
        labels.filter(
          (_, j) => sides[j] === sides[i] && split[j].family === s.family,
        ),
      ).size > 1;
    const out = `${clash && s.given ? s.given[0] + ". " : ""}${s.family}`;
    return out.length > 11 ? out.slice(0, 10) + "…" : out;
  });
}
export function mergeWindows(windows: Window[]) {
  const out: Window[] = [];
  for (const w of [...windows].sort((a, b) => a.start - b.start)) {
    const last = out[out.length - 1];
    if (last && w.start <= last.end) last.end = Math.max(last.end, w.end);
    else out.push({ ...w });
  }
  return out;
}
export function sharedWindows(bars: Lane[], players: string[]): Window[] {
  let windows: Window[] = [];
  players.forEach((player, i) => {
    const next = bars
      .filter((b) => laneKey(b) === player)
      .map((b) => ({ start: b.start_elapsed, end: b.end_elapsed }));
    windows = i
      ? windows.flatMap((a) =>
          next.flatMap((b) =>
            Math.min(a.end, b.end) > Math.max(a.start, b.start)
              ? [
                  {
                    start: Math.max(a.start, b.start),
                    end: Math.min(a.end, b.end),
                  },
                ]
              : [],
          ),
        )
      : next;
  });
  return mergeWindows(windows);
}
export function aggregateSegments(segments: Segment[]) {
  const groups = new Map<
    number,
    {
      side: Side;
      dictIndex: number;
      duration: number;
      pm: number;
      hasPm: boolean;
      firstStart: number;
      windows: Window[];
    }
  >();
  for (const s of segments) {
    const g = groups.get(s.dictIndex) ?? {
      side: s.side,
      dictIndex: s.dictIndex,
      duration: 0,
      pm: 0,
      hasPm: false,
      firstStart: s.start,
      windows: [],
    };
    g.duration += s.end - s.start;
    g.firstStart = Math.min(g.firstStart, s.start);
    g.windows.push({ start: s.start, end: s.end });
    if (s.pm != null) {
      g.pm += s.pm;
      g.hasPm = true;
    }
    groups.set(s.dictIndex, g);
  }
  return [...groups.values()]
    .map((g) => ({
      ...g,
      windows: g.windows.sort((a, b) => a.start - b.start),
    }))
    .sort(
      (a, b) =>
        b.duration - a.duration ||
        a.firstStart - b.firstStart ||
        a.dictIndex - b.dictIndex,
    );
}
export function sharedLineups(
  model: RibbonModel,
  players: string[],
  windows: Window[],
) {
  if (players.length < 2) return [];
  return (["own", "opp"] as Side[]).flatMap((side) => {
    const player = players.find((p) => p.startsWith(side + ":"));
    if (!player) return [];
    return aggregateSegments(
      windows.flatMap((w) =>
        windowSegments(
          model.lanes,
          model.steps,
          model.dict,
          side,
          player.slice(side.length + 1),
          w.start,
          w.end,
        ),
      ),
    );
  });
}
export function marginScale(margin: { elapsed: number; margin: number }[]) {
  const maxAbs =
    Math.max(
      0,
      ...margin
        .filter((r) => Number.isFinite(r.elapsed) && Number.isFinite(r.margin))
        .map((r) => Math.abs(r.margin)),
    ) || 1;
  const interval = [2, 5, 10, 20, 25].find((i) => maxAbs / i <= 4) ?? 25,
    ticks = Array.from(
      { length: Math.floor(maxAbs / interval) },
      (_, i) => (i + 1) * interval,
    );
  return {
    maxAbs,
    interval,
    ticks: [...ticks]
      .reverse()
      .map((v) => -v)
      .concat(ticks),
  };
}
export function marginPath(
  margin: { elapsed: number; margin: number }[],
  total: number,
  width: number,
  top: number,
  height: number,
  gutter: number,
  max = marginScale(margin).maxAbs,
) {
  return margin
    .filter((r) => Number.isFinite(r.elapsed) && Number.isFinite(r.margin))
    .map(
      (r, i) =>
        `${i ? "H" : "M"} ${(gutter + (r.elapsed * (width - gutter)) / total).toFixed(2)}${i ? " V" : ""} ${(top + height / 2 - ((r.margin / max) * height) / 2).toFixed(2)}`,
    )
    .join(" ");
}
