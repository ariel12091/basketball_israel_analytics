// Gameflow runs (docs/shared_definitions.md, "Gameflow run" and "Gameflow
// run standout"): stretches one team dominated, found from the score alone,
// with the lineup changes around them and the results that stand out against
// the rest of the same game. Counts come from the readers (score steps and
// credited events); this file only selects and compares.
import type {
  EventKind,
  GameflowEvent,
  RibbonModel,
  Score,
  Side,
} from "./gameflow";
import { rRound } from "./round";
import { aggregateSegments, points, type Segment } from "./gameflow";

/** Run rule: net >= RUN_MIN_NET while the other team scores <= RUN_MAX_OPP. */
export const RUN_MIN_NET = 10;
export const RUN_MAX_OPP = 6;
export const RUN_MAX = 3;
/** A lineup change "lines up" with a run when it came at most this many
 *  seconds before the run's first point. */
export const CHANGE_WINDOW = 120;

export type Run = {
  side: Side;
  /** Elapsed seconds of the run team's first and last scoring event. */
  start: number;
  end: number;
  pts: number;
  oppPts: number;
  net: number;
  /** The largest swing of the game: the first run taken. */
  defining: boolean;
  /** Own team's margin just before the run and at its end. */
  fromMargin: number;
  toMargin: number;
};

/**
 * "Q3 2:25": the period and the clock left in it. A moment exactly on a
 * period line belongs to the period it ends, unless `opening` (a five that
 * came on there started the next period: "Q3 10:00", not "Q2 0:00").
 */
export function periodClock(elapsed: number, bounds: number[], opening = false) {
  let i = bounds.findIndex((b) => (opening ? elapsed < b : elapsed <= b));
  if (i < 0) i = bounds.length - 1;
  const left = Math.max(0, Math.round(bounds[i] - elapsed));
  return `${i < 4 ? `Q${i + 1}` : `OT${i - 3}`} ${Math.floor(left / 60)}:${String(left % 60).padStart(2, "0")}`;
}

/**
 * A stretch of the game as every Gameflow surface names it (user
 * 2026-11-12: one format, the runs' "Q1 3:45 – Q1 0:48"). A start on a
 * period line opens the next period.
 */
export const windowLabel = (w: { start: number; end: number }, bounds: number[]) =>
  `${periodClock(w.start, bounds, true)} – ${periodClock(w.end, bounds)}`;

type Scoring = { elapsed: number; side: Side; pts: number; before: number; after: number };

/** Every scoring event, in game order, from consecutive score steps. */
function scoringEvents(steps: Score[]): Scoring[] {
  const out: Scoring[] = [];
  let own = 0,
    opp = 0,
    margin = 0;
  for (const s of steps) {
    if (s.own == null) continue;
    const o = Number(s.own),
      p = Number(s.own) - Number(s.margin);
    // One step can carry both teams' increments; own's is taken first.
    if (o > own)
      out.push({ elapsed: s.elapsed, side: "own", pts: o - own, before: margin, after: o - opp });
    if (p > opp)
      out.push({ elapsed: s.elapsed, side: "opp", pts: p - opp, before: o - opp, after: o - p });
    own = Math.max(own, o);
    opp = Math.max(opp, p);
    margin = own - opp;
  }
  return out;
}

export function detectRuns(steps: Score[]): Run[] {
  const ev = scoringEvents(steps),
    cand: Omit<Run, "defining">[] = [];
  for (let i = 0; i < ev.length; i++) {
    let a = 0,
      b = 0;
    for (let j = i; j < ev.length; j++) {
      if (ev[j].side === ev[i].side) a += ev[j].pts;
      else b += ev[j].pts;
      if (b > RUN_MAX_OPP) break;
      if (ev[j].side === ev[i].side && a - b >= RUN_MIN_NET)
        cand.push({
          side: ev[i].side,
          start: ev[i].elapsed,
          end: ev[j].elapsed,
          pts: a,
          oppPts: b,
          net: a - b,
          fromMargin: ev[i].before,
          toMargin: ev[j].after,
        });
    }
  }
  cand.sort((x, y) => y.net - x.net || x.end - x.start - (y.end - y.start) || x.start - y.start);
  const taken: Run[] = [];
  for (const c of cand) {
    if (taken.some((t) => c.start <= t.end && c.end >= t.start)) continue;
    taken.push({ ...c, defining: taken.length === 0 });
    if (taken.length === RUN_MAX) break;
  }
  return taken.sort((x, y) => x.start - y.start);
}

export type RunLineup = {
  side: Side;
  /** When the last of the five on the floor at the run's first point came on. */
  onSince: number;
  cameIn: string[];
  wentOut: string[];
  /** Lineup changes after the run's first point, up to its last. */
  changesDuring: number;
  stayedOn: boolean;
};
export type RunLineups = RunLineup[];

export function runLineups(model: RibbonModel, run: Run): RunLineups {
  const sides: Side[] = run.side === "own" ? ["own", "opp"] : ["opp", "own"];
  return sides.map((side) => {
    const lanes = model.lanes.filter((l) => l.side === side);
    // The five on the floor at t: segments with start <= t < end.
    const fiveAt = (t: number) =>
      lanes.filter((l) => l.start_elapsed <= t && t < l.end_elapsed);
    const now = fiveAt(run.start);
    const bars = model.bars.filter((b) => b.side === side);
    const onSince = Math.max(
      ...now.map(
        (l) =>
          bars.find(
            (b) =>
              b.player_key === l.player_key &&
              b.start_elapsed <= run.start &&
              run.start < b.end_elapsed,
          )?.start_elapsed ?? l.start_elapsed,
      ),
    );
    const before = fiveAt(onSince - 0.5),
      keys = (ls: typeof lanes) => new Set(ls.map((l) => l.player_key)),
      nowKeys = keys(now),
      beforeKeys = keys(before);
    const label = (ls: typeof lanes, other: Set<string>) =>
      [...new Map(ls.filter((l) => !other.has(l.player_key)).map((l) => [l.player_key, l.player_label])).values()].sort();
    const starts = [
      ...new Set(
        lanes
          .filter((l) => l.start_elapsed > run.start && l.start_elapsed <= run.end)
          .map((l) => l.start_elapsed),
      ),
    ].sort((a, b) => a - b);
    let prev = [...nowKeys].sort().join(","),
      changes = 0;
    for (const t of starts) {
      const k = [...keys(fiveAt(t))].sort().join(",");
      if (k !== prev) changes++;
      prev = k;
    }
    // No five before it (tip-off, or a gap in the lanes): nobody came in.
    const known = before.length > 0;
    return {
      side,
      onSince: Number.isFinite(onSince) ? onSince : run.start,
      cameIn: known ? label(now, beforeKeys) : [],
      wentOut: known ? label(before, nowKeys) : [],
      changesDuring: changes,
      stayedOn: changes === 0,
    };
  });
}

/** Rates only: points per possession restates the run, so it is never a
 *  cause (user 2026-11-11). */
export type StandoutMetric = EventKind;
export type Standout = {
  side: Side;
  metric: StandoutMetric;
  /** made/att for shooting; turnovers/possessions; offensive rebounds/own
   *  missed shots. */
  inRun: { made: number; att: number };
  rest: { made: number; att: number };
  /** A three-point streak: shooting variance, never credited to a lineup. */
  variance: boolean;
};

const MIN_ATT: Record<StandoutMetric, number> = {
  fg2: 6,
  fg3: 6,
  ft: 6,
  oreb: 6,
  tov: 8,
};
/** Gap needed, in percentage points. */
const MIN_GAP: Record<StandoutMetric, number> = {
  fg2: 20,
  fg3: 20,
  ft: 20,
  oreb: 20,
  tov: 20,
};
const METRICS: StandoutMetric[] = ["fg3", "fg2", "ft", "tov", "oreb"];

function count(rows: GameflowEvent[], metric: StandoutMetric) {
  const poss = rows.filter((e) => e.poss_end).length;
  switch (metric) {
    case "tov":
      return { made: rows.filter((e) => e.kind === "tov").length, att: poss };
    case "oreb":
      return {
        made: rows.filter((e) => e.kind === "oreb").length,
        att: rows.filter((e) => (e.kind === "fg2" || e.kind === "fg3") && e.made === false).length,
      };
    default: {
      const shots = rows.filter((e) => e.kind === metric);
      return { made: shots.filter((e) => e.made).length, att: shots.length };
    }
  }
}

/**
 * At most two results that stand out against the rest of the same game, for
 * either team. At most one is a three-point line (shooting variance), so a
 * run's non-luck cause is not crowded out by both teams' threes.
 */
export function runStandouts(events: GameflowEvent[], run: Run): Standout[] {
  const found: (Standout & { score: number })[] = [];
  for (const side of ["own", "opp"] as Side[]) {
    const mine = events.filter((e) => e.side === side && e.off),
      inside = mine.filter((e) => e.elapsed >= run.start && e.elapsed <= run.end),
      outside = mine.filter((e) => e.elapsed < run.start || e.elapsed > run.end);
    for (const metric of METRICS) {
      const inRun = count(inside, metric),
        rest = count(outside, metric),
        min = MIN_ATT[metric];
      if (inRun.att < min || rest.att < min) continue;
      const gap = 100 * Math.abs(inRun.made / inRun.att - rest.made / rest.att);
      if (gap < MIN_GAP[metric]) continue;
      found.push({
        side,
        metric,
        inRun,
        rest,
        variance: metric === "fg3",
        score: gap / MIN_GAP[metric],
      });
    }
  }
  const out: Standout[] = [];
  for (const f of found.sort((a, b) => b.score - a.score)) {
    if (f.variance && out.some((o) => o.variance)) continue;
    const { side, metric, inRun, rest, variance } = f;
    out.push({ side, metric, inRun, rest, variance });
    if (out.length === 2) break;
  }
  return out;
}

const swing = (m: number) => (m > 0 ? `up ${m}` : m < 0 ? `down ${-m}` : "level");
const pct = (made: number, att: number) => `${rRound((100 * made) / att, 0)}%`;
function standoutLine(team: string, s: Standout) {
  // The rate first, its counts after (user 2026-11-11: percentages, with the
  // counts that keep a small sample honest).
  const run = `${pct(s.inRun.made, s.inRun.att)}`,
    runN = `(${s.inRun.made}/${s.inRun.att})`,
    rest = `${pct(s.rest.made, s.rest.att)} the rest of the game (${s.rest.made}/${s.rest.att})`;
  switch (s.metric) {
    case "fg3":
      return `${team} ${run} from three ${runN}, ${rest} — shooting variance`;
    case "fg2":
      return `${team} ${run} inside the arc ${runN}, ${rest}`;
    case "ft":
      return `${team} ${run} from the line ${runN}, ${rest}`;
    case "tov":
      return `${team} turned it over on ${run} of possessions ${runN}, ${rest}`;
    case "oreb":
      return `${team} rebounded ${run} of its missed shots ${runN}, ${rest}`;
  }
}

/**
 * The run's tooltip, line by line (spec: window and swing, then both teams'
 * lineups with the run team first, then the standouts, then an "Estimated
 * cause" worded as a timing match). The margin reads from the run team's side.
 */
export function runExplanation(
  model: RibbonModel,
  run: Run,
  names: Record<Side, string>,
  standouts: Standout[] = runStandouts(model.events, run),
  lineups: RunLineups = runLineups(model, run),
): string[] {
  const at = (t: number) => periodClock(t, model.bounds),
    since = (t: number) => periodClock(t, model.bounds, true),
    sign = run.side === "own" ? 1 : -1;
  const lines = [
    `${names[run.side]} ${run.pts}-${run.oppPts} · ${at(run.start)} – ${at(run.end)} · from ${swing(sign * run.fromMargin)} to ${swing(sign * run.toMargin)}`,
    ...lineups.map((l) => {
      const change = l.cameIn.length
        ? ` (${l.cameIn.join(", ")} in for ${l.wentOut.join(", ")})`
        : "";
      const during = l.changesDuring
        ? `; ${l.changesDuring} change${l.changesDuring === 1 ? "" : "s"} during the run`
        : "; stayed on throughout";
      const five =
        !l.cameIn.length && l.onSince <= 0
          ? "the starting five"
          : `this five had been on since ${since(l.onSince)}${change}`;
      return `${names[l.side]}: ${five}${during}`;
    }),
    ...standouts.map((s) => standoutLine(names[s.side], s)),
  ];
  // A change "lines up" only when it came shortly before the run's first
  // point; when both teams changed, neither is credited (spec anti-goal).
  const close = lineups.filter(
    (l) => l.cameIn.length && run.start - l.onSince <= CHANGE_WINDOW,
  );
  if (standouts.length && standouts.every((s) => s.variance)) {
    const hot = standouts.some(
      (s) => s.inRun.made / s.inRun.att > s.rest.made / s.rest.att,
    );
    lines.push(
      `Estimated cause: ${hot ? "hot" : "cold"} three-point shooting; no lineup effect is claimed.`,
    );
  } else if (close.length === 2)
    lines.push(
      `Estimated cause: both teams changed lineups shortly before the run (${close.map((l) => `${names[l.side]} at ${since(l.onSince)}`).join(", ")}), so the swing is not credited to either; this is a timing match, not proof.`,
    );
  else if (close.length === 1)
    lines.push(
      `Estimated cause: ${names[close[0].side]}'s lineup change at ${since(close[0].onSince)} lines up with the run; this is a timing match, not proof.`,
    );
  else
    lines.push(
      standouts.length
        ? "Estimated cause: same fives as before the run; the results above are what stood out."
        : "Estimated cause: same fives as before the run; no lineup change is claimed.",
    );
  return lines;
}

/**
 * Both teams' lineups during a run (user 2026-11-11: a click on its band),
 * run team first, each with only its time inside the run's window and the
 * +/- over that time; each team's lineups longest first.
 */
export function runLineupGroups(model: RibbonModel, run: Run) {
  const order: Side[] = run.side === "own" ? ["own", "opp"] : ["opp", "own"];
  return order.flatMap((side) => {
    const seen = new Set<string>(),
      segments: Segment[] = [];
    for (const l of model.lanes) {
      if (l.side !== side || l.end_elapsed <= run.start || l.start_elapsed >= run.end)
        continue;
      // A lane row per player per segment: one segment per lineup stretch.
      const key = `${l.lineup_key}:${l.start_elapsed}`;
      if (seen.has(key)) continue;
      seen.add(key);
      const start = Math.max(run.start, l.start_elapsed),
        end = Math.min(run.end, l.end_elapsed);
      segments.push({
        side,
        start,
        end,
        dictIndex: model.dict.findIndex(
          (d) => d.side === side && d.lineup_key === l.lineup_key,
        ),
        pm: points(side, start, end, model.steps).pm,
      });
    }
    return aggregateSegments(segments).sort((a, b) => b.duration - a.duration);
  });
}
