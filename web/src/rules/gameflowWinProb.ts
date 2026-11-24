// Win probability on Gameflow (docs/shared_definitions.md, "Win probability"
// and "WPA"). The chances come from basketball_test.game_win_probability()
// only (rule 1); this file puts its rows on the ribbon's clock, turns them to
// the page team's side and picks the rows a moment or a run reads.
import type { RawWinProb, Side } from "./gameflow";

/** The state at the start of one possession (or the final row), page side. */
export type WinPoint = {
  elapsed: number;
  /** The page team's chance of winning. */
  own: number;
  ownScore: number;
  oppScore: number;
  final: boolean;
};

/** A row's moment on the ribbon: regulation counts seconds_left to the end of
 *  regulation, an overtime to the end of that overtime. */
const elapsedOf = (period: number, secondsLeft: number) =>
  (period <= 4 ? 2400 : 2400 + 300 * (period - 4)) - secondsLeft;

export function winSeries(rows: RawWinProb[] | null | undefined, team: number): WinPoint[] {
  return [...(rows ?? [])]
    .sort((a, b) => Number(a.possession_no) - Number(b.possession_no))
    .map((r) => {
      const home = Number(r.home_team_id) === team,
        p = Number(r.home_win_probability),
        hs = Number(r.home_score),
        as = Number(r.away_score);
      return {
        elapsed: elapsedOf(Number(r.period), Number(r.seconds_left)),
        own: home ? p : 1 - p,
        ownScore: home ? hs : as,
        oppScore: home ? as : hs,
        final: r.is_final === true,
      };
    });
}

/** The chance at a moment: the last possession that began at or before it
 *  (tip-off reads the pre-game chance, the end reads the result). */
export function winChanceAt(series: WinPoint[], t: number): WinPoint | null {
  if (!series.length) return null;
  let at = series[0];
  for (const p of series) {
    if (p.elapsed > t) break;
    at = p;
  }
  return at;
}

/**
 * The rows a run reads (WPA contract row): the possession before its first
 * point and the possession after its last. Possessions start at whole-second
 * times, so a score and the next possession often share a second, and a
 * free-throw trip or an offensive rebound can put the next possession start
 * after the run's last point. So among the possessions around each end, the
 * one at the run's own margin is taken (every row carries the score), else
 * the nearest by time. Pinned on every run candidate of the three fixtures.
 */
export function swingPoints(
  series: WinPoint[],
  run: { start: number; end: number; fromMargin?: number; toMargin?: number },
): { from: WinPoint; to: WinPoint } | null {
  if (!series.length) return null;
  const margin = (p: WinPoint) => p.ownScore - p.oppScore;
  // Start: from the last possession that began before the first point up to
  // that point's second; the latest one at the margin before the run.
  const prev = series.filter((p) => p.elapsed < run.start).at(-1),
    near = series.filter((p) => p.elapsed >= (prev?.elapsed ?? 0) && p.elapsed <= run.start),
    from = [...near].reverse().find((p) => margin(p) === run.fromMargin) ?? prev ?? series[0];
  // End: from the last possession that began by the last point up to the
  // first that began after it; the first one at the margin after the run.
  const last = winChanceAt(series, run.end)!,
    next = series.find((p) => p.elapsed > run.end),
    tail = series.filter((p) => p.elapsed >= last.elapsed && p.elapsed <= (next?.elapsed ?? Infinity)),
    to = tail.find((p) => margin(p) === run.toMargin) ?? last;
  return { from, to };
}

/** A run's swing from the run team's side; null with no series. */
export function runSwing(
  series: WinPoint[],
  run: { side: Side; start: number; end: number; fromMargin?: number; toMargin?: number },
): { from: number; to: number } | null {
  const pts = swingPoints(series, run);
  if (!pts) return null;
  const side = (p: WinPoint) => (run.side === "own" ? p.own : 1 - p.own);
  return { from: side(pts.from), to: side(pts.to) };
}

/** Whole percent; never 0% or 100% before the final row. */
export function winPct(p: WinPoint, side: Side = "own"): string {
  return pctLabel(side === "own" ? p.own : 1 - p.own, p.final);
}
export function pctLabel(chance: number, final = false): string {
  const v = Math.round(100 * chance);
  if (!final && v <= 0) return "<1%";
  if (!final && v >= 100) return ">99%";
  return `${v}%`;
}

/** Both teams' chance at a moment, as the score readouts show it. */
export function winLabels(series: WinPoint[], t: number): { own: string; opp: string } | null {
  const p = winChanceAt(series, t);
  return p ? { own: winPct(p, "own"), opp: winPct(p, "opp") } : null;
}
