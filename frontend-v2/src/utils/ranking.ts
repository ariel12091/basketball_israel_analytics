/** Shared ranking and auto-min-poss utilities used by Tab 1 and Tab 2 */

export const AUTO_TOP_PCT = 0.35;
export const RANKING_BASELINE = 100;
export const RANKING_MIN_PCT = 0.25;
export const SHOT_MIN_FGA = 50;

/** Compute auto min poss (Tab 1): floor to nearest step so top PCT by usage is always included */
export function autoMinPoss(data: { poss: number }[], step = 10): number {
  if (!data.length) return 0;
  const sorted = [...data].sort((a, b) => b.poss - a.poss);
  const topN = Math.max(1, Math.ceil(sorted.length * AUTO_TOP_PCT));
  const minNeeded = sorted[Math.min(topN - 1, sorted.length - 1)].poss;
  return Math.floor(minNeeded / step) * step;
}

/** Compute auto min poss (Tab 2): ceiling to nearest step so at most targetRows qualify */
export const AUTO_TARGET_ROWS = 150;
export function autoMinPossTarget(data: { poss: number }[], step = 10, targetRows = AUTO_TARGET_ROWS): number {
  if (!data.length) return 0;
  const vals = data.map(d => d.poss).filter(v => isFinite(v));
  if (!vals.length) return 0;
  vals.sort((a, b) => b - a);
  if (vals.length <= targetRows) return 0;
  const kth = vals[targetRows - 1];
  return Math.ceil(kth / step) * step;
}

/** Mirrors R adaptive_baseline(): use RANKING_BASELINE when >=25% qualify, else use p75 */
export function adaptiveBaseline(possVec: number[]): number {
  const n = possVec.length;
  if (n === 0) return 0;
  const pctAbove = possVec.filter(p => p >= RANKING_BASELINE).length / n;
  if (pctAbove >= RANKING_MIN_PCT) return RANKING_BASELINE;
  const sorted = [...possVec].sort((a, b) => a - b);
  const idx = Math.floor(sorted.length * (1 - RANKING_MIN_PCT));
  return sorted[Math.min(idx, sorted.length - 1)];
}

/** Mirrors R percent_rank(): (rank - 1) / (n - 1), null for unqualified */
export function percentileRank(vals: (number | null)[]): (number | null)[] {
  const valid = vals
    .map((v, i) => (v !== null ? { v: v as number, i } : null))
    .filter(Boolean) as { v: number; i: number }[];
  const n = valid.length;
  const result = new Array<number | null>(vals.length).fill(null);
  if (n <= 1) {
    valid.forEach(({ i }) => {
      result[i] = 0.5;
    });
    return result;
  }
  // Sort and assign ranks (average ties)
  valid.sort((a, b) => a.v - b.v);
  const ranks = new Array<number>(n);
  let i = 0;
  while (i < n) {
    let j = i;
    while (j < n - 1 && valid[j + 1].v === valid[i].v) j++;
    const avgRank = (i + j) / 2;
    for (let k = i; k <= j; k++) ranks[k] = avgRank;
    i = j + 1;
  }
  valid.forEach(({ i: origIdx }, sortedIdx) => {
    result[origIdx] = ranks[sortedIdx] / (n - 1);
  });
  return result;
}

/** Compute weighted average FG% from qualifying rows (>= minFga total attempts) */
export function computeShotAvgs<T>(
  data: T[],
  fg2MadeKey: keyof T,
  fg2AttKey: keyof T,
  fg3MadeKey: keyof T,
  fg3AttKey: keyof T,
  minFga = SHOT_MIN_FGA,
): { avg2: number; avg3: number } {
  let sum2m = 0, sum2a = 0, sum3m = 0, sum3a = 0;
  for (const p of data) {
    const totalAtt = (p[fg2AttKey] as number) + (p[fg3AttKey] as number);
    if (totalAtt < minFga) continue;
    sum2m += p[fg2MadeKey] as number;
    sum2a += p[fg2AttKey] as number;
    sum3m += p[fg3MadeKey] as number;
    sum3a += p[fg3AttKey] as number;
  }
  return {
    avg2: sum2a > 0 ? Math.round((sum2m / sum2a) * 100) : 53,
    avg3: sum3a > 0 ? Math.round((sum3m / sum3a) * 100) : 34,
  };
}
