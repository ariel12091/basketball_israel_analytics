import type { Row, StatFilter } from "./gameLogs";
/** Whether a filter names a column only another view offers (user
 * 2026-11-22: filter Four Factors lineups by 3P%, say). */
export function needsOtherView(
  filters: StatFilter[],
  own: string[],
  other: string[],
): boolean {
  return filters.some((f) => !own.includes(f.col) && other.includes(f.col));
}
/** The other views a page must read for its stat filters: for each filter on
 * a column this view lacks, the first other view (in `cols` order) that
 * offers it. Empty when every filter is the view's own. */
export function viewsForFilters<V extends string>(
  filters: StatFilter[],
  cols: Record<V, string[]>,
  view: V,
): V[] {
  const out: V[] = [];
  for (const f of filters) {
    if (cols[view].includes(f.col)) continue;
    const v = (Object.keys(cols) as V[]).find(
      (k) => k !== view && cols[k].includes(f.col),
    );
    if (v && !out.includes(v)) out.push(v);
  }
  return out;
}
/** Each row gains the fields it lacks from the other view's row with the
 * same key, so a stat filter can read them; its own fields never change. A
 * row with no match gains nothing, and a filter on a missing value fails it
 * (applyStatFilters). */
export function withOtherView<T extends Row>(
  rows: T[],
  other: Row[],
  key: (r: Row) => string,
): T[] {
  const byKey = new Map(other.map((r) => [key(r), r]));
  return rows.map((r) => {
    const o = byKey.get(key(r));
    if (!o) return r;
    const out: Row = { ...r };
    for (const [k, v] of Object.entries(o)) if (!(k in out)) out[k] = v;
    return out as T;
  });
}
/** app/R/helpers.R apply_stat_filters: missing and nonnumeric values fail. */
export function applyStatFilters(
  rows: Row[],
  filters: StatFilter[] = [],
): Row[] {
  return rows.filter((row) =>
    filters.every(
      (f) =>
        row[f.col] != null &&
        Number.isFinite(Number(row[f.col])) &&
        (f.op === "le"
          ? Number(row[f.col]) <= f.value
          : Number(row[f.col]) >= f.value),
    ),
  );
}
