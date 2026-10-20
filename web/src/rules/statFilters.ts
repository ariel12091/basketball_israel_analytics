import type { Row, StatFilter } from "./gameLogs";
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
