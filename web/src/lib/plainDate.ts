// Calendar dates ("YYYY-MM-DD", no time of day) as UTC-midnight Dates, so
// every reader uses the getUTC* methods and no visitor's timezone can move
// a day. The one place that builds such a Date from text.

const SHAPE = /^\d{4}-\d{2}-\d{2}$/;

/** "2026-01-05" -> that day at 00:00 UTC; null for anything else, including
 * impossible days ("2026-02-30"). */
export function parsePlainDate(s: string): Date | null {
  if (!SHAPE.test(s)) return null;
  const [y, m, d] = s.split("-").map(Number);
  const date = new Date(Date.UTC(y, m - 1, d));
  return date.getUTCMonth() === m - 1 && date.getUTCDate() === d ? date : null;
}

/** The UTC-midnight Date back as "YYYY-MM-DD". */
export const formatPlainDate = (d: Date): string =>
  d.toISOString().slice(0, 10);

/** "2026-01-31" + 1 day -> "2026-02-01"; null when `s` is not a date. */
export function addDays(s: string, days: number): string | null {
  const d = parsePlainDate(s);
  if (!d) return null;
  d.setUTCDate(d.getUTCDate() + days);
  return formatPlainDate(d);
}
