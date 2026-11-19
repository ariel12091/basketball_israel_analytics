// One JSON line per event in Vercel's runtime log (web/CLAUDE.md "Logs").
// Privacy rule: fields name the page or route, league, status, timing and the
// kind of failure. Never an IP, cookie, session or client id, a query string,
// or anything a visitor typed.
export type LogFields = Record<string, string | number | boolean | null | undefined>;

export function logEvent(kind: string, fields: LogFields = {}, level: "log" | "warn" | "error" = "log") {
  const line: Record<string, unknown> = { kind };
  for (const [k, v] of Object.entries(fields)) if (v !== undefined) line[k] = v;
  console[level](JSON.stringify(line));
}
