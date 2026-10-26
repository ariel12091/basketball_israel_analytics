import "server-only";
import { AsyncLocalStorage } from "node:async_hooks";
import { Deadline, withSnapshot, type Query } from "./db";
import type { League } from "../rules/gameLogs";

export const VERSION_SQL = {
  il: "SELECT value FROM basketball_test.app_meta WHERE key='etl_full_last_success'",
  euro: "SELECT COALESCE(MAX(completed_at),MAX(started_at))::text AS value FROM euroleague.load_runs WHERE status='completed'",
};
export async function readVersion(q: Query, league: League): Promise<string> {
  const rows = await q<{ value: unknown }>(
    league === "il" ? VERSION_SQL.il : VERSION_SQL.euro,
    [],
  );
  return rows[0]?.value == null ? "unknown" : String(rows[0].value);
}

// The version an API request names (`v`, lib/dataVersion.ts), visible only to
// that request's own async work. A warm-instance answer read at exactly that
// version may be reused without a probe: the CDN already serves such an
// address from its cache, and the answer's version is never taken from it.
const requested = new AsyncLocalStorage<{ scope: string; version: string }>();
const scopeOf = (league: string) => (league === "il" ? "il" : "euro");
export function enterRequestedVersion(league: string, version: string | null) {
  if (version && version !== "unknown")
    requested.enterWith({ scope: scopeOf(league), version });
}
export function requestedVersion(league: League): string | undefined {
  const hint = requested.getStore();
  return hint?.scope === scopeOf(league) ? hint.version : undefined;
}

// Coalesce only concurrent probes. Never retain a settled version by TTL:
// each warm instance must observe publication before reusing an answer.
const pending = new Map<string, Promise<string>>();
export function currentDataVersion(league: League): Promise<string> {
  const scope = league === "il" ? "il" : "euro";
  const hit = pending.get(scope);
  if (hit) return hit;
  const promise = withSnapshot(new Deadline(), (q) => readVersion(q, league));
  pending.set(scope, promise);
  const clear = () => {
    if (pending.get(scope) === promise) pending.delete(scope);
  };
  void promise.then(clear, clear);
  return promise;
}
