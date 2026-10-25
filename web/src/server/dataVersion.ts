import "server-only";
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
