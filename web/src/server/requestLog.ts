// Request context for the API's log lines (web/CLAUDE.md "Logs"): each route's
// GET is wrapped once in logged(route, handler), so handleError knows which
// route and league failed, and a slow answer writes a `slow_request` line.
import { AsyncLocalStorage } from "node:async_hooks";
import { logEvent } from "../lib/logEvent";
import { BusyError, DeadlineError } from "./deadline";
import { ParamError } from "./paramError";

/** An answer slower than this writes a `slow_request` line. */
export const SLOW_MS = 4000;
const LEAGUES = new Set(["il", "euroleague", "eurocup"]);

type Context = { route: string; league: string | null; started: number };
const store = new AsyncLocalStorage<Context>();

/** The league named in an /api/v1/{league}/ path; null when the path names
 * none, "other" for anything else (never the raw path text). */
export function leagueOfPath(pathname: string): string | null {
  const segment = /^\/api\/v1\/([^/]+)\/[^/]+/.exec(pathname)?.[1];
  if (!segment) return null;
  return LEAGUES.has(segment) ? segment : "other";
}

export function logged<A extends unknown[]>(
  route: string,
  handler: (...args: A) => Promise<Response>,
): (...args: A) => Promise<Response> {
  return async (...args: A) => {
    const request = args[0] instanceof Request ? args[0] : null;
    const context: Context = {
      route,
      league: request ? leagueOfPath(new URL(request.url).pathname) : null,
      started: performance.now(),
    };
    const response = await store.run(context, () => handler(...args));
    const ms = Math.round(performance.now() - context.started);
    if (ms >= SLOW_MS)
      logEvent("slow_request", { route, league: context.league, status: response.status, ms }, "warn");
    return response;
  };
}

/** The current request's route, league and elapsed time, if it has one. */
export function requestContext(): { route?: string; league?: string | null; ms?: number } {
  const c = store.getStore();
  return c ? { route: c.route, league: c.league, ms: Math.round(performance.now() - c.started) } : {};
}

export type FailureKind = "param" | "busy" | "timeout" | "database" | "error";

/** What kind of failure an error is, and the database's SQLSTATE when it
 * has one (five letters or digits; never the message, which may quote SQL). */
export function failureOf(e: unknown): { reason: FailureKind; code?: string; name: string } {
  const code = (e as { code?: unknown })?.code;
  const sqlState = typeof code === "string" && /^[0-9A-Z]{5}$/.test(code) ? code : undefined;
  const name = e instanceof Error ? e.name : "UnknownError";
  if (e instanceof ParamError) return { reason: "param", name };
  if (e instanceof BusyError) return { reason: "busy", name };
  if (e instanceof DeadlineError || sqlState === "57014") return { reason: "timeout", code: sqlState, name };
  if (sqlState) return { reason: "database", code: sqlState, name };
  return { reason: "error", name };
}

/** For a server-rendered page's first load: `.catch(pageFailed("onoff", l))`
 * writes a `page_load_failed` line and resolves to null, so the page shows
 * its error card exactly as before. */
export function pageFailed(page: string, league: string) {
  return (e: unknown): null => {
    const f = failureOf(e);
    logEvent(
      "page_load_failed",
      { page, league: LEAGUES.has(league) ? league : "other", reason: f.reason, code: f.code, name: f.name },
      f.reason === "error" || f.reason === "database" ? "error" : "warn",
    );
    return null;
  };
}
