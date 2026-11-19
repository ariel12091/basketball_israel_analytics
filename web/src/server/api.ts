import { BusyError } from "./deadline";
import { logEvent } from "../lib/logEvent";
import { failureOf, requestContext } from "./requestLog";
import { enterRequestedVersion } from "./dataVersion";
import { queryFromSearch, type Query } from "../lib/tabQuery";
import type { League } from "../rules/gameLogs";
/** A route's query without the client's `v` (lib/dataVersion.ts), which only
 * names the cache entry and never changes the answer; `v` becomes this
 * request's version hint (dataVersion.ts requestedVersion). */
export function answerParams(url: URL): URLSearchParams {
  const league = /^\/api\/v1\/([a-z]+)\//.exec(url.pathname)?.[1];
  if (league) enterRequestedVersion(league, url.searchParams.get("v"));
  const search = new URLSearchParams(url.searchParams);
  search.delete("v");
  return search;
}
/** Vercel's CDN keeps an answer for a year only when the address names the
 * version the answer was read at: a new publication changes every address,
 * so a cached answer is never served for newer data. Anything else (no `v`,
 * a stale `v`, an unversioned answer) is not stored anywhere. */
export function answerJson(
  answer: { meta?: { version?: string } },
  url: URL,
): Response {
  return Response.json(answer, { headers: cacheHeaders(answer, url) });
}
/** answerJson's caching, for any body (format=csv too). */
export function cacheHeaders(
  answer: { meta?: { version?: string } },
  url: URL,
): Record<string, string> {
  const v = url.searchParams.get("v");
  const version = answer.meta?.version;
  const pinned = !!v && !!version && version !== "unknown" && v === version;
  return pinned
    ? {
        "Cache-Control": "public, max-age=0, must-revalidate",
        "Vercel-CDN-Cache-Control": "public, s-maxage=31536000, immutable",
      }
    : { "Cache-Control": "no-store", "Vercel-CDN-Cache-Control": "no-store" };
}
export function apiError(
  status: number,
  param: string | null,
  message: string,
) {
  return Response.json(
    { error: message, param },
    {
      status,
      headers: {
        "Cache-Control": "no-store",
        ...(status === 503 ? { "Retry-After": "2" } : {}),
      },
    },
  );
}
/** A page's query from a request, as the page would read its URL
 * (queryFromSearch with the tab's defaults); a key the page does not read is
 * refused, so a typo never silently changes the table. */
export function pageQuery(
  league: League,
  search: URLSearchParams,
  defaults: Record<string, string>,
): Query {
  const values = Object.fromEntries(search);
  const q = queryFromSearch(league, values, defaults);
  const unknown = Object.keys(values).find((k) => !(k in q));
  if (unknown) throw new ParamError(unknown, "Unknown parameter");
  return q;
}

/** `shape=table`: the finished table a freshly opened page link shows
 * (filters, auto bars, rate view applied) instead of the page's population.
 * Read and removed here, so answer functions never see it. */
export function answerShape(search: URLSearchParams): "population" | "table" {
  const v = search.get("shape");
  search.delete("shape");
  if (v == null || v === "") return "population";
  if (v === "table") return "table";
  throw new ParamError("shape", "shape is table");
}
/** `format=csv` (a finished table as the page's CSV export) or json. */
export function answerFormat(search: URLSearchParams): "json" | "csv" {
  const v = search.get("format");
  search.delete("format");
  if (v == null || v === "" || v === "json") return "json";
  if (v === "csv") return "csv";
  throw new ParamError("format", "format is json or csv");
}

export { ParamError } from "./paramError";
import { ParamError } from "./paramError";
export function rawQueryGuard(url: URL) {
  const raw = url.search.slice(1);
  if (Buffer.byteLength(raw, "utf8") > 4096)
    throw new ParamError("query", "Query exceeds 4096 bytes");
  const seen = new Set<string>();
  for (const part of raw.split("&").filter(Boolean)) {
    const split = part.indexOf("="),
      parts =
        split < 0 ? [part, ""] : [part.slice(0, split), part.slice(split + 1)];
    let name: string;
    try {
      name = decodeURIComponent(parts[0].replace(/\+/g, " "));
      decodeURIComponent(parts[1].replace(/\+/g, " "));
    } catch {
      throw new ParamError("query", "Malformed UTF-8 or percent encoding");
    }
    if (!name) throw new ParamError("query", "Empty parameter name");
    if (seen.has(name)) throw new ParamError(name, "Repeated parameter");
    seen.add(name);
  }
}
/** The API's error answer, and its `api_error` log line with the route,
 * league and time from the request context (server/requestLog.ts). A refused
 * parameter is logged without its name or value (a visitor may have typed
 * either). */
export function handleError(e: unknown): Response {
  const f = failureOf(e);
  const status = f.reason === "param" ? 400 : f.reason === "timeout" ? 504 : 503;
  logEvent(
    "api_error",
    { ...requestContext(), status, reason: f.reason, code: f.code, name: f.name },
    f.reason === "param" ? "log" : f.reason === "busy" || f.reason === "timeout" ? "warn" : "error",
  );
  if (e instanceof ParamError) return apiError(400, e.param, e.message);
  if (e instanceof BusyError) return apiError(503, null, e.message);
  if (f.reason === "timeout") return apiError(504, null, "Request deadline exceeded");
  return apiError(503, null, "Data temporarily unavailable");
}
