import { BusyError, DeadlineError } from "./deadline";
import { enterRequestedVersion } from "./dataVersion";
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
  const v = url.searchParams.get("v");
  const version = answer.meta?.version;
  const pinned = !!v && !!version && version !== "unknown" && v === version;
  return Response.json(answer, {
    headers: pinned
      ? {
          "Cache-Control": "public, max-age=0, must-revalidate",
          "Vercel-CDN-Cache-Control": "public, s-maxage=31536000, immutable",
        }
      : { "Cache-Control": "no-store", "Vercel-CDN-Cache-Control": "no-store" },
  });
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
export class ParamError extends Error {
  constructor(
    public param: string,
    message: string,
  ) {
    super(message);
  }
}
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
export function handleError(e: unknown): Response {
  if (e instanceof ParamError) return apiError(400, e.param, e.message);
  if (e instanceof BusyError) return apiError(503, null, e.message);
  if (e instanceof DeadlineError || (e as { code?: string })?.code === "57014")
    return apiError(504, null, "Request deadline exceeded");
  console.error(
    "Game Logs request failed",
    e instanceof Error ? e.name : "UnknownError",
  );
  return apiError(503, null, "Data temporarily unavailable");
}
