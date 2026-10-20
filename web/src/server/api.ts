import { BusyError, DeadlineError } from "./deadline";
export function cacheHeaders(
  snapshot: { state: string; version: number },
  requestedV: number | null,
): Headers {
  return new Headers({
    "Vercel-CDN-Cache-Control": `s-maxage=${snapshot.state === "published" && snapshot.version === requestedV ? 31536000 : 60}`,
    "Cache-Control": "public, max-age=0",
  });
}
export function versionRedirect(url: URL, current: number): Response | null {
  if (url.searchParams.get("v") === String(current)) return null;
  const target = new URL(url);
  target.searchParams.set("v", String(current));
  return new Response(null, {
    status: 307,
    headers: { Location: target.toString(), "Cache-Control": "no-store" },
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
