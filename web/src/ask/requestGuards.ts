// The checks both Ask routes make on a POST (/api/v1/ask, /api/v1/ask/feedback).

/** Browsers send Origin on every POST: refuse what another site's page sends
 * on a visitor's behalf. A request with no Origin (not a browser) passes. */
export function sameSite(request: Request): boolean {
  const origin = request.headers.get("origin");
  const host =
    request.headers.get("x-forwarded-host") ??
    request.headers.get("host") ??
    new URL(request.url).host;
  try {
    return !origin || new URL(origin).host === host;
  } catch {
    return false;
  }
}

/** The caller's address, for the in-memory rate limit only (never logged or
 * stored). */
export function clientIp(request: Request): string {
  return (request.headers.get("x-forwarded-for") ?? "").split(",")[0].trim() || "unknown";
}
