import { NextRequest, NextResponse } from "next/server";
import { WindowLimiter } from "./lib/windowLimiter";

// Best-effort admission per warm proxy instance, not a distributed quota.
// The database's own queue/deadlines remain independent safeguards.
const limiter = new WindowLimiter(60_000);
const readers = new Set([
  "home",
  "onoff",
  "lineups",
  "lineup-roster",
  "lineup-games",
  "team-ratings",
  "game-logs",
  "gameflow",
  "player-stats",
  "player-stat-filters",
  "compare",
]);
const cookie = "ibpl_session";
function limit(name: string, fallback: number) {
  const value = Number(process.env[name] ?? fallback);
  if (!Number.isInteger(value) || value < 1 || value > 10_000)
    throw new Error(`Invalid ${name}`);
  return value;
}
function rejection(
  request: NextRequest,
  status: number,
  retry: number,
  message: string,
) {
  const headers = { "Cache-Control": "no-store", "Retry-After": String(retry) };
  if (request.nextUrl.pathname.startsWith("/api/"))
    return NextResponse.json(
      { error: message, param: null },
      { status, headers },
    );
  return new NextResponse(
    `<!doctype html><html lang="en"><head><meta name="viewport" content="width=device-width,initial-scale=1"><title>IBPL Analytics</title></head><body style="font:18px system-ui;margin:2rem auto;padding:1rem;max-width:36rem"><main><h1>Please try again shortly</h1><p>${message}</p><p>Please wait about ${retry} seconds, then <a href="">try again</a>.</p></main></body></html>`,
    {
      status,
      headers: { ...headers, "Content-Type": "text/html; charset=utf-8" },
    },
  );
}
function dashboardReader(pathname: string): string | null {
  if (pathname === "/api/v1/version") return "version";
  const match = /^(?:\/api\/v1)?\/(il|euroleague|eurocup)\/([^/]+)\/?$/.exec(
    pathname,
  );
  return match && readers.has(match[2]) ? `${match[1]}:${match[2]}` : null;
}
export function proxy(request: NextRequest) {
  const reader = dashboardReader(request.nextUrl.pathname);
  if (!reader || !["GET", "HEAD"].includes(request.method))
    return NextResponse.next();
  const saved = request.cookies.get(cookie)?.value;
  const session =
    saved && /^[a-f0-9-]{36}$/.test(saved) ? saved : crypto.randomUUID();
  const ip =
    (request.headers.get("x-forwarded-for") ?? "")
      .split(",")[0]
      .trim()
      .slice(0, 128) || "unknown";
  let result;
  try {
    result = limiter.check(
      [
        {
          key: `session:${session}:${reader}`,
          limit: limit("DASHBOARD_SESSION_RATE_LIMIT", 50),
        },
        {
          key: `ip:${ip}:${reader}`,
          limit: limit("DASHBOARD_IP_RATE_LIMIT", 5000),
        },
      ],
      Date.now(),
    );
  } catch {
    return rejection(
      request,
      503,
      2,
      "Data temporarily unavailable; try again shortly.",
    );
  }
  if (!result.ok) {
    return rejection(
      request,
      429,
      result.retryAfterSec,
      "Too many requests; try again shortly",
    );
  }
  const response = NextResponse.next();
  if (session !== saved) {
    response.cookies.set(cookie, session, {
      httpOnly: true,
      sameSite: "lax",
      secure: request.nextUrl.protocol === "https:",
      path: "/",
      maxAge: 86400,
    });
    response.headers.set("Cache-Control", "no-store");
  }
  return response;
}
export const config = {
  matcher: [
    "/api/v1/:path*",
    "/il/:path*",
    "/euroleague/:path*",
    "/eurocup/:path*",
  ],
};
