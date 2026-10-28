import { afterEach, expect, it, vi } from "vitest";
import { NextRequest } from "next/server";
afterEach(() => vi.unstubAllEnvs());
const request = (path: string, cookie?: string, ip = "test-ip") => new NextRequest(`http://localhost${path}`, {
  headers: { "x-forwarded-for": ip, ...(cookie ? { cookie: `ibpl_session=${cookie}` } : {}) },
});
for (const league of ["il", "euroleague", "eurocup"]) {
  it(`${league}: SSR and API share the per-session reader allowance and return retry guidance`, async () => {
    vi.resetModules(); vi.stubEnv("DASHBOARD_SESSION_RATE_LIMIT", "1");
    const { proxy } = await import("./proxy");
    const first = proxy(request(`/${league}/onoff`));
    expect(first.status).toBe(200);
    const session = first.cookies.get("ibpl_session")!.value;
    const second = proxy(request(`/api/v1/${league}/onoff`, session));
    expect(second.status).toBe(429);
    expect(second.headers.get("Retry-After")).toBe("60");
    expect(second.headers.get("Cache-Control")).toBe("no-store");
    expect((await second.json()).error).toMatch(/Too many/);
    expect(proxy(request(`/${league}/lineups`, session)).status).toBe(200);
  });
}
it("rotating session cookies cannot bypass the per-IP reader allowance", async () => {
  vi.resetModules(); vi.stubEnv("DASHBOARD_IP_RATE_LIMIT", "2");
  const { proxy } = await import("./proxy");
  expect(proxy(request("/il/onoff")).status).toBe(200);
  expect(proxy(request("/il/onoff")).status).toBe(200);
  expect(proxy(request("/il/onoff")).status).toBe(429);
});
it("Production defaults to 300 per minute per IP and reader; Previews to 5000", async () => {
  vi.resetModules(); vi.stubEnv("VERCEL_ENV", "production");
  const { proxy } = await import("./proxy");
  const statuses = Array.from({ length: 301 }, () => proxy(request("/il/onoff", undefined, "prod-ip")).status);
  expect(statuses.slice(0, 300).every((s) => s === 200)).toBe(true);
  expect(statuses[300]).toBe(429);
  vi.stubEnv("VERCEL_ENV", "preview");
  const preview = Array.from({ length: 301 }, () => proxy(request("/il/onoff", undefined, "preview-ip")).status);
  expect(preview.every((s) => s === 200)).toBe(true);
});
it("invalid limiter configuration fails closed while assets and static guide remain unaffected", async () => {
  vi.resetModules(); vi.stubEnv("DASHBOARD_SESSION_RATE_LIMIT", "NaN");
  const { proxy } = await import("./proxy");
  expect(proxy(request("/il/game-logs")).status).toBe(503);
  expect(proxy(request("/_next/static/test.js")).status).toBe(200);
  expect(proxy(request("/api/v1/il/guide")).status).toBe(200);
  expect(proxy(request("/api/v1/ask")).status).toBe(200);
});
