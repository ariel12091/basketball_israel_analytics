import { test, expect } from "@playwright/test";

for (const league of ["il", "euroleague", "eurocup"]) {
  test(`${league} API and page share request admission`, async ({ page, context }) => {
    for (let i = 0; i < 50; i++) {
      const response = await context.request.get(`/api/v1/${league}/game-logs?season=not-a-season`);
      expect(response.status()).toBe(400);
    }
    const denied = await context.request.get(`/api/v1/${league}/game-logs?season=not-a-season`);
    expect(denied.status()).toBe(429);
    expect(denied.headers()["cache-control"]).toBe("no-store");
    expect(Number(denied.headers()["retry-after"])).toBeGreaterThan(0);
    const response = await page.goto(`/${league}/game-logs`);
    expect(response?.status()).toBe(429);
    await expect(page.getByRole("heading", { name: "Please try again shortly" })).toBeVisible();
    expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth)).toBe(true);
  });
}
