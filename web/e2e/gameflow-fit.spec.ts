import { test, expect } from "@playwright/test";
import { loadGameflow } from "../test/gameflowGoldens";
// The first look at the wide chart needs no scrolling (user 2026-11-04).
for (const [width, height] of [
  [1920, 1080],
  [1366, 768],
  [1280, 720],
] as const)
  test(`Gameflow fits a ${width}x${height} window`, async ({ page }, info) => {
    test.skip(info.project.name === "phone", "phones keep the quarter cards");
    const { g, answer, url } = loadGameflow("euroleague_overtime.json");
    await page.setViewportSize({ width, height });
    await page.route("**/api/v1/**", (route) =>
      route.fulfill({
        json: new URL(route.request().url()).pathname.endsWith("/version")
          ? { leagues: [{ league: g.case.league, version: "golden" }] }
          : answer,
      }),
    );
    await page.goto(url);
    const chart = page.locator("svg.ibpl-ribbon:not(.ibpl-gameflow-gutter)");
    await expect(chart).toHaveCount(1);
    // Every player's row is drawn, and the last one is inside the window.
    await expect
      .poll(async () => {
        const box = (await chart.boundingBox())!;
        return box.y + box.height;
      })
      .toBeLessThanOrEqual(height);
    const box = (await chart.boundingBox())!;
    const column = (await page.locator(".ibpl-gameflow-full").boundingBox())!;
    expect(box.width).toBeCloseTo(column.width, 0);
    await expect(page.locator(".ibpl-ribbon-row")).toHaveCount(
      new Set(answer.bars.map((b) => `${b.side}:${b.player_key}`)).size,
    );
  });
