import type { Page } from "@playwright/test";
import { test, expect } from "./preview";
/** Opens the filters panel: every tab is on the drawer layout (Filters
 * button at the toolbar's right end), as lineup-controls.spec.ts does. */
async function openFilters(page: Page) {
  const button = page.locator(".chips-drawer-toggle");
  if ((await button.getAttribute("aria-expanded")) !== "true")
    await button.click();
  await expect(page.getByRole("dialog", { name: "Filters" })).toBeVisible();
}
for (const [league, season] of [
  ["il", 2026],
  ["euroleague", 2025],
  ["eurocup", 2025],
] as const) {
  test(`live ${league}: Player Stats server render`, async ({ page }, info) => {
    test.skip(
      process.env.LIVE_DB_SMOKE !== "1",
      "Read-only database smoke is opt-in",
    );
    await page.setExtraHTTPHeaders({});
    const errors: string[] = [];
    page.on("pageerror", (e) => errors.push(e.message));
    const response = await page.goto(
      `/${league}/player-stats?season=${season}`,
    );
    expect(await response!.text()).toContain('data-column="Player"');
    await expect(
      page.locator('.ibpl-dt-scroll tbody td[data-column="Player"]').first(),
    ).toBeVisible();
    expect(
      await page.evaluate(
        () => document.documentElement.scrollWidth <= innerWidth + 1,
      ),
    ).toBe(true);
    await page.screenshot({
      path: `test-results/live-player-${league}-${info.project.name}.png`,
      fullPage: true,
    });
    expect(errors).toEqual([]);
  });
}
test.skip(!process.env.LIVE_DB_SMOKE, "Opt-in read-only live database checks");
// The server renders the first table from the database here.
test.use({ extraHTTPHeaders: {} });
for (const [league, season] of [
  ["il", 2026],
  ["euroleague", 2025],
  ["eurocup", 2025],
] as const)
  test(`live ${league}: Team Ratings server render`, async ({ page }, info) => {
    const errors: string[] = [];
    page.on("pageerror", (e) => errors.push(e.message));
    const response = await page.goto(
      `/${league}/team-ratings?season=${season}`,
    );
    expect(await response!.text()).toContain('data-column="team_name"');
    await expect(
      page.locator('tbody td[data-column="team_name"]').first(),
    ).toBeVisible({ timeout: 30000 });
    if (info.project.name === "phone") {
      await openFilters(page);
      await page.keyboard.press("Escape");
    }
    expect(
      await page.evaluate(
        () => document.documentElement.scrollWidth <= innerWidth,
      ),
    ).toBe(true);
    await page.screenshot({
      path: `../output/playwright/live-team-${league}-${info.project.name}.png`,
      fullPage: true,
    });
    expect(errors).toEqual([]);
  });
for (const [league, season] of [
  ["il", 2026],
  ["euroleague", 2025],
  ["eurocup", 2025],
] as const)
  test(`live ${league}: Lineups server render and modal`, async ({
    page,
  }, info) => {
    const errors: string[] = [];
    page.on("pageerror", (e) => errors.push(e.message));
    const rosterReady = page.waitForResponse(
      (r) => r.url().includes("/lineup-roster") && r.ok(),
    );
    await page.goto(`/${league}/lineups?season=${season}`);
    await rosterReady;
    await expect(
      page.locator(".ibpl-dt-scroll tbody tr").first(),
    ).toContainText("TOTAL", { timeout: 30000 });
    const lineup = page.locator("button.ibpl-lineup-link").first();
    await lineup.scrollIntoViewIfNeeded();
    await expect(lineup).toBeInViewport();
    await lineup.evaluate(
      () =>
        new Promise<void>((resolve) =>
          requestAnimationFrame(() => requestAnimationFrame(() => resolve())),
        ),
    );
    await lineup.click();
    // The game log opens in place beneath its lineup (no modal).
    const log = page.locator(".ibpl-row-detail .ibpl-lineup-games");
    await expect(log.locator("tbody tr").first()).toBeVisible({
      timeout: 30000,
    });
    await page.screenshot({
      path: `../output/playwright/live-lineups-${league}-${info.project.name}.png`,
      fullPage: true,
    });
    await log.getByRole("button", { name: "Close game log" }).click();
    await expect(log).toHaveCount(0);
    expect(
      await page.evaluate(
        () => document.documentElement.scrollWidth <= innerWidth,
      ),
    ).toBe(true);
    expect(errors).toEqual([]);
  });
for (const [league, season] of [
  ["il", 2026],
  ["euroleague", 2025],
  ["eurocup", 2025],
]) {
  test(`live ${league}: data and phone/desktop interactions`, async ({
    page,
  }, info) => {
    page.on("pageerror", (e) => console.error("PAGE ERROR", e.message));
    page.on("console", (m) => {
      if (m.type() === "error") console.error("CONSOLE", m.text());
    });
    page.on("requestfailed", (r) =>
      console.error("REQUEST FAILED", r.url(), r.failure()?.errorText),
    );
    page.on("response", (r) => {
      if (r.url().includes("/api/"))
        console.log("API RESPONSE", r.status(), r.url());
    });
    await page.goto(`/${league}/game-logs?season=${season}`);
    await expect(page.locator(".ibpl-dt-scroll tbody tr").first()).toBeVisible({
      timeout: 30000,
    });
    await expect(page.locator(".dataTables_info")).toContainText("entries");
    if (info.project.name === "phone") await openFilters(page);
    await page.screenshot({
      path: `../output/playwright/live-${league}-${info.project.name}.png`,
      fullPage: true,
    });
  });
}

for (const [league, season] of [
  ["il", 2026],
  ["euroleague", 2025],
  ["eurocup", 2025],
]) {
  test(`live ${league}: On/Off server render`, async ({ page }, info) => {
    const errors: string[] = [];
    page.on("pageerror", (e) => errors.push(e.message));
    await page.goto(`/${league}/onoff?season=${season}`);
    await expect(
      page.locator('.ibpl-dt-scroll tbody td[data-column="Player"]').first(),
    ).toBeVisible({ timeout: 30000 });
    await expect(page.locator(".dataTables_info")).toContainText("entries");
    expect(
      await page.evaluate(
        () => document.documentElement.scrollWidth <= innerWidth,
      ),
    ).toBe(true);
    await page.screenshot({
      path: `../output/playwright/live-onoff-${league}-${info.project.name}.png`,
      fullPage: true,
    });
    expect(errors).toEqual([]);
  });
}

// Lineups "# Starters" and speed, both leagues, through the deployed API: full
// completed seasons, 2- and 3-player groups, the filters whose plans regressed
// on 2026-11-01 (EuroLeague migration 057: 34 s; Israeli fetch_lineups_all:
// 90 s+). Every request must answer within the 20 s budget, and no group may
// show more starters than players. Twin of app/tests/testthat/test-lineup-starters-db.R.
// Weekly only (LIVE_HEAVY=1, the workflow's Sunday run against production):
// the ten requests carry no data version, so the CDN never keeps them, and on
// every Preview push they cost about 2 GiB of database temp writes and reads
// (2026-11-14: 43 runs were ~95% of all filtered 2-3 player Lineups calls).
// SQL reaches the database without a web push, so a per-push run caught nothing
// a weekly one would miss.
for (const [league, season, filter] of [
  ["il", 2026, "&home_away=away"],
  ["il", 2026, "&clutch=true"],
  ["il", 2026, "&view=four_factors&last_n=5"],
  ["euroleague", 2025, "&home_away=away"],
  ["eurocup", 2025, "&last_n=10"],
] as const)
  for (const size of [2, 3])
    test(`live ${league}: ${size}-player lineups${filter} within budget`, async ({ page }, info) => {
      test.skip(process.env.LIVE_HEAVY !== "1", "Weekly heavy check (LIVE_HEAVY=1)");
      test.skip(info.project.name !== "desktop", "API check, once per run");
      const t0 = Date.now();
      const response = await page.goto(`/api/v1/${league}/lineups?season=${season}&size=${size}${filter}`);
      const ms = Date.now() - t0;
      expect(response!.status(), `${ms} ms`).toBe(200);
      expect(ms).toBeLessThan(20000);
      const { rows } = (await response!.json()) as { rows: { num_starters: number | null }[] };
      expect(rows.length).toBeGreaterThan(0);
      const over = rows.filter((r) => r.num_starters != null && r.num_starters > size + 1e-9);
      expect(over, "groups showing more starters than players").toEqual([]);
    });
