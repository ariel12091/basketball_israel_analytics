import { test, expect } from "@playwright/test";
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
      await page
        .getByRole("button", { name: "Show Filters", exact: true })
        .click();
      await expect(
        page.locator(`#${league === "il" ? "tr" : "euroteam"}-filters`),
      ).toBeVisible();
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
    await expect(
      page.getByRole("dialog").locator("tbody tr").first(),
    ).toBeVisible({ timeout: 30000 });
    await page.screenshot({
      path: `../output/playwright/live-lineups-${league}-${info.project.name}.png`,
      fullPage: true,
    });
    await page.keyboard.press("Escape");
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
    if (info.project.name === "phone")
      await page
        .getByRole("button", { name: "Show Filters", exact: true })
        .click();
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
