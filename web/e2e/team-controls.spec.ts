import { test, expect } from "@playwright/test";
import { loadTeam } from "../test/teamGoldens";
for (const league of ["il", "euroleague", "eurocup"] as const)
  test(`Team Ratings ${league}: filters reset with league-specific view behavior`, async ({
    page,
  }) => {
    const base = loadTeam(`${league}_four_factors_default.json`),
      summary = loadTeam(`${league}_summary_default.json`);
    await page.route("**/api/v1/**", (route) => {
      const url = new URL(route.request().url());
      return route.fulfill({
        json: url.pathname.endsWith("/version")
          ? { leagues: [] }
          : url.searchParams.get("view") === "four_factors"
            ? base.answer
            : summary.answer,
      });
    });
    await page.goto(
      `/${league}/team-ratings?season=${base.g.query.season}&view=four_factors&last_n=5`,
    );
    await expect(
      page.locator(".ibpl-dt-scroll tbody tr").first(),
    ).toBeVisible();
    // Filters live in the panel under the Filters button; its Clear all
    // runs the tab's reset (Israeli: back to Summary; EuroLeague keeps view).
    await page.locator(".chips-drawer-toggle").click();
    const panel = page.getByRole("dialog", { name: "Filters" });
    await panel.getByRole("checkbox", { name: "Clutch", exact: true }).check();
    await expect(page).toHaveURL(/clutch=true/);
    await panel
      .getByRole("button", { name: "Clear all filters", exact: true })
      .click();
    await expect(page).not.toHaveURL(/last_n=|clutch=true/);
    if (league === "il") {
      await expect(page).not.toHaveURL(/view=four_factors/);
      await expect(page.locator('th[data-column="off_pace"]')).toBeVisible();
    } else {
      await expect(page).toHaveURL(/view=four_factors/);
      await expect(page.locator('th[data-column="off_efg"]')).toBeVisible();
    }
  });
test("Traditional: opponent side and possession scaling update the URL and values", async ({
  page,
}, info) => {
  const normal = loadTeam("il_traditional_default.json"),
    defense = loadTeam("il_traditional_defense.json"),
    scaled = loadTeam("il_traditional_defense75.json");
  await page.route("**/api/v1/**", (route) => {
    const url = new URL(route.request().url());
    return route.fulfill({
      json: url.pathname.endsWith("/version")
        ? { leagues: [] }
        : url.searchParams.get("side") === "defense"
          ? url.searchParams.get("display") === "per75"
            ? scaled.answer
            : defense.answer
          : normal.answer,
    });
  });
  await page.goto("/il/team-ratings?season=2026&view=traditional");
  await expect(page.locator(".dataTables_info")).toContainText("14 entries");
  await page.getByRole("switch", { name: "Opponent statistics" }).check();
  await expect(page).toHaveURL(/side=defense/);
  const mode = page.getByRole("combobox", {
    name: "Traditional display mode",
    exact: true,
  });
  await mode.fill("Per 75");
  await mode.press("ArrowDown");
  await mode.press("Enter");
  await mode.press("Escape");
  await expect(page).toHaveURL(/display=per75/);
  await expect(
    page.getByRole("button", { name: "Download Opponent CSV", exact: true }),
  ).toBeVisible();
  await expect
    .poll(async () =>
      page.locator('td[data-column="poss_on_floor"]').first().textContent(),
    )
    .toBe("75.0");
  // Tap the sorting area, clear of the separate touch explanation button.
  await page
    .locator('th[data-column="pts"]')
    .click({ position: { x: 6, y: 6 } });
  await expect(page.locator('th[data-column="pts"]')).toHaveAttribute(
    "aria-sort",
    "ascending",
  );
  expect(
    await page.evaluate(
      () => document.documentElement.scrollWidth <= innerWidth,
    ),
  ).toBe(true);
  await page.screenshot({
    path: `../output/playwright/team-traditional-controls-${info.project.name}.png`,
    fullPage: true,
  });
});
